/*
 * Copyright 2013 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bitcoinj.protocols.channels;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.bitcoinj.core.*;
import org.bitcoinj.crypto.TransactionSignature;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptBuilder;
import org.bitcoinj.utils.Threading;
import org.bitcoinj.wallet.SendRequest;
import org.bitcoinj.wallet.Wallet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spongycastle.crypto.params.KeyParameter;

import javax.annotation.Nullable;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Version 1 of the payment channel server state object. Common functionality is
 * present in the parent class.
 */
public class PaymentChannelV1ServerState extends PaymentChannelServerState {
    private static final Logger log = LoggerFactory.getLogger(PaymentChannelV1ServerState.class);

    // The client key for the multi-sig contract
    // We currently also use the serverKey for payouts, but this is not required
    protected ECKey clientKey;

    // The refund/change transaction output that goes back to the client
    private TransactionOutput clientOutput;
    private long refundTransactionUnlockTimeSecs;

    PaymentChannelV1ServerState(final StoredServerChannel storedServerChannel, Wallet wallet, TransactionBroadcaster broadcaster) throws VerificationException {
        super(storedServerChannel, wallet, broadcaster);
        this.clientKey = ECKey.fromPublicOnly(getContractScript().getChunks().get(1).data);
        this.clientOutput = checkNotNull(storedServerChannel.clientOutput);
        this.refundTransactionUnlockTimeSecs = storedServerChannel.refundTransactionUnlockTimeSecs;
        if (storedServerChannel.getCloseTransaction() == null) {
            stateMachine.transition(State.READY);
        } else {
            stateMachine.transition(State.CLOSED);
        }
    }

    /**
     * Creates a new state object to track the server side of a payment channel.
     *
     * @param broadcaster The peer group which we will broadcast transactions to, this should have multiple peers
     * @param wallet The wallet which will be used to complete transactions
     * @param serverKey The private key which we use for our part of the multi-sig contract
     *                  (this MUST be fresh and CANNOT be used elsewhere)
     * @param minExpireTime The earliest time at which the client can claim the refund transaction (UNIX timestamp of block)
     */
    public PaymentChannelV1ServerState(TransactionBroadcaster broadcaster, Wallet wallet, ECKey serverKey, long minExpireTime) {
        super(broadcaster, wallet, serverKey, minExpireTime);
        stateMachine.transition(State.WAITING_FOR_REFUND_TRANSACTION);
    }

    @Override
    public Multimap<State, State> getStateTransitions() {
        Multimap<State, State> result = MultimapBuilder.enumKeys(State.class).arrayListValues().build();
        result.put(State.UNINITIALISED, State.READY);
        result.put(State.UNINITIALISED, State.CLOSED);
        result.put(State.UNINITIALISED, State.WAITING_FOR_REFUND_TRANSACTION);
        result.put(State.WAITING_FOR_REFUND_TRANSACTION, State.WAITING_FOR_MULTISIG_CONTRACT);
        result.put(State.WAITING_FOR_MULTISIG_CONTRACT, State.WAITING_FOR_MULTISIG_ACCEPTANCE);
        result.put(State.WAITING_FOR_MULTISIG_ACCEPTANCE, State.READY);
        result.put(State.READY, State.CLOSING);
        result.put(State.CLOSING, State.CLOSED);
        for (State state : State.values()) {
            result.put(state, State.ERROR);
        }
        return result;
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public TransactionOutput getClientOutput() {
        return clientOutput;
    }

    @Override
    protected Script getSignedScript() {
        return getContractScript();
    }

    /**
     * Called when the client provides the refund transaction.
     * The refund transaction must have one input from the multisig contract (that we don't have yet) and one output
     * that the client creates to themselves. This object will later be modified when we start getting paid.
     *
     * @param refundTx The refund transaction, this object will be mutated when payment is incremented.
     * @param clientMultiSigPubKey The client's pubkey which is required for the multisig output
     * @return Our signature that makes the refund transaction valid
     * @throws VerificationException If the transaction isnt valid or did not meet the requirements of a refund transaction.
     */
    public byte[] provideRefundTransaction(Transaction refundTx, byte[] clientMultiSigPubKey) throws VerificationException {
        lock.lock();
        try {
            checkNotNull(refundTx);
            checkNotNull(clientMultiSigPubKey);
            stateMachine.checkState(State.WAITING_FOR_REFUND_TRANSACTION);
            log.info("Provided with refund transaction: {}", refundTx);
            // Do a few very basic syntax sanity checks.
            refundTx.verify();
            // Verify that the refund transaction has a single input (that we can fill to sign the multisig output).
            if (refundTx.getInputs().size() != 1)
                throw new VerificationException("Refund transaction does not have exactly one input");
            // Verify that the refund transaction has a time lock on it and a sequence number that does not disable lock time.
            if (refundTx.getInput(0).getSequenceNumber() == TransactionInput.NO_SEQUENCE)
                throw new VerificationException("Refund transaction's input's sequence number disables lock time");
            if (refundTx.getLockTime() < minExpireTime)
                throw new VerificationException("Refund transaction has a lock time too soon");
            // Verify the transaction has one output (we don't care about its contents, its up to the client)
            // Note that because we sign with SIGHASH_NONE|SIGHASH_ANYOENCANPAY the client can later add more outputs and
            // inputs, but we will need only one output later to create the paying transactions
            if (refundTx.getOutputs().size() != 1)
                throw new VerificationException("Refund transaction does not have exactly one output");

            refundTransactionUnlockTimeSecs = refundTx.getLockTime();

            // Sign the refund tx with the scriptPubKey and return the signature. We don't have the spending transaction
            // so do the steps individually.
            clientKey = ECKey.fromPublicOnly(clientMultiSigPubKey);
            Script multisigPubKey = ScriptBuilder.createMultiSigOutputScript(2, ImmutableList.of(clientKey, serverKey));
            // We are really only signing the fact that the transaction has a proper lock time and don't care about anything
            // else, so we sign SIGHASH_NONE and SIGHASH_ANYONECANPAY.
            TransactionSignature sig = refundTx.calculateSignature(0, serverKey, multisigPubKey, Transaction.SigHash.NONE, true);
            log.info("Signed refund transaction.");
            this.clientOutput = refundTx.getOutput(0);
            stateMachine.transition(State.WAITING_FOR_MULTISIG_CONTRACT);
            return sig.encodeToBitcoin();
        } finally {
            lock.unlock();
        }
    }

    protected Script createOutputScript() {
        return ScriptBuilder.createMultiSigOutputScript(2, ImmutableList.<ECKey>of(clientKey, serverKey));
    }

    protected ECKey getClientKey() {
        return clientKey;
    }

    @Override
    void signInput(Transaction tx, Transaction.SigHash hashType,
                   boolean anyoneCanPay, @Nullable KeyParameter userKey) {
        signMultisigInput(tx, hashType, anyoneCanPay, userKey);
    }

    // Signs the first input of the transaction which must spend the multisig contract.
    private void signMultisigInput(Transaction tx, Transaction.SigHash hashType,
                                   boolean anyoneCanPay, @Nullable KeyParameter userKey) {
        TransactionSignature signature = tx.calculateSignature(0, serverKey, userKey, getContractScript(), hashType, anyoneCanPay);
        byte[] mySig = signature.encodeToBitcoin();
        Script scriptSig = ScriptBuilder.createMultiSigInputScriptBytes(ImmutableList.of(bestValueSignature, mySig));
        tx.getInput(0).setScriptSig(scriptSig);
    }

    /**
     * Gets the client's refund transaction which they can spend to get the entire channel value back if it reaches its
     * lock time.
     */
    public long getRefundTransactionUnlockTime() {
        lock.lock();
        try {
            checkState(getState().compareTo(State.WAITING_FOR_MULTISIG_CONTRACT) > 0 && getState() != State.ERROR);
            return refundTransactionUnlockTimeSecs;
        } finally {
            lock.unlock();
        }
    }
}
