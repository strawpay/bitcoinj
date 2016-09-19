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

import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptBuilder;
import org.bitcoinj.store.MemoryBlockStore;
import org.bitcoinj.testing.TestWithWallet;
import org.bitcoinj.wallet.SendRequest;
import org.bitcoinj.wallet.Wallet;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static org.bitcoinj.core.Coin.*;
import static org.bitcoinj.testing.FakeTxBuilder.createFakeTx;
import static org.bitcoinj.testing.FakeTxBuilder.makeSolvedTestBlock;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class PaymentChannelFeeTest extends TestWithWallet {
    private ECKey serverKey;
    private Wallet serverWallet;
    private PaymentChannelServerState serverState;
    private PaymentChannelClientState clientState;
    private TransactionBroadcaster mockBroadcaster;
    private BlockingQueue<TxFuturePair> broadcasts;
    private static final Coin HALF_COIN = Coin.valueOf(0, 50);

    /**
     * We use parameterized tests to run the channel connection tests with each
     * version of the channel.
     */
    @Parameterized.Parameters(name = "{index}: PaymentChannelStateTest({0})")
    public static Collection<PaymentChannelClient.VersionSelector> data() {
        return Arrays.asList(
                PaymentChannelClient.VersionSelector.VERSION_1,
                PaymentChannelClient.VersionSelector.VERSION_2_ALLOW_1);
    }

    @Parameterized.Parameter
    public PaymentChannelClient.VersionSelector versionSelector;

    /**
     * Returns <code>true</code> if we are using a protocol version that requires the exchange of refunds.
     */
    private boolean useRefunds() {
        return versionSelector == PaymentChannelClient.VersionSelector.VERSION_1;
    }

    private static class TxFuturePair {
        Transaction tx;
        SettableFuture<Transaction> future;

        public TxFuturePair(Transaction tx, SettableFuture<Transaction> future) {
            this.tx = tx;
            this.future = future;
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        Utils.setMockClock(); // Use mock clock

        final Context context = new Context(PARAMS, 100, Coin.ZERO, true);  // min fee required for this test
        Context.propagate(context);

        wallet = new Wallet(context);
        myKey = wallet.currentReceiveKey();
        myAddress = myKey.toAddress(context.getParams());
        blockStore = new MemoryBlockStore(context.getParams());
        chain = new BlockChain(context, wallet, blockStore);

        wallet.addExtension(new StoredPaymentChannelClientStates(wallet, new TransactionBroadcaster() {
            @Override
            public TransactionBroadcast broadcastTransaction(Transaction tx) {
                fail();
                return null;
            }
        }));
        sendMoneyToWallet(AbstractBlockChain.NewBlockType.BEST_CHAIN, COIN);
        chain = new BlockChain(context, wallet, blockStore); // Recreate chain as sendMoneyToWallet will confuse it
        serverWallet = new Wallet(context);
        serverKey = serverWallet.freshReceiveKey();
        chain.addWallet(serverWallet);

        broadcasts = new LinkedBlockingQueue<TxFuturePair>();
        mockBroadcaster = new TransactionBroadcaster() {
            @Override
            public TransactionBroadcast broadcastTransaction(Transaction tx) {
                SettableFuture<Transaction> future = SettableFuture.create();
                broadcasts.add(new TxFuturePair(tx, future));
                return TransactionBroadcast.createMockBroadcast(tx, future);
            }
        };
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private PaymentChannelClientState makeClientState(Wallet wallet, ECKey myKey, ECKey serverKey, Coin value, long time) {
        switch (versionSelector) {
            case VERSION_1:
                return new PaymentChannelV1ClientState(wallet, myKey, serverKey, value, time);
            case VERSION_2_ALLOW_1:
            case VERSION_2:
                return new PaymentChannelV2ClientState(wallet, myKey, serverKey, value, time);
            default:
                return null;
        }
    }

    private PaymentChannelServerState makeServerState(TransactionBroadcaster broadcaster, Wallet wallet, ECKey serverKey, long time) {
        switch (versionSelector) {
            case VERSION_1:
                return new PaymentChannelV1ServerState(broadcaster, wallet, serverKey, time);
            case VERSION_2_ALLOW_1:
            case VERSION_2:
                return new PaymentChannelV2ServerState(broadcaster, wallet, serverKey, time);
            default:
                return null;
        }
    }

    private PaymentChannelV1ClientState clientV1State() {
        if (clientState instanceof PaymentChannelV1ClientState) {
            return (PaymentChannelV1ClientState) clientState;
        } else {
            return null;
        }
    }

    private PaymentChannelV1ServerState serverV1State() {
        if (serverState instanceof PaymentChannelV1ServerState) {
            return (PaymentChannelV1ServerState) serverState;
        } else {
            return null;
        }
    }

    private PaymentChannelV2ClientState clientV2State() {
        if (clientState instanceof PaymentChannelV2ClientState) {
            return (PaymentChannelV2ClientState) clientState;
        } else {
            return null;
        }
    }

    private PaymentChannelV2ServerState serverV2State() {
        if (serverState instanceof PaymentChannelV2ServerState) {
            return (PaymentChannelV2ServerState) serverState;
        } else {
            return null;
        }
    }

    private PaymentChannelServerState.State getInitialServerState() {
        switch (versionSelector) {
            case VERSION_1:
                return PaymentChannelServerState.State.WAITING_FOR_REFUND_TRANSACTION;
            case VERSION_2_ALLOW_1:
            case VERSION_2:
                return PaymentChannelServerState.State.WAITING_FOR_MULTISIG_CONTRACT;
            default:
                return null;
        }
    }

    private PaymentChannelClientState.State getInitialClientState() {
        switch (versionSelector) {
            case VERSION_1:
                return PaymentChannelClientState.State.INITIATED;
            case VERSION_2_ALLOW_1:
            case VERSION_2:
                return PaymentChannelClientState.State.SAVE_STATE_IN_WALLET;
            default:
                return null;
        }
    }

    @Test
    public void feesTest() throws Exception {
        // Test that transactions are getting the necessary fees
        Context.propagate(new Context(PARAMS, 100, Coin.ZERO, true));

        // Spend the client wallet's one coin
        final SendRequest request = SendRequest.to(new ECKey().toAddress(PARAMS), COIN);
        request.ensureMinRequiredFee = false;
        wallet.sendCoinsOffline(request);
        assertEquals(Coin.ZERO, wallet.getBalance());

        chain.add(makeSolvedTestBlock(blockStore.getChainHead().getHeader(),
                createFakeTx(PARAMS, CENT.add(Transaction.REFERENCE_DEFAULT_MIN_TX_FEE), myAddress)));
        assertEquals(CENT.add(Transaction.REFERENCE_DEFAULT_MIN_TX_FEE), wallet.getBalance());

        Utils.setMockClock(); // Use mock clock
        final long EXPIRE_TIME = Utils.currentTimeMillis()/1000 + 60*60*24;

        serverState = makeServerState(mockBroadcaster, serverWallet, serverKey, EXPIRE_TIME);
        assertEquals(getInitialServerState(), serverState.getState());

        // Clearly SATOSHI is far too small to be useful
        clientState = makeClientState(wallet, myKey, ECKey.fromPublicOnly(serverKey.getPubKey()), Coin.SATOSHI, EXPIRE_TIME);
        assertEquals(PaymentChannelClientState.State.NEW, clientState.getState());
        try {
            clientState.initiate();
            fail();
        } catch (ValueOutOfRangeException e) {}

        clientState = makeClientState(wallet, myKey, ECKey.fromPublicOnly(serverKey.getPubKey()),
                Transaction.MIN_NONDUST_OUTPUT.subtract(Coin.SATOSHI).add(Transaction.REFERENCE_DEFAULT_MIN_TX_FEE),
                EXPIRE_TIME);
        assertEquals(PaymentChannelClientState.State.NEW, clientState.getState());
        try {
            clientState.initiate();
            fail();
        } catch (ValueOutOfRangeException e) {}

        // Verify that MIN_NONDUST_OUTPUT + MIN_TX_FEE is accepted
        clientState = makeClientState(wallet, myKey, ECKey.fromPublicOnly(serverKey.getPubKey()),
                Transaction.MIN_NONDUST_OUTPUT.add(Transaction.REFERENCE_DEFAULT_MIN_TX_FEE), EXPIRE_TIME);
        assertEquals(PaymentChannelClientState.State.NEW, clientState.getState());
        // We'll have to pay REFERENCE_DEFAULT_MIN_TX_FEE twice (multisig+refund), and we'll end up getting back nearly nothing...
        clientState.initiate();
        assertEquals(Transaction.REFERENCE_DEFAULT_MIN_TX_FEE.multiply(2), clientState.getRefundTxFees());
        assertEquals(getInitialClientState(), clientState.getState());

        // Now actually use a more useful CENT
        clientState = makeClientState(wallet, myKey, ECKey.fromPublicOnly(serverKey.getPubKey()), CENT, EXPIRE_TIME);
        assertEquals(PaymentChannelClientState.State.NEW, clientState.getState());
        clientState.initiate();
        assertEquals(Transaction.REFERENCE_DEFAULT_MIN_TX_FEE.multiply(2), clientState.getRefundTxFees());
        assertEquals(getInitialClientState(), clientState.getState());

        if (useRefunds()) {
            // Send the refund tx from client to server and get back the signature.
            Transaction refund = new Transaction(PARAMS, clientV1State().getIncompleteRefundTransaction().bitcoinSerialize());
            byte[] refundSig = serverV1State().provideRefundTransaction(refund, myKey.getPubKey());
            assertEquals(PaymentChannelServerState.State.WAITING_FOR_MULTISIG_CONTRACT, serverState.getState());
            // This verifies that the refund can spend the multi-sig output when run.
            clientV1State().provideRefundSignature(refundSig, null);
        }
        assertEquals(PaymentChannelClientState.State.SAVE_STATE_IN_WALLET, clientState.getState());
        clientState.fakeSave();
        assertEquals(PaymentChannelClientState.State.PROVIDE_MULTISIG_CONTRACT_TO_SERVER, clientState.getState());

        // Get the multisig contract
        Transaction multisigContract = new Transaction(PARAMS, clientState.getContract().bitcoinSerialize());
        assertEquals(PaymentChannelClientState.State.READY, clientState.getState());

        // Provide the server with the multisig contract and simulate successful propagation/acceptance.
        if (!useRefunds()) {
            serverV2State().provideClientKey(clientState.myKey.getPubKey());
        }
        serverState.provideContract(multisigContract);
        assertEquals(PaymentChannelServerState.State.WAITING_FOR_MULTISIG_ACCEPTANCE, serverState.getState());
        TxFuturePair pair = broadcasts.take();
        pair.future.set(pair.tx);
        assertEquals(PaymentChannelServerState.State.READY, serverState.getState());

        // Both client and server are now in the ready state. Simulate a few micropayments
        Coin totalPayment = Coin.ZERO;

        // We can send as little as we want - its up to the server to get the fees right
        byte[] signature = clientState.incrementPaymentBy(Coin.SATOSHI, null).signature.encodeToBitcoin();
        totalPayment = totalPayment.add(Coin.SATOSHI);
        serverState.incrementPayment(CENT.subtract(totalPayment), signature);

        // We can't refund more than the contract is worth...
        try {
            serverState.incrementPayment(CENT.add(SATOSHI), signature);
            fail();
        } catch (ValueOutOfRangeException e) {}

        // We cannot send just under the total value - our refund would make it unspendable. So the client
        // will correct it for us to be larger than the requested amount, to make the change output zero.
        PaymentChannelClientState.IncrementedPayment payment =
                clientState.incrementPaymentBy(CENT.subtract(Transaction.MIN_NONDUST_OUTPUT), null);
        assertEquals(CENT.subtract(SATOSHI), payment.amount);
        totalPayment = totalPayment.add(payment.amount);

        // The server also won't accept it if we do that.
        try {
            serverState.incrementPayment(Transaction.MIN_NONDUST_OUTPUT.subtract(Coin.SATOSHI), signature);
            fail();
        } catch (ValueOutOfRangeException e) {}

        serverState.incrementPayment(CENT.subtract(totalPayment), payment.signature.encodeToBitcoin());

        // And settle the channel.
        serverState.close();
        assertEquals(PaymentChannelServerState.State.CLOSING, serverState.getState());
        pair = broadcasts.take();  // settle
        pair.future.set(pair.tx);
        assertEquals(PaymentChannelServerState.State.CLOSED, serverState.getState());
        serverState.close();
        assertEquals(PaymentChannelServerState.State.CLOSED, serverState.getState());
    }

}
