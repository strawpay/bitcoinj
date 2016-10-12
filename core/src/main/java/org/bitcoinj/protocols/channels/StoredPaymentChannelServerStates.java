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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import net.jcip.annotations.GuardedBy;
import org.bitcoinj.core.*;
import org.bitcoinj.utils.Threading;
import org.bitcoinj.wallet.Wallet;
import org.bitcoinj.wallet.WalletExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.*;

/**
 * Keeps track of a set of {@link StoredServerChannel}s and expires them 2 hours before their refund transactions
 * unlock.
 */
public class StoredPaymentChannelServerStates implements WalletExtension {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(StoredPaymentChannelServerStates.class);

    static final String EXTENSION_ID = StoredPaymentChannelServerStates.class.getName();
    static final int MAX_SECONDS_TO_WAIT_FOR_BROADCASTER_TO_BE_SET = 10;

    @GuardedBy("lock") @VisibleForTesting final Map<Sha256Hash, StoredServerChannel> mapChannels = new HashMap<Sha256Hash, StoredServerChannel>();
    private Wallet wallet;
    private final SettableFuture<TransactionBroadcaster> broadcasterFuture = SettableFuture.create();

    private final Timer channelTimeoutHandler = new Timer(true);

    private final ReentrantLock lock = Threading.lock("StoredPaymentChannelServerStates");

    /**
     * The offset between the refund transaction's lock time and the time channels will be automatically closed.
     * This defines a window during which we must get the last payment transaction verified, ie it should allow time for
     * network propagation and for the payment transaction to be included in a block. Note that the channel expire time
     * is measured in terms of our local clock, and the refund transaction's lock time is measured in terms of Bitcoin
     * block header timestamps, which are allowed to drift up to two hours in the future, as measured by relaying nodes.
     */
    public static final long CHANNEL_EXPIRE_OFFSET = -2*60*60;

    /**
     * Creates a new PaymentChannelServerStateManager and associates it with the given {@link Wallet} and
     * {@link TransactionBroadcaster} which are used to complete and announce payment transactions.
     */
    public StoredPaymentChannelServerStates(@Nullable Wallet wallet, TransactionBroadcaster broadcaster) {
        setTransactionBroadcaster(broadcaster);
        this.wallet = wallet;
    }

    /**
     * Creates a new PaymentChannelServerStateManager and associates it with the given {@link Wallet}
     *
     * Use this constructor if you use WalletAppKit, it will provide the broadcaster for you (no need to call the setter)
     */
    public StoredPaymentChannelServerStates(@Nullable Wallet wallet) {
        this.wallet = wallet;
    }

    /**
     * Use this setter if the broadcaster is not available during instantiation and you're not using WalletAppKit.
     * This setter will let you delay the setting of the broadcaster until the Bitcoin network is ready.
     *
     * @param broadcaster Used when the payment channels are closed
     */
    public final void setTransactionBroadcaster(TransactionBroadcaster broadcaster) {
        this.broadcasterFuture.set(checkNotNull(broadcaster));
    }

    /** Returns this extension from the given wallet, or null if no such extension was added. */
    @Nullable
    public static StoredPaymentChannelServerStates getFromWallet(Wallet wallet) {
        return (StoredPaymentChannelServerStates) wallet.getExtensions().get(EXTENSION_ID);
    }

    /**
     * <p>Closes the given channel using {@link ServerConnectionEventHandler#closeChannel()} and
     * {@link PaymentChannelV1ServerState#close()} to notify any connected client of channel closure and to complete and
     * broadcast the latest payment transaction.</p>
     *
     * <p>Removes the given channel from this set of {@link StoredServerChannel}s and notifies the wallet of a change to
     * this wallet extension.</p>
     */
    public void closeChannel(final StoredServerChannel storedServerChannel) {

        // Lock and find out if we need state, then unlock and lock again to respect lock order !
        final PaymentChannelServer connectedHandler = storedServerChannel.getConnectedHandler();
        if (connectedHandler != null)
            connectedHandler.close();

        final Transaction close = storedServerChannel.getCloseTransaction();
        final boolean hasCloseTx = close != null;

        // now we can lock first on state if we need to use it.
        if (hasCloseTx) {
            log.debug("Channel was already closed {} ", storedServerChannel.contract.getHash());
            if (close.getConfidence(wallet.getContext().getConfidenceTable()).getDepthInBlocks() == 0) {
                log.debug("Broadcasting unconfirmed close again {} ", close.getHash());
                getBroadcaster().broadcastTransaction(close);
            }
            watchTxConfirmations(storedServerChannel.contract.getHash(), close);
        } else {
            log.debug("Channel not closed yet {} ", storedServerChannel.contract.getHash());
            final PaymentChannelServerState state = storedServerChannel.getOrCreateState(wallet, getBroadcaster());
            try {
                state.close();
                storedServerChannel.setCloseTransaction(state.closeTx);
                if (state.closeTx != null) {
                    watchTxConfirmations(storedServerChannel.contract.getHash(), state.closeTx);
                }
            } catch (InsufficientMoneyException e) {
                log.error("Exception when closing channel", e);
                removeChannel(storedServerChannel.contract.getHash());
            }
         }

        // Check independent of other things if contract is settled somehow, remove channel later (when settle is deep in chain).
        checkIfSettled(storedServerChannel.contract.getHash());
        updatedChannel(storedServerChannel);
    }

    private boolean checkIfSettled(final Sha256Hash contractHash) {
        log.debug("checkIfSettled contract={}", contractHash);

        final Transaction contract = wallet.getTransaction(contractHash);
        if (contract != null) {
            if (!contract.getOutput(0).isAvailableForSpending()) {
                log.debug("contract.getOutput(0).isMine(wallet) {}", contract.getOutput(0).isMine(wallet));
                log.debug("contract.getOutput(0).isWatched(wallet) {}", contract.getOutput(0).isWatched(wallet));
                log.debug("contract.getOutput(0).getParentTransactionDepthInBlocks() {}", contract.getOutput(0).getParentTransactionDepthInBlocks());

                final Transaction settle = contract.getOutput(0).getSpentBy().getParentTransaction();
                if (settle == null) {
                    log.warn("No settle tx found when contract={} getOutput(0).isAvailableForSpending = false", contractHash);
                    return false;
                } else {
                    watchTxConfirmations(contractHash, settle);
                    return true;
                }
            } else {
                log.debug("contract {} getOutput(0).isAvailableForSpending=true", contractHash);
                return false;
            }
        } else {
            log.warn("No tx found in wallet for contract={}", contractHash);
            return false;
        }
    }

    private void watchTxConfirmations(final Sha256Hash contractHash, final Transaction settle) {
        final TransactionConfidence confidence = settle.getConfidence(wallet.getContext().getConfidenceTable());
        int numConfirms = wallet.getContext().getEventHorizon();
        log.info("Watching contract {} settling with {} of depth = {}, waiting for depth {}",
                contractHash, settle.getHashAsString(),
                confidence.getDepthInBlocks(), numConfirms);

        ListenableFuture<TransactionConfidence> future = confidence.getDepthFuture(numConfirms, Threading.USER_THREAD);
        Futures.addCallback(future, new FutureCallback<TransactionConfidence>() {
            @Override
            public void onSuccess(TransactionConfidence result) {
                log.info("Deleting channel from wallet: {} when tx {} is safely confirmed, depth = {}",
                        contractHash, settle.getHashAsString(), result.getDepthInBlocks());
                removeChannel(contractHash);
            }

            @Override
            public void onFailure(Throwable t) {
                Throwables.propagate(t);
            }
        });
    }



    private void removeChannel(Sha256Hash contractHash) {
        lock.lock();
        final StoredServerChannel channel;
        try {
            channel = mapChannels.remove(contractHash);
            if (channel == null)
                return;
            if (channel.watcher != null)
                channel.watcher.removeListeners(channel);
        } finally {
            lock.unlock();
        }
        updatedChannel(channel);
    }

    /**
     * If the broadcaster has not been set for MAX_SECONDS_TO_WAIT_FOR_BROADCASTER_TO_BE_SET seconds, then
     * the programmer probably forgot to set it and we should throw exception.
     */
    private TransactionBroadcaster getBroadcaster() {
        try {
            return broadcasterFuture.get(MAX_SECONDS_TO_WAIT_FOR_BROADCASTER_TO_BE_SET, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            String err = "Transaction broadcaster not set";
            log.error(err);
            throw new RuntimeException(err,e);
        }
    }

    /**
     * Gets the {@link StoredServerChannel} with the given channel id (ie contract transaction hash).
     */
    public StoredServerChannel getChannel(Sha256Hash id) {
        lock.lock();
        try {
            return mapChannels.get(id);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get a copy of all {@link StoredServerChannel}s
     */
    public Map<Sha256Hash, StoredServerChannel> getChannelMap() {
        lock.lock();
        try {
            return ImmutableMap.copyOf(mapChannels);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Notifies the set of stored states that a channel has been updated. Use to notify the wallet of an update to this
     * wallet extension.
     */
    public void updatedChannel(final StoredServerChannel channel) {
        //log.info("Stored server channel {} was updated", channel.hashCode());
        log.debug("Stored server channel {} was updated", channel);
        wallet.addOrUpdateExtension(this);
    }

    /**
     * <p>Puts the given channel in the channels map and automatically closes it 2 hours before its refund transaction
     * becomes spendable.</p>
     *
     * <p>Because there must be only one, canonical {@link StoredServerChannel} per channel, this method throws if the
     * channel is already present in the set of channels.</p>
     */
    public void putChannel(final StoredServerChannel channel) {
        lock.lock();
        try {
            checkArgument(mapChannels.put(channel.contract.getHash(), checkNotNull(channel)) == null);
            log.info("Added channel to map: {}", channel);
        } finally {
            lock.unlock();
        }

        // Watch this channel for various events, once the broadcaster is ready..
        Runnable initiateWatchingChannel = new Runnable() {
            @Override
            public void run() {
                channel.lock.lock();
                try {
                    if (mapChannels.containsKey(channel.contract.getHash()))
                        channel.watcher = new PaymentChannelWatcher(channel);
                    else
                        log.info("Channel was removed before we initiated watching it!");
                } finally {
                    channel.lock.unlock();
                }
            }
        };
        log.debug("Waiting for peer group before initiate watching channel for expiry and txs.");
        broadcasterFuture.addListener(initiateWatchingChannel, Threading.USER_THREAD);

        updatedChannel(channel);
    }

    @Override
    public String getWalletExtensionID() {
        return EXTENSION_ID;
    }

    @Override
    public boolean isWalletExtensionMandatory() {
        return false;
    }

    @Override
    public byte[] serializeWalletExtension() {
        lock.lock();
        try {
            final NetworkParameters params = getNetworkParameters();
            // If we haven't attached to a wallet yet we can't check against network parameters
            final boolean hasMaxMoney = params != null ? params.hasMaxMoney() : true;
            final Coin networkMaxMoney = params != null ? params.getMaxMoney() : NetworkParameters.MAX_MONEY;
            ServerState.StoredServerPaymentChannels.Builder builder = ServerState.StoredServerPaymentChannels.newBuilder();
            for (final StoredServerChannel ch : mapChannels.values()) {
                ch.lock.lock();
                try {
                    // First a few asserts to make sure things won't break
                    // TODO: Pull MAX_MONEY from network parameters
                    checkState(ch.bestValueToMe.signum() >= 0 &&
                            (!hasMaxMoney || ch.bestValueToMe.compareTo(networkMaxMoney) <= 0));
                    checkState(ch.refundTransactionUnlockTimeSecs > 0);
                    checkNotNull(ch.myKey.getPrivKeyBytes());
                    ServerState.StoredServerPaymentChannel.Builder channelBuilder = ServerState.StoredServerPaymentChannel.newBuilder()
                            .setMajorVersion(ch.majorVersion)
                            .setBestValueToMe(ch.bestValueToMe.value)
                            .setRefundTransactionUnlockTimeSecs(ch.refundTransactionUnlockTimeSecs)
                            .setContractTransaction(ByteString.copyFrom(ch.contract.unsafeBitcoinSerialize()))
                            .setMyKey(ByteString.copyFrom(ch.myKey.getPrivKeyBytes()));
                    if (ch.majorVersion == 1) {
                        channelBuilder.setClientOutput(ByteString.copyFrom(ch.clientOutput.unsafeBitcoinSerialize()));
                    } else {
                        channelBuilder.setClientKey(ByteString.copyFrom(ch.clientKey.getPubKey()));
                    }
                    if (ch.bestValueSignature != null)
                        channelBuilder.setBestValueSignature(ByteString.copyFrom(ch.bestValueSignature));
                    if (ch.getCloseTransaction() != null)
                        channelBuilder.setCloseTransaction(ByteString.copyFrom(ch.getCloseTransaction().unsafeBitcoinSerialize()));
                    builder.addChannels(channelBuilder);
                } finally {
                    ch.lock.unlock();
                }
            }
            return builder.build().toByteArray();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deserializeWalletExtension(Wallet containingWallet, byte[] data) throws Exception {
        lock.lock();
        try {
            this.wallet = containingWallet;
            ServerState.StoredServerPaymentChannels states = ServerState.StoredServerPaymentChannels.parseFrom(data);
            NetworkParameters params = containingWallet.getParams();
            for (ServerState.StoredServerPaymentChannel storedState : states.getChannelsList()) {
                final int majorVersion = storedState.getMajorVersion();
                TransactionOutput clientOutput = null;
                ECKey clientKey = null;
                if (majorVersion == 1) {
                    clientOutput = new TransactionOutput(params, null, storedState.getClientOutput().toByteArray(), 0);
                } else {
                    clientKey = ECKey.fromPublicOnly(storedState.getClientKey().toByteArray());
                }
                final Transaction contract, close;
                {
                    contract = params.getDefaultSerializer().makeTransaction(storedState.getContractTransaction().toByteArray());
                    close = storedState.hasCloseTransaction() ?
                            params.getDefaultSerializer().makeTransaction(storedState.getCloseTransaction().toByteArray()) :
                            null;
                }
                StoredServerChannel channel = new StoredServerChannel(null,
                        majorVersion,
                        contract,
                        clientOutput,
                        close,
                        storedState.getRefundTransactionUnlockTimeSecs(),
                        ECKey.fromPrivate(storedState.getMyKey().toByteArray()),
                        clientKey,
                        Coin.valueOf(storedState.getBestValueToMe()),
                        storedState.hasBestValueSignature() ? storedState.getBestValueSignature().toByteArray() : null);
                putChannel(channel);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        lock.lock();
        try {
            StringBuilder buf = new StringBuilder();
            for (StoredServerChannel stored : mapChannels.values()) {
                buf.append(stored);
            }
            return buf.toString();
        } finally {
            lock.unlock();
        }
    }

    private @Nullable NetworkParameters getNetworkParameters() {
        return wallet != null ? wallet.getNetworkParameters() : null;
    }

    /**
     * Watch a single channel. Responsible for auto closing, detecting a dead contract, close or a refund transaction confirmations pertaining to a channel.
     * */
    class PaymentChannelWatcher {
        private final Logger log = LoggerFactory.getLogger(PaymentChannelWatcher.class);
        private final TransactionConfidence.Listener confidenceListener;
        private final int eventHorizon = wallet.getContext().getEventHorizon();
        private final TimerTask autoCloseTask;

        private PaymentChannelWatcher(final StoredServerChannel storedChannel) {
            final Sha256Hash contractHash = storedChannel.contract.getHash();
            log.debug("PaymentChannelWatcher for contract {}", contractHash);

            confidenceListener = new TransactionConfidence.Listener() {
                public void onConfidenceChanged(final TransactionConfidence confidence, ChangeReason reason) {
                    final Sha256Hash txHash = confidence.getTransactionHash();
                    if (txHash.equals(storedChannel.contract.getHash())) {
                        if (reason == ChangeReason.TYPE &&
                                confidence.getConfidenceType() == TransactionConfidence.ConfidenceType.DEAD) {
                            storedChannel.lock.lock();
                            try {
                                if (storedChannel.overridingTx != null) {
                                    // maybe a new overriding tx...
                                    removeListenerFromTx(storedChannel.overridingTx);
                                }
                                storedChannel.overridingTx = confidence.getOverridingTransaction();
                            } finally {
                                storedChannel.lock.unlock();
                            }
                            observeFinalTxConfidence(storedChannel.overridingTx, this, contractHash, "overriding");
                        }
                    } else if (confidence.getDepthInBlocks() >= eventHorizon) {
                        // TODO We should detect and watch if a refund tx occurs also.
                        String detectedTransactionType = detectTerminatingTransactionType(storedChannel, txHash);
                        if (detectedTransactionType != null) {
                            log.info("Deleting channel {} from wallet when {} tx {} is safely confirmed, depth = {}",
                                    contractHash, detectedTransactionType, txHash, confidence.getDepthInBlocks());
                            removeChannel(contractHash);
                        } else {
                            log.warn("Detecting unknown transaction {} when watching contract {}", txHash, contractHash);
                        }
                    }
                }
            };

            final TransactionConfidence contractConfidence = storedChannel.contract.getConfidence(wallet.getContext());

            // Will detect if it turns DEAD.
            contractConfidence.addEventListener(Threading.USER_THREAD, confidenceListener);

            if (contractConfidence.getConfidenceType() == TransactionConfidence.ConfidenceType.DEAD) {
                storedChannel.overridingTx = contractConfidence.getOverridingTransaction();
                log.warn("Channel contract {} is DEAD and overridden by {}", contractHash, storedChannel.overridingTx);
                observeFinalTxConfidence(storedChannel.overridingTx, confidenceListener, contractHash, "overriding");
            }

            if (storedChannel.getCloseTransaction() != null) {
                log.debug("Channel contract {} has close transaction.", contractHash);
                observeFinalTxConfidence(storedChannel.getCloseTransaction(), confidenceListener, contractHash, "existing close");
            } else if (storedChannel.refundTransactionUnlockTimeSecs < Utils.currentTimeSeconds()) {
                log.warn("Channel contract {} has expired.", contractHash);
            }

            // Add the difference between real time and Utils.now() so that test-cases can use a mock clock.
            Date autocloseTime = new Date((storedChannel.refundTransactionUnlockTimeSecs + CHANNEL_EXPIRE_OFFSET) * 1000L
                    + (System.currentTimeMillis() - Utils.currentTimeMillis()));
            log.info("Scheduling channel for automatic closure at {}: {}", autocloseTime, storedChannel);
            autoCloseTask = new TimerTask() {
                @Override
                public void run() {
                    log.info("Auto-closing channel: {}", storedChannel);
                    try {
                        closeChannel(storedChannel);
                    } catch (Exception e) {
                        // Something went wrong closing the channel - we catch
                        // here or else we take down the whole Timer.
                        log.error("Auto-closing channel failed", e);
                    }
                }
            };

            channelTimeoutHandler.schedule(autoCloseTask, autocloseTime);
        }

        private String detectTerminatingTransactionType(StoredServerChannel ch, Sha256Hash txHash) {
            ch.lock.lock();
            try {
                return (ch.getCloseTransaction() != null) && ch.getCloseTransaction().getHash().equals(txHash) ? "close" :
                        (ch.overridingTx != null) && ch.overridingTx.getHash().equals(txHash) ? "overriding contract" : null;
            } finally {
                ch.lock.unlock();
            }
        }

        void removeListeners(final StoredServerChannel channel) {
            channel.lock.lock();
            try {
                boolean cancelledTimerTask = autoCloseTask.cancel();
                if (cancelledTimerTask) log.debug("Cancelled expiry timer for contract {}", channel.contract.getHash());

                removeListenerFromTx(channel.getCloseTransaction());
                removeListenerFromTx(channel.contract);
                removeListenerFromTx(channel.overridingTx);

            } finally {
                channel.lock.unlock();
            }
        }

        private void removeListenerFromTx(Transaction tx) {
            final Context context = wallet.getContext();
            if (tx != null) {
                boolean removed = tx.getConfidence(context).removeEventListener(confidenceListener);
                if (removed) log.debug("Removed confidence listener from tx {}", tx.getHash());
            }
        }

        private void observeFinalTxConfidence(final Transaction finalTx, final TransactionConfidence.Listener listener, final Sha256Hash contract, final String label) {
            // When we see the transaction get enough confirmations, we can just delete the record
            // of this channel from the wallet, because we're not going to need any of that any more.
            final TransactionConfidence txConfidence = finalTx.getConfidence(wallet.getContext());
            final int depth = txConfidence.getDepthInBlocks();
            log.info("Watching contract {} settling with {} {} of depth = {}", contract, label, finalTx.getHashAsString(), depth);
            if (depth >= eventHorizon) {
                log.info("... execute confidence handler as depth is beyond event horizon {}", eventHorizon);
                Threading.USER_THREAD.execute(new Runnable() {
                    @Override
                    public void run() {
                        listener.onConfidenceChanged(txConfidence, TransactionConfidence.Listener.ChangeReason.DEPTH);
                    }
                });
            } else {
                log.info("...waiting for depth {}", eventHorizon);
                txConfidence.addEventListener(Threading.USER_THREAD, listener);
            }
        }
    }


}
