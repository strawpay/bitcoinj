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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import net.jcip.annotations.GuardedBy;
import org.bitcoinj.core.*;
import org.bitcoinj.utils.Threading;
import org.bitcoinj.wallet.Wallet;
import org.bitcoinj.wallet.WalletExtension;
import org.bitcoinj.wallet.listeners.WalletCoinsReceivedEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * This class maintains a set of {@link StoredClientChannel}s, automatically (re)broadcasting the contract transaction
 * and broadcasting the refund transaction over the given {@link TransactionBroadcaster}.
 */
public class StoredPaymentChannelClientStates implements WalletExtension {
    private static final Logger log = LoggerFactory.getLogger(StoredPaymentChannelClientStates.class);
    static final String EXTENSION_ID = StoredPaymentChannelClientStates.class.getName();
    static final int MAX_SECONDS_TO_WAIT_FOR_BROADCASTER_TO_BE_SET = 10;

    // TODO Should match/relate to when a server closes safe amount of time before expiry
    static final int SECONDS_LEFT_BEFORE_EXPIRY_TO_SELECT_CHANNEL = 30 * 60;

    @GuardedBy("lock") @VisibleForTesting final HashMultimap<Sha256Hash, StoredClientChannel> mapChannels = HashMultimap.create();
    @VisibleForTesting final Timer channelTimeoutHandler = new Timer(true);

    private Wallet containingWallet;
    private final SettableFuture<TransactionBroadcaster> announcePeerGroupFuture = SettableFuture.create();

    protected final ReentrantLock lock = Threading.lock("StoredPaymentChannelClientStates");

    void lock() {
        containingWallet.lockWalletAndThen(lock);
    }

    void unlock() {
        containingWallet.unlockLockAndThenWallet(lock);
    }

    /**
     * Creates a new StoredPaymentChannelClientStates and associates it with the given {@link Wallet} and
     * {@link TransactionBroadcaster} which are used to complete and announce contract and refund
     * transactions.
     */
    public StoredPaymentChannelClientStates(@Nullable Wallet containingWallet, TransactionBroadcaster announcePeerGroup) {
        log.debug("Constructed with TransactionBroadcaster");
        setTransactionBroadcaster(announcePeerGroup);
        this.containingWallet = containingWallet;
        propagateContextToTimer(containingWallet);
    }

    /**
     * Creates a new StoredPaymentChannelClientStates and associates it with the given {@link Wallet}
     *
     * Use this constructor if you use WalletAppKit, it will provide the broadcaster for you (no need to call the setter)
     */
    public StoredPaymentChannelClientStates(@Nullable Wallet containingWallet) {
        log.debug("Constructed without TransactionBroadcaster");
        this.containingWallet = containingWallet;
        propagateContextToTimer(containingWallet);
    }

    private void propagateContextToTimer(@Nullable final Wallet wallet) {
        if (wallet != null) {
            final Context context = wallet.getContext();
            channelTimeoutHandler.schedule(new TimerTask() {
                @Override
                public void run() {
                    Context.propagate(context);
                }
            }, 0L);
        }
    }

    /**
     * Use this setter if the broadcaster is not available during instantiation and you're not using WalletAppKit.
     * This setter will let you delay the setting of the broadcaster until the Bitcoin network is ready.
     *
     * @param transactionBroadcaster which is used to complete and announce contract and refund transactions.
     */
    public final void setTransactionBroadcaster(TransactionBroadcaster transactionBroadcaster) {
        log.debug("TransactionBroadcaster set on extension");
        this.announcePeerGroupFuture.set(checkNotNull(transactionBroadcaster));
    }

    /** Returns this extension from the given wallet, or null if no such extension was added. */
    @Nullable
    public static StoredPaymentChannelClientStates getFromWallet(Wallet wallet) {
        return (StoredPaymentChannelClientStates) wallet.getExtensions().get(EXTENSION_ID);
    }

    /** Returns the outstanding amount of money sent back to us for all channels to this server added together. */
    public Coin getBalanceForServer(Sha256Hash id) {
        Coin balance = Coin.ZERO;
        final long nowSeconds = Utils.currentTimeSeconds();
        final long marginSeconds = SECONDS_LEFT_BEFORE_EXPIRY_TO_SELECT_CHANNEL;

        lock();
        try {
            Set<StoredClientChannel> setChannels = mapChannels.get(id);
            for (StoredClientChannel channel : setChannels) {
                channel.lock.lock();
                try {
                    if (channel.close != null || nowSeconds - channel.expiryTimeSeconds() >= marginSeconds) continue;
                    balance = balance.add(channel.valueToMe);
                } finally {
                    channel.lock.unlock();
                }
            }
            return balance;
        } finally {
            unlock();
        }
    }

    /**
     * Returns the number of seconds from now until this servers next channel will expire, or zero if no unexpired
     * channels found.
     */
    public long getSecondsUntilExpiry(Sha256Hash id) {
        lock();
        try {
            final Set<StoredClientChannel> setChannels = mapChannels.get(id);
            final long nowSeconds = Utils.currentTimeSeconds();
            int earliestTime = Integer.MAX_VALUE;
            for (StoredClientChannel channel : setChannels) {
                channel.lock.lock();
                try {
                    if (channel.expiryTimeSeconds() > nowSeconds)
                        earliestTime = Math.min(earliestTime, (int) channel.expiryTimeSeconds());
                } finally {
                    channel.lock.unlock();
                }
            }
            return earliestTime == Integer.MAX_VALUE ? 0 : earliestTime - nowSeconds;
        } finally {
            unlock();
        }
    }

    /**
     * Finds an inactive channel with the given id and returns it, or returns null.
     */
    @Nullable
    StoredClientChannel getUsableChannelForServerID(Sha256Hash id) {
        lock();
        try {
            Set<StoredClientChannel> setChannels = mapChannels.get(id);
            final long nowSeconds = Utils.currentTimeSeconds();
            final long marginSeconds = SECONDS_LEFT_BEFORE_EXPIRY_TO_SELECT_CHANNEL;
            for (StoredClientChannel channel : setChannels) {
                channel.lock.lock();
                try {
                    // Check if the channel is usable (has money, inactive) and if so, activate it.
                    log.info("Considering channel {} contract {}", channel.hashCode(), channel.contract.getHash());
                    if (channel.close != null || channel.valueToMe.equals(Coin.ZERO) ||
                            nowSeconds - channel.expiryTimeSeconds() >= marginSeconds) {
                        log.info("  ... but is closed, expired or empty");
                        continue;
                    }
                    if (!channel.active) {
                        log.info("  ... activating");
                        channel.active = true;
                        return channel;
                    }
                    log.info("  ... but is already active");
                } finally {
                    channel.lock.unlock();
                }
            }
        } finally {
            unlock();
        }
        return null;
    }

    /**
     * Finds a channel with the given id and contract hash and returns it, or returns null.
     */
    @Nullable
    public StoredClientChannel getChannel(Sha256Hash id, Sha256Hash contractHash) {
        lock();
        try {
            Set<StoredClientChannel> setChannels = mapChannels.get(id);
            for (StoredClientChannel channel : setChannels) {
                channel.lock.lock();
                try {
                    if (channel.contract.getHash().equals(contractHash))
                        return channel;
                } finally {
                    channel.lock.unlock();
                }
            }
            return null;
        } finally {
            unlock();
        }
    }

    /**
     * Get a copy of all {@link StoredClientChannel}s
     */
    public Multimap<Sha256Hash, StoredClientChannel> getChannelMap() {
        lock();
        try {
            // lock every stored channel to ensure any changes are synchronized
            for (Map.Entry<Sha256Hash, Collection<StoredClientChannel>> storedChannelsWithId : mapChannels.asMap().entrySet()) {
                for (StoredClientChannel storedChannel : storedChannelsWithId.getValue()) {
                    storedChannel.lock.lock();
                    storedChannel.lock.unlock();
                }
            }

            return ImmutableMultimap.copyOf(mapChannels);
        } finally {
            unlock();
        }
    }

    /**
     * Get an immutable view of current status for all known channels {@link ClientChannelStatusView}s
     */
    public List<ClientChannelStatusView> getClientChannelsStatusView() {
        TxConfidenceTable confidenceTable = containingWallet.getContext().getConfidenceTable();

        lock();
        try {
            ArrayList<ClientChannelStatusView> channels = new ArrayList<ClientChannelStatusView>();
            for (Map.Entry<Sha256Hash, Collection<StoredClientChannel>> storedChannelsWithId : mapChannels.asMap().entrySet()) {
                for (StoredClientChannel storedChannel : storedChannelsWithId.getValue()) {
                    storedChannel.lock.lock();
                    try {
                        channels.add(new ClientChannelStatusView(storedChannel, confidenceTable));
                    } finally {
                        storedChannel.lock.unlock();
                    }
                }
            }

            return channels;
        } finally {
            unlock();
        }
    }



    /**
     * Notifies the set of stored states that a channel has been updated. Use to notify the wallet of an update to this
     * wallet extension.
     */
    void updatedChannel(final StoredClientChannel channel) {
        log.info("Stored client channel {} was updated", channel.hashCode());
        containingWallet.addOrUpdateExtension(this);
    }

    /**
     * Adds the given channel to this set of stored states, broadcasting the contract and refund transactions when the
     * channel expires and notifies the wallet of an update to this wallet extension
     */
    void putChannel(final StoredClientChannel channel) {
        putChannel(channel, true);
    }

    // Adds this channel and optionally notifies the wallet of an update to this extension (used during deserialize)
    private void putChannel(final StoredClientChannel channel, boolean updateWallet) {
        lock();
        channel.lock.lock();
        try {
            log.debug("putChannel {}", channel.contract.getHashAsString());
            mapChannels.put(channel.id, channel);
        } catch (Exception e) {
            log.error("putChannel failed", e);
        } finally {
            channel.lock.unlock();
            unlock();
        }

        // Watch this channel for various events, once the broadcaster is ready..
        Runnable initiateWatchingChannel = new Runnable() {
            @Override
            public void run() {
                lock();
                channel.lock.lock();
                try {
                    if (mapChannels.containsEntry(channel.id, channel))
                        channel.watcher = new PaymentChannelWatcher(channel);
                    else
                        log.info("Channel was removed before we initiated watching it!");
                } finally {
                    channel.lock.unlock();
                    unlock();
                }
            }
        };
        log.debug("Waiting for peer group before initiate watching channel for expiry and txs.");
        announcePeerGroupFuture.addListener(initiateWatchingChannel, Threading.USER_THREAD);

        if (updateWallet)
            updatedChannel(channel);
    }

    static private void observeAndLogBroadcast(TransactionBroadcast tb, final String txLabel) {
        final ListenableFuture<Transaction> f = tb.future();
        f.addListener(new Runnable() {
            public void run() {
                try {
                    if (f.isCancelled()) {
                        log.debug("{} broadcast is cancelled", txLabel);
                    } else {
                        log.debug("{} {} broadcast.", txLabel, f.get().getHashAsString());
                    }
                } catch (ExecutionException e) {
                    log.error("Exception during broadcast", e);
                } catch (InterruptedException e) {
                    log.error("Interrupted waiting for broadcast", e);
                }
            }
        }, Threading.USER_THREAD);
    }

    /**
     * If the peer group has not been set for MAX_SECONDS_TO_WAIT_FOR_BROADCASTER_TO_BE_SET seconds, then
     * the programmer probably forgot to set it and we should throw exception.
     */
    private TransactionBroadcaster getAnnouncePeerGroup() {
        try {
            return announcePeerGroupFuture.get(MAX_SECONDS_TO_WAIT_FOR_BROADCASTER_TO_BE_SET, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            String err = "Transaction broadcaster not set";
            log.error(err);
            throw new RuntimeException(err, e);
        }
    }

    /**
     * <p>Removes the channel with the given id from this set of stored states and notifies the wallet of an update to
     * this wallet extension.</p>
     *
     * <p>Note that the channel will still have its contract and refund transactions broadcast via the connected
     * {@link TransactionBroadcaster} as long as this {@link StoredPaymentChannelClientStates} continues to
     * exist in memory.</p>
     */
    void removeChannel(StoredClientChannel channel) {
        lock();
        try {
            if (channel.watcher != null)
                channel.watcher.removeListeners(channel);
            mapChannels.remove(channel.id, channel);
            updatedChannel(channel);
        } finally {
            unlock();
        }
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
        lock();
        try {
            final NetworkParameters params = getNetworkParameters();
            // If we haven't attached to a wallet yet we can't check against network parameters
            final boolean hasMaxMoney = params != null ? params.hasMaxMoney() : true;
            final Coin networkMaxMoney = params != null ? params.getMaxMoney() : NetworkParameters.MAX_MONEY;
            ClientState.StoredClientPaymentChannels.Builder builder = ClientState.StoredClientPaymentChannels.newBuilder();
            for (StoredClientChannel channel : mapChannels.values()) {
                channel.lock.lock();
                try {
                    // First a few asserts to make sure things won't break
                    checkState(channel.valueToMe.signum() >= 0 &&
                            (!hasMaxMoney || channel.valueToMe.compareTo(networkMaxMoney) <= 0));
                    checkState(channel.refundFees.signum() >= 0 &&
                            (!hasMaxMoney || channel.refundFees.compareTo(networkMaxMoney) <= 0));
                    checkNotNull(channel.myKey.getPubKey());
                    if (channel.refund.getConfidence().getSource() != TransactionConfidence.Source.SELF) {
                        // TODO it seems to occur in rare cases that source is not SELF. Figure out why.
                        log.warn("channel.refund.getConfidence().getSource() != TransactionConfidence.Source.SELF");
                    }
                    checkNotNull(channel.myKey.getPubKey());
                    final ClientState.StoredClientPaymentChannel.Builder value = ClientState.StoredClientPaymentChannel.newBuilder()
                            .setMajorVersion(channel.majorVersion)
                            .setId(ByteString.copyFrom(channel.id.getBytes()))
                            .setContractTransaction(ByteString.copyFrom(channel.contract.unsafeBitcoinSerialize()))
                            .setRefundFees(channel.refundFees.value)
                            .setRefundTransaction(ByteString.copyFrom(channel.refund.unsafeBitcoinSerialize()))
                            .setMyKey(ByteString.copyFrom(new byte[0])) // Not  used, but protobuf message requires
                            .setMyPublicKey(ByteString.copyFrom(channel.myKey.getPubKey()))
                            .setServerKey(ByteString.copyFrom(channel.serverKey.getPubKey()))
                            .setValueToMe(channel.valueToMe.value)
                            .setExpiryTime(channel.expiryTime);
                    if (channel.close != null)
                        value.setCloseTransactionHash(ByteString.copyFrom(channel.close.getHash().getBytes()));
                    builder.addChannels(value);
                } finally {
                    channel.lock.unlock();
                }
            }
            return builder.build().toByteArray();
        } finally {
            unlock();
        }
    }

    @Override
    public void deserializeWalletExtension(final Wallet containingWallet, final byte[] data) throws Exception {

        // This is the special case when the containingWallet member is set. Thus we need to use the argument to synchronize state.
        containingWallet.lockWalletAndThen(lock);
        log.debug("deserializeWalletExtension");
        try {
            checkState(this.containingWallet == null || this.containingWallet == containingWallet);
            this.containingWallet = containingWallet;
            propagateContextToTimer(containingWallet);
            NetworkParameters params = containingWallet.getParams();
            ClientState.StoredClientPaymentChannels states = ClientState.StoredClientPaymentChannels.parseFrom(data);
            log.debug("deserializeWalletExtension read {} channels protobuf objects", states.getChannelsCount());

            for (ClientState.StoredClientPaymentChannel storedState : states.getChannelsList()) {
                Transaction refundTransaction = params.getDefaultSerializer().makeTransaction(storedState.getRefundTransaction().toByteArray());
                refundTransaction.getConfidence().setSource(TransactionConfidence.Source.SELF);
                ECKey myKey = (storedState.getMyKey().isEmpty()) ?
                        containingWallet.findKeyFromPubKey(storedState.getMyPublicKey().toByteArray()) :
                        ECKey.fromPrivate(storedState.getMyKey().toByteArray());
                ECKey serverKey = storedState.hasServerKey() ? ECKey.fromPublicOnly(storedState.getServerKey().toByteArray()) : null;
                StoredClientChannel channel = new StoredClientChannel(storedState.getMajorVersion(),
                        Sha256Hash.wrap(storedState.getId().toByteArray()),
                        params.getDefaultSerializer().makeTransaction(storedState.getContractTransaction().toByteArray()),
                        refundTransaction,
                        myKey,
                        serverKey,
                        Coin.valueOf(storedState.getValueToMe()),
                        Coin.valueOf(storedState.getRefundFees()),
                        storedState.getExpiryTime(),
                        false);
                if (storedState.hasCloseTransactionHash()) {
                    Sha256Hash closeTxHash = Sha256Hash.wrap(storedState.getCloseTransactionHash().toByteArray());
                    channel.close = containingWallet.getTransaction(closeTxHash);
                }
                putChannel(channel, false);
            }
        } finally {
            containingWallet.unlockLockAndThenWallet(lock);
        }
    }

    @Override
    public String toString() {
        lock();

        try {
            StringBuilder buf = new StringBuilder("Client payment channel states:\n");
            for (StoredClientChannel channel : mapChannels.values()) {
                channel.lock.lock();
                try {
                    buf.append("  ").append(channel).append("\n");
                } finally {
                    channel.lock.unlock();
                }
            }
            return buf.toString();
        } finally {
            unlock();
        }
    }

    private @Nullable NetworkParameters getNetworkParameters() {
        return this.containingWallet != null ? this.containingWallet.getNetworkParameters() : null;
    }

    /** Watch channels that are not active, i.e. has a connection and used by an PaymentChanelClientState object.
     * Responsible for detecting a close or a refund transaction confirmation pertaining to a channel.
     * */
    class PaymentChannelWatcher {
        private final Logger log = LoggerFactory.getLogger(PaymentChannelWatcher.class);
        private final TransactionConfidence.Listener confidenceListener;
        private final int eventHorizon = containingWallet.getContext().getEventHorizon();
        private final TimerTask expiryTask;
        private final WalletCoinsReceivedEventListener receivedCoinsListener;

        private PaymentChannelWatcher(final StoredClientChannel storedChannel) {
            final String contractHash = storedChannel.contract.getHashAsString();
            log.debug("PaymentChannelWatcher for contract {}", contractHash);

            confidenceListener = new TransactionConfidence.Listener() {
                public void onConfidenceChanged(final TransactionConfidence confidence, ChangeReason reason) {
                    final Sha256Hash txHash = confidence.getTransactionHash();
                    if (txHash == storedChannel.contract.getHash()) {
                        if (reason == ChangeReason.TYPE &&
                                confidence.getConfidenceType() == TransactionConfidence.ConfidenceType.DEAD) {
                            storedChannel.lock.lock();
                            try {
                                storedChannel.overridingTx = confidence.getOverridingTransaction();
                            } finally {
                                storedChannel.lock.unlock();
                            }
                            watchConfirmations(storedChannel.overridingTx, this, contractHash, "overriding");
                        }
                    } else if (confidence.getDepthInBlocks() >= eventHorizon) {
                        log.info("Deleting channel from wallet: {} when {} {} is safely confirmed, depth = {}",
                                contractHash, getLabel(txHash, storedChannel), txHash, confidence.getDepthInBlocks());
                        removeChannel(storedChannel);
                    }
                }
            };

            final TransactionConfidence contractConfidence = storedChannel.contract.getConfidence(containingWallet.getContext());
            if (contractConfidence.getConfidenceType() == TransactionConfidence.ConfidenceType.DEAD) {
                storedChannel.overridingTx = contractConfidence.getOverridingTransaction();
                watchConfirmations(storedChannel.overridingTx, confidenceListener, contractHash, "overriding");
            }
            // Register a listener that watches out for the server closing the channel.
            if (storedChannel.close != null) {
                log.debug("Channel contract {} has close transaction.", contractHash);
                watchConfirmations(storedChannel.close, confidenceListener, contractHash, "existing close");
            }
            if (storedChannel.expiryTimeSeconds() < Utils.currentTimeSeconds()) {
                log.debug("Channel contract {} has expired.", contractHash);
                watchConfirmations(storedChannel.refund, confidenceListener, contractHash, "refund");
            }

            Date expiryTime = new Date(storedChannel.expiryTimeSeconds() * 1000 + (System.currentTimeMillis() - Utils.currentTimeMillis()));
            log.debug("expiryTask for expiry at {} of channel {}", expiryTime, contractHash);
            expiryTask = new TimerTask() {
                @Override
                public void run() {
                    log.debug("expiryTask for contract {} - running at {}", contractHash, new Date(System.currentTimeMillis()));

                    lock();
                    storedChannel.lock.lock();

                    final TransactionBroadcaster peerGroup;
                    final Transaction contract;
                    final Transaction refund;
                    final Coin valueToMe;
                    final String refundStr;

                    try {
                        peerGroup = getAnnouncePeerGroup();
                        contract = storedChannel.contract;
                        refund = storedChannel.refund;
                        valueToMe = storedChannel.valueToMe;
                        refundStr = refund.getHashAsString();
                    } finally {
                        storedChannel.lock.unlock();
                        unlock();
                    }

                    if (valueToMe.equals(Coin.ZERO) && contractConfidence.getDepthInBlocks() >= eventHorizon) {
                        // TODO It should really be detected and removed after a Close transaction.
                        log.info("Channel {} with established contract exhausted (no value to client) and expired", contractHash);
                        removeChannel(storedChannel);
                    } else {
                        // Continue with the timer thread but holding no locks to wallet or extension as we need to talk to peerGroup (lock order is PeerGroup -> Wallet)
                        try {
                            // TODO does not seem to work by detecting spent...
                            if (contract.getOutput(0).isAvailableForSpending()) {
                                log.debug("TimerTask - contract {} expired but not settled...", contractHash);

                                if (contractConfidence.getDepthInBlocks() < eventHorizon) {
                                    TransactionBroadcast contractBroadcast = peerGroup.broadcastTransaction(contract);
                                    log.debug("TimerTask - broadcasting contract {}", contractHash);
                                    observeAndLogBroadcast(contractBroadcast, "contract");
                                }

                                // This task evaluates at expiry, so even if channel is closed, that was less than eventHorizon
                                // ago (as then this task would be removed). Send expiry unconditionally for safety.
                                log.debug("TimerTask - broadcasting refund {}", refundStr);
                                TransactionBroadcast refundBroadcast = peerGroup.broadcastTransaction(refund);
                                observeAndLogBroadcast(refundBroadcast, "refund");
                            } else {
                                log.debug("TimerTask - contract {} looks spent.", storedChannel.contract.getHashAsString());
                            }

                        } catch (Exception e) {
                            // Something went wrong closing the channel - we catch
                            // here or else we take down the whole Timer.
                            log.error("Auto-closing channel failed", e);
                        }
                    }
                }

            };

            channelTimeoutHandler.schedule(expiryTask, expiryTime);


            receivedCoinsListener = new WalletCoinsReceivedEventListener() {
                @Override
                public void onCoinsReceived(Wallet wallet, Transaction tx, Coin prevBalance, Coin newBalance) {
                    lock();
                    storedChannel.lock.lock();
                    try {
                        if (storedChannel.active) {
                            log.debug("onCoinsReceived {} isSettlementTransaction={} for contract {} - Channel is active, PaymentChannelClientState is responsible.",
                                    tx.getHash(), isSettlementTransaction(storedChannel.contract, tx), contractHash);
                        } else if (tx.getHash() == storedChannel.refund.getHash()) {
                            log.debug("onCoinsReceived refund tx {}", tx.getHash());
                            watchConfirmations(storedChannel.refund, confidenceListener, contractHash, "refund");
                        } else if (isSettlementTransaction(storedChannel.contract, tx)) {
                            log.info("onCoinsReceived settlement tx {} closed contract {}", tx.getHash(), contractHash);
                            // Record the fact that it was closed along with the transaction that closed it.
                            storedChannel.close = tx;
                            updatedChannel(storedChannel);
                            watchConfirmations(storedChannel.close, confidenceListener, contractHash, "detected close");
                        }
                    } finally {
                        storedChannel.lock.unlock();
                        unlock();
                    }
                }
            };

            containingWallet.addCoinsReceivedEventListener(Threading.USER_THREAD, receivedCoinsListener);
        }

        void removeListeners(final StoredClientChannel channel) {
            channel.lock.lock();
            try {
                boolean cancelledTimerTask = expiryTask.cancel();
                if (cancelledTimerTask) log.debug("Cancelled expiry timer for contract {}", channel.contract.getHash());

                removeListenerFromTx(channel.close);
                removeListenerFromTx(channel.refund);
                removeListenerFromTx(channel.contract);
                removeListenerFromTx(channel.overridingTx);

                boolean removed = containingWallet.removeCoinsReceivedEventListener(receivedCoinsListener);
                if (removed) log.debug("Removed receive coins listener for contract {}", channel.contract.getHash());
            } finally {
                channel.lock.unlock();
            }
        }

        private void removeListenerFromTx(Transaction tx) {
            final Context context = containingWallet.getContext();
            if (tx != null) {
                boolean removed = tx.getConfidence(context).removeEventListener(confidenceListener);
                if (removed) log.debug("Removed confidence listener from tx {}", tx.getHash());
            }
        }

        private String getLabel(final Sha256Hash txHash, final StoredClientChannel channel) {
            channel.lock.lock();
            try {
                return (channel.close != null) && channel.close.getHash() == txHash ? "close" :
                        (channel.refund != null) && channel.refund.getHash() == txHash ? "refund" :
                                (channel.overridingTx != null) && channel.overridingTx.getHash() == txHash ? "overriding contract" : "unknown";
            } finally {
                channel.lock.unlock();
            }
        }

        private void watchConfirmations(final Transaction finalTx, final TransactionConfidence.Listener listener, final String contract, final String label) {
            // When we see the transaction get enough confirmations, we can just delete the record
            // of this channel from the wallet, because we're not going to need any of that any more.
            final TransactionConfidence txConfidence = finalTx.getConfidence(containingWallet.getContext());
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

        /**
         * Returns true if the tx is a valid settlement transaction.
         */
        private boolean isSettlementTransaction(Transaction contract, Transaction tx) {
            try {
                tx.verify();
                tx.getInput(0).verify(contract.getOutput(0));
                return true;
            } catch (VerificationException e) {
                return false;
            }
        }

    }


}

/**
 * Represents the state of a channel once it has been opened in such a way that it can be stored and used to resume a
 * channel which was interrupted (eg on connection failure) or keep track of refund transactions which need broadcast
 * when they expire.
 */
class StoredClientChannel {
    final ReentrantLock lock = Threading.lock("StoredClientChannel");

    final int majorVersion;
    final Sha256Hash id;
    // The expiry time of the contract in protocol v2.
    final long expiryTime;
    final ECKey myKey;
    final ECKey serverKey;
    Transaction contract, refund;
    // The transaction that closed the channel (generated by the server)
    Transaction close;
    // The transaction that overrides the contract if it changes to DEAD.
    Transaction overridingTx;

    Coin valueToMe, refundFees;

    // In-memory flag to indicate intent to resume this channel (or that the channel is already in use)
    boolean active = false;
    StoredPaymentChannelClientStates.PaymentChannelWatcher watcher;

    StoredClientChannel(int majorVersion, Sha256Hash id, Transaction contract, Transaction refund, ECKey myKey, ECKey serverKey, Coin valueToMe,
                        Coin refundFees, long expiryTime, boolean active) {
        this.majorVersion = majorVersion;
        this.id = id;
        this.contract = contract;
        this.refund = refund;
        this.myKey = myKey;
        this.serverKey = serverKey;
        this.valueToMe = valueToMe;
        this.refundFees = refundFees;
        this.expiryTime = expiryTime;
        this.active = active;
    }

    long expiryTimeSeconds() {
        lock.lock();
        try {

            switch (majorVersion) {
                case 1:
                    return refund.getLockTime() + 60 * 5;
                case 2:
                    return expiryTime + 60 * 5;
                default:
                    throw new IllegalStateException("Invalid version");
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        lock.lock();
        try {
            final String newline = String.format(Locale.US, "%n");
            final String closeStr = close == null ? "still open" : close.toString().replaceAll(newline, newline + "   ");
            return String.format(Locale.US, "Stored client channel for server ID %s (%s)%n" +
                            "    Version:     %d%n" +
                            "    Key:         %s%n" +
                            "    Server key:  %s%n" +
                            "    Value left:  %s%n" +
                            "    Refund fees: %s%n" +
                            "    Expiry     : %s%n" +
                            "    Contract:  %s" +
                            "Refund:    %s" +
                            "Close:     %s",
                    id, active ? "active" : "inactive", majorVersion, myKey, serverKey, valueToMe, refundFees, expiryTime,
                    contract.toString().replaceAll(newline, newline + "    "),
                    refund.toString().replaceAll(newline, newline + "    "),
                    closeStr);
        } finally {
            lock.unlock();
        }
    }
}