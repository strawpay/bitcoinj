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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
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
    }

    /**
     * Creates a new StoredPaymentChannelClientStates and associates it with the given {@link Wallet}
     *
     * Use this constructor if you use WalletAppKit, it will provide the broadcaster for you (no need to call the setter)
     */
    public StoredPaymentChannelClientStates(@Nullable Wallet containingWallet) {
        log.debug("Constructed without TransactionBroadcaster");
        this.containingWallet = containingWallet;
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
                if (channel.close != null || nowSeconds - channel.expiryTimeSeconds() >= marginSeconds) continue;
                balance = balance.add(channel.valueToMe);
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
                if (channel.expiryTimeSeconds() > nowSeconds)
                    earliestTime = Math.min(earliestTime, (int) channel.expiryTimeSeconds());
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
                if (channel.contract.getHash().equals(contractHash))
                    return channel;
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
                    channels.add(new ClientChannelStatusView(storedChannel, confidenceTable));
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
        try {
            log.debug("putChannel {}", channel.contract.getHashAsString());
            mapChannels.put(channel.id, channel);
        } catch (Exception e) {
            log.error("putChannel failed", e);
        } finally {
            unlock();
        }

        // Watch this channel for various events, once the broadcaster is ready..
        Runnable initiateWatchingChannel = new Runnable() {
            @Override
            public void run() {
                lock();
                try {
                    // Expiry timer
                    installExpiryTimer(channel);
                    // Transaction Watcher
                    watchForCloseOrRefundTx(channel);
                } finally {
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
                        log.debug("{} {} broadcasted.", txLabel, f.get().getHashAsString());
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
                // First a few asserts to make sure things won't break
                checkState(channel.valueToMe.signum() >= 0 &&
                        (!hasMaxMoney || channel.valueToMe.compareTo(networkMaxMoney) <= 0));
                checkState(channel.refundFees.signum() >= 0 &&
                        (!hasMaxMoney || channel.refundFees.compareTo(networkMaxMoney) <= 0));
                checkNotNull(channel.myKey.getPubKey());
                log.debug("channel.refund.getConfidence().getSource() = {}", channel.refund.getConfidence().getSource().toString());
                if (channel.refund.getConfidence().getSource() != TransactionConfidence.Source.SELF) {
                    log.debug("channel.refund.getConfidence().getSource() != TransactionConfidence.Source.SELF");
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
            for (StoredClientChannel channel : mapChannels.values())
                buf.append("  ").append(channel).append("\n");
            return buf.toString();
        } finally {
            unlock();
        }
    }

    private @Nullable NetworkParameters getNetworkParameters() {
        return this.containingWallet != null ? this.containingWallet.getNetworkParameters() : null;
    }


    private void installExpiryTimer(final StoredClientChannel storedChannel) {
        Date expiryTime = new Date(storedChannel.expiryTimeSeconds() * 1000 + (System.currentTimeMillis() - Utils.currentTimeMillis()));
        final String channelStr = storedChannel.contract.getHashAsString();
        log.debug("installExpiryTimer for expiry at {} of channel {}", expiryTime, channelStr);
        channelTimeoutHandler.schedule(new TimerTask() {
            @Override
            public void run() {
                lock();

                final TransactionBroadcaster peerGroup;
                final Transaction contract;
                final String contractStr;
                final Transaction refund;
                final String refundStr;

                try {
                    log.debug("TimerTask - running at {}", new Date(System.currentTimeMillis()));
                    log.debug("TimerTask - Synchronizing on storedChannel {}", channelStr);
                    log.debug("TimerTask - Locked storedChannel {}", channelStr);

                    peerGroup = getAnnouncePeerGroup();
                    contract = storedChannel.contract;
                    contractStr = contract.getHashAsString();
                    refund = storedChannel.refund;
                    refundStr = refund.getHashAsString();
                } finally {
                    unlock();
                }

                // Continue with the timer thread but holding no locks to wallet or extension as we need to talk to peerGroup (lock order is PeerGroup -> Wallet)
                try {
                    // TODO does not seem to work by detecting spent...
                    if (contract.getOutput(0).isAvailableForSpending()) {
                        log.debug("TimerTask - contract {} expired but not settled...", contractStr);

                        TransactionBroadcast contractBroadcast = peerGroup.broadcastTransaction(contract);
                        log.debug("TimerTask - broadcasting contract {}", contractStr);
                        observeAndLogBroadcast(contractBroadcast, "contract");

                        log.debug("TimerTask - broadcasting refund {}", refundStr);
                        try {
                            containingWallet.receivePending(refund, null);
                        } catch (VerificationException e) {
                            throw new RuntimeException(e);   // Cannot fail to verify a tx we created ourselves.
                        }
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

        }, expiryTime);
    }

    private void watchForCloseOrRefundTx(StoredClientChannel storedChannel) {
        new PaymentChannelWatcher(storedChannel);
    }

    /** Watch channels that are not active, i.e. has a connection and used by an PaymentChanelClientState object.
     * Responsible for detecting a close or a refund transaction confirmation pertaining to a channel.
     * */
    class PaymentChannelWatcher {
        private final Logger log = LoggerFactory.getLogger(PaymentChannelWatcher.class);

        final private StoredClientChannel storedChannel;

        private Transaction contract() { return storedChannel.contract; }

        private PaymentChannelWatcher(StoredClientChannel storedChannel) {
            this.storedChannel = storedChannel;
            log.debug("PaymentChannelWatcher for contract {}", contract().getHashAsString());
            init();
        }

        private void init() {
            // Register a listener that watches out for the server closing the channel.
            if (storedChannel.close != null) {
                log.debug("Channel contract {} has close transaction.", contract().getHashAsString());
                watchTxConfirmations(storedChannel.close);
            }
            if (storedChannel.expiryTimeSeconds() < Utils.currentTimeSeconds()) {
                log.debug("Channel contract {} has expired.", contract().getHashAsString());
                watchTxConfirmations(storedChannel.refund);
            }

            containingWallet.addCoinsReceivedEventListener(Threading.USER_THREAD, new WalletCoinsReceivedEventListener() {
                @Override
                public void onCoinsReceived(Wallet wallet, Transaction tx, Coin prevBalance, Coin newBalance) {
                    lock();
                    try {
                        if (storedChannel.active) {
                            log.debug("Channel is active, PaymentChannelClientState is responsible.");
                        } else if (tx.getHash() == storedChannel.refund.getHash()) {
                            log.debug("onCoinsReceived refund tx {}", tx.getHash());
                            watchTxConfirmations(tx);
                        } else if (isSettlementTransaction(contract(), tx)) {
                            log.info("onCoinsReceived settlement tx {} closed contract {}", tx.getHash(), contract().getHashAsString());
                            // Record the fact that it was closed along with the transaction that closed it.
                            storedChannel.close = tx;
                            updatedChannel(storedChannel);
                            watchTxConfirmations(tx);
                        }
                    } finally {
                        unlock();
                    }
                }
            });
        }


        private void watchTxConfirmations(final Transaction finalTx) {
            // When we see the transaction get enough confirmations, we can just delete the record
            // of this channel from the wallet, because we're not going to need any of that any more.
            final TransactionConfidence confidence = finalTx.getConfidence();
            int numConfirms = containingWallet.getContext().getEventHorizon();
            log.info("Watching contract {} settling with {} of depth = {}, waiting for depth {}",
                    contract().getHashAsString(), finalTx.getHashAsString(),
                    confidence.getDepthInBlocks(), numConfirms);

            ListenableFuture<TransactionConfidence> future = confidence.getDepthFuture(numConfirms, Threading.USER_THREAD);
            Futures.addCallback(future, new FutureCallback<TransactionConfidence>() {
                @Override
                public void onSuccess(TransactionConfidence result) {
                    lock();
                    try {
                        log.info("Deleting channel from wallet: {} when tx {} is safely confirmed, depth = {}",
                                contract().getHashAsString(), finalTx.getHashAsString(), result.getDepthInBlocks());
                        removeChannel(storedChannel);
                    } finally {
                        unlock();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    Throwables.propagate(t);
                }
            });
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
    int majorVersion;
    Sha256Hash id;
    Transaction contract, refund;
    // The expiry time of the contract in protocol v2.
    long expiryTime;
    // The transaction that closed the channel (generated by the server)
    Transaction close;
    ECKey myKey;
    ECKey serverKey;
    Coin valueToMe, refundFees;

    // In-memory flag to indicate intent to resume this channel (or that the channel is already in use)
    boolean active = false;

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
        switch (majorVersion) {
            case 1:
                return refund.getLockTime() + 60 * 5;
            case 2:
                return expiryTime + 60 * 5;
            default:
                throw new IllegalStateException("Invalid version");
        }
    }

    @Override
    public String toString() {
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
    }
}