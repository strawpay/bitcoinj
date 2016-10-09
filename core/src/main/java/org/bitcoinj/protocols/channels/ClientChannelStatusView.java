package org.bitcoinj.protocols.channels;

import org.bitcoinj.core.Coin;
import org.bitcoinj.core.Sha256Hash;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.core.TxConfidenceTable;

/** Immutable view of a  known client channel read from a wallet.*/
public class ClientChannelStatusView {

    final public Sha256Hash id;
    final public Transaction contract, refund;
    final public long expiryTime;
    final public Transaction close;
    final public Coin valueToMe, refundFees;
    final public int contractDepth, closeDepth, refundDepth;
    final public boolean active;
    final public boolean usable;
    final public boolean expired;

    /**
     *  Reads the status from the stored channel, synchronizing on the stored channel instance.
     *  The depth status of tx:s is requested using the confidence table.
     *  */
    ClientChannelStatusView(StoredClientChannel storedChannel, boolean usable, boolean expired, TxConfidenceTable table) {
        this.id = storedChannel.id;
        this.contract = storedChannel.contract;
        this.expiryTime = storedChannel.expiryTime;
        this.close = storedChannel.close;
        this.refund = storedChannel.refund;
        this.valueToMe = storedChannel.valueToMe;
        this.refundFees = storedChannel.refundFees;
        this.active = storedChannel.active;
        this.usable = usable;
        this.expired = expired;

        contractDepth = contract.getConfidence(table).getDepthInBlocks();
        closeDepth = close != null ? close.getConfidence(table).getDepthInBlocks() : 0;
        refundDepth = refund != null ? refund.getConfidence(table).getDepthInBlocks() : 0;
    }

}
