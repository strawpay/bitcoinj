package org.bitcoinj.protocols.channels;

import org.bitcoinj.core.*;

/** Immutable view of a  known client channel read from a wallet.*/
public class ClientChannelStatusView {

    final public Sha256Hash id;
    final public Transaction contract, refund;
    final public long expiryTime;
    final public Transaction close;
    final public Coin valueToMe, refundFees;
    final public int contractDepth, closeDepth, refundDepth;
    final public boolean active;

    /**
     *  Reads the status from the stored channel, synchronizing on the stored channel instance.
     *  The depth status of tx:s is requested using the confidence table.
     *  */
    ClientChannelStatusView(StoredClientChannel storedChannel, TxConfidenceTable table) {
        synchronized (storedChannel) {
            this.id = storedChannel.id;
            this.contract = storedChannel.contract;
            this.expiryTime = storedChannel.expiryTime;
            this.close = storedChannel.close;
            this.refund = storedChannel.refund;
            this.valueToMe = storedChannel.valueToMe;
            this.refundFees = storedChannel.refundFees;
            this.active = storedChannel.active;
        }

        contractDepth = contract.getConfidence(table).getDepthInBlocks();
        closeDepth = close != null ? close.getConfidence(table).getDepthInBlocks() : 0;
        refundDepth = refund != null ? refund.getConfidence(table).getDepthInBlocks() : 0;
    }

}
