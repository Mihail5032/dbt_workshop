package ru.x5.model;

import java.io.Serializable;

/**
 * Lightweight wrapper for one transaction: txnKey + pre-serialized segments as byte[].
 *
 * Passed through keyBy: Flink serializes String + byte[] natively (PojoSerializer),
 * without falling back to Kryo — this is the key performance optimization.
 *
 * Binary format of payload is defined by IdocParser (serialization) and
 * TransactionProcessor (deserialization).
 */
public class TransactionBundle implements Serializable {
    private static final long serialVersionUID = 3L;

    /** Transaction key: retailStoreId|businessDayDate|workstationId|transactionSequenceNumber */
    private String txnKey;

    /** Pre-serialized segments (RAW + PST RowData) in compact binary format */
    private byte[] payload;

    public TransactionBundle() {}

    public TransactionBundle(String txnKey, byte[] payload) {
        this.txnKey = txnKey;
        this.payload = payload;
    }

    public String getTxnKey() { return txnKey; }
    public void setTxnKey(String txnKey) { this.txnKey = txnKey; }

    public byte[] getPayload() { return payload; }
    public void setPayload(byte[] payload) { this.payload = payload; }
}