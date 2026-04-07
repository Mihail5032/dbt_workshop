package ru.x5.model;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Обёртка для одной транзакции: содержит все сегменты (RetailLineItem, Tender, и т.д.)
 * с одинаковым ключом rtl_txn_rk.
 *
 * Передаётся между операторами: IdocParser → keyBy(txnKey) → TransactionProcessor.
 */
public class TransactionBundle implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Ключ транзакции: retailStoreId|businessDayDate|workstationId|transactionSequenceNumber */
    private String txnKey;

    /** Все сегменты этой транзакции (Transaction, RetailLineItem, Tender, и т.д.) */
    private List<BaseTransactionKey> segments;

    /** Время обработки IDOC */
    private LocalDateTime timestamp;

    /** Дата обработки IDOC */
    private LocalDate date;

    public TransactionBundle() {}

    public TransactionBundle(String txnKey, List<BaseTransactionKey> segments,
                             LocalDateTime timestamp, LocalDate date) {
        this.txnKey = txnKey;
        this.segments = segments;
        this.timestamp = timestamp;
        this.date = date;
    }

    public String getTxnKey() { return txnKey; }
    public void setTxnKey(String txnKey) { this.txnKey = txnKey; }

    public List<BaseTransactionKey> getSegments() { return segments; }
    public void setSegments(List<BaseTransactionKey> segments) { this.segments = segments; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    public LocalDate getDate() { return date; }
    public void setDate(LocalDate date) { this.date = date; }
}
