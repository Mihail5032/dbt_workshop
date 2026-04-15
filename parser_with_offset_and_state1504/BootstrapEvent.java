package ru.x5.model;

import java.io.Serializable;

/**
 * Событие из bootstrap-источника (Iceberg).
 *
 * KEY  — один уже записанный txnKey (из raw_bptransaction за окно windowDays)
 * END  — маркер «bootstrap source прочитал всё», эмитится ровно один раз в конце run()
 *
 * Разделение на два типа нужно, т.к. из одного SourceFunction мы эмитим
 * и ключи (идут keyed в TransactionProcessor), и сигнал END (broadcast во все subtask-и).
 * Собственно разделение делает BootstrapSplitter через side output.
 */
public class BootstrapEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Kind { KEY, END }

    private Kind kind;
    private String txnKey;   // только для KEY

    public BootstrapEvent() {}

    public static BootstrapEvent key(String txnKey) {
        BootstrapEvent e = new BootstrapEvent();
        e.kind = Kind.KEY;
        e.txnKey = txnKey;
        return e;
    }

    public static BootstrapEvent end() {
        BootstrapEvent e = new BootstrapEvent();
        e.kind = Kind.END;
        return e;
    }

    public Kind getKind() { return kind; }
    public void setKind(Kind kind) { this.kind = kind; }
    public String getTxnKey() { return txnKey; }
    public void setTxnKey(String txnKey) { this.txnKey = txnKey; }
}
