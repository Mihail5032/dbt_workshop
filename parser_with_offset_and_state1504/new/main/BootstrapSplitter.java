package ru.x5.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import ru.x5.model.BootstrapEvent;
import ru.x5.model.TransactionBundle;

/**
 * Разделяет поток BootstrapEvent:
 *   KEY → main output как TransactionBundle(txnKey, null)
 *         (идёт через keyBy(txnKey) в TransactionProcessor вместе с Kafka-bundle-ами)
 *   END → side output (потом broadcast-ится всем subtask-ам TransactionProcessor)
 *
 * TransactionBundle с payload == null — общепринятый маркер bootstrap-ключа.
 */
public class BootstrapSplitter extends ProcessFunction<BootstrapEvent, TransactionBundle> {

    public static final OutputTag<Boolean> END_TAG =
            new OutputTag<Boolean>("bootstrap-end") {};

    @Override
    public void processElement(BootstrapEvent e, Context ctx, Collector<TransactionBundle> out) {
        if (e == null || e.getKind() == null) return;
        switch (e.getKind()) {
            case KEY:
                if (e.getTxnKey() != null && !e.getTxnKey().isEmpty()) {
                    out.collect(new TransactionBundle(e.getTxnKey(), null));
                }
                break;
            case END:
                ctx.output(END_TAG, Boolean.TRUE);
                break;
        }
    }
}
