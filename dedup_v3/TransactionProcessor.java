package ru.x5.process;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.x5.model.*;

import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

/**
 * Второй оператор пайплайна: принимает TransactionBundle (keyed по rtl_txn_rk),
 * дедуплицирует по ValueState с TTL 72ч,
 * и эмитит все RAW и PST сегменты через side outputs.
 *
 * Один хеш на транзакцию покрывает ВСЕ сегменты разом.
 * Не нужны отдельные KeySelector-ы для каждого типа сегмента.
 */
public class TransactionProcessor extends KeyedProcessFunction<String, TransactionBundle, RowData> {

    private static final Logger log = LogManager.getLogger("ru.x5.parser");
    private static final long TTL_HOURS = 72L;
    private static final long LOG_INTERVAL = 100_000L;

    private final Map<String, Schema> icebergSchemas;
    private final Map<String, Schema> pstSchemas;

    private transient ValueState<Boolean> seenState;
    private transient long totalCount = 0;
    private transient long duplicateCount = 0;

    public TransactionProcessor(Map<String, Schema> icebergSchemas, Map<String, Schema> pstSchemas) {
        this.icebergSchemas = icebergSchemas;
        this.pstSchemas = pstSchemas;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(TTL_HOURS))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("seen-txn", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(descriptor);

        log.info("TXN_PROCESSOR_INIT: started, TTL={}h, subtask={}",
                TTL_HOURS, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    @Override
    public void processElement(TransactionBundle bundle, Context ctx, Collector<RowData> out) throws Exception {
        totalCount++;

        // ======================== ДЕДУПЛИКАЦИЯ ========================
        if (seenState.value() != null) {
            duplicateCount++;
            if (duplicateCount <= 10 || duplicateCount % 1000 == 0) {
                log.warn("TXN_DEDUP_DROP: key={}, total_dupes={}, subtask={}",
                        bundle.getTxnKey(), duplicateCount,
                        getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            }
            return; // пропускаем ВСЮ транзакцию со всеми сегментами
        }
        seenState.update(true);

        // ======================== ЭМИТ СЕГМЕНТОВ ========================
        TimestampData timestampDataXml = TimestampData.fromLocalDateTime(bundle.getTimestamp());
        LocalDate dateXml = bundle.getDate();
        List<BaseTransactionKey> segments = bundle.getSegments();

        // Вычисляем per-transaction флаги
        Set<String> autoMDSet = getAutoMDSet(segments);
        Set<String> emptyEanSet = getEmptyEanSet(segments);
        boolean isCertParty = checkIsCertParty(segments);
        Map<String, String> noteFieldValueMap = buildNoteFieldValueMap(segments);

        for (BaseTransactionKey btk : segments) {
            try {
                // RAW: конвертируем в RowData и эмитим в side output
                String segmentName = btk.getSegmentName();
                Schema icebergSchema = icebergSchemas.get(segmentName);
                if (icebergSchema == null) continue;

                RowData rowData = btk.toRowData(icebergSchema, timestampDataXml, dateXml);
                OutputTag<RowData> tag = StreamSideOutputTag.getTag(segmentName);
                if (tag != null) {
                    ctx.output(tag, rowData);
                }

                // PST: применяем бизнес-логику и эмитим в PST side output
                emitPst(btk, rowData, icebergSchema, ctx, timestampDataXml, dateXml,
                        isCertParty, autoMDSet, emptyEanSet, noteFieldValueMap);

            } catch (Exception e) {
                log.error("Error processing segment " + btk.getSegmentName()
                        + " for txn " + btk.getTransactionKey(), e);
            }
        }

        // Синтетические тендеры
        try {
            processTenderSynthetic(ctx, segments, timestampDataXml, dateXml);
        } catch (Exception e) {
            log.error("Error processing synthetic tender pst ", e);
        }

        // Периодическая статистика
        if (totalCount % LOG_INTERVAL == 0) {
            log.info("TXN_PROCESSOR_STATS: total={}, duplicates={}, passed={}, subtask={}",
                    totalCount, duplicateCount, totalCount - duplicateCount,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }

    // ======================== PST-логика ========================

    private void emitPst(BaseTransactionKey btk, RowData rowData, Schema icebergSchema,
                         Context ctx, TimestampData timestampDataXml, LocalDate dateXml,
                         boolean isCertParty, Set<String> autoMDSet, Set<String> emptyEanSet,
                         Map<String, String> noteFieldValueMap) {

        if (btk instanceof Transaction) {
            Schema pstSchema = pstSchemas.get("PST_BPTRANSACTION");
            if (pstSchema != null) {
                Transaction transaction = (Transaction) btk;
                RowData pstRowData = transaction.toRowDataPst(rowData, icebergSchema, pstSchema);
                ctx.output(StreamSideOutputTag.getTag("PST_BPTRANSACTION"), pstRowData);
            }
        }

        if (btk instanceof FinancialMovemen) {
            Schema pstSchema = pstSchemas.get("PST_BPFINANCIALMOVEMEN");
            if (pstSchema != null) {
                FinancialMovemen fm = (FinancialMovemen) btk;
                String txnKey = buildTxnKey(fm);
                String noteFieldValue = noteFieldValueMap.get(txnKey);
                RowData pstRowData = fm.toRowDataPst(rowData, icebergSchema, pstSchema, noteFieldValue);
                ctx.output(StreamSideOutputTag.getTag("PST_BPFINANCIALMOVEMEN"), pstRowData);
            }
        }

        if (btk instanceof TransactionExtension) {
            Schema pstSchema = pstSchemas.get("PST_BPTRANSACTEXTENSIO");
            if (pstSchema != null) {
                TransactionExtension ext = (TransactionExtension) btk;
                RowData pstRowData = ext.toRowDataPst(rowData, icebergSchema, pstSchema);
                ctx.output(StreamSideOutputTag.getTag("PST_BPTRANSACTEXTENSIO"), pstRowData);
            }
        }

        if (btk instanceof FinacialMovement) {
            Schema pstSchema = pstSchemas.get("PST_BPFINANCIALMOVEMENTEXTENSIO");
            if (pstSchema != null) {
                FinacialMovement fmt = (FinacialMovement) btk;
                RowData pstRowData = fmt.toRowDataPst(rowData, icebergSchema, pstSchema);
                ctx.output(StreamSideOutputTag.getTag("PST_BPFINANCIALMOVEMENTEXTENSIO"), pstRowData);
            }
        }

        if (btk instanceof RetailLineItem) {
            Schema pstSchema = pstSchemas.get("PST_BPRETAILLINEITEM");
            if (pstSchema != null) {
                RetailLineItem retailLineItem = (RetailLineItem) btk;
                if (retailLineItem.getHasEan() == null || Boolean.TRUE.equals(retailLineItem.getHasEan())) {
                    String key = retailLineItem.createKeyLineItem();
                    RowData pstRowData = retailLineItem.toRowDataPst(pstSchema, timestampDataXml, dateXml,
                            autoMDSet.contains(key));
                    ctx.output(StreamSideOutputTag.getTag("PST_BPRETAILLINEITEM"), pstRowData);
                }
            }
        }

        if (btk instanceof LineItemExtension) {
            Schema pstSchema = pstSchemas.get("PST_BPLINEITEMEXTENSIO");
            if (pstSchema != null) {
                LineItemExtension lineItemExtension = (LineItemExtension) btk;
                String key = lineItemExtension.createKeyLineItem();
                if (!emptyEanSet.contains(key)) {
                    RowData pstRowData = lineItemExtension.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                    ctx.output(StreamSideOutputTag.getTag("PST_BPLINEITEMEXTENSIO"), pstRowData);
                }
            }
        }

        if (btk instanceof LineItemDiscount) {
            Schema pstSchema = pstSchemas.get("PST_BPLINEITEMDISCOUNT");
            if (pstSchema != null) {
                LineItemDiscount lineItemDiscount = (LineItemDiscount) btk;
                String key = lineItemDiscount.createKeyLineItem();
                if (!emptyEanSet.contains(key)) {
                    RowData pstRowData = lineItemDiscount.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                    ctx.output(StreamSideOutputTag.getTag("PST_BPLINEITEMDISCOUNT"), pstRowData);
                }
            }
        }

        if (btk instanceof LineItemDiscExt) {
            Schema pstSchema = pstSchemas.get("PST_BPLINEITEMDISCEXT");
            if (pstSchema != null) {
                LineItemDiscExt lineItemDiscExt = (LineItemDiscExt) btk;
                String key = lineItemDiscExt.createKeyLineItem();
                if (!emptyEanSet.contains(key)) {
                    RowData pstRowData = lineItemDiscExt.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                    ctx.output(StreamSideOutputTag.getTag("PST_BPLINEITEMDISCEXT"), pstRowData);
                }
            }
        }

        if (btk instanceof TransactDiscExt) {
            Schema pstSchema = pstSchemas.get("PST_BPTRANSACTDISCEXT");
            if (pstSchema != null) {
                TransactDiscExt transactDiscExt = (TransactDiscExt) btk;
                RowData pstRowData = transactDiscExt.toRowData(pstSchema, timestampDataXml, dateXml);
                ctx.output(StreamSideOutputTag.getTag("PST_BPTRANSACTDISCEXT"), pstRowData);
            }
        }

        if (btk instanceof Tender) {
            Schema pstSchema = pstSchemas.get("PST_BPTENDER");
            if (pstSchema != null) {
                Tender tender = (Tender) btk;
                RowData pstRowData = tender.toRowDataPst(rowData, icebergSchema, pstSchema, isCertParty);
                ctx.output(StreamSideOutputTag.getTag("PST_BPTENDER"), pstRowData);
            }
        }

        if (btk instanceof TenderExtension) {
            Schema pstSchema = pstSchemas.get("PST_BPTENDEREXTENSIONS");
            if (pstSchema != null) {
                TenderExtension tenderExt = (TenderExtension) btk;
                RowData pstRowData = tenderExt.toRowDataPst(rowData, icebergSchema, pstSchema);
                ctx.output(StreamSideOutputTag.getTag("PST_BPTENDEREXTENSIONS"), pstRowData);
            }
        }
    }

    // ======================== Синтетические тендеры ========================

    private void processTenderSynthetic(Context ctx, List<BaseTransactionKey> segments,
                                         TimestampData timestampDataXml, LocalDate dateXml) {
        Schema pstSchema = pstSchemas.get("PST_BPTENDER");
        if (pstSchema == null) return;

        TenderPstProcessor tenderPstProcessor = new TenderPstProcessor(segments);
        List<Tender> syntheticTenders = tenderPstProcessor.prepareTenderPst();

        Set<String> writtenSyntheticKeys = new HashSet<>();
        for (Tender tender : syntheticTenders) {
            if ("3101".equals(tender.getTenderTypeCode())) {
                String synKey = tender.getRetailStoreId() + "|" + tender.getBusinessDayDate() + "|"
                        + tender.getWorkstationId() + "|" + tender.getTransactionSequenceNumber()
                        + "|" + tender.getTenderAmount();
                if (writtenSyntheticKeys.contains(synKey)) {
                    continue;
                }
                writtenSyntheticKeys.add(synKey);
                Schema rawSchema = icebergSchemas.get("E1BPTENDER");
                if (rawSchema != null) {
                    RowData rawRowData = tender.toRowData(rawSchema, timestampDataXml, dateXml);
                    RowData pstRowData = tender.toRowDataPst(rawRowData, rawSchema, pstSchema, false);
                    ctx.output(StreamSideOutputTag.getTag("PST_BPTENDER"), pstRowData);
                }
            }
        }
    }

    // ======================== Вычисление per-transaction флагов ========================

    private Set<String> getAutoMDSet(List<BaseTransactionKey> segments) {
        Set<String> autoMDSet = new HashSet<>();
        for (BaseTransactionKey key : segments) {
            if (key instanceof LineItemExtension) {
                LineItemExtension ext = (LineItemExtension) key;
                if (ext.getFieldGroup() != null && ext.getFieldGroup().equals("POS")
                        && ext.getFieldName() != null && ext.getFieldName().equals("AUTO_MD")
                        && ext.getFieldValue() != null && ext.getFieldValue().equals("Y")) {
                    autoMDSet.add(ext.createKeyLineItem());
                }
            }
        }
        return autoMDSet;
    }

    private Set<String> getEmptyEanSet(List<BaseTransactionKey> segments) {
        Set<String> emptyEanSet = new HashSet<>();
        for (BaseTransactionKey btk : segments) {
            if (btk instanceof RetailLineItem) {
                RetailLineItem rli = (RetailLineItem) btk;
                if (rli.getMatnrPadded() != null &&
                        Objects.equals(rli.getLfnum(), "00001") &&
                        (rli.getEan11() == null || rli.getEan11().isEmpty())) {
                    emptyEanSet.add(rli.createKeyLineItem());
                }
            }
        }
        return emptyEanSet;
    }

    private boolean checkIsCertParty(List<BaseTransactionKey> segments) {
        return segments.stream()
                .filter(x -> x instanceof TenderExtension)
                .map(x -> (TenderExtension) x)
                .anyMatch(x -> x.getFieldName() != null && x.getFieldName().contains("CERT_PARTY")
                        && x.getFieldValue() != null && x.getFieldValue().contains("RU02"));
    }

    private Map<String, String> buildNoteFieldValueMap(List<BaseTransactionKey> segments) {
        Map<String, String> map = new HashMap<>();
        for (BaseTransactionKey btk : segments) {
            if (btk instanceof TransactionExtension) {
                TransactionExtension ext = (TransactionExtension) btk;
                if (ext.getFieldName() != null && ext.getFieldName().contains("DOCNUMBER")) {
                    map.put(buildTxnKey(ext), ext.getFieldValue());
                }
            }
        }
        return map;
    }

    private String buildTxnKey(BaseTransactionKey btk) {
        return String.join("|",
                btk.getRetailStoreId() != null ? btk.getRetailStoreId() : "",
                btk.getBusinessDayDate() != null ? btk.getBusinessDayDate() : "",
                btk.getWorkstationId() != null ? btk.getWorkstationId() : "",
                btk.getTransactionSequenceNumber() != null ? btk.getTransactionSequenceNumber() : "");
    }
}
