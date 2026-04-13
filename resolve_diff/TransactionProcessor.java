package ru.x5.process;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.x5.model.TransactionBundle;

import java.time.Duration;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Second pipeline operator: receives TransactionBundle (keyed by rtl_txn_rk),
 * deduplicates via ValueState with 72h TTL,
 * deserializes pre-built RowData from byte[] payload,
 * and forwards to side outputs.
 *
 * Binary format of payload matches IdocParser.serializeEntries().
 */
public class TransactionProcessor extends KeyedProcessFunction<String, TransactionBundle, RowData> {

    private static final Logger log = LogManager.getLogger("ru.x5.parser");
    private static final long TTL_HOURS = 72L;
    private static final long LOG_INTERVAL = 100_000L;

    private final Map<String, Schema> icebergSchemas;
    private final Map<String, Schema> pstSchemas;
    private final CatalogLoader catalogLoader;
    private final TableIdentifier txnTableId;
    private final boolean recoveryPreload;

    private transient ValueState<Boolean> seenState;
    private transient long totalCount = 0;
    private transient long duplicateCount = 0;
    private transient long preloadDropCount = 0;

    // RowData deserializers matching IdocParser's serializers
    private transient Map<String, TypeSerializer<RowData>> rawSerializers;
    private transient Map<String, TypeSerializer<RowData>> pstSerializers;

    // Recovery: предзагруженные ключи из Iceberg для дедупликации без чекпоинта
    private transient Set<String> preloadedKeys;
    private transient long firstElementTs;

    public TransactionProcessor(Map<String, Schema> icebergSchemas, Map<String, Schema> pstSchemas) {
        this(icebergSchemas, pstSchemas, null, null, false);
    }

    public TransactionProcessor(Map<String, Schema> icebergSchemas, Map<String, Schema> pstSchemas,
                                CatalogLoader catalogLoader, TableIdentifier txnTableId,
                                boolean recoveryPreload) {
        this.icebergSchemas = icebergSchemas;
        this.pstSchemas = pstSchemas;
        this.catalogLoader = catalogLoader;
        this.txnTableId = txnTableId;
        this.recoveryPreload = recoveryPreload;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
        // === ValueState with TTL for dedup ===
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(TTL_HOURS))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10, true)
                .build();

        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("seen-txn", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(descriptor);

        // === Create RowData serializers from Iceberg schemas ===
        this.rawSerializers = new HashMap<>();
        for (Map.Entry<String, Schema> entry : icebergSchemas.entrySet()) {
            rawSerializers.put(entry.getKey(), createSerializer(entry.getValue()));
        }
        this.pstSerializers = new HashMap<>();
        for (Map.Entry<String, Schema> entry : pstSchemas.entrySet()) {
            pstSerializers.put(entry.getKey(), createSerializer(entry.getValue()));
        }

        // === Recovery: предзагрузка ключей из Iceberg ===
        if (recoveryPreload && catalogLoader != null && txnTableId != null) {
            LocalDate cutoff = LocalDate.now().minusDays(3);
            String cutoffStr = cutoff.toString(); // yyyy-MM-dd

            try (TableLoader loader = TableLoader.fromCatalog(catalogLoader, txnTableId)) {
                loader.open();
                Table table = loader.loadTable();
                preloadedKeys = new HashSet<>();
                try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                        .select("retailstoreid", "businessdaydate", "workstationid", "transactionsequencenumber")
                        .where(Expressions.greaterThanOrEqual("businessdaydate", cutoffStr))
                        .build()) {
                    for (Record record : records) {
                        String key = String.join("|",
                                nullSafe(record.getField("retailstoreid")),
                                nullSafe(record.getField("businessdaydate")),
                                nullSafe(record.getField("workstationid")),
                                nullSafe(record.getField("transactionsequencenumber")));
                        preloadedKeys.add(key);
                    }
                }
            } catch (Exception e) {
                log.warn("RECOVERY_PRELOAD: failed to load keys from Iceberg, subtask={}: {}",
                        getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), e.getMessage());
                preloadedKeys = null;
            }

            if (preloadedKeys != null) {
                log.info("RECOVERY_PRELOAD: loaded {} keys from Iceberg (cutoff={}), subtask={}",
                        preloadedKeys.size(), cutoffStr,
                        getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            }
        }

        log.info("TXN_PROCESSOR_INIT: started, TTL={}h, recoveryPreload={}, subtask={}",
                TTL_HOURS, recoveryPreload,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    private static String nullSafe(Object value) {
        return value != null ? value.toString() : "";
    }

    @SuppressWarnings("unchecked")
    private TypeSerializer<RowData> createSerializer(Schema icebergSchema) {
        RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
        return (TypeSerializer<RowData>) (TypeSerializer<?>)
                new org.apache.flink.table.runtime.typeutils.RowDataSerializer(rowType);
    }

    @Override
    public void processElement(TransactionBundle bundle, Context ctx, Collector<RowData> out) throws Exception {
        totalCount++;

        // ======================== DEDUP ========================
        if (seenState.value() != null) {
            duplicateCount++;
            log.info("TXN_DEDUP_DROP: key={}, total_dupes={}, subtask={}",
                    bundle.getTxnKey(), duplicateCount,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            return;
        }

        // Recovery: проверка предзагруженных ключей из Iceberg
        if (preloadedKeys != null && preloadedKeys.contains(bundle.getTxnKey())) {
            seenState.update(true);
            preloadedKeys.remove(bundle.getTxnKey());
            preloadDropCount++;
            log.info("TXN_DEDUP_PRELOAD_DROP: key={}, preload_drops={}, remaining={}, subtask={}",
                    bundle.getTxnKey(), preloadDropCount, preloadedKeys.size(),
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            return;
        }

        seenState.update(true);

        // ======================== DESERIALIZE & FORWARD ========================
        deserializeAndEmit(bundle.getPayload(), ctx);

        // Recovery: очистка preloadedKeys после прогрева (72ч)
        if (preloadedKeys != null) {
            if (firstElementTs == 0) firstElementTs = System.currentTimeMillis();
            if (System.currentTimeMillis() - firstElementTs > TTL_HOURS * 3_600_000L) {
                log.info("RECOVERY_PRELOAD_CLEANUP: clearing {} remaining keys, preload_drops={}, subtask={}",
                        preloadedKeys.size(), preloadDropCount,
                        getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
                preloadedKeys = null;
            }
        }

        // Periodic stats
        if (totalCount % LOG_INTERVAL == 0) {
            log.info("TXN_PROCESSOR_STATS: total={}, duplicates={}, preload_drops={}, passed={}, subtask={}",
                    totalCount, duplicateCount, preloadDropCount,
                    totalCount - duplicateCount - preloadDropCount,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }

    /**
     * Deserializes byte[] payload (binary format from IdocParser) and emits
     * each segment's RowData to the appropriate side output.
     */
    private void deserializeAndEmit(byte[] payload, Context ctx) throws Exception {
        if (payload == null || payload.length == 0) return;

        DataInputDeserializer input = new DataInputDeserializer(payload);
        int count = input.readInt();

        for (int i = 0; i < count; i++) {
            // RAW segment
            boolean hasRaw = input.readBoolean();
            if (hasRaw) {
                String segmentName = input.readUTF();
                TypeSerializer<RowData> ser = rawSerializers.get(segmentName);
                if (ser != null) {
                    RowData rawRowData = ser.deserialize(input);
                    OutputTag<RowData> tag = StreamSideOutputTag.getTag(segmentName);
                    if (tag != null) {
                        ctx.output(tag, rawRowData);
                    }
                }
            }

            // PST segment
            boolean hasPst = input.readBoolean();
            if (hasPst) {
                String pstSegmentName = input.readUTF();
                TypeSerializer<RowData> ser = pstSerializers.get(pstSegmentName);
                if (ser != null) {
                    RowData pstRowData = ser.deserialize(input);
                    OutputTag<RowData> tag = StreamSideOutputTag.getTag(pstSegmentName);
                    if (tag != null) {
                        ctx.output(tag, pstRowData);
                    }
                }
            }
        }
    }
}
