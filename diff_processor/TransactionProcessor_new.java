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
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.x5.model.TransactionBundle;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

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

    private transient ValueState<Boolean> seenState;
    private transient long totalCount = 0;
    private transient long duplicateCount = 0;

    // RowData deserializers matching IdocParser's serializers
    private transient Map<String, TypeSerializer<RowData>> rawSerializers;
    private transient Map<String, TypeSerializer<RowData>> pstSerializers;

    public TransactionProcessor(Map<String, Schema> icebergSchemas, Map<String, Schema> pstSchemas) {
        this.icebergSchemas = icebergSchemas;
        this.pstSchemas = pstSchemas;
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

        log.info("TXN_PROCESSOR_INIT: started, TTL={}h, subtask={}",
                TTL_HOURS,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
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

        seenState.update(true);

        // ======================== DESERIALIZE & FORWARD ========================
        deserializeAndEmit(bundle.getPayload(), ctx);

        // Periodic stats
        if (totalCount % LOG_INTERVAL == 0) {
            log.info("TXN_PROCESSOR_STATS: total={}, duplicates={}, passed={}, subtask={}",
                    totalCount, duplicateCount,
                    totalCount - duplicateCount,
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