package ru.x5.process;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
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
import java.util.Iterator;
import java.util.Map;

/**
 * Замена TransactionProcessor для миграционного режима (bootstrap.enabled=true).
 *
 * Вход:
 *   - keyed DataStream<TransactionBundle> (union Kafka-bundle + bootstrap-ключей)
 *     * payload != null  — Kafka-сообщение, обычная обработка
 *     * payload == null  — bootstrap-ключ из Iceberg, помечает дедуп-стейт
 *   - broadcast DataStream<Boolean> (сигнал END из IcebergBootstrapSource)
 *
 * Выход: стандартные side output с RowData (как в TransactionProcessor).
 *
 * Проблема ordering: для одного txnKey Kafka-bundle может прийти раньше bootstrap-bundle-а.
 * Поэтому Kafka-bundle, пришедший пока END ещё не получен, уходит в per-key буфер с
 * processing-time таймером-fallback (на случай если bootstrap-ключа для этого ключа нет
 * вообще — значит транзакция новая и её надо эмитить после таймаута).
 *
 * После прихода END — каждый следующий Kafka-bundle (любого ключа) сначала флашит
 * свой буфер (ленивый флаш). Для ключей, у которых после END не приходит новых bundle —
 * сработает processing-time таймер и тоже флашнет.
 *
 * Производительность обычного режима (bootstrap.enabled=false): эта функция НЕ используется
 * — в DataStreamJob вместо неё цепляется обычный TransactionProcessor, никакого оверхеда.
 */
public class BootstrappingTransactionProcessor
        extends KeyedBroadcastProcessFunction<String, TransactionBundle, Boolean, RowData> {

    private static final Logger log = LogManager.getLogger("ru.x5.parser");
    private static final long TTL_HOURS = 72L;
    private static final long LOG_INTERVAL = 100_000L;

    /** Broadcast state: ключ "done" → true после приёма END. */
    public static final MapStateDescriptor<String, Boolean> BOOTSTRAP_DONE_DESC =
            new MapStateDescriptor<>("bootstrap-done-state",
                    BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.BOOLEAN_TYPE_INFO);

    private final Map<String, Schema> icebergSchemas;
    private final Map<String, Schema> pstSchemas;
    private final long bootstrapWaitMs;

    // Keyed state
    private transient ValueState<Boolean> seenState;
    private transient ListState<byte[]> bufferState;
    private transient ValueState<Long> bufferTimerState;

    private transient Map<String, TypeSerializer<RowData>> rawSerializers;
    private transient Map<String, TypeSerializer<RowData>> pstSerializers;

    private transient long totalCount = 0;
    private transient long duplicateCount = 0;
    private transient long bootstrapKeyCount = 0;
    private transient long bufferedCount = 0;

    public BootstrappingTransactionProcessor(Map<String, Schema> icebergSchemas,
                                              Map<String, Schema> pstSchemas,
                                              long bootstrapWaitMs) {
        this.icebergSchemas = icebergSchemas;
        this.pstSchemas = pstSchemas;
        this.bootstrapWaitMs = bootstrapWaitMs;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(TTL_HOURS))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupIncrementally(10, true)
                .build();

        ValueStateDescriptor<Boolean> seenDesc = new ValueStateDescriptor<>("seen-txn", Boolean.class);
        seenDesc.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(seenDesc);

        // Буфер Kafka-payload-ов, пришедших до END (или до bootstrap-ключа этого же txnKey).
        // TTL не ставим — буфер гарантированно очищается либо флашем по таймеру,
        // либо при приёме bootstrap-ключа, либо при следующем Kafka-bundle (ленивый флаш).
        ListStateDescriptor<byte[]> bufDesc = new ListStateDescriptor<>("kafka-buffer", byte[].class);
        bufferState = getRuntimeContext().getListState(bufDesc);

        ValueStateDescriptor<Long> timerDesc = new ValueStateDescriptor<>("buffer-timer", Long.class);
        bufferTimerState = getRuntimeContext().getState(timerDesc);

        // Сериализаторы — такие же как в TransactionProcessor
        this.rawSerializers = new HashMap<>();
        for (Map.Entry<String, Schema> e : icebergSchemas.entrySet()) {
            rawSerializers.put(e.getKey(), createSerializer(e.getValue()));
        }
        this.pstSerializers = new HashMap<>();
        for (Map.Entry<String, Schema> e : pstSchemas.entrySet()) {
            pstSerializers.put(e.getKey(), createSerializer(e.getValue()));
        }

        log.info("BOOTSTRAPPING_TXN_PROCESSOR_INIT: TTL={}h, waitMs={}, subtask={}",
                TTL_HOURS, bootstrapWaitMs, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    @SuppressWarnings("unchecked")
    private TypeSerializer<RowData> createSerializer(Schema icebergSchema) {
        RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
        return (TypeSerializer<RowData>) (TypeSerializer<?>)
                new org.apache.flink.table.runtime.typeutils.RowDataSerializer(rowType);
    }

    @Override
    public void processElement(TransactionBundle bundle,
                                ReadOnlyContext ctx,
                                Collector<RowData> out) throws Exception {

        // === 1) Bootstrap-ключ: payload == null ===
        if (bundle.getPayload() == null) {
            bootstrapKeyCount++;
            seenState.update(true);
            // Очищаем буфер (всё, что было Kafka для этого же ключа — дубликаты)
            clearBufferAndTimer(ctx);
            return;
        }

        totalCount++;

        // === 2) Kafka-bundle: уже видели ключ → дроп ===
        if (seenState.value() != null) {
            duplicateCount++;
            if (duplicateCount % LOG_INTERVAL == 0) {
                log.info("TXN_DEDUP_DROP: total_dupes={}, subtask={}",
                        duplicateCount, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            }
            return;
        }

        // === 3) Проверяем — завершён ли bootstrap? ===
        ReadOnlyBroadcastState<String, Boolean> bs = ctx.getBroadcastState(BOOTSTRAP_DONE_DESC);
        Boolean done = bs.get("done");
        boolean bootstrapDone = done != null && done;

        if (!bootstrapDone) {
            // Bootstrap ещё не завершён — буферизуем, ставим fallback-таймер (один на ключ).
            bufferState.add(bundle.getPayload());
            bufferedCount++;
            if (bufferTimerState.value() == null) {
                long fireAt = ctx.timerService().currentProcessingTime() + bootstrapWaitMs;
                ctx.timerService().registerProcessingTimeTimer(fireAt);
                bufferTimerState.update(fireAt);
            }
            return;
        }

        // === 4) Bootstrap завершён — сначала лениво флашим буфер этого ключа, потом сам bundle ===
        flushBuffer(ctx, out);
        emitIfNew(bundle.getPayload(), ctx, out);

        if (totalCount % LOG_INTERVAL == 0) {
            log.info("BOOTSTRAPPING_TXN_STATS: total={}, dupes={}, bootstrap_keys={}, buffered={}, subtask={}",
                    totalCount, duplicateCount, bootstrapKeyCount, bufferedCount,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }

    @Override
    public void processBroadcastElement(Boolean signal,
                                         Context ctx,
                                         Collector<RowData> out) throws Exception {
        BroadcastState<String, Boolean> bs = ctx.getBroadcastState(BOOTSTRAP_DONE_DESC);
        bs.put("done", Boolean.TRUE);
        log.info("BOOTSTRAP_END received, subtask={}. Buffered Kafka-bundles will flush " +
                 "lazily (on next bundle per key) or via processing-time timer.",
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        // Явно пройти по всем ключам и флашнуть отсюда не получается:
        // KeyedStateFunction в applyToKeyedState не имеет доступа ни к Collector, ни к TimerService.
        // Поэтому опираемся на два механизма:
        //   (a) ленивый флаш при следующем Kafka-bundle этого ключа,
        //   (b) fallback-таймер (bootstrapWaitMs с момента первой буферизации) — см. onTimer.
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
        // Таймер-fallback: bundle-ы лежат в буфере уже bootstrapWaitMs.
        // Считаем, что bootstrap для этого ключа не придёт, и флашим.
        flushBuffer(ctx, out);
        bufferTimerState.clear();
    }

    // ======================== Helpers ========================

    private void flushBuffer(KeyedBroadcastProcessFunction<String, TransactionBundle, Boolean, RowData>
                                     .ReadOnlyContext ctx,
                             Collector<RowData> out) throws Exception {
        Iterable<byte[]> buf = bufferState.get();
        if (buf == null) return;
        Iterator<byte[]> it = buf.iterator();
        if (!it.hasNext()) return;
        while (it.hasNext()) {
            byte[] p = it.next();
            if (seenState.value() != null) {
                // Уже видели — остальные bundle этого ключа тоже дубликаты
                duplicateCount++;
                continue;
            }
            seenState.update(true);
            deserializeAndEmit(p, ctx, out);
        }
        bufferState.clear();
        Long t = bufferTimerState.value();
        if (t != null) {
            // Нельзя из ReadOnlyContext удалить таймер напрямую — но пусть просто сработает
            // onTimer, там bufferState пустой → noop.
            // Явное удаление сделать можно только из Context (processBroadcast) или
            // OnTimerContext (onTimer). В ReadOnlyContext это недоступно.
        }
    }

    /** Версия flushBuffer с удалением таймера — доступно из onTimer (OnTimerContext). */
    private void flushBuffer(OnTimerContext ctx, Collector<RowData> out) throws Exception {
        Iterable<byte[]> buf = bufferState.get();
        if (buf == null) return;
        Iterator<byte[]> it = buf.iterator();
        if (!it.hasNext()) return;
        while (it.hasNext()) {
            byte[] p = it.next();
            if (seenState.value() != null) {
                duplicateCount++;
                continue;
            }
            seenState.update(true);
            deserializeAndEmitFromTimer(p, ctx, out);
        }
        bufferState.clear();
    }

    private void emitIfNew(byte[] payload,
                            KeyedBroadcastProcessFunction<String, TransactionBundle, Boolean, RowData>
                                    .ReadOnlyContext ctx,
                            Collector<RowData> out) throws Exception {
        if (seenState.value() != null) {
            duplicateCount++;
            return;
        }
        seenState.update(true);
        deserializeAndEmit(payload, ctx, out);
    }

    private void clearBufferAndTimer(
            KeyedBroadcastProcessFunction<String, TransactionBundle, Boolean, RowData>.ReadOnlyContext ctx)
            throws Exception {
        bufferState.clear();
        Long t = bufferTimerState.value();
        if (t != null) {
            // Удалить таймер из ReadOnlyContext нельзя.
            // Оставляем — когда он сработает, буфер пустой → noop.
            bufferTimerState.clear();
        }
    }

    /**
     * Десериализация payload и отправка RowData в side outputs.
     * Формат совпадает с TransactionProcessor.deserializeAndEmit / IdocParser.serializeEntries.
     */
    private void deserializeAndEmit(byte[] payload,
                                     KeyedBroadcastProcessFunction<String, TransactionBundle, Boolean, RowData>
                                             .ReadOnlyContext ctx,
                                     Collector<RowData> out) throws Exception {
        if (payload == null || payload.length == 0) return;
        DataInputDeserializer input = new DataInputDeserializer(payload);
        int count = input.readInt();

        for (int i = 0; i < count; i++) {
            boolean hasRaw = input.readBoolean();
            if (hasRaw) {
                String segmentName = input.readUTF();
                TypeSerializer<RowData> ser = rawSerializers.get(segmentName);
                if (ser != null) {
                    RowData rd = ser.deserialize(input);
                    OutputTag<RowData> tag = StreamSideOutputTag.getTag(segmentName);
                    if (tag != null) ctx.output(tag, rd);
                }
            }
            boolean hasPst = input.readBoolean();
            if (hasPst) {
                String pstSegmentName = input.readUTF();
                TypeSerializer<RowData> ser = pstSerializers.get(pstSegmentName);
                if (ser != null) {
                    RowData rd = ser.deserialize(input);
                    OutputTag<RowData> tag = StreamSideOutputTag.getTag(pstSegmentName);
                    if (tag != null) ctx.output(tag, rd);
                }
            }
        }
    }

    /** Аналогичный метод, но с OnTimerContext — у него свой output() API. */
    private void deserializeAndEmitFromTimer(byte[] payload,
                                              OnTimerContext ctx,
                                              Collector<RowData> out) throws Exception {
        if (payload == null || payload.length == 0) return;
        DataInputDeserializer input = new DataInputDeserializer(payload);
        int count = input.readInt();

        for (int i = 0; i < count; i++) {
            boolean hasRaw = input.readBoolean();
            if (hasRaw) {
                String segmentName = input.readUTF();
                TypeSerializer<RowData> ser = rawSerializers.get(segmentName);
                if (ser != null) {
                    RowData rd = ser.deserialize(input);
                    OutputTag<RowData> tag = StreamSideOutputTag.getTag(segmentName);
                    if (tag != null) ctx.output(tag, rd);
                }
            }
            boolean hasPst = input.readBoolean();
            if (hasPst) {
                String pstSegmentName = input.readUTF();
                TypeSerializer<RowData> ser = pstSerializers.get(pstSegmentName);
                if (ser != null) {
                    RowData rd = ser.deserialize(input);
                    OutputTag<RowData> tag = StreamSideOutputTag.getTag(pstSegmentName);
                    if (tag != null) ctx.output(tag, rd);
                }
            }
        }
    }
}
