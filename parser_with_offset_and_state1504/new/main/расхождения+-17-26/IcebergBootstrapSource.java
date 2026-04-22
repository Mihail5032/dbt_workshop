package ru.x5.process;

import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.x5.model.BootstrapEvent;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bounded SourceFunction, который читает txnKey из Iceberg-таблицы raw_bptransaction
 * за последние windowDays бизнес-дней (фильтр по businessdaydate) и эмитит их как
 * BootstrapEvent.KEY. После завершения чтения эмитит BootstrapEvent.END.
 *
 * ГАРАНТИЯ ТАЙМАУТА:
 * Независимо от того, как долго Iceberg читает таблицу или закрывает итератор,
 * END эмитится НЕ ПОЗЖЕ чем через maxRunMs с момента старта run(). Это реализовано
 * через watchdog-тред, который:
 *   - взводится в начале run();
 *   - по истечении maxRunMs выставляет running=false (прерывает цикл чтения)
 *     и принудительно эмитит END, если он ещё не был отправлен.
 *
 * Дубль END невозможен — его эмиссия защищена AtomicBoolean endEmitted.
 *
 * Parallelism этого source-а = 1 (конфигурируется в DataStreamJob).
 */
public class IcebergBootstrapSource extends RichSourceFunction<BootstrapEvent> {
    private static final Logger log = LogManager.getLogger("ru.x5.parser");
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final int windowDays;
    private final long maxRunMs;            // ← жёсткий таймаут на всю работу run()
    private final Map<String, String> catalogProps;
    private final Map<String, String> hadoopProps;

    private volatile boolean running = true;
    private final AtomicBoolean endEmitted = new AtomicBoolean(false);

    public IcebergBootstrapSource(String catalogName, String schemaName, String tableName,
                                   int windowDays,
                                   long maxRunMs,
                                   Map<String, String> catalogProps,
                                   Map<String, String> hadoopProps) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.windowDays = windowDays;
        this.maxRunMs = maxRunMs;
        this.catalogProps = catalogProps;
        this.hadoopProps = hadoopProps;
    }

    @Override
    public void run(SourceContext<BootstrapEvent> ctx) throws Exception {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        for (Map.Entry<String, String> e : hadoopProps.entrySet()) {
            hadoopConf.set(e.getKey(), e.getValue());
        }

        CatalogLoader catalogLoader = CatalogLoader.hive(catalogName, hadoopConf, catalogProps);
        TableIdentifier tid = TableIdentifier.of(schemaName, tableName);

        LocalDate cutoffDate = LocalDate.now(ZoneOffset.UTC).minusDays(windowDays);
        log.info("BOOTSTRAP: start reading {}.{} where businessdaydate >= {} ({} days back), maxRunMs={}",
                schemaName, tableName, cutoffDate, windowDays, maxRunMs);

        final long startTs = System.currentTimeMillis();

        // === Watchdog: через maxRunMs принудительно закрываем bootstrap и эмитим END ===
        // Поток демон, чтобы не блокировать завершение JVM.
        Thread watchdog = new Thread(() -> {
            try {
                Thread.sleep(maxRunMs);
            } catch (InterruptedException ie) {
                return; // нормальный выход — основной поток успел эмитнуть END и прервал нас
            }
            if (!endEmitted.get()) {
                log.warn("BOOTSTRAP_WATCHDOG: maxRunMs={} exceeded, forcing END emit and stopping iteration",
                        maxRunMs);
                running = false;
                try {
                    emitEndOnce(ctx, "watchdog-timeout");
                } catch (Exception e) {
                    log.error("BOOTSTRAP_WATCHDOG: failed to emit END", e);
                }
            }
        }, "bootstrap-watchdog");
        watchdog.setDaemon(true);
        watchdog.start();

        long emitted = 0;
        long skipped = 0;

        try (TableLoader tl = TableLoader.fromCatalog(catalogLoader, tid)) {
            tl.open();
            Table table = tl.loadTable();

            try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                    .where(Expressions.greaterThanOrEqual("businessdaydate", cutoffDate.toString()))
                    .select("retailstoreid", "businessdaydate",
                            "workstationid", "transactionsequencenumber")
                    .build()) {
                for (Record r : records) {
                    if (!running) break;

                    String store = asString(r.getField("retailstoreid"));
                    String day = dayFromIcebergValue(r.getField("businessdaydate"));
                    String ws = asString(r.getField("workstationid"));
                    String txn = asString(r.getField("transactionsequencenumber"));

                    if (store == null || day == null || ws == null || txn == null) {
                        skipped++;
                        continue;
                    }
                    String txnKey = store + "|" + day + "|" + ws + "|" + txn;
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(BootstrapEvent.key(txnKey));
                    }
                    emitted++;
                    if (emitted % 100_000 == 0) {
                        long elapsed = System.currentTimeMillis() - startTs;
                        log.info("BOOTSTRAP: emitted {} keys (elapsed {} ms, deadline {} ms)",
                                emitted, elapsed, maxRunMs);
                    }
                }
                // Эмитим END ДО close() итератора — на случай зависания close().
                emitEndOnce(ctx, "iteration-done pre-close (emitted=" + emitted + ", skipped=" + skipped + ")");
            }
        } catch (Exception e) {
            log.error("BOOTSTRAP: failed while reading Iceberg, emitting END anyway", e);
            emitEndOnce(ctx, "exception: " + e.getClass().getSimpleName());
        }

        // Гарантированный финальный emit — если по какой-то причине ни одна из веток выше не сработала.
        emitEndOnce(ctx, "run-end final-safety (emitted=" + emitted + ", skipped=" + skipped + ")");

        // Прерываем watchdog, чтобы он не висел до конца maxRunMs впустую.
        watchdog.interrupt();

        long elapsed = System.currentTimeMillis() - startTs;
        log.info("BOOTSTRAP: run() returning. emitted={}, skipped={}, elapsed={} ms", emitted, skipped, elapsed);
    }

    /**
     * Эмитит BootstrapEvent.END ровно один раз. Повторные вызовы — noop.
     * Безопасно вызывать из любых тредов (watchdog / main).
     */
    private void emitEndOnce(SourceContext<BootstrapEvent> ctx, String reason) throws Exception {
        if (endEmitted.compareAndSet(false, true)) {
            log.info("BOOTSTRAP: sending END (reason={})", reason);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(BootstrapEvent.end());
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    private static String asString(Object o) {
        return o == null ? null : o.toString();
    }

    /** businessdaydate в Iceberg — DATE (epoch day). В txnKey — строка YYYYMMDD. */
    private static String dayFromIcebergValue(Object v) {
        if (v == null) return null;
        if (v instanceof LocalDate) return ((LocalDate) v).format(YYYYMMDD);
        if (v instanceof Integer) return LocalDate.ofEpochDay((int) v).format(YYYYMMDD);
        if (v instanceof Long) return LocalDate.ofEpochDay((long) v).format(YYYYMMDD);
        // fallback на случай если схема строковая
        String s = v.toString();
        return s.isEmpty() ? null : s;
    }
}
