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

/**
 * Bounded SourceFunction, который читает txnKey из Iceberg-таблицы raw_bptransaction
 * за последние windowDays бизнес-дней (фильтр по businessdaydate, не по load_ts)
 * и эмитит их как BootstrapEvent.KEY.
 *
 * После прохода по всем записям эмитит один BootstrapEvent.END — именно он используется
 * в TransactionProcessor как broadcast-сигнал, что bootstrap завершён и буфер можно флашить.
 *
 * Parallelism этого source-а должен быть 1 (конфигурируется в DataStreamJob), потому что
 * IcebergGenerics.read читает всю таблицу и шардинг по subtask-ам здесь не реализован.
 * Дальше ключи через keyBy(txnKey) распределяются между subtask-ами TransactionProcessor.
 *
 * ВАЖНО: формат businessdaydate в Iceberg — DATE (epoch day), а в txnKey
 * (см. IdocParser.buildTxnKey) — строка YYYYMMDD. Делаем обратную конвертацию.
 */
public class IcebergBootstrapSource extends RichSourceFunction<BootstrapEvent> {
    private static final Logger log = LogManager.getLogger("ru.x5.parser");
    private static final DateTimeFormatter YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final int windowDays;
    private final Map<String, String> catalogProps;
    private final Map<String, String> hadoopProps;

    private volatile boolean running = true;

    public IcebergBootstrapSource(String catalogName, String schemaName, String tableName,
                                   int windowDays,
                                   Map<String, String> catalogProps,
                                   Map<String, String> hadoopProps) {
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.windowDays = windowDays;
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

        // Фильтруем по businessdaydate (день продаж), а не по load_ts.
        // Это защищает от дублей даже если запись была записана в Iceberg давно
        // (back-fill / late IDOC), но её бизнес-день ещё попадает в окно.
        // businessdaydate в Iceberg хранится как DATE (epoch day), фильтр — по LocalDate.
        LocalDate cutoffDate = LocalDate.now(ZoneOffset.UTC).minusDays(windowDays);
        log.info("BOOTSTRAP: start reading {}.{} where businessdaydate >= {} ({} days back)",
                schemaName, tableName, cutoffDate, windowDays);

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
                        log.info("BOOTSTRAP: emitted {} keys", emitted);
                    }
                }
            }
        } catch (Exception e) {
            log.error("BOOTSTRAP: failed while reading Iceberg, emitting END anyway", e);
        }

        log.info("BOOTSTRAP: finished, emitted {} keys, skipped {}, sending END", emitted, skipped);
        synchronized (ctx.getCheckpointLock()) {
            ctx.collect(BootstrapEvent.end());
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
