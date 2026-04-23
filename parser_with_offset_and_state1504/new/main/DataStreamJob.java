package ru.x5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import ru.x5.config.PropertiesHolder;
import ru.x5.factory.KafkaSourceFactory;
import ru.x5.factory.StreamExecutionEnvironmentFactory;
import ru.x5.model.BootstrapEvent;
import ru.x5.model.TransactionBundle;
import ru.x5.process.BootstrapSplitter;
import ru.x5.process.BootstrappingTransactionProcessor;
import ru.x5.process.IcebergBootstrapSource;
import ru.x5.process.IdocParser;
import ru.x5.process.StreamSideOutputTag;
import ru.x5.process.TransactionProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Пайплайн с дедупликацией на уровне транзакции.
 *
 * Базовый путь (bootstrap.enabled=false, default):
 *   Kafka → IdocParser → keyBy(txnKey) → TransactionProcessor → sinks
 *
 * Миграционный путь (bootstrap.enabled=true):
 *   Kafka → IdocParser ─┐
 *                       ├─(union)─→ keyBy(txnKey) ─connect─→ BootstrappingTransactionProcessor → sinks
 *   IcebergBootstrap → Splitter─┘                              ↑
 *                              └─ END → broadcast ────────────┘
 *
 * Производительность базового пути сохранена полностью: в ветке !bootstrapEnabled
 * используется старый TransactionProcessor без KeyedBroadcastProcessFunction и буферов.
 */
public class DataStreamJob {

    public static final String CATALOG = "core_flow_ing_raw";
    public static final String SCHEMA = "core_flow_ing_raw";
    private static final String UID_SOURCE = "kafka-source";
    private static final String UID_PARSER = "idoc-parser";
    private static final String UID_PROCESSOR = "transaction-processor";
    private static final String UID_BOOTSTRAP_SRC = "iceberg-bootstrap-source";
    private static final String UID_BOOTSTRAP_SPLIT = "bootstrap-splitter";

    private final Map<String, TableIdentifier> tableMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        new DataStreamJob().run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.getStreamExecutionEnvironment();

        DataStreamSource<String> stream = env.fromSource(
                KafkaSourceFactory.buildKafkaSource(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source").setParallelism(10);

        PropertiesHolder config = PropertiesHolder.getInstance();

        // ============ Catalog (общий для всего джоба) ============
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("hive.metastore.client.connect.timeout", "60000");

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("type", "hive");
        catalogProps.put("io-impl", config.getIoImpl());
        catalogProps.put("warehouse", config.getWarehouse());
        catalogProps.put("uri", config.getHiveMetastoreUris());
        catalogProps.put("s3.endpoint", config.getS3Endpoint());
        catalogProps.put("s3.path-style-access", config.getPathStyleAccess());
        catalogProps.put("client.region", config.getClientRegion());
        catalogProps.put("s3.access-key-id", config.getAccessKey());
        catalogProps.put("s3.secret-access-key", config.getSecretKey());
        catalogProps.put("fs.s3a.connection.maximum", "100");
        catalogProps.put("fs.s3a.connection.timeout", "60000");
        catalogProps.put("fs.s3a.socket.timeout", "60000");
        catalogProps.put("fs.s3a.attempts.maximum", "5");

        CatalogLoader catalogLoader = CatalogLoader.hive(CATALOG, hadoopConf, catalogProps);

        // ============ RAW таблицы ============
        Map<String, TableLoader> tableLoaders = new HashMap<>();
        Map<String, Schema> icebergSchemas = new HashMap<>();

        tableMap.put("E1BPTRANSACTION", TableIdentifier.of(SCHEMA, "raw_bptransaction"));
        tableMap.put("E1BPSOURCEDOCUMENTLI", TableIdentifier.of(SCHEMA, "raw_bpsourcedocumentli"));
        tableMap.put("E1BPFINACIALMOVEMENT", TableIdentifier.of(SCHEMA, "raw_bpfinacialmovement"));
        tableMap.put("E1BPFINANCIALMOVEMEN", TableIdentifier.of(SCHEMA, "raw_bpfinancialmovemen"));
        tableMap.put("E1BPLINEITEMDISCEXT", TableIdentifier.of(SCHEMA, "raw_bplineitemdiscext"));
        tableMap.put("E1BPLINEITEMDISCOUNT", TableIdentifier.of(SCHEMA, "raw_bplineitemdiscount"));
        tableMap.put("E1BPLINEITEMEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bplineitemextensio"));
        tableMap.put("E1BPLINEITEMTAX", TableIdentifier.of(SCHEMA, "raw_bplineitemtax"));
        tableMap.put("E1BPRETAILLINEITEM", TableIdentifier.of(SCHEMA, "raw_bpretaillineitem"));
        tableMap.put("E1BPRETAILTOTALS", TableIdentifier.of(SCHEMA, "raw_bpretailtotals"));
        tableMap.put("E1BPTENDER", TableIdentifier.of(SCHEMA, "raw_bptender"));
        tableMap.put("E1BPTENDEREXTENSIONS", TableIdentifier.of(SCHEMA, "raw_bptenderextensions"));
        tableMap.put("E1BPTENDERTOTALS", TableIdentifier.of(SCHEMA, "raw_bptendertotals"));
        tableMap.put("E1BPTRANSACTIONDISCO", TableIdentifier.of(SCHEMA, "raw_bptransactiondisco"));
        tableMap.put("E1BPTRANSACTEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bptransactextensio"));
        tableMap.put("E1BPTRANSACTDISCEXT", TableIdentifier.of(SCHEMA, "raw_bptransactdiscext"));

        for (Map.Entry<String, TableIdentifier> entry : tableMap.entrySet()) {
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, entry.getValue());
            tableLoaders.put(entry.getKey(), loader);
            icebergSchemas.put(entry.getKey(), getIcebergSchema(loader));
        }

        // ============ PST таблицы ============
        Map<String, TableIdentifier> pstTableMap = new HashMap<>();
        pstTableMap.put("PST_BPTRANSACTION",              TableIdentifier.of(SCHEMA, "raw_bptransaction_test"));
        pstTableMap.put("PST_BPFINANCIALMOVEMEN",          TableIdentifier.of(SCHEMA, "raw_bpfinancialmovemen_test"));
        pstTableMap.put("PST_BPTRANSACTEXTENSIO",          TableIdentifier.of(SCHEMA, "raw_bptransactextensio_test"));
        pstTableMap.put("PST_BPFINANCIALMOVEMENTEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bpfinacialmovement_test"));
        pstTableMap.put("PST_BPRETAILLINEITEM",            TableIdentifier.of(SCHEMA, "raw_bpretaillineitem_test"));
        pstTableMap.put("PST_BPLINEITEMEXTENSIO",          TableIdentifier.of(SCHEMA, "raw_bplineitemextensio_test"));
        pstTableMap.put("PST_BPLINEITEMDISCOUNT",          TableIdentifier.of(SCHEMA, "raw_bplineitemdiscount_test"));
        pstTableMap.put("PST_BPLINEITEMDISCEXT",           TableIdentifier.of(SCHEMA, "raw_bplineitemdiscext_test"));
        pstTableMap.put("PST_BPTRANSACTDISCEXT",           TableIdentifier.of(SCHEMA, "raw_bptransactdiscext_test"));
        pstTableMap.put("PST_BPTENDER",                    TableIdentifier.of(SCHEMA, "raw_bptender_test"));
        pstTableMap.put("PST_BPTENDEREXTENSIONS",          TableIdentifier.of(SCHEMA, "raw_bptenderextensions_test"));

        Map<String, Schema> pstSchemas = new HashMap<>();
        for (Map.Entry<String, TableIdentifier> entry : pstTableMap.entrySet()) {
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, entry.getValue());
            pstSchemas.put(entry.getKey(), getIcebergSchema(loader));
        }

        // ======================== Этап 1: IdocParser ========================
        Schema sourceDocSchema = icebergSchemas.get("E1BPSOURCEDOCUMENTLI");
        SingleOutputStreamOperator<TransactionBundle> parsed = stream
                .uid(UID_SOURCE)
                .process(new IdocParser(sourceDocSchema, icebergSchemas, pstSchemas))
                .setParallelism(10)
                .uid(UID_PARSER);

        // SourceDocument — per-IDOC, эмитится из IdocParser через side output
        DataStream<RowData> sourceDocStream = parsed.getSideOutput(IdocParser.SOURCE_DOC_TAG);
        TableLoader sourceDocLoader = TableLoader.fromCatalog(catalogLoader,
                TableIdentifier.of(SCHEMA, "raw_bpsourcedocumentli_test"));
        FlinkSink.forRowData(sourceDocStream)
                .tableLoader(sourceDocLoader)
                .writeParallelism(1)
                .upsert(false)
                .uidPrefix("sink-sourcedocument")
                .set("write.target-file-size-bytes", "268435456")
                .append();

        // ======================== Этап 2: TransactionProcessor ========================
        SingleOutputStreamOperator<RowData> processed;

        if (config.isBootstrapEnabled()) {
            // ---------- МИГРАЦИОННЫЙ ПУТЬ ----------
            // Hadoop/catalog props для bootstrap-source (SourceFunction создаёт свой CatalogLoader,
            // потому что оригинальный не сериализуем между JM и TM).
            Map<String, String> bootstrapHadoopProps = new HashMap<>();
            bootstrapHadoopProps.put("hive.metastore.client.connect.timeout", "60000");
            bootstrapHadoopProps.put("hive.metastore.uris", config.getHiveMetastoreUris());
            bootstrapHadoopProps.put("fs.s3a.endpoint", config.getS3Endpoint());
            bootstrapHadoopProps.put("fs.s3a.path.style.access", config.getPathStyleAccess());
            bootstrapHadoopProps.put("fs.s3a.access.key", config.getAccessKey());
            bootstrapHadoopProps.put("fs.s3a.secret.key", config.getSecretKey());
            bootstrapHadoopProps.put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

            Map<String, String> bootstrapCatalogProps = new HashMap<>();
            bootstrapCatalogProps.put("type", "hive");
            bootstrapCatalogProps.put("io-impl", config.getIoImpl());
            bootstrapCatalogProps.put("warehouse", config.getWarehouse());
            bootstrapCatalogProps.put("uri", config.getHiveMetastoreUris());
            bootstrapCatalogProps.put("s3.endpoint", config.getS3Endpoint());
            bootstrapCatalogProps.put("s3.path-style-access", config.getPathStyleAccess());
            bootstrapCatalogProps.put("client.region", config.getClientRegion());
            bootstrapCatalogProps.put("s3.access-key-id", config.getAccessKey());
            bootstrapCatalogProps.put("s3.secret-access-key", config.getSecretKey());

            IcebergBootstrapSource bootstrapSource = new IcebergBootstrapSource(
                    config.getBootstrapCatalog(),
                    config.getBootstrapSchema(),
                    config.getBootstrapTable(),
                    config.getBootstrapWindowDays(),
                    config.getBootstrapWaitMs(),   // ← жёсткий таймаут watchdog-а
                    bootstrapCatalogProps,
                    bootstrapHadoopProps);

            DataStream<BootstrapEvent> bootstrapRaw = env
                    .addSource(bootstrapSource)
                    .setParallelism(1)
                    .uid(UID_BOOTSTRAP_SRC)
                    .name("IcebergBootstrapSource");

            // Делим на ключи (main) + END (side)
            SingleOutputStreamOperator<TransactionBundle> bootstrapBundles = bootstrapRaw
                    .process(new BootstrapSplitter())
                    .setParallelism(1)
                    .uid(UID_BOOTSTRAP_SPLIT)
                    .name("BootstrapSplitter");

            DataStream<Boolean> endSignal = bootstrapBundles.getSideOutput(BootstrapSplitter.END_TAG);

            // Broadcast END во все subtask-и TransactionProcessor
            BroadcastStream<Boolean> broadcastEnd = endSignal
                    .broadcast(BootstrappingTransactionProcessor.BOOTSTRAP_DONE_DESC);

            // Union Kafka-bundle + bootstrap-key bundle, keyBy, connect с broadcast, process
            DataStream<TransactionBundle> unified = parsed.union(bootstrapBundles);

            processed = unified
                    .keyBy(TransactionBundle::getTxnKey)
                    .connect(broadcastEnd)
                    .process(new BootstrappingTransactionProcessor(
                            icebergSchemas, pstSchemas, config.getBootstrapWaitMs()))
                    .setParallelism(25)
                    .uid(UID_PROCESSOR);

        } else {
            // ---------- ОБЫЧНЫЙ ПУТЬ ----------
            processed = parsed
                    .keyBy(TransactionBundle::getTxnKey)
                    .process(new TransactionProcessor(icebergSchemas, pstSchemas))
                    .setParallelism(25)
                    .uid(UID_PROCESSOR);
        }

        // === RAW sinks — 4 сегмента без PST-логики ===
        Map<String, TableIdentifier> rawOnlyTableMap = new HashMap<>();
        rawOnlyTableMap.put("E1BPLINEITEMTAX",      TableIdentifier.of(SCHEMA, "raw_bplineitemtax_test"));
        rawOnlyTableMap.put("E1BPRETAILTOTALS",     TableIdentifier.of(SCHEMA, "raw_bpretailtotals_test"));
        rawOnlyTableMap.put("E1BPTENDERTOTALS",     TableIdentifier.of(SCHEMA, "raw_bptendertotals_test"));
        rawOnlyTableMap.put("E1BPTRANSACTIONDISCO", TableIdentifier.of(SCHEMA, "raw_bptransactiondisco_test"));

        for (Map.Entry<String, TableIdentifier> entry : rawOnlyTableMap.entrySet()) {
            String segment = entry.getKey();
            OutputTag<RowData> tag = StreamSideOutputTag.getTag(segment);
            DataStream<RowData> sideStream = processed.getSideOutput(tag);
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, entry.getValue());

            FlinkSink.forRowData(sideStream)
                    .tableLoader(loader)
                    .writeParallelism(1)
                    .upsert(false)
                    .uidPrefix("sink-" + segment.toLowerCase())
                    .set("write.target-file-size-bytes", "268435456")
                    .append();
        }

        // === PST sinks ===
        Map<String, Integer> sinkParallelism = new HashMap<>();
        sinkParallelism.put("PST_BPLINEITEMEXTENSIO", 3);
        sinkParallelism.put("PST_BPTRANSACTEXTENSIO", 3);
        sinkParallelism.put("PST_BPRETAILLINEITEM", 3);
        sinkParallelism.put("PST_BPTENDEREXTENSIONS", 2);
        sinkParallelism.put("PST_BPTRANSACTION", 2);
        sinkParallelism.put("PST_BPTENDER", 2);

        Map<String, TableLoader> pstSinkLoaders = new HashMap<>();
        for (Map.Entry<String, TableIdentifier> entry : pstTableMap.entrySet()) {
            String pstKey = entry.getKey();
            OutputTag<RowData> tag = StreamSideOutputTag.getTag(pstKey);
            DataStream<RowData> pstStream = processed.getSideOutput(tag);
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, entry.getValue());
            pstSinkLoaders.put(pstKey, loader);

            int parallelism = sinkParallelism.getOrDefault(pstKey, 1);

            FlinkSink.forRowData(pstStream)
                    .tableLoader(loader)
                    .writeParallelism(parallelism)
                    .upsert(false)
                    .uidPrefix("sink-" + pstKey.toLowerCase())
                    .set("write.target-file-size-bytes", "268435456")
                    .append();
        }

        env.execute("XML Parser with transaction-level dedup + optional Iceberg bootstrap");
        closeTableLoaders(tableLoaders);
        closeTableLoaders(pstSinkLoaders);
        env.close();
    }

    private Schema getIcebergSchema(TableLoader tableLoaderDynamic) throws IOException {
        Table icebergTableDynamic;
        try (tableLoaderDynamic) {
            tableLoaderDynamic.open();
            icebergTableDynamic = tableLoaderDynamic.loadTable();
        }
        return icebergTableDynamic.schema();
    }

    private void closeTableLoaders(Map<String, TableLoader> tableLoaders) {
        tableLoaders.values().forEach(loader -> {
            try { loader.close(); }
            catch (Exception e) { e.printStackTrace(); }
        });
    }
}
