package ru.x5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
import ru.x5.model.TransactionBundle;
import ru.x5.process.IdocParser;
import ru.x5.process.StreamSideOutputTag;
import ru.x5.process.TransactionProcessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Новый пайплайн с дедупликацией на уровне транзакции (подход тим лида):
 *
 * Kafka → IdocParser (парсит XML, группирует по транзакциям)
 *       → keyBy(txnKey)
 *       → TransactionProcessor (деdup по ValueState + эмит всех сегментов)
 *       → sinks
 *
 * Один хеш на транзакцию покрывает ВСЕ сегменты разом.
 * DeduplicationFilter, PstDeduplicationFilter, PstTransactionKeySelector — убраны.
 */
public class DataStreamJob {
    public static final String CATALOG = "core_flow_ing_raw";
    public static final String SCHEMA = "core_flow_ing_raw";
    private static final String UID_SOURCE = "kafka-source";
    private static final String UID_PARSER = "idoc-parser";
    private static final String UID_PROCESSOR = "transaction-processor";
    private final Map<String, TableIdentifier> tableMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        DataStreamJob job = new DataStreamJob();
        job.run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.getStreamExecutionEnvironment();
        DataStreamSource<String> stream = env.fromSource(
                KafkaSourceFactory.buildKafkaSource(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source").setParallelism(10);

        PropertiesHolder config = PropertiesHolder.getInstance();

        // === Catalog config ===
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

        // === RAW таблицы (iceberg schemas для конвертации сегментов в RowData) ===
        Map<String, TableLoader> tableLoaders = new HashMap<>();
        Map<String, Schema> icebergSchemas = new HashMap<>();

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
        tableMap.put("E1BPTRANSACTDISCEXT", TableIdentifier.of(SCHEMA, "raw_bptransactdiscext"));
        tableMap.put("E1BPTRANSACTION", TableIdentifier.of(SCHEMA, "raw_bptransaction"));
        tableMap.put("E1BPTRANSACTIONDISCO", TableIdentifier.of(SCHEMA, "raw_bptransactiondisco"));
        tableMap.put("E1BPTRANSACTEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bptransactextensio"));

        for (Map.Entry<String, TableIdentifier> entry : tableMap.entrySet()) {
            String segment = entry.getKey();
            TableIdentifier tableId = entry.getValue();
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, tableId);
            tableLoaders.put(segment, loader);
            icebergSchemas.put(segment, getIcebergSchema(loader));
        }

        // === PST таблицы (пишутся в raw_* таблицы, но с применённой PST-логикой) ===
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

        // ======================== ПАЙПЛАЙН ========================

        // Этап 1: Парсинг IDOC → TransactionBundle (один на транзакцию)
        Schema sourceDocSchema = icebergSchemas.get("E1BPSOURCEDOCUMENTLI");
        SingleOutputStreamOperator<TransactionBundle> parsed = stream
                .uid(UID_SOURCE)
                .process(new IdocParser(sourceDocSchema))
                .setParallelism(10)
                .uid(UID_PARSER);

        // SourceDocument — per-IDOC, эмитится из IdocParser через side output
        // ЗАКОММЕНТИРОВАНО ДЛЯ ТЕСТА — пишем только PST sinks в _test таблицы
//        DataStream<RowData> sourceDocStream = parsed.getSideOutput(IdocParser.SOURCE_DOC_TAG);
//        TableLoader sourceDocLoader = tableLoaders.get("E1BPSOURCEDOCUMENTLI");
//        FlinkSink.forRowData(sourceDocStream)
//                .tableLoader(sourceDocLoader)
//                .writeParallelism(1)
//                .upsert(false)
//                .uidPrefix("sink-sourcedocument")
//                .set("write.target-file-size-bytes", "268435456")
//                .append();

        // Этап 2: keyBy(txnKey) → TransactionProcessor (деdup + эмит)
        SingleOutputStreamOperator<RowData> processed = parsed
                .keyBy(TransactionBundle::getTxnKey)
                .process(new TransactionProcessor(icebergSchemas, pstSchemas))
                .setParallelism(15)
                .uid(UID_PROCESSOR);

        // === RAW sinks — 4 сегмента без PST-логики, пишем в _test таблицы ===
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

        // === PST sinks — 11 сегментов с PST-логикой ===
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

        env.execute("XML Parser with transaction-level dedup");
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
            try {
                loader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}