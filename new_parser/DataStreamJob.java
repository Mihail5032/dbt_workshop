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
import ru.x5.process.RawDataProcessFunction;
import ru.x5.process.StreamSideOutputTag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataStreamJob {
    public static final String CATALOG = "core_flow_ing_raw";
    public static final String SCHEMA = "core_flow_ing_raw";
    private static final String UID_SOURCE = "kafka-source";
    private static final String UID_PROCESS = "raw-data-process";
    private final Map<String, TableIdentifier> tableMap = new HashMap<>();


    public static void main(String[] args) throws Exception {
        DataStreamJob dataStreamJob = new DataStreamJob();
        dataStreamJob.run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.getStreamExecutionEnvironment();
        DataStreamSource<String> stream = env.fromSource(
                KafkaSourceFactory.buildKafkaSource(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source").setParallelism(10);

        PropertiesHolder config = PropertiesHolder.getInstance();

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.endpoint", config.getS3Endpoint());
        hadoopConf.set("fs.s3a.path.style.access", config.getPathStyleAccess());
        hadoopConf.set("fs.s3a.access.key", config.getAccessKey());
        hadoopConf.set("fs.s3a.secret.key", config.getSecretKey());
        hadoopConf.set("fs.s3a.connection.ssl.enabled", config.getConnectionsSslEnabled());
        hadoopConf.set("fs.s3a.impl", config.getS3Impl());
        hadoopConf.set("fs.s3a.aws.credentials.provider", config.getAwsCredentialsProvider());
        hadoopConf.set("fs.s3a.region", config.getClientRegion());
        hadoopConf.set("hive.metastore.uris", config.getHiveMetastoreUris());
        hadoopConf.setBoolean("fs.s3a.impl.disable.cache", Boolean.parseBoolean(config.getImplDisCache()));

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse", config.getWarehouse());
        catalogProps.put("io-impl", config.getIoImpl());
        catalogProps.put("s3.endpoint", config.getS3Endpoint());
        catalogProps.put("s3.path-style-access", config.getPathStyleAccess());
        catalogProps.put("s3.access-key-id", config.getAccessKey());
        catalogProps.put("s3.secret-access-key", config.getSecretKey());
        catalogProps.put("client.region", config.getClientRegion());

        CatalogLoader catalogLoader = CatalogLoader.hive(CATALOG, hadoopConf, catalogProps);

        Map<String, TableLoader> tableLoaders = new HashMap<>();
        Map<String, Schema> icebergSchemas = new HashMap<>();

        tableMap.put("E1BPSOURCEDOCUMENTLI", TableIdentifier.of(SCHEMA, "raw_bpsourcedocumentli"));
        tableMap.put("E1BPFINACIALMOVEMENT", TableIdentifier.of(SCHEMA, "raw_bpfinacialmovement"));
        tableMap.put("E1BPFINANCIALMOVEMEN", TableIdentifier.of(SCHEMA, "raw_bpfinancialmovemen"));
        tableMap.put("E1BPLINEITEMDISCEXT", TableIdentifier.of(SCHEMA, "raw_bplineitemdiscext"));
        tableMap.put("E1BPLINEITEMDISCOUNT", TableIdentifier.of(SCHEMA, "raw_bplineitemdiscount"));
        tableMap.put("E1BPLINEITEMEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bplineitemextensio"));
//        tableMap.put("E1BPLINEITEMEXTENSIO", TableIdentifier.of(SCHEMA, "bplineitemextensio_pre"));
        tableMap.put("E1BPLINEITEMTAX", TableIdentifier.of(SCHEMA, "raw_bplineitemtax"));
        tableMap.put("E1BPRETAILLINEITEM", TableIdentifier.of(SCHEMA, "raw_bpretaillineitem"));
        tableMap.put("E1BPRETAILTOTALS", TableIdentifier.of(SCHEMA, "raw_bpretailtotals"));
        tableMap.put("E1BPTENDER", TableIdentifier.of(SCHEMA, "raw_bptender"));
//        tableMap.put("E1BPTENDER", TableIdentifier.of(SCHEMA, "bptender_pre"));
        tableMap.put("E1BPTENDEREXTENSIONS", TableIdentifier.of(SCHEMA, "raw_bptenderextensions"));
//        tableMap.put("E1BPTENDEREXTENSIONS", TableIdentifier.of(SCHEMA, "bptenderextensions_pre"));
        tableMap.put("E1BPTENDERTOTALS", TableIdentifier.of(SCHEMA, "raw_bptendertotals"));
        tableMap.put("E1BPTRANSACTDISCEXT", TableIdentifier.of(SCHEMA, "raw_bptransactdiscext"));
        tableMap.put("E1BPTRANSACTION", TableIdentifier.of(SCHEMA, "raw_bptransaction"));
//        tableMap.put("E1BPTRANSACTION", TableIdentifier.of(SCHEMA, "bptransaction_pre"));
        tableMap.put("E1BPTRANSACTIONDISCO", TableIdentifier.of(SCHEMA, "raw_bptransactiondisco"));
        tableMap.put("E1BPTRANSACTEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bptransactextensio"));
//        tableMap.put("E1BPTRANSACTEXTENSIO", TableIdentifier.of(SCHEMA, "bptransactextensio_pre"));
       // tableMap.put("E1BPLINEITEMVOID", TableIdentifier.of(SCHEMA, "raw_bplineitemvoid"));
        // tableMap.put("E1BPPOSTVOIDDETAILS", TableIdentifier.of(SCHEMA, "raw_bppostvoiddetails"));

        for (Map.Entry<String, TableIdentifier> entry : tableMap.entrySet()) {
            String segment = entry.getKey();
            TableIdentifier tableId = entry.getValue();
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, tableId);
            tableLoaders.put(segment, loader);
            Schema schema = getIcebergSchema(loader);
            icebergSchemas.put(segment, schema);
        }


        // PST: пишем в RAW таблицы с применённой PST-логикой
        Map<String, TableIdentifier> pstTableMap = new HashMap<>();
        pstTableMap.put("PST_BPTRANSACTION", TableIdentifier.of(SCHEMA, "raw_bptransaction"));
        pstTableMap.put("PST_BPFINANCIALMOVEMEN", TableIdentifier.of(SCHEMA, "raw_bpfinancialmovemen"));
        pstTableMap.put("PST_BPTRANSACTEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bptransactextensio"));
        pstTableMap.put("PST_BPFINANCIALMOVEMENTEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bpfinacialmovement"));

        pstTableMap.put("PST_BPRETAILLINEITEM", TableIdentifier.of(SCHEMA, "raw_bpretaillineitem"));
        pstTableMap.put("PST_BPLINEITEMEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bplineitemextensio"));
        pstTableMap.put("PST_BPLINEITEMDISCOUNT", TableIdentifier.of(SCHEMA, "raw_bplineitemdiscount"));
        pstTableMap.put("PST_BPLINEITEMDISCEXT", TableIdentifier.of(SCHEMA, "raw_bplineitemdiscext"));
        pstTableMap.put("PST_BPTRANSACTDISCEXT", TableIdentifier.of(SCHEMA, "raw_bptransactdiscext"));
        pstTableMap.put("PST_BPTENDER", TableIdentifier.of(SCHEMA, "raw_bptender"));
        pstTableMap.put("PST_BPTENDEREXTENSIONS", TableIdentifier.of(SCHEMA, "raw_bptenderextensions"));


        Map<String, Schema> pstSchemas = new HashMap<>();
        for (Map.Entry<String, TableIdentifier> entry : pstTableMap.entrySet()) {
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, entry.getValue());
            pstSchemas.put(entry.getKey(), getIcebergSchema(loader));
        }

        SingleOutputStreamOperator<RowData> rowData = stream
                .uid(UID_SOURCE)
                .process(new RawDataProcessFunction(icebergSchemas, pstSchemas))
                .setParallelism(10)
                .uid(UID_PROCESS);

        // RAW sinks — только для 5 сегментов без PST-логики
        java.util.Set<String> rawOnlySegments = java.util.Set.of(
                "E1BPSOURCEDOCUMENTLI", "E1BPLINEITEMTAX", "E1BPRETAILTOTALS",
                "E1BPTENDERTOTALS", "E1BPTRANSACTIONDISCO"
        );
        for (String segment : rawOnlySegments) {
            OutputTag<RowData> tag = StreamSideOutputTag.getTag(segment);
            TableLoader loader = tableLoaders.get(segment);

            DataStream<RowData> sideStream = rowData.getSideOutput(tag);

            FlinkSink.forRowData(sideStream)
                    .tableLoader(loader)
                    .writeParallelism(1)
                    .upsert(false)
                    .uidPrefix("sink-" + segment.toLowerCase())
                    .set("write.target-file-size-bytes", "268435456")
                    .append();
        }

        // PST sinks — параллелизм зависит от нагрузки на sink
        Map<String, Integer> sinkParallelism = new HashMap<>();
        sinkParallelism.put("PST_BPLINEITEMEXTENSIO", 3);      // 192M 
        sinkParallelism.put("PST_BPTRANSACTEXTENSIO", 3);      // 103M 
        sinkParallelism.put("PST_BPRETAILLINEITEM", 3);        // 65M 
        sinkParallelism.put("PST_BPTENDEREXTENSIONS", 2);      // 45M 
        sinkParallelism.put("PST_BPTRANSACTION", 2);           // 13M 
        sinkParallelism.put("PST_BPTENDER", 2);                // 11M 

        Map<String, TableLoader> pstSinkLoaders = new HashMap<>();
        for (Map.Entry<String, TableIdentifier> entry : pstTableMap.entrySet()) {
            String pstKey = entry.getKey();
            OutputTag<RowData> tag = StreamSideOutputTag.getTag(pstKey);
            DataStream<RowData> pstStream = rowData.getSideOutput(tag);
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

        env.execute("XML Parser with correction 11 pst in raw");
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