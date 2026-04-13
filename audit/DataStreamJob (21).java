package ru.x5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
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
import ru.x5.metrics.CountTap;

import java.io.IOException;

public class DataStreamJob {
    public static final String CATALOG = "core_flow_ing_raw";
    public static final String SCHEMA = "core_flow_ing_raw";

    // ❌ УДАЛИЛИ статический блок с System.setProperty – он не нужен и только мешает

    public static void main(String[] args) throws Exception {
        DataStreamJob dataStreamJob = new DataStreamJob();
        dataStreamJob.run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.getStreamExecutionEnvironment();
        DataStreamSource<String> stream = env.fromSource(
                KafkaSourceFactory.buildKafkaSource(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        SingleOutputStreamOperator<String> streamCounted = stream
                .map(new CountTap<>("00_kafka_in"))
                .name("tap_00_kafka_in");

        PropertiesHolder config = PropertiesHolder.getInstance();

        // ✅ ПРАВИЛЬНЫЙ ПОРЯДОК: сначала Iceberg-свойства (s3.endpoint и т.д.), потом Hadoop-настройки (fs.s3a.*)
        CatalogLoader catalogLoader = CatalogLoader.hive(
                CATALOG,
                config.getEntriesS3(),      // ← Iceberg-специфичные (warehouse, s3.endpoint, path-style-access)
                config.getEntriesHMS()       // ← Hadoop-настройки (hive.metastore.uris, fs.s3a.access.key и т.д.)
        );

        TableIdentifier tableIdentifier = TableIdentifier.of(SCHEMA, "raw_table_audit");
        TableLoader tableLoaderDynamic = TableLoader.fromCatalog(catalogLoader, tableIdentifier);

        Schema icebergSchema = getIcebergSchema(tableLoaderDynamic);
        SingleOutputStreamOperator<RowData> rowData = streamCounted
                .process(new RawDataProcessFunction(icebergSchema))
                .name("01_parse_xml");

        SingleOutputStreamOperator<RowData> rowDataCounted = rowData
                .map(new CountTap<>("01_rowdata_out"))
                .name("tap_01_rowdata_out");

        FlinkSink.forRowData(rowDataCounted)
                .tableLoader(tableLoaderDynamic)
                .writeParallelism(1)
                .upsert(false)
                .set("target-file-size-bytes", "268435456")
                .append();

        env.execute("XML Parser <raw_table_test>");
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
}
