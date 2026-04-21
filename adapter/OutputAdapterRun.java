package ru;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;
import ru.Processors.AdapterRunProcessorFunction;

import java.nio.charset.StandardCharsets;

import static ru.Utils.AwsUtils.getEntriesHMS;
import static ru.Utils.AwsUtils.getEntriesS3;
import static ru.Utils.EntitiesUtils.*;

@Slf4j
public class OutputAdapterRun {

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        final StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        Configuration hadoopConf = getEntriesHMS();

        CatalogLoader catalogLoader = CatalogLoader.hive("datatransfer", hadoopConf, getEntriesS3());

        // Update table identifier to match your actual table name from SQL script
        TableIdentifier tableIdentifier = TableIdentifier.of("wpufib", "final_table_with_ref");
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
        Table icebergTable = catalogLoader.loadCatalog().loadTable(tableIdentifier);

        // Check the current snapshot ID
        long currentSnapshotId = icebergTable.currentSnapshot().snapshotId();
        log.info("Current Snapshot ID: " + currentSnapshotId);

        DataStream<RowData> stream = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(true)
                .build();

        org.apache.flink.table.api.Table flinkTable = tableEnv.fromDataStream(stream);
        tableEnv.createTemporaryView("final_table_with_ref", flinkTable);

        // Query to get the financial data
        org.apache.flink.table.api.Table resultTable = tableEnv.sqlQuery("SELECT * FROM final_table_with_ref");

        DataStream<String> resultStream = tableEnv.toChangelogStream(resultTable)
                // Key by rtl_txn_fin_rk (один IDOC = одно финансовое движение)
                .keyBy(row -> new String((byte[]) row.getField("rtl_txn_fin_rk"), StandardCharsets.UTF_8))
                .process(new AdapterRunProcessorFunction());

        resultStream.print();
        env.execute("Flink WPUFIB Adapter Job");
    }
}
