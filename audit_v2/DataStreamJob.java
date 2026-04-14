/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    static {
        PropertiesHolder props = PropertiesHolder.getInstance();
        System.setProperty("aws.accessKeyId", props.getAccessKey());
        System.setProperty("aws.secretAccessKey", props.getSecretKey());
        System.setProperty("aws.region", "us-east-1");
    }

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

        // ✅ СЧЁТЧИК #1: сколько строк пришло из Kafka
        SingleOutputStreamOperator<String> streamCounted = stream
                .map(new CountTap<>("00_kafka_in"))
                .name("tap_00_kafka_in");

        PropertiesHolder config = PropertiesHolder.getInstance();
        CatalogLoader catalogLoader = CatalogLoader.hive(CATALOG, config.getEntriesHMS(), config.getEntriesS3());

        TableIdentifier tableIdentifier = TableIdentifier.of(SCHEMA, "raw_table_audit"); //wpuwbw_const
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
