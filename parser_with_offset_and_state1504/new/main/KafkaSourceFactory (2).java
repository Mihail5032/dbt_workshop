package ru.x5.factory;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.log4j.Logger;
import ru.x5.config.PropertiesHolder;
import ru.x5.deserializer.CustomDeserializer;

import java.net.URL;

public class KafkaSourceFactory {
    private static final Logger log = Logger.getLogger(KafkaSourceFactory.class);

    public static KafkaSource<String> buildKafkaSource() {
        PropertiesHolder pros = PropertiesHolder.getInstance();
        String keytabLocation = pros.getKeytabLocation();
        if (keytabLocation.startsWith("classpath")) {
            String keytabName = keytabLocation.split(":")[1];
            URL resource = KafkaSourceFactory.class.getClassLoader().getResource(keytabName);
            keytabLocation = resource.getPath();
        }
        String jaas = String.format(
                "com.sun.security.auth.module.Krb5LoginModule required" +
                        " useKeyTab=true" +
                        " storeKey=true" +
                        " debug=true" +
                        " keyTab=%s" +
                        " serviceName=%s" +
                        " principal=%s;",
                "\"" + keytabLocation + "\"",
                "kafka",
                "\"" + pros.getKeytabPrincipal() + "\""
        );

        log.info("JAAS FILE: " + jaas);

        // === Стартовые оффсеты ===
        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
                startOffsets;
        Long startTsMs = pros.getKafkaStartTimestampMs();
        log.info("KAFKA_OFFSET_DEBUG: kafka.start.timestamp.ms raw value = ["
                + pros.getKafkaStartTimestampMs() + "]");
        log.info("KAFKA_OFFSET_DEBUG: startTsMs = " + startTsMs
                + ", isNull = " + (startTsMs == null));
        if (startTsMs != null) {
            log.info("KAFKA_OFFSET_DEBUG: using TIMESTAMP offsets = " + startTsMs);
            startOffsets = OffsetsInitializer.timestamp(startTsMs);
        } else {
            log.info("KAFKA_OFFSET_DEBUG: using COMMITTED offsets, fallback EARLIEST");
            startOffsets = OffsetsInitializer.committedOffsets(
                    org.apache.kafka.clients.consumer.OffsetResetStrategy.EARLIEST);
        }

        return KafkaSource.<String>builder()
                .setProperty("bootstrap.servers", "kafka.nodes=" + pros.getBoostrapServers())
                .setBootstrapServers(pros.getBoostrapServers())
                .setTopics(pros.getTopicName())
                .setGroupId("srv.data_stream_group_id_1")
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "GSSAPI")
                .setProperty("sasl.jaas.config", jaas)
                .setProperty("sasl.kerberos.service.name", "kafka")
                .setStartingOffsets(startOffsets)
                .setValueOnlyDeserializer(new CustomDeserializer(String.class))
                .build();
    }
}
