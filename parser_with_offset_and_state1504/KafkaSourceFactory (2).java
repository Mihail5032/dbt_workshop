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
        // Если задан kafka.start.timestamp.ms — стартуем с timestamp (миграционный режим).
        // Иначе — сохраняем текущее поведение (earliest).
        // При restore из чекпоинта Flink автоматически игнорирует startingOffsets
        // и использует оффсеты из state — ничего дополнительно не требуется.
        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
                startOffsets;
        Long startTsMs = pros.getKafkaStartTimestampMs();
        if (startTsMs != null) {
            log.info("Kafka start offsets: TIMESTAMP " + startTsMs);
            startOffsets = OffsetsInitializer.timestamp(startTsMs);
        } else {
            log.info("Kafka start offsets: EARLIEST (default)");
            startOffsets = OffsetsInitializer.earliest();
        }

        return KafkaSource.<String>builder()
                .setProperty("bootstrap.servers", "kafka.nodes=" + pros.getBoostrapServers())
                .setBootstrapServers(pros.getBoostrapServers())
                .setTopics(pros.getTopicName())
                .setGroupId("srv.data_stream_group_id_2")
                .setProperty("security.protocol", "SASL_PLAINTEXT")
                .setProperty("sasl.mechanism", "GSSAPI")
                .setProperty("sasl.jaas.config", jaas)
                .setProperty("sasl.kerberos.service.name", "kafka")
                .setStartingOffsets(startOffsets)
                .setValueOnlyDeserializer(new CustomDeserializer(String.class))
                .build();
    }
}
