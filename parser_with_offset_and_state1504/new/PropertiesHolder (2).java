package ru.x5.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.log4j.Logger;
import ru.x5.exceptions.CommonParserException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Класс для хранения конфигураций из property-файла
 */
public class PropertiesHolder {
    private static final Logger log = Logger.getLogger(PropertiesHolder.class);

    // Ключи конфигураций
    private static final String S3_ENDPOINT = "s3.endpoint";
    private static final String PATH_STYLE_ACCESS = "s3.path.style.access";
    private static final String ACCESS_KEY = "s3.access.key.id";
    private static final String SECRET_KEY = "s3.secret.access.key";
    private static final String CONNECTIONS_SSL_ENABLED = "connection.ssl.enabled";
    private static final String S3_IMPL = "s3.impl";
    private static final String IO_IMPL = "io.impl";
    private static final String AWS_CREDENTIALS_PROVIDER = "s3.aws.credentials.provider";
    private static final String CLIENT_REGION = "client.region";
    private static final String WAREHOUSE = "warehouse";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String IMPL_DIS_CACHE = "impl.disable.cache";


    // KAFKA
    private static final String BOOSTRAP_SERVERS = "bootstrap.servers";
    private static final String TOPIC_NAME = "topic.name";
    private static final String KEYTAB_LOCATION = "keytab.location";
    private static final String KEYTAB_PRINCIPAL = "keytab.principal";

    // === Migration: Kafka start offset by timestamp ===
    // Если задано — KafkaSource стартует с OffsetsInitializer.timestamp(ms),
    // иначе fallback на earliest(). При восстановлении из чекпоинта Flink
    // эту настройку игнорирует автоматически.
    private static final String KAFKA_START_TIMESTAMP_MS = "kafka.start.timestamp.ms";

    // === Migration: bootstrap дедуп-стейта из Iceberg ===
    private static final String BOOTSTRAP_ENABLED       = "bootstrap.enabled";        // default false
    private static final String BOOTSTRAP_WINDOW_DAYS   = "bootstrap.window.days";    // default 3
    private static final String BOOTSTRAP_CATALOG       = "bootstrap.catalog";        // default "hive"
    private static final String BOOTSTRAP_SCHEMA        = "bootstrap.schema";         // default "core_flow_ing_raw"
    private static final String BOOTSTRAP_TABLE         = "bootstrap.table";          // default "raw_bptransaction"
    private static final String BOOTSTRAP_WAIT_MS       = "bootstrap.wait.ms";        // default 600000 (10 мин)

    private static volatile PropertiesHolder instance;

    private final Properties props;

    private PropertiesHolder() {
        try {
            props = new Properties();
            props.load(PropertiesHolder.class.getClassLoader().getResourceAsStream("application.properties"));
        }
        catch (IOException ex) {
            log.error("Could not read properties");
            throw new CommonParserException(ex);
        }
    }

    public static PropertiesHolder getInstance() {
        if (instance == null) {
            synchronized (PropertiesHolder.class) {
                if (instance == null) {
                    instance = new PropertiesHolder();
                }
            }
        }
        return instance;
    }

    // S3

    public String getS3Endpoint() {
        return props.getProperty(S3_ENDPOINT);

    }

    public String getPathStyleAccess() {
        return props.getProperty(PATH_STYLE_ACCESS);

    }

    public String getAccessKey() {
        return props.getProperty(ACCESS_KEY);

    }

    public String getSecretKey() {
        return props.getProperty(SECRET_KEY);

    }

    public String getConnectionsSslEnabled() {
        return props.getProperty(CONNECTIONS_SSL_ENABLED);

    }

    public String getS3Impl() {
        return props.getProperty(S3_IMPL);

    }

    public String getIoImpl() {
        return props.getProperty(IO_IMPL);

    }

    public String getAwsCredentialsProvider() {
        return props.getProperty(AWS_CREDENTIALS_PROVIDER);

    }

    public String getClientRegion() {
        return props.getProperty(CLIENT_REGION);

    }

    public String getWarehouse() {
        return props.getProperty(WAREHOUSE);
    }

    public String getHiveMetastoreUris() {
        return props.getProperty(HIVE_METASTORE_URIS);
    }

    public String getImplDisCache() {
        return props.getProperty(IMPL_DIS_CACHE);
    }

    // KAFKA

    public String getBoostrapServers() {
        return props.getProperty(BOOSTRAP_SERVERS);
    }

    public String getTopicName() {
        return props.getProperty(TOPIC_NAME);
    }

    public String getKeytabLocation() {
        return props.getProperty(KEYTAB_LOCATION);
    }

    public String getKeytabPrincipal() {
        return props.getProperty(KEYTAB_PRINCIPAL);
    }

    // === Migration: Kafka start offset by timestamp ===

    /** Возвращает null, если свойство не задано — тогда KafkaSource использует earliest(). */
    public Long getKafkaStartTimestampMs() {
        String v = props.getProperty(KAFKA_START_TIMESTAMP_MS);
        if (v == null || v.trim().isEmpty()) return null;
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            log.warn("Invalid " + KAFKA_START_TIMESTAMP_MS + "=" + v + ", ignoring");
            return null;
        }
    }

    // === Migration: bootstrap дедуп-стейта из Iceberg ===

    public boolean isBootstrapEnabled() {
        return Boolean.parseBoolean(props.getProperty(BOOTSTRAP_ENABLED, "false"));
    }

    public int getBootstrapWindowDays() {
        try {
            return Integer.parseInt(props.getProperty(BOOTSTRAP_WINDOW_DAYS, "3"));
        } catch (NumberFormatException e) {
            return 3;
        }
    }

    public String getBootstrapCatalog() {
        return props.getProperty(BOOTSTRAP_CATALOG, "hive");
    }

    public String getBootstrapSchema() {
        return props.getProperty(BOOTSTRAP_SCHEMA, "core_flow_ing_raw");
    }

    public String getBootstrapTable() {
        return props.getProperty(BOOTSTRAP_TABLE, "raw_bptransaction");
    }

    public long getBootstrapWaitMs() {
        try {
            return Long.parseLong(props.getProperty(BOOTSTRAP_WAIT_MS, "600000"));
        } catch (NumberFormatException e) {
            return 600_000L;
        }
    }
}
