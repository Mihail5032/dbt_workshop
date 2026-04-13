package ru.x5.config;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.log4j.Logger;
import ru.x5.exceptions.CommonParserException;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class PropertiesHolder {
    private static final Logger log = Logger.getLogger(PropertiesHolder.class);

    // Ключи конфигураций
    private static final String HIVE_METASTORE_LOCAL = "hive.metastore.local";
    private static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    private static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    private static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    private static final String FS_S3A_CONNECTIONS_SSL_ENABLED = "fs.s3a.connection.ssl.enabled";
    private static final String FS_S3A_AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
    private static final String FS_S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    private static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    private static final String FS_S3A_REGION = "fs.s3a.region";
    private static final String FILE_IO_IMPL = CatalogProperties.FILE_IO_IMPL;
    private static final String PATH_STYLE_ACCESS = S3FileIOProperties.PATH_STYLE_ACCESS;
    private static final String S3_ENDPOINT = S3FileIOProperties.ENDPOINT;
    private static final String WAREHOUSE = "warehouse";
    private static final String LOCAL_RUN = "local.run";
    private static final String ORCHESTRATOR_URI = "orchestrator.uri";

    // KAFKA
    private static final String BOOSTRAP_SERVERS = "bootstrap.servers";
    private static final String TOPIC_NAME = "topic.name";
    private static final String KEYTAB_LOCATION = "keytab.location";
    private static final String KEYTAB_PRINCIPAL = "keytab.principal";

    private static volatile PropertiesHolder instance;
    private final Properties props;

    private PropertiesHolder() {
        try {
            props = new Properties();
            props.load(PropertiesHolder.class.getClassLoader().getResourceAsStream("application.properties"));
        } catch (IOException ex) {
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

    // ✅ ВСЕ НАСТРОЙКИ ДЛЯ HADOOP (Hive Metastore + S3A)
    public Map<String, String> getEntriesHMS() {
        Map<String, String> conf = new HashMap<>();
        conf.put(HIVE_METASTORE_LOCAL, props.getProperty(HIVE_METASTORE_LOCAL));
        conf.put(HIVE_METASTORE_URIS, props.getProperty(HIVE_METASTORE_URIS));
        conf.put(FS_S3A_ACCESS_KEY, props.getProperty(FS_S3A_ACCESS_KEY));
        conf.put(FS_S3A_SECRET_KEY, props.getProperty(FS_S3A_SECRET_KEY));
        conf.put(FS_S3A_CONNECTIONS_SSL_ENABLED, props.getProperty(FS_S3A_CONNECTIONS_SSL_ENABLED));
        conf.put(FS_S3A_AWS_CREDENTIALS_PROVIDER, props.getProperty(FS_S3A_AWS_CREDENTIALS_PROVIDER));
        conf.put(FS_S3A_PATH_STYLE_ACCESS, props.getProperty(FS_S3A_PATH_STYLE_ACCESS));
        conf.put(FS_S3A_ENDPOINT, props.getProperty(FS_S3A_ENDPOINT));
        conf.put(FS_S3A_REGION, props.getProperty(FS_S3A_REGION));
        conf.put(FILE_IO_IMPL, props.getProperty(FILE_IO_IMPL));
        return conf;
    }

    // ✅ НАСТРОЙКИ ДЛЯ ICEBERG (S3FileIO)
    public Map<String, String> getEntriesS3() {
        Map<String, String> conf = new HashMap<>();
        conf.put(WAREHOUSE, props.getProperty(WAREHOUSE));
        conf.put(PATH_STYLE_ACCESS, props.getProperty(PATH_STYLE_ACCESS));
        conf.put(S3_ENDPOINT, props.getProperty(S3_ENDPOINT));
        // при необходимости можно добавить другие s3.* настройки
        return conf;
    }

    public boolean isLocalRun() {
        return Boolean.parseBoolean(props.getProperty(LOCAL_RUN, "false"));
    }

    public String getOrchestratorUri() {
        return props.getProperty(ORCHESTRATOR_URI);
    }

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

    public String getAccessKey() {
        return props.getProperty(FS_S3A_ACCESS_KEY);
    }

    public String getSecretKey() {
        return props.getProperty(FS_S3A_SECRET_KEY);
    }
}
