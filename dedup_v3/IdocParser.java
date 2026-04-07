package ru.x5.process;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.x5.config.PropertiesHolder;
import ru.x5.decoder.CustomDecoder;
import ru.x5.model.*;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Первый оператор пайплайна: парсит IDOC из Kafka, применяет коррекции,
 * обогащает из справочников, группирует сегменты по транзакциям
 * и эмитит по одному TransactionBundle на транзакцию.
 *
 * SourceDocument (метаданные IDOC) эмитится отдельно через side output.
 */
public class IdocParser extends ProcessFunction<String, TransactionBundle> {

    private static final Logger log = LogManager.getLogger("ru.x5.parser");

    /** Side output для SourceDocument (один на IDOC, минуя дедупликацию) */
    public static final OutputTag<RowData> SOURCE_DOC_TAG =
            new OutputTag<RowData>("raw_bpsourcedocumentli") {};

    public static final String SCHEMA_DICT = "dictionaries";
    public static final String CATALOG_DICT = "dictionaries";

    private final Schema sourceDocSchema;
    private final TransactionCorrector corrector;

    // Справочники
    private final HashMap<String, List<MarmFm>> marmFmMap = new HashMap<>();
    private final HashMap<String, List<MeanFm>> meanFmMap = new HashMap<>();
    private Long lastSnapshotMean = -1L;
    private Long lastSnapshotMarm = -1L;
    private transient CatalogLoader catalogLoaderDict;
    private transient TableIdentifier tableIdentifierMean;
    private transient TableIdentifier tableIdentifierMarm;

    private transient long processedCount = 0;
    private static final long LOG_INTERVAL = 100_000L;

    private static final JAXBContext contextJAXB;
    static {
        try {
            contextJAXB = JAXBContext.newInstance(IDocWrapper.class);
        } catch (JAXBException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public IdocParser(Schema sourceDocSchema) {
        this.sourceDocSchema = sourceDocSchema;
        this.corrector = new TransactionCorrector();
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
        PropertiesHolder config = PropertiesHolder.getInstance();

        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.endpoint", config.getS3Endpoint());
        hadoopConf.set("fs.s3a.path.style.access", config.getPathStyleAccess());
        hadoopConf.set("fs.s3a.access.key", config.getAccessKey());
        hadoopConf.set("fs.s3a.secret.key", config.getSecretKey());
        hadoopConf.set("fs.s3a.connection.ssl.enabled", config.getConnectionsSslEnabled());
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
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

        this.catalogLoaderDict = CatalogLoader.hive(CATALOG_DICT, hadoopConf, catalogProps);
        this.tableIdentifierMean = TableIdentifier.of(SCHEMA_DICT, "mean_fm");
        this.tableIdentifierMarm = TableIdentifier.of(SCHEMA_DICT, "marm_fm");
    }

    @Override
    public void processElement(String kafkaValue, Context ctx, Collector<TransactionBundle> out) {
        processedCount++;
        if (processedCount % LOG_INTERVAL == 0) {
            log.info("IDOC_PARSER_STATS: processed={}", processedCount);
        }

        LocalDateTime now = LocalDateTime.now();
        TimestampData timestampDataXml = TimestampData.fromLocalDateTime(now);
        LocalDate dateXml = now.toLocalDate();

        try (InputStream inputStream = CustomDecoder.decodeAndDecompress(kafkaValue)) {
            Unmarshaller unmarshaller = contextJAXB.createUnmarshaller();
            unmarshaller.setEventHandler(event -> {
                log.warn("JAXB validation event: " + event.getMessage());
                return true;
            });
            IDocWrapper doc = (IDocWrapper) unmarshaller.unmarshal(inputStream);

            // === Коррекции (на уровне всего IDOC) ===
            List<BaseTransactionKey> corrected;
            try {
                corrected = corrector.performCorrection(doc);
            } catch (Exception e) {
                log.error("Correction failed, writing raw segments without correction", e);
                corrected = corrector.getListBaseTransactionKeys(
                        doc.getIdoc().getPostrCreateMultip());
            }

            // === Обогащение RetailLineItem из справочников ===
            List<RetailLineItem> retailLineItems = corrected.stream()
                    .filter(item -> item instanceof RetailLineItem)
                    .map(item -> (RetailLineItem) item)
                    .collect(Collectors.toList());
            try {
                getDataRetailLineItemFromDictionaries(retailLineItems);
            } catch (Exception e) {
                log.error("Failed to retrieve dictionaries data for retaillineitem", e);
            }

            // === Группировка по транзакциям ===
            Map<String, List<BaseTransactionKey>> txnGroups = new LinkedHashMap<>();
            for (BaseTransactionKey btk : corrected) {
                String key = buildTxnKey(btk);
                txnGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(btk);
            }

            // Эмит: один TransactionBundle на транзакцию
            for (Map.Entry<String, List<BaseTransactionKey>> entry : txnGroups.entrySet()) {
                out.collect(new TransactionBundle(entry.getKey(), entry.getValue(), now, dateXml));
            }

            // === SourceDocument — per-IDOC, через side output ===
            SourceDocument sourceDoc = doc.getIdoc().getPostrCreateMultip().getSourceDocument();
            if (sourceDoc != null && sourceDocSchema != null) {
                ctx.output(SOURCE_DOC_TAG,
                        sourceDoc.toRowData(sourceDocSchema, timestampDataXml, dateXml));
            }

        } catch (Exception e) {
            log.warn("Error message: " + kafkaValue);
            log.error("Error on message parsing: ", e);
        }
    }

    private String buildTxnKey(BaseTransactionKey btk) {
        return String.join("|",
                btk.getRetailStoreId() != null ? btk.getRetailStoreId() : "",
                btk.getBusinessDayDate() != null ? btk.getBusinessDayDate() : "",
                btk.getWorkstationId() != null ? btk.getWorkstationId() : "",
                btk.getTransactionSequenceNumber() != null ? btk.getTransactionSequenceNumber() : "");
    }

    // ======================== Справочники ========================

    private void getDataRetailLineItemFromDictionaries(List<RetailLineItem> retailLineItemList) {
        boolean isUpdate = false;
        for (RetailLineItem retailLineItem : retailLineItemList) {
            if (retailLineItem.getItemId() != null && retailLineItem.getItemId().contains("UP")) {
                String[] parts = retailLineItem.getItemId().split("UP", 2);
                if (parts.length == 2) {
                    if (!isUpdate) {
                        updateDictMean();
                        updateDictMarm();
                        isUpdate = true;
                    }
                    String partBefore = parts[0];
                    String partAfter = parts[1];
                    retailLineItem.setMatnrPadded(String.format("%18s", partBefore).replace(' ', '0'));
                    retailLineItem.setUmrezVal(partAfter);
                    List<MarmFm> marmFmList = marmFmMap.getOrDefault(retailLineItem.getMatnrPadded(), Collections.emptyList());
                    for (MarmFm marmFm : marmFmList) {
                        if (retailLineItem.getMatnrPadded().equals(marmFm.getMatnr()) &&
                                retailLineItem.getUmrezVal().equals(marmFm.getUmrez())) {
                            retailLineItem.setMatnr(marmFm.getMatnr());
                            retailLineItem.setUmrez(marmFm.getUmrez());
                            retailLineItem.setMeinh(marmFm.getMeinh());
                            List<MeanFm> meanFmList = meanFmMap.getOrDefault(marmFm.getMatnr(), Collections.emptyList());
                            for (MeanFm meanFm : meanFmList) {
                                if (marmFm.getMatnr().equals(meanFm.getMatnr()) && marmFm.getMeinh() != null &&
                                        marmFm.getMeinh().equals(meanFm.getMeinh()) && meanFm.getLfnum() != null
                                        && meanFm.getLfnum().equals("00001")) {
                                    retailLineItem.setLfnum(meanFm.getLfnum());
                                    retailLineItem.setEan11(meanFm.getEan11());
                                    retailLineItem.setHasEan(meanFm.getEan11() != null && !meanFm.getEan11().isEmpty());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    public void updateDictMean() {
        try (TableLoader loader = TableLoader.fromCatalog(catalogLoaderDict, tableIdentifierMean)) {
            loader.open();
            Table table = loader.loadTable();
            Snapshot currentSnapshot = table.currentSnapshot();
            long snapshotId = currentSnapshot.snapshotId();
            if (lastSnapshotMean != snapshotId) {
                try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                    meanFmMap.clear();
                    for (Record record : records) {
                        MeanFm meanFm = new MeanFm();
                        meanFm.setMatnr((String) record.getField("matnr"));
                        meanFm.setMeinh((String) record.getField("meinh"));
                        meanFm.setLfnum((String) record.getField("lfnum"));
                        meanFm.setEan11((String) record.getField("ean11"));
                        meanFmMap.computeIfAbsent(meanFm.getMatnr(), m -> new ArrayList<>()).add(meanFm);
                    }
                }
                lastSnapshotMean = snapshotId;
                log.info("Dictionary 'mean_fm' updated from snapshot " + snapshotId);
            }
        } catch (Exception e) {
            log.warn("Error updating dictionary 'mean_fm': ", e);
        }
    }

    public void updateDictMarm() {
        try (TableLoader loader = TableLoader.fromCatalog(catalogLoaderDict, tableIdentifierMarm)) {
            loader.open();
            Table table = loader.loadTable();
            Snapshot currentSnapshot = table.currentSnapshot();
            long snapshotId = currentSnapshot.snapshotId();
            if (lastSnapshotMarm != snapshotId) {
                try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
                    marmFmMap.clear();
                    for (Record record : records) {
                        MarmFm marmFm = new MarmFm();
                        marmFm.setMatnr((String) record.getField("matnr"));
                        marmFm.setMeinh((String) record.getField("meinh"));
                        marmFm.setUmrez((String) record.getField("umrez"));
                        marmFmMap.computeIfAbsent(marmFm.getMatnr(), m -> new ArrayList<>()).add(marmFm);
                    }
                }
                lastSnapshotMarm = snapshotId;
                log.info("Dictionary 'marm_fm' updated from snapshot " + snapshotId);
            }
        } catch (Exception e) {
            log.warn("Error updating dictionary 'marm_fm': ", e);
        }
    }
}
