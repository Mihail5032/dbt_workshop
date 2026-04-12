package ru.x5.process;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
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
 * First pipeline operator: parses IDOC from Kafka, applies corrections,
 * enriches from dictionaries, converts ALL segments to RowData (RAW + PST),
 * serializes them to compact byte[], groups by transaction, and emits
 * lightweight TransactionBundle(txnKey, byte[]).
 *
 * SourceDocument (IDOC metadata) is emitted separately via side output.
 */
public class IdocParser extends ProcessFunction<String, TransactionBundle> {

    private static final Logger log = LogManager.getLogger("ru.x5.parser");

    /** Side output for SourceDocument (one per IDOC, bypasses dedup) */
    public static final OutputTag<RowData> SOURCE_DOC_TAG =
            new OutputTag<RowData>("raw_bpsourcedocumentli") {};

    public static final String SCHEMA_DICT = "dictionaries";
    public static final String CATALOG_DICT = "dictionaries";

    private final Schema sourceDocSchema;
    private final Map<String, Schema> icebergSchemas;
    private final Map<String, Schema> pstSchemas;
    private final TransactionCorrector corrector;

    // Dictionaries
    private final HashMap<String, List<MarmFm>> marmFmMap = new HashMap<>();
    private final HashMap<String, List<MeanFm>> meanFmMap = new HashMap<>();
    private Long lastSnapshotMean = -1L;
    private Long lastSnapshotMarm = -1L;
    private transient CatalogLoader catalogLoaderDict;
    private transient TableIdentifier tableIdentifierMean;
    private transient TableIdentifier tableIdentifierMarm;

    private transient long processedCount = 0;
    private static final long LOG_INTERVAL = 100_000L;

    // Reusable JAXB unmarshaller (single-threaded in Flink operator)
    private transient Unmarshaller unmarshaller;

    // RowData serializers for compact binary encoding (avoids Kryo in keyBy)
    private transient Map<String, TypeSerializer<RowData>> rawSerializers;
    private transient Map<String, TypeSerializer<RowData>> pstSerializers;
    private transient DataOutputSerializer outputBuffer;

    private static final JAXBContext contextJAXB;
    static {
        try {
            contextJAXB = JAXBContext.newInstance(IDocWrapper.class);
        } catch (JAXBException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public IdocParser(Schema sourceDocSchema, Map<String, Schema> icebergSchemas, Map<String, Schema> pstSchemas) {
        this.sourceDocSchema = sourceDocSchema;
        this.icebergSchemas = icebergSchemas;
        this.pstSchemas = pstSchemas;
        this.corrector = new TransactionCorrector();
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) throws Exception {
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

        // Reuse unmarshaller — safe in single-threaded Flink operator
        this.unmarshaller = contextJAXB.createUnmarshaller();
        this.unmarshaller.setEventHandler(event -> {
            log.warn("JAXB validation event: " + event.getMessage());
            return true;
        });

        // Create RowData serializers from Iceberg schemas
        this.rawSerializers = new HashMap<>();
        for (Map.Entry<String, Schema> entry : icebergSchemas.entrySet()) {
            rawSerializers.put(entry.getKey(), createSerializer(entry.getValue()));
        }
        this.pstSerializers = new HashMap<>();
        for (Map.Entry<String, Schema> entry : pstSchemas.entrySet()) {
            pstSerializers.put(entry.getKey(), createSerializer(entry.getValue()));
        }

        this.outputBuffer = new DataOutputSerializer(8192);
    }

    @SuppressWarnings("unchecked")
    private TypeSerializer<RowData> createSerializer(Schema icebergSchema) {
        RowType rowType = FlinkSchemaUtil.convert(icebergSchema);
        return (TypeSerializer<RowData>) (TypeSerializer<?>)
                new org.apache.flink.table.runtime.typeutils.RowDataSerializer(rowType);
    }

    // ======================== Inner class for temp storage ========================

    private static final class SegmentEntry {
        final String segmentName;
        final RowData rawRowData;
        final String pstSegmentName;
        final RowData pstRowData;

        SegmentEntry(String segmentName, RowData rawRowData,
                     String pstSegmentName, RowData pstRowData) {
            this.segmentName = segmentName;
            this.rawRowData = rawRowData;
            this.pstSegmentName = pstSegmentName;
            this.pstRowData = pstRowData;
        }
    }

    // ======================== processElement ========================

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
            IDocWrapper doc = (IDocWrapper) unmarshaller.unmarshal(inputStream);

            // === Corrections (IDOC level) ===
            List<BaseTransactionKey> corrected;
            try {
                corrected = corrector.performCorrection(doc);
            } catch (Exception e) {
                log.error("Correction failed, writing raw segments without correction", e);
                corrected = corrector.getListBaseTransactionKeys(
                        doc.getIdoc().getPostrCreateMultip());
            }

            // === Enrich RetailLineItem from dictionaries ===
            List<RetailLineItem> retailLineItems = corrected.stream()
                    .filter(item -> item instanceof RetailLineItem)
                    .map(item -> (RetailLineItem) item)
                    .collect(Collectors.toList());
            try {
                getDataRetailLineItemFromDictionaries(retailLineItems);
            } catch (Exception e) {
                log.error("Failed to retrieve dictionaries data for retaillineitem", e);
            }

            // === Group by transaction key ===
            Map<String, List<BaseTransactionKey>> txnGroups = new LinkedHashMap<>();
            for (BaseTransactionKey btk : corrected) {
                String key = buildTxnKey(btk);
                txnGroups.computeIfAbsent(key, k -> new ArrayList<>()).add(btk);
            }

            // === Build RowData, serialize to byte[], emit TransactionBundle ===
            for (Map.Entry<String, List<BaseTransactionKey>> entry : txnGroups.entrySet()) {
                String txnKey = entry.getKey();
                List<BaseTransactionKey> groupSegments = entry.getValue();

                List<SegmentEntry> entries = buildSegmentEntries(
                        groupSegments, timestampDataXml, dateXml);
                byte[] payload = serializeEntries(entries);

                out.collect(new TransactionBundle(txnKey, payload));
            }

            // === SourceDocument - per-IDOC, via side output ===
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

    // ======================== Build segment entries ========================

    private List<SegmentEntry> buildSegmentEntries(List<BaseTransactionKey> groupSegments,
                                                    TimestampData timestampDataXml, LocalDate dateXml) {
        // Compute per-transaction flags
        Set<String> autoMDSet = getAutoMDSet(groupSegments);
        Set<String> emptyEanSet = getEmptyEanSet(groupSegments);
        boolean isCertParty = checkIsCertParty(groupSegments);
        Map<String, String> noteFieldValueMap = buildNoteFieldValueMap(groupSegments);

        List<SegmentEntry> result = new ArrayList<>();

        for (BaseTransactionKey btk : groupSegments) {
            try {
                String segmentName = btk.getSegmentName();
                Schema icebergSchema = icebergSchemas.get(segmentName);
                if (icebergSchema == null) continue;

                // RAW RowData
                RowData rawRowData = btk.toRowData(icebergSchema, timestampDataXml, dateXml);

                // PST RowData
                String pstSegmentName = null;
                RowData pstRowData = null;

                if (btk instanceof Transaction) {
                    Schema pstSchema = pstSchemas.get("PST_BPTRANSACTION");
                    if (pstSchema != null) {
                        Transaction transaction = (Transaction) btk;
                        pstRowData = transaction.toRowDataPst(rawRowData, icebergSchema, pstSchema);
                        pstSegmentName = "PST_BPTRANSACTION";
                    }
                } else if (btk instanceof FinancialMovemen) {
                    Schema pstSchema = pstSchemas.get("PST_BPFINANCIALMOVEMEN");
                    if (pstSchema != null) {
                        FinancialMovemen fm = (FinancialMovemen) btk;
                        String txnKey = buildTxnKey(fm);
                        String noteFieldValue = noteFieldValueMap.get(txnKey);
                        pstRowData = fm.toRowDataPst(rawRowData, icebergSchema, pstSchema, noteFieldValue);
                        pstSegmentName = "PST_BPFINANCIALMOVEMEN";
                    }
                } else if (btk instanceof TransactionExtension) {
                    Schema pstSchema = pstSchemas.get("PST_BPTRANSACTEXTENSIO");
                    if (pstSchema != null) {
                        TransactionExtension ext = (TransactionExtension) btk;
                        pstRowData = ext.toRowDataPst(rawRowData, icebergSchema, pstSchema);
                        pstSegmentName = "PST_BPTRANSACTEXTENSIO";
                    }
                } else if (btk instanceof FinacialMovement) {
                    Schema pstSchema = pstSchemas.get("PST_BPFINANCIALMOVEMENTEXTENSIO");
                    if (pstSchema != null) {
                        FinacialMovement fmt = (FinacialMovement) btk;
                        pstRowData = fmt.toRowDataPst(rawRowData, icebergSchema, pstSchema);
                        pstSegmentName = "PST_BPFINANCIALMOVEMENTEXTENSIO";
                    }
                } else if (btk instanceof RetailLineItem) {
                    Schema pstSchema = pstSchemas.get("PST_BPRETAILLINEITEM");
                    if (pstSchema != null) {
                        RetailLineItem retailLineItem = (RetailLineItem) btk;
                        if (retailLineItem.getHasEan() == null || Boolean.TRUE.equals(retailLineItem.getHasEan())) {
                            String key = retailLineItem.createKeyLineItem();
                            pstRowData = retailLineItem.toRowDataPst(pstSchema, timestampDataXml, dateXml,
                                    autoMDSet.contains(key));
                            pstSegmentName = "PST_BPRETAILLINEITEM";
                        }
                    }
                } else if (btk instanceof LineItemExtension) {
                    Schema pstSchema = pstSchemas.get("PST_BPLINEITEMEXTENSIO");
                    if (pstSchema != null) {
                        LineItemExtension lineItemExtension = (LineItemExtension) btk;
                        String key = lineItemExtension.createKeyLineItem();
                        if (!emptyEanSet.contains(key)) {
                            pstRowData = lineItemExtension.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                            pstSegmentName = "PST_BPLINEITEMEXTENSIO";
                        }
                    }
                } else if (btk instanceof LineItemDiscount) {
                    Schema pstSchema = pstSchemas.get("PST_BPLINEITEMDISCOUNT");
                    if (pstSchema != null) {
                        LineItemDiscount lineItemDiscount = (LineItemDiscount) btk;
                        String key = lineItemDiscount.createKeyLineItem();
                        if (!emptyEanSet.contains(key)) {
                            pstRowData = lineItemDiscount.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                            pstSegmentName = "PST_BPLINEITEMDISCOUNT";
                        }
                    }
                } else if (btk instanceof LineItemDiscExt) {
                    Schema pstSchema = pstSchemas.get("PST_BPLINEITEMDISCEXT");
                    if (pstSchema != null) {
                        LineItemDiscExt lineItemDiscExt = (LineItemDiscExt) btk;
                        String key = lineItemDiscExt.createKeyLineItem();
                        if (!emptyEanSet.contains(key)) {
                            pstRowData = lineItemDiscExt.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                            pstSegmentName = "PST_BPLINEITEMDISCEXT";
                        }
                    }
                } else if (btk instanceof TransactDiscExt) {
                    Schema pstSchema = pstSchemas.get("PST_BPTRANSACTDISCEXT");
                    if (pstSchema != null) {
                        TransactDiscExt transactDiscExt = (TransactDiscExt) btk;
                        pstRowData = transactDiscExt.toRowData(pstSchema, timestampDataXml, dateXml);
                        pstSegmentName = "PST_BPTRANSACTDISCEXT";
                    }
                } else if (btk instanceof Tender) {
                    Schema pstSchema = pstSchemas.get("PST_BPTENDER");
                    if (pstSchema != null) {
                        Tender tender = (Tender) btk;
                        pstRowData = tender.toRowDataPst(rawRowData, icebergSchema, pstSchema, isCertParty);
                        pstSegmentName = "PST_BPTENDER";
                    }
                } else if (btk instanceof TenderExtension) {
                    Schema pstSchema = pstSchemas.get("PST_BPTENDEREXTENSIONS");
                    if (pstSchema != null) {
                        TenderExtension tenderExt = (TenderExtension) btk;
                        pstRowData = tenderExt.toRowDataPst(rawRowData, icebergSchema, pstSchema);
                        pstSegmentName = "PST_BPTENDEREXTENSIONS";
                    }
                }

                result.add(new SegmentEntry(segmentName, rawRowData, pstSegmentName, pstRowData));

            } catch (Exception e) {
                log.error("Error processing segment " + btk.getSegmentName()
                        + " for txn " + btk.getTransactionKey(), e);
            }
        }

        // === Synthetic tenders ===
        try {
            addSyntheticTenders(result, groupSegments, timestampDataXml, dateXml);
        } catch (Exception e) {
            log.error("Error processing synthetic tender pst ", e);
        }

        return result;
    }

    // ======================== Serialization ========================

    /**
     * Serializes segment entries to compact byte[] using Flink's RowDataSerializer.
     *
     * Binary format:
     *   int: segment count
     *   for each segment:
     *     boolean: hasRaw
     *     if hasRaw: UTF segmentName, then RowData via RowDataSerializer
     *     boolean: hasPst
     *     if hasPst: UTF pstSegmentName, then RowData via RowDataSerializer
     */
    private byte[] serializeEntries(List<SegmentEntry> entries) {
        try {
            outputBuffer.clear();
            outputBuffer.writeInt(entries.size());

            for (SegmentEntry entry : entries) {
                boolean hasRaw = entry.segmentName != null && entry.rawRowData != null;
                boolean hasPst = entry.pstSegmentName != null && entry.pstRowData != null;

                outputBuffer.writeBoolean(hasRaw);
                if (hasRaw) {
                    outputBuffer.writeUTF(entry.segmentName);
                    TypeSerializer<RowData> ser = rawSerializers.get(entry.segmentName);
                    ser.serialize(entry.rawRowData, outputBuffer);
                }

                outputBuffer.writeBoolean(hasPst);
                if (hasPst) {
                    outputBuffer.writeUTF(entry.pstSegmentName);
                    TypeSerializer<RowData> ser = pstSerializers.get(entry.pstSegmentName);
                    ser.serialize(entry.pstRowData, outputBuffer);
                }
            }

            return outputBuffer.getCopyOfBuffer();
        } catch (Exception e) {
            log.error("Failed to serialize segment entries", e);
            return new byte[0];
        }
    }

    // ======================== Synthetic tenders ========================

    private void addSyntheticTenders(List<SegmentEntry> result, List<BaseTransactionKey> groupSegments,
                                      TimestampData timestampDataXml, LocalDate dateXml) {
        Schema pstSchema = pstSchemas.get("PST_BPTENDER");
        if (pstSchema == null) return;

        TenderPstProcessor tenderPstProcessor = new TenderPstProcessor(groupSegments);
        List<Tender> syntheticTenders = tenderPstProcessor.prepareTenderPst();

        Set<String> writtenSyntheticKeys = new HashSet<>();
        for (Tender tender : syntheticTenders) {
            if ("3101".equals(tender.getTenderTypeCode())) {
                String synKey = tender.getRetailStoreId() + "|" + tender.getBusinessDayDate() + "|"
                        + tender.getWorkstationId() + "|" + tender.getTransactionSequenceNumber()
                        + "|" + tender.getTenderAmount();
                if (writtenSyntheticKeys.contains(synKey)) {
                    continue;
                }
                writtenSyntheticKeys.add(synKey);
                Schema rawSchema = icebergSchemas.get("E1BPTENDER");
                if (rawSchema != null) {
                    RowData rawRowData = tender.toRowData(rawSchema, timestampDataXml, dateXml);
                    RowData pstRowData = tender.toRowDataPst(rawRowData, rawSchema, pstSchema, false);
                    // Synthetic tenders: no RAW output, only PST
                    result.add(new SegmentEntry(null, null, "PST_BPTENDER", pstRowData));
                }
            }
        }
    }

    // ======================== Per-transaction flags ========================

    private Set<String> getAutoMDSet(List<BaseTransactionKey> segments) {
        Set<String> autoMDSet = new HashSet<>();
        for (BaseTransactionKey key : segments) {
            if (key instanceof LineItemExtension) {
                LineItemExtension ext = (LineItemExtension) key;
                if ("POS".equals(ext.getFieldGroup())
                        && "AUTO_MD".equals(ext.getFieldName())
                        && "Y".equals(ext.getFieldValue())) {
                    autoMDSet.add(ext.createKeyLineItem());
                }
            }
        }
        return autoMDSet;
    }

    private Set<String> getEmptyEanSet(List<BaseTransactionKey> segments) {
        Set<String> emptyEanSet = new HashSet<>();
        for (BaseTransactionKey btk : segments) {
            if (btk instanceof RetailLineItem) {
                RetailLineItem rli = (RetailLineItem) btk;
                if (rli.getMatnrPadded() != null &&
                        Objects.equals(rli.getLfnum(), "00001") &&
                        (rli.getEan11() == null || rli.getEan11().isEmpty())) {
                    emptyEanSet.add(rli.createKeyLineItem());
                }
            }
        }
        return emptyEanSet;
    }

    private boolean checkIsCertParty(List<BaseTransactionKey> segments) {
        for (BaseTransactionKey x : segments) {
            if (x instanceof TenderExtension) {
                TenderExtension te = (TenderExtension) x;
                if (te.getFieldName() != null && te.getFieldName().contains("CERT_PARTY")
                        && te.getFieldValue() != null && te.getFieldValue().contains("RU02")) {
                    return true;
                }
            }
        }
        return false;
    }

    private Map<String, String> buildNoteFieldValueMap(List<BaseTransactionKey> segments) {
        Map<String, String> map = new HashMap<>();
        for (BaseTransactionKey btk : segments) {
            if (btk instanceof TransactionExtension) {
                TransactionExtension ext = (TransactionExtension) btk;
                if (ext.getFieldName() != null && ext.getFieldName().contains("DOCNUMBER")) {
                    map.put(buildTxnKey(ext), ext.getFieldValue());
                }
            }
        }
        return map;
    }

    // ======================== Utils ========================

    private String buildTxnKey(BaseTransactionKey btk) {
        return String.join("|",
                btk.getRetailStoreId() != null ? btk.getRetailStoreId() : "",
                btk.getBusinessDayDate() != null ? btk.getBusinessDayDate() : "",
                btk.getWorkstationId() != null ? btk.getWorkstationId() : "",
                btk.getTransactionSequenceNumber() != null ? btk.getTransactionSequenceNumber() : "");
    }

    // ======================== Dictionaries ========================

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