package ru.x5.process;


import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
// Дедупликация убрана — выполняется в DeduplicationFilter до этого оператора
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
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
import ru.x5.model.*;
import ru.x5.decoder.CustomDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;


public class RawDataProcessFunction extends KeyedProcessFunction<String, String, RowData> {

    private static final Logger log = LogManager.getLogger("ru.x5.parser");

    public static final String SCHEMA_DICT = "dictionaries";
    public static final String CATALOG_DICT = "dictionaries";

    private final TransactionCorrector corrector;
    private final Map<String, Schema> icebergSchemas;
    private final Map<String, Schema> pstSchemas;

    // Счётчик для периодического логирования
    private transient long processedCount = 0;
    private static final long LOG_INTERVAL = 100_000L;
    private final HashMap<String, List<MarmFm>> marmFmMap = new HashMap<>();
    private final HashMap<String, List<MeanFm>> meanFmMap = new HashMap<>();
    private Long lastSnapshotMean = -1L;
    private Long lastSnapshotMarm = -1L;
    private transient CatalogLoader catalogLoaderDict;
    private transient TableIdentifier tableIdentifierMean;
    private transient TableIdentifier tableIdentifierMarm;

    private static final JAXBContext contextJAXB;

    static {
        try {
            contextJAXB = JAXBContext.newInstance(IDocWrapper.class);
        } catch (JAXBException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public RawDataProcessFunction(Map<String, Schema> icebergSchemas, Map<String, Schema> pstSchemas) {
        this.corrector = new TransactionCorrector();
        this.icebergSchemas = icebergSchemas;
        this.pstSchemas = pstSchemas;
    }

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext){
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.s3a.endpoint", "http://minio.minio.svc.cluster.local:9000");
        hadoopConf.set("fs.s3a.path.style.access", "true");
        hadoopConf.set("fs.s3a.access.key", "PeY5qkotF86JtHWbkjKe");
        hadoopConf.set("fs.s3a.secret.key", "xIGISyJTDSmhjhwmDeTe0bCm6jCFjGRPcrN0C2JY");

        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");
        hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        hadoopConf.set("fs.s3a.region", "us-east-1");
        hadoopConf.set("hive.metastore.uris", "thrift://hms.s3-hms.svc.cluster.local:9083");
        hadoopConf.setBoolean("fs.s3a.impl.disable.cache", true);

        // Catalog properties — S3FileIO вместо HadoopFileIO (обходит Flink S3 plugin)
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse", "s3a://warehouse");
        catalogProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProps.put("s3.endpoint", "http://minio.minio.svc.cluster.local:9000");
        catalogProps.put("s3.path-style-access", "true");
        catalogProps.put("s3.access-key-id", "PeY5qkotF86JtHWbkjKe");
        catalogProps.put("s3.secret-access-key", "xIGISyJTDSmhjhwmDeTe0bCm6jCFjGRPcrN0C2JY");

        catalogProps.put("client.region", "us-east-1");

        this.catalogLoaderDict = CatalogLoader.hive(CATALOG_DICT, hadoopConf, catalogProps);
        this.tableIdentifierMean = TableIdentifier.of(SCHEMA_DICT, "mean_fm");
        this.tableIdentifierMarm = TableIdentifier.of(SCHEMA_DICT, "marm_fm");
    }

    @Override
    public void processElement(String kafkaValue, Context context, Collector<RowData> out){
        processedCount++;
        // Логируем каждые 100К — чтобы не убить диск на 100 млн записей
        if (processedCount % LOG_INTERVAL == 0) {
            log.info("RAW_PROCESS_STATS: processed={}, subtask={}",
                    processedCount, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
        TimestampData timestampDataXml = TimestampData.fromLocalDateTime(LocalDateTime.now());
        LocalDate dateXml = timestampDataXml.toLocalDateTime().toLocalDate();
        try (InputStream inputStream = CustomDecoder.decodeAndDecompress(kafkaValue)) {
            Unmarshaller unmarshaller = contextJAXB.createUnmarshaller();
            // Fix: продолжать при ошибках JAXB-валидации
            unmarshaller.setEventHandler(event -> {
                log.warn("JAXB validation event: " + event.getMessage());
                return true;
            });
            IDocWrapper doc = (IDocWrapper) unmarshaller.unmarshal(inputStream);

            // Fix: fallback если коррекция упала
            List<BaseTransactionKey> corrBaseTransactionKeyList;
            try {
                corrBaseTransactionKeyList = corrector.performCorrection(doc);
            } catch (Exception e) {
                log.error("Correction failed, writing raw segments without correction", e);
                corrBaseTransactionKeyList = corrector.getListBaseTransactionKeys(
                        doc.getIdoc().getPostrCreateMultip());
            }
            List<RetailLineItem> retailLineItems = corrBaseTransactionKeyList.stream()
                    .filter(item -> item instanceof RetailLineItem)
                    .map(item -> (RetailLineItem) item)
                    .collect(Collectors.toList());
            try {
                getDataRetailLineItemFromDictionaries(retailLineItems); //добавление информации из справочников
            }catch(Exception e){
                log.error("Failed to retrieve dictionaries data for retaillineitem", e);
            }
            Set<String> autoMDSet = getAutoMDSet(corrBaseTransactionKeyList);   //получение автоуценок для retaillineitem
            Set<String> emptyEanSet = getEmptyEanSet(retailLineItems);
            // PST Tender: определяем наличие CERT_PARTY=RU02 среди TenderExtension
            boolean isCertParty = corrBaseTransactionKeyList.stream()
                    .filter(x -> x instanceof TenderExtension)
                    .map(x -> (TenderExtension) x)
                    .anyMatch(x -> x.getFieldName() != null && x.getFieldName().contains("CERT_PARTY")
                            && x.getFieldValue() != null && x.getFieldValue().contains("RU02"));
            // PST FinancialMovemen: собираем NOTE fieldvalue из TransactionExtension
            Map<String, String> noteFieldValueMap = new HashMap<>();
            Schema pstFinancialMovemenSchema = pstSchemas.get("PST_BPFINANCIALMOVEMEN");
            if (pstFinancialMovemenSchema != null) {
                for (BaseTransactionKey btk : corrBaseTransactionKeyList) {
                    if (btk instanceof TransactionExtension) {
                        TransactionExtension ext = (TransactionExtension) btk;
                        if (ext.getFieldName() != null && ext.getFieldName().contains("DOCNUMBER")) {
                            String txnKey = buildTxnKey(ext);
                            noteFieldValueMap.put(txnKey, ext.getFieldValue());
                        }
                    }
                }
            }
            for (BaseTransactionKey baseTransactionKey : corrBaseTransactionKeyList) {
                try {
                    // Существующая логика: запись в raw-таблицу
                    String segmentName = baseTransactionKey.getSegmentName();
                    Schema icebergSchema = getSchemaForSegment(segmentName);
                    if (icebergSchema == null) continue;
                    RowData rowData = baseTransactionKey.toRowData(icebergSchema, timestampDataXml, dateXml);
                    routeToSegment(context, rowData, segmentName);

                    // PST: для Transaction объектов формируем PST RowData из raw RowData
                    if (baseTransactionKey instanceof Transaction) {
                        Schema pstSchema = pstSchemas.get("PST_BPTRANSACTION");
                        if (pstSchema != null) {
                            Transaction transaction = (Transaction) baseTransactionKey;
                            RowData pstRowData = transaction.toRowDataPst(rowData, icebergSchema, pstSchema);
                            context.output(StreamSideOutputTag.getTag("PST_BPTRANSACTION"), pstRowData);
                        }
                    }

                    // PST: для FinancialMovemen объектов формируем PST RowData из raw + NOTE-логика
                    if (baseTransactionKey instanceof FinancialMovemen && pstFinancialMovemenSchema != null) {
                        FinancialMovemen fm = (FinancialMovemen) baseTransactionKey;
                        String txnKey = buildTxnKey(fm);
                        String noteFieldValue = noteFieldValueMap.get(txnKey);
                        RowData pstRowData = fm.toRowDataPst(rowData, icebergSchema, pstFinancialMovemenSchema, noteFieldValue);
                        context.output(StreamSideOutputTag.getTag("PST_BPFINANCIALMOVEMEN"), pstRowData);
                    }

                    // PST: для TransactionExtension объектов формируем PST RowData из raw
                    if (baseTransactionKey instanceof TransactionExtension) {
                        Schema pstSchema = pstSchemas.get("PST_BPTRANSACTEXTENSIO");
                        if (pstSchema != null) {
                            TransactionExtension ext = (TransactionExtension) baseTransactionKey;
                            RowData pstRowData = ext.toRowDataPst(rowData, icebergSchema, pstSchema);
                            context.output(StreamSideOutputTag.getTag("PST_BPTRANSACTEXTENSIO"), pstRowData);
                        }
                    }

                    // PST: для FinacialMovement объектов формируем PST RowData из raw
                    if (baseTransactionKey instanceof FinacialMovement) {
                        Schema pstSchema = pstSchemas.get("PST_BPFINANCIALMOVEMENTEXTENSIO");
                        if (pstSchema != null) {
                            FinacialMovement fmt = (FinacialMovement) baseTransactionKey;
                            RowData pstRowData = fmt.toRowDataPst(rowData, icebergSchema, pstSchema);
                            context.output(StreamSideOutputTag.getTag("PST_BPFINANCIALMOVEMENTEXTENSIO"), pstRowData);
                        }
                    }
                    if (baseTransactionKey instanceof RetailLineItem) {
                        Schema pstSchema = pstSchemas.get("PST_BPRETAILLINEITEM");
                        if (pstSchema != null) {
                            RetailLineItem retailLineItem = (RetailLineItem) baseTransactionKey;
                            if (retailLineItem.getHasEan() == null || Boolean.TRUE.equals(retailLineItem.getHasEan())) {  //если ean найден или нет маппинга
                                String key = retailLineItem.createKeyLineItem();
                                RowData pstRowData = retailLineItem.toRowDataPst(pstSchema, timestampDataXml, dateXml,
                                        autoMDSet.contains(key));
                                context.output(StreamSideOutputTag.getTag("PST_BPRETAILLINEITEM"), pstRowData);
                            }
                        }
                    }

                    if (baseTransactionKey instanceof LineItemExtension) {
                        Schema pstSchema = pstSchemas.get("PST_BPLINEITEMEXTENSIO");
                        if (pstSchema != null) {
                            LineItemExtension lineItemExtension = (LineItemExtension) baseTransactionKey;
                            String key = lineItemExtension.createKeyLineItem();
                            if (!emptyEanSet.contains(key)) {   //удаление с пустым ean
                                RowData pstRowData = lineItemExtension.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                                context.output(StreamSideOutputTag.getTag("PST_BPLINEITEMEXTENSIO"), pstRowData);
                            }
                        }
                    }

                    if (baseTransactionKey instanceof LineItemDiscount) {
                        Schema pstSchema = pstSchemas.get("PST_BPLINEITEMDISCOUNT");
                        if (pstSchema != null) {
                            LineItemDiscount lineItemDiscount = (LineItemDiscount) baseTransactionKey;
                            String key = lineItemDiscount.createKeyLineItem();
                            if (!emptyEanSet.contains(key)) {   //удаление с пустым ean
                                RowData pstRowData = lineItemDiscount.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                                context.output(StreamSideOutputTag.getTag("PST_BPLINEITEMDISCOUNT"), pstRowData);
                            }
                        }
                    }

                    if (baseTransactionKey instanceof LineItemDiscExt) {
                        Schema pstSchema = pstSchemas.get("PST_BPLINEITEMDISCEXT");
                        if (pstSchema != null) {
                            LineItemDiscExt lineItemDiscExt = (LineItemDiscExt) baseTransactionKey;
                            String key = lineItemDiscExt.createKeyLineItem();
                            if (!emptyEanSet.contains(key)) {   //удаление с пустым ean
                                RowData pstRowData = lineItemDiscExt.toRowDataPst(pstSchema, timestampDataXml, dateXml);
                                context.output(StreamSideOutputTag.getTag("PST_BPLINEITEMDISCEXT"), pstRowData);
                            }
                        }
                    }

                    if (baseTransactionKey instanceof TransactDiscExt) {
                        Schema pstSchema = pstSchemas.get("PST_BPTRANSACTDISCEXT");
                        if (pstSchema != null) {
                            TransactDiscExt transactDiscExt = (TransactDiscExt) baseTransactionKey;
                            RowData pstRowData = transactDiscExt.toRowData(pstSchema, timestampDataXml, dateXml);   //в pst записывается без изменений
                            context.output(StreamSideOutputTag.getTag("PST_BPTRANSACTDISCEXT"), pstRowData);
                        }
                    }

                    // PST: для Tender объектов формируем PST RowData из raw (3108→3123 при CERT_PARTY=RU02)
                    if (baseTransactionKey instanceof Tender) {
                        Schema pstSchema = pstSchemas.get("PST_BPTENDER");
                        if (pstSchema != null) {
                            Tender tender = (Tender) baseTransactionKey;
                            RowData pstRowData = tender.toRowDataPst(rowData, icebergSchema, pstSchema, isCertParty);
                            context.output(StreamSideOutputTag.getTag("PST_BPTENDER"), pstRowData);
                        }
                    }

                    // PST: для TenderExtension объектов формируем PST RowData из raw (CERT_PRICE → часть после ".")
                    if (baseTransactionKey instanceof TenderExtension) {
                        Schema pstSchema = pstSchemas.get("PST_BPTENDEREXTENSIONS");
                        if (pstSchema != null) {
                            TenderExtension tenderExt = (TenderExtension) baseTransactionKey;
                            RowData pstRowData = tenderExt.toRowDataPst(rowData, icebergSchema, pstSchema);
                            context.output(StreamSideOutputTag.getTag("PST_BPTENDEREXTENSIONS"), pstRowData);
                        }
                    }

                } catch (Exception e) {
                    log.error("Error processing segment " + baseTransactionKey.getSegmentName()
                            + " for txn " + baseTransactionKey.getTransactionKey(), e);
                }
            }
            // Синтетические тендеры (коррекция разницы сумм + тендеры для txnType=1014) → PST
            try {
                processTenderSynthetic(context, corrBaseTransactionKeyList, timestampDataXml, dateXml);
            } catch (Exception e) {
                log.error("Error processing synthetic tender pst ", e);
            }
            SourceDocument sourceDoc = doc.getIdoc().getPostrCreateMultip().getSourceDocument();
            String segmentName = sourceDoc.getSegmentName();
            Schema icebergSchema = getSchemaForSegment(segmentName);
            routeToSegment(context, sourceDoc.toRowData(icebergSchema, timestampDataXml, dateXml), segmentName);
        } catch (Exception e) {
            log.warn("Error message: " + kafkaValue);
            log.error("Error on message parsing: ", e);
        }
    }

    /**
     * Создаёт синтетические тендеры (коррекция разницы сумм, тендеры для txnType=1014)
     * и записывает их в PST-таблицу PST_BPTENDER.
     * Бизнес-логика из TenderPstProcessor.prepareFirstPart/prepareSecondPart.
     */
    private void processTenderSynthetic(Context context, List<BaseTransactionKey> corrBaseTransactionKeyList,
                                         TimestampData timestampDataXml, LocalDate dateXml) {
        Schema pstSchema = pstSchemas.get("PST_BPTENDER");
        if (pstSchema == null) return;

        TenderPstProcessor tenderPstProcessor = new TenderPstProcessor(corrBaseTransactionKeyList);
        List<Tender> syntheticTenders = tenderPstProcessor.prepareTenderPst();

        // prepareThirdPart мутирует существующие тендеры (3108→3123) — эта логика
        // теперь в Tender.toRowDataPst(), поэтому фильтруем только синтетические тендеры
        // (те, у которых tenderTypeCode = "3101" — созданные prepareFirstPart/prepareSecondPart)
        // Дедупликация: один синтетический тендер на транзакцию
        Set<String> writtenSyntheticKeys = new HashSet<>();
        for (Tender tender : syntheticTenders) {
            if ("3101".equals(tender.getTenderTypeCode())) {
                String synKey = tender.getRetailStoreId() + "|" + tender.getBusinessDayDate() + "|"
                        + tender.getWorkstationId() + "|" + tender.getTransactionSequenceNumber()
                        + "|" + tender.getTenderAmount();
                if (writtenSyntheticKeys.contains(synKey)) {
                    continue; // пропускаем дубль
                }
                writtenSyntheticKeys.add(synKey);
                Schema rawSchema = getSchemaForSegment("E1BPTENDER");
                if (rawSchema != null) {
                    RowData rawRowData = tender.toRowData(rawSchema, timestampDataXml, dateXml);
                    RowData pstRowData = tender.toRowDataPst(rawRowData, rawSchema, pstSchema, false);
                    context.output(StreamSideOutputTag.getTag("PST_BPTENDER"), pstRowData);
                }
            }
        }
    }

    private Set<String> getEmptyEanSet(List<RetailLineItem> retailLineItemList) {
        Set<String> emptyEanSet = new HashSet<>();  //получение товарных позиций без ean
        for (RetailLineItem retailLineItem : retailLineItemList) {
            if (retailLineItem.getMatnrPadded() != null &&
                    Objects.equals(retailLineItem.getLfnum(), "00001") &&  //удаление строк - маппинг существует, но ean пустой
                    (retailLineItem.getEan11() == null || retailLineItem.getEan11().isEmpty())) {
                String retailLineItemKey = retailLineItem.createKeyLineItem();
                emptyEanSet.add(retailLineItemKey);
            }
        }
        return emptyEanSet;
    }

    private Set<String> getAutoMDSet(List<BaseTransactionKey> corrBaseTransactionKeyList) {
        Set<String> autoMDSet = new HashSet<>();
        for (BaseTransactionKey key : corrBaseTransactionKeyList) {
            if (key instanceof LineItemExtension) {
                LineItemExtension ext = (LineItemExtension) key;
                if (ext.getFieldGroup() != null && ext.getFieldGroup().equals("POS")
                        && ext.getFieldName() != null && ext.getFieldName().equals("AUTO_MD")
                        && ext.getFieldValue() != null && ext.getFieldValue().equals("Y")) {
                    String itemKey = ext.createKeyLineItem();
                    autoMDSet.add(itemKey);  //сбор позиций с автоуценкой для подмены retailtypecode
                }
            }
        }
        return autoMDSet;
    }

    private void routeToSegment(Context context, RowData rowData, String segmentName) {
        if (segmentName == null || segmentName.isEmpty()) {
            log.warn("Null or empty segment-name, skipping RowData");
            return;
        }
        OutputTag<RowData> tag = StreamSideOutputTag.getTag(segmentName);
        if (tag == null) {
            log.warn("Unknown segment-name: "+segmentName);
            return;
        }
        Schema schema = icebergSchemas.get(segmentName);
        if (schema == null) {
            log.warn("No schema found for segment: "+segmentName);
            return;
        }
        context.output(tag, rowData);
    }
    private Schema getSchemaForSegment(String segmentName) {
        Schema schema = icebergSchemas.get(segmentName);
        if (schema == null) {
            log.warn("No schema found for segment: " + segmentName);
        }
        return schema;
    }

    private String buildTxnKey(BaseTransactionKey btk) {
        return String.join("|",
                btk.getRetailStoreId() != null ? btk.getRetailStoreId() : "",
                btk.getBusinessDayDate() != null ? btk.getBusinessDayDate() : "",
                btk.getWorkstationId() != null ? btk.getWorkstationId() : "",
                btk.getTransactionSequenceNumber() != null ? btk.getTransactionSequenceNumber() : ""
        );
    }

    private void getDataRetailLineItemFromDictionaries(List<RetailLineItem> retailLineItemList) {
        boolean isUpdate = false;
        for (RetailLineItem retailLineItem : retailLineItemList) {
            if (retailLineItem.getItemId() != null && retailLineItem.getItemId().contains("UP")) {
                String[] parts = retailLineItem.getItemId().split("UP", 2);
                if (parts.length == 2) {
                    if (!isUpdate) {    //обновление справочников
                        updateDictMean();
                        updateDictMarm();
                        isUpdate = true;
                    }
                    String partBefore = parts[0];
                    String partAfter = parts[1];
                    retailLineItem.setMatnrPadded(String.format("%18s", partBefore).replace(' ', '0'));
                    retailLineItem.setUmrezVal(partAfter);
                    List<MarmFm> marmFmList = marmFmMap.getOrDefault(retailLineItem.getMatnrPadded(), Collections.emptyList());
                    //поиск по справочникам
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
                try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                        .build()) {
                    meanFmMap.clear();
                    for (Record record : records) {
                        MeanFm meanFm = new MeanFm();
                        meanFm.setMatnr((String) record.getField("matnr"));
                        meanFm.setMeinh((String) record.getField("meinh"));
                        meanFm.setLfnum((String) record.getField("lfnum"));
                        meanFm.setEan11((String) record.getField("ean11"));
                        List<MeanFm> meanFmlist = meanFmMap.computeIfAbsent(meanFm.getMatnr(), m -> new ArrayList<>());
                        meanFmlist.add(meanFm);
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
                try (CloseableIterable<Record> records = IcebergGenerics.read(table)
                        .build()) {
                    marmFmMap.clear();
                    for (Record record : records) {
                        MarmFm marmFm = new MarmFm();
                        marmFm.setMatnr((String) record.getField("matnr"));
                        marmFm.setMeinh((String) record.getField("meinh"));
                        marmFm.setUmrez((String) record.getField("umrez"));
                        List<MarmFm> marmFmlist = marmFmMap.computeIfAbsent(marmFm.getMatnr(), m -> new ArrayList<>());
                        marmFmlist.add(marmFm);
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