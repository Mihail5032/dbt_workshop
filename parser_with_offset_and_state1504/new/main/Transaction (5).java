package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;

import java.time.LocalDate;
import java.time.LocalDateTime;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class Transaction extends BaseTransactionKey {
    @XmlElement(name = "BEGINDATETIMESTAMP")
    private String beginDateTimestamp;
    @XmlElement(name = "ENDDATETIMESTAMP")
    private String endDateTimestamp;
    @XmlElement(name = "DEPARTMENT")
    private String department;
    @XmlElement(name = "OPERATORQUALIFIER")
    private String operatorQualifier;
    @XmlElement(name = "OPERATORID")
    private String operatorId;
    @XmlElement(name = "TRANSACTIONCURRENCY")
    private String transactionCurrency;
    @XmlElement(name = "TRANSACTIONCURRENCY_ISO")
    private String transactionCurrencyISO;
    @XmlElement(name = "PARTNERQUALIFIER")
    private String partnerQualifier;
    @XmlElement(name = "PARTNERID")
    private String partnerId;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName())
                .begindatetimestamp(beginDateTimestamp).enddatetimestamp(endDateTimestamp)
                .department(department).operatorqualifier(operatorQualifier)
                .operatorid(operatorId)
                .transactioncurrency(transactionCurrency)
                .transactioncurrency_iso(transactionCurrencyISO)
                .partnerqualifier(partnerQualifier).partnerid(partnerId)
                .is_aligned_tran(String.valueOf(super.getIs_aligned_tran()))
                .build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }

    @Override
    public String getSegmentName(){return "E1BPTRANSACTION";}

    // ======================== PST-логика ========================

    /**
     * Формирует RowData для PST-таблицы staging_new.bptransaction_new_pst_2.
     * Читает данные из rawRowData (результат toRowData()), применяет PRE+PST бизнес-логику.
     * Объединяет трансформации из bptransaction_new_pre и bptransaction_new_pst.
     *
     * @param rawRowData — RowData из toRowData() (raw-слой)
     * @param rawSchema  — схема raw-таблицы (raw_bptransaction)
     * @param pstSchema  — схема PST-таблицы (bptransaction_new_pst_2)
     */
    public RowData toRowDataPst(RowData rawRowData, Schema rawSchema, Schema pstSchema) {
        int schemaSize = pstSchema.columns().size();
        GenericRowData pstRowData = new GenericRowData(schemaSize);

        // ---- Копируем поля напрямую из raw RowData (типы совпадают) ----
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "rtl_txn_rk");              // varbinary
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "operatorid");               // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "retailstoreid");            // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactioncurrency");      // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactionsequencenumber");// varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactiontypecode");      // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "workstationid");            // varchar (vprok.express уже применён)
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts");                   // timestamp
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_date_xml");            // date
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts_xml");              // timestamp
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "is_aligned_tran");         // boolean

        // ---- PRE-логика: извлекаем timestamps (raw может быть timestamp(6) или varchar) ----
        Object rawBeginObj = extractField(rawRowData, rawSchema, "begindatetimestamp");
        Object rawEndObj = extractField(rawRowData, rawSchema, "enddatetimestamp");

        LocalDateTime beginRaw = extractLocalDateTime(rawBeginObj);
        LocalDateTime endRaw = extractLocalDateTime(rawEndObj);

        // begindatetimestamp: если null → берём end
        LocalDateTime beginResult = (beginRaw == null) ? endRaw : beginRaw;

        // enddatetimestamp: если begin > end ИЛИ end is null → берём begin
        LocalDateTime endResult;
        if (endRaw == null || (beginRaw != null && beginRaw.isAfter(endRaw))) {
            endResult = beginRaw;
        } else {
            endResult = endRaw;
        }

        // Устанавливаем begindatetimestamp (timestamp(6)) в PST
        setPstField(pstRowData, pstSchema, "begindatetimestamp",
                beginResult != null ? TimestampData.fromLocalDateTime(beginResult) : null);

        // Устанавливаем enddatetimestamp (timestamp(6)) в PST
        setPstField(pstRowData, pstSchema, "enddatetimestamp",
                endResult != null ? TimestampData.fromLocalDateTime(endResult) : null);

        // ---- businessdaydate: raw может быть date или varchar + PST-коррекция для type '11%' ----
        Object rawBizDateObj = extractField(rawRowData, rawSchema, "businessdaydate");
        LocalDate businessDay = extractLocalDate(rawBizDateObj);

        // PST-коррекция: transactiontypecode начинается с "11"
        Object rawTxnTypeObj = extractField(rawRowData, rawSchema, "transactiontypecode");
        String txnTypeCode = extractString(rawTxnTypeObj);

        if (txnTypeCode != null && txnTypeCode.startsWith("11")
                && endResult != null && businessDay != null) {
            LocalDate endDate = endResult.toLocalDate();
            if (!endDate.equals(businessDay)) {
                businessDay = endDate;
            }
        }

        setPstField(pstRowData, pstSchema, "businessdaydate",
                businessDay != null ? (int) businessDay.toEpochDay() : null);

        return pstRowData;
    }

}