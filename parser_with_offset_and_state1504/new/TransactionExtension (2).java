package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;

import java.time.LocalDate;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class TransactionExtension extends BaseTransactionKey {
    @XmlElement(name = "FIELDGROUP")
    private String fieldGroup;
    @XmlElement(name = "FIELDNAME")
    private String fieldName;
    @XmlElement(name = "FIELDVALUE")
    private String fieldValue;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        String fieldGroupStr = StringUtils.isBlank(fieldGroup) ? fieldGroup : fieldGroup.substring(0, Math.min(fieldGroup.length(), 5));
        return RowTablePart.fromBase(basePart)
                .segment_name(getSegmentName())
                .fieldgroup(fieldGroupStr)
                .fieldname(prepareFieldName())
                .fieldvalue(prepareFieldValue())
                .is_head_bonid(prepareIsHeadBonid())
                .is_aligned_tran(String.valueOf(super.getIs_aligned_tran()))
                .build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }

    @Override
    public String getSegmentName(){return "E1BPTRANSACTEXTENSIO";}

    private String prepareFieldName() {
        if (StringUtils.isBlank(fieldName)) {
            return fieldName;
        }
        if (fieldName.contains("DOCNUMBER")) {
            return "NOTE";
        }
        if (fieldName.contains("ReceiptNum")) {
            return "DOCNUMBER";
        }
        return fieldName.substring(0, Math.min(fieldName.length(), 10));
    }

    /**
     * Расхождение #16: для fieldname = 'DOCNUMBER' (в XML это ReceiptNum) обрезаем fieldvalue
     * до 10 символов с конца (берём первые 10). В SAP поле ZCHECKNO имеет длину 10.
     */
    private String prepareFieldValue() {
        if (fieldValue != null
                && fieldName != null && fieldName.contains("ReceiptNum")
                && fieldValue.length() > 10) {
            return fieldValue.substring(0, 10);
        }
        return fieldValue;
    }

    private String prepareIsHeadBonid() {
        if (StringUtils.isBlank(fieldGroup) || StringUtils.isBlank(fieldName)) {
            return "0";
        }
        if(fieldGroup.contains("HEAD") && fieldName.contains("BONID")) {
            return "1";
        } else {
            return "0";
        }

    }

    // ======================== PST-логика ========================

    /**
     * Формирует RowData для PST-таблицы staging_new.bptransactextensio_new_pst_2.
     * Читает данные из rawRowData (результат toRowData()), применяет PST бизнес-логику.
     *
     * @param rawRowData — RowData из toRowData() (raw-слой)
     * @param rawSchema  — схема raw-таблицы (bptransactextensio_pre)
     * @param pstSchema  — схема PST-таблицы (bptransactextensio_new_pst_2)
     */
    public RowData toRowDataPst(RowData rawRowData, Schema rawSchema, Schema pstSchema) {
        int schemaSize = pstSchema.columns().size();
        GenericRowData pstRowData = new GenericRowData(schemaSize);

        // ---- Копируем поля напрямую из raw RowData (типы совпадают) ----
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "rtl_txn_rk");              // varbinary
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "fieldgroup");               // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "fieldname");                // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "retailstoreid");            // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactionsequencenumber");// varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactiontypecode");      // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "workstationid");            // varchar
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts");                  // timestamp
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_date_xml");            // date
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts_xml");              // timestamp

        // ---- businessdaydate: raw может быть date (int) или varchar (StringData) ----
        Object rawBizDate = extractField(rawRowData, rawSchema, "businessdaydate");
        LocalDate bizDate = extractLocalDate(rawBizDate);
        if (bizDate != null) {
            setPstField(pstRowData, pstSchema, "businessdaydate", (int) bizDate.toEpochDay());
        }

        // ---- fieldvalue: NULL когда fieldname = 'MobileNum' или 'MailClient' ----
        boolean isMasked = fieldName != null
                && (fieldName.contains("MobileNum") || fieldName.contains("MailClient"));
        if (!isMasked) {
            copyField(rawRowData, rawSchema, pstRowData, pstSchema, "fieldvalue");
        }
        // else: fieldvalue остаётся null в PST

        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "is_aligned_tran");

        return pstRowData;
    }

}