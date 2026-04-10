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
public class TenderExtension extends BaseTransactionKey {
    @XmlElement(name = "TENDERSEQUENCENUMBER")
    private String tenderSequenceNumber;
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
        String fieldNameStr = StringUtils.isBlank(fieldName) ? fieldName : fieldName.substring(0, Math.min(fieldName.length(), 10));
        return RowTablePart.fromBase(basePart)
                .segment_name(getSegmentName())
                .tendersequencenumber(tenderSequenceNumber)
                .fieldgroup(fieldGroupStr)
                .fieldname(fieldNameStr)
                .fieldvalue(fieldValue)
                .is_aligned_tran(String.valueOf(super.getIs_aligned_tran()))
                .build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }

    @Override
    public String getSegmentName(){return "E1BPTENDEREXTENSIONS";}

    // ======================== PST-логика ========================

    /**
     * Формирует RowData для PST-таблицы bptenderextensions_new_pst_2.
     * Читает данные из rawRowData (результат toRowData()), применяет PST бизнес-логику.
     *
     * @param rawRowData — RowData из toRowData() (raw-слой)
     * @param rawSchema  — схема raw-таблицы (raw_bptenderextensions)
     * @param pstSchema  — схема PST-таблицы (bptenderextensions_new_pst_2)
     */
    public RowData toRowDataPst(RowData rawRowData, Schema rawSchema, Schema pstSchema) {
        int schemaSize = pstSchema.columns().size();
        GenericRowData pstRowData = new GenericRowData(schemaSize);

        // ---- Копируем поля напрямую из raw RowData (типы совпадают) ----
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "rtl_txn_rk");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "rtl_txn_tender_rk");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "retailstoreid");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactionsequencenumber");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactiontypecode");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "workstationid");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "tendersequencenumber");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "fieldgroup");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "fieldname");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_date_xml");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts_xml");

        // ---- businessdaydate: raw varchar → PST date ----
        Object rawBizDate = extractField(rawRowData, rawSchema, "businessdaydate");
        LocalDate bizDate = extractLocalDate(rawBizDate);
        if (bizDate != null) {
            setPstField(pstRowData, pstSchema, "businessdaydate", (int) bizDate.toEpochDay());
        }

        // ---- fieldvalue: CERT_PRICE → берём часть после "." ----
        Object rawFieldName = extractField(rawRowData, rawSchema, "fieldname");
        Object rawFieldValue = extractField(rawRowData, rawSchema, "fieldvalue");
        String fn = extractString(rawFieldName);
        String fv = extractString(rawFieldValue);

        if (fn != null && fn.contains("CERT_PRICE") && fv != null && fv.contains(".")) {
            String[] parts = fv.split("\\.");
            if (parts.length > 1) {
                setPstField(pstRowData, pstSchema, "fieldvalue", StringData.fromString(parts[1]));
            } else {
                setPstField(pstRowData, pstSchema, "fieldvalue", StringData.fromString(fv));
            }
        } else {
            copyField(rawRowData, rawSchema, pstRowData, pstSchema, "fieldvalue");
        }

        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "is_aligned_tran");

        return pstRowData;
    }
}