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

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class FinacialMovement extends BaseTransactionKey {
    @XmlElement(name = "FINANCIALSEQUENCENUMBER")
    private String financialSequenceNumber;
    @XmlElement(name = "FIELDGROUP")
    private String fieldGroup;
    @XmlElement(name = "FIELDNAME")
    private String fieldName;
    @XmlElement(name = "FIELDVALUE")
    private String fieldValue;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName()).financialsequencenumber(financialSequenceNumber).fieldgroup(fieldGroup)
                .fieldname(fieldName).fieldvalue(fieldValue).build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }
    @Override
    public String getSegmentName(){return "E1BPFINACIALMOVEMENT";}

    // ======================== PST-логика ========================

    /**
     * Формирует RowData для PST-таблицы staging_new.bpfinancialmovementextensio_new_pst_2.
     * Читает данные из rawRowData (результат toRowData()), применяет PST бизнес-логику.
     *
     * @param rawRowData — RowData из toRowData() (raw-слой)
     * @param rawSchema  — схема raw-таблицы (raw_bpfinacialmovement)
     * @param pstSchema  — схема PST-таблицы (bpfinancialmovementextensio_new_pst_2)
     */
    public RowData toRowDataPst(RowData rawRowData, Schema rawSchema, Schema pstSchema) {
        int schemaSize = pstSchema.columns().size();
        GenericRowData pstRowData = new GenericRowData(schemaSize);

        // Копируем поля напрямую из raw RowData (типы совпадают: varbinary, varchar, timestamp, date)
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "rtl_txn_rk");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "rtl_txn_fin_rk");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "retailstoreid");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactiontypecode");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "workstationid");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactionsequencenumber");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "fieldvalue");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_date_xml");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts_xml");

        // businessdaydate: raw может быть date (int) или varchar (StringData)
        Object rawBizDate = extractField(rawRowData, rawSchema, "businessdaydate");
        LocalDate bizDate = extractLocalDate(rawBizDate);
        if (bizDate != null) {
            setPstField(pstRowData, pstSchema, "businessdaydate", (int) bizDate.toEpochDay());
        }

        // fieldgroup: raw=varchar → PST=varchar SUBSTRING(1,5)
        Object rawFieldGroup = extractField(rawRowData, rawSchema, "fieldgroup");
        if (rawFieldGroup instanceof StringData) {
            String fg = ((StringData) rawFieldGroup).toString();
            if (fg != null && !fg.isEmpty()) {
                String truncated = fg.substring(0, Math.min(fg.length(), 5));
                setPstField(pstRowData, pstSchema, "fieldgroup", StringData.fromString(truncated));
            }
        }

        // fieldname: raw=varchar → PST=varchar SUBSTRING(1,10)
        Object rawFieldName = extractField(rawRowData, rawSchema, "fieldname");
        if (rawFieldName instanceof StringData) {
            String fn = ((StringData) rawFieldName).toString();
            if (fn != null && !fn.isEmpty()) {
                String truncated = fn.substring(0, Math.min(fn.length(), 10));
                setPstField(pstRowData, pstSchema, "fieldname", StringData.fromString(truncated));
            }
        }

        return pstRowData;
    }

}