package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;

import java.math.BigDecimal;
import java.time.LocalDate;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class FinancialMovemen extends BaseTransactionKey {
    @XmlElement(name = "FINANCIALSEQUENCENUMBER")
    private String financialSequenceNumber;
    @XmlElement(name = "FINANCIALTYPECODE")
    private String financialTypeCode;
    @XmlElement(name = "ACCOUNTID")
    private String accountId;
    @XmlElement(name = "ACCOUNTASSIGNMENTOBJECT")
    private String accountAssignmentObject;
    @XmlElement(name = "AMOUNT")
    private String amount;
    @XmlElement(name = "FINANCIALCURRENCY")
    private String financialCurrency;
    @XmlElement(name = "FINANCIALCURRENCY_ISO")
    private String financialCurrencyISO;
    @XmlElement(name = "REFERERENCEID")
    private String refererenceId;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName())
                .financialsequencenumber(financialSequenceNumber).financialtypecode(financialTypeCode)
                .accountid(accountId).accountassignmentobject(accountAssignmentObject)
                .amount(amount).financialcurrency(financialCurrency)
                .financialcurrency_iso(financialCurrencyISO).refererenceid(refererenceId).build()
                .toRowData(icebergSchema, timestampDataXml, dateXml);
    }
    @Override
    public String getSegmentName(){return "E1BPFINANCIALMOVEMEN";}

    // ======================== PST-логика ========================

    /**
     * Формирует RowData для PST-таблицы staging_new.bpfinancialmovemen_new_pst.
     * Читает данные из rawRowData (результат toRowData()), применяет PST бизнес-логику.
     *
     * @param rawRowData     — RowData из toRowData() (raw-слой)
     * @param rawSchema      — схема raw-таблицы
     * @param pstSchema      — схема PST-таблицы
     * @param noteFieldValue — значение fieldvalue из TransactionExtension (fieldname='NOTE'), может быть null
     */
    public RowData toRowDataPst(RowData rawRowData, Schema rawSchema, Schema pstSchema, String noteFieldValue) {
        int schemaSize = pstSchema.columns().size();
        GenericRowData pstRowData = new GenericRowData(schemaSize);

        // Копируем поля напрямую из raw RowData (типы совпадают: varbinary, varchar, timestamp, date)
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "rtl_txn_rk");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "rtl_txn_fin_rk");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "financialcurrency");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "financialcurrency_iso");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "financialsequencenumber");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "financialtypecode");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "retailstoreid");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactionsequencenumber");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "transactiontypecode");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "workstationid");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_date_xml");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts_xml");

        // businessdaydate: raw может быть date (int) или varchar (StringData)
        Object rawBizDate = extractField(rawRowData, rawSchema, "businessdaydate");
        LocalDate bizDate = extractLocalDate(rawBizDate);
        if (bizDate != null) {
            setPstField(pstRowData, pstSchema, "businessdaydate", (int) bizDate.toEpochDay());
        }

        // amount: raw=varchar → PST=decimal(32,2) (конвертация типа + NOTE-логика)
        boolean applyNoteLogic = noteFieldValue != null
                && noteFieldValue.length() >= 2
                && noteFieldValue.charAt(1) == '2'
                && financialTypeCode != null
                && !"3169".equals(financialTypeCode)
                && !"3170".equals(financialTypeCode);

        Object rawAmount = extractField(rawRowData, rawSchema, "amount");
        if (rawAmount instanceof StringData) {
            BigDecimal amountDecimal = parseAmount(((StringData) rawAmount).toString());
            if (amountDecimal != null) {
                if (applyNoteLogic) {
                    amountDecimal = amountDecimal.negate();
                }
                setPstField(pstRowData, pstSchema, "amount", DecimalData.fromBigDecimal(amountDecimal, 32, 2));
            }
        }

        // refererenceid — вычисляется из NOTE fieldvalue (замена 2-го символа на '1')
        if (applyNoteLogic) {
            String computedRefId = noteFieldValue.substring(0, 1) + "1" + noteFieldValue.substring(2);
            setPstField(pstRowData, pstSchema, "refererenceid", StringData.fromString(computedRefId));
        }

        return pstRowData;
    }

}