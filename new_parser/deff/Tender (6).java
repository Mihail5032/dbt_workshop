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
public class Tender extends BaseTransactionKey {
    @XmlElement(name = "TENDERSEQUENCENUMBER")
    private String tenderSequenceNumber;
    @XmlElement(name = "TENDERTYPECODE")
    private String tenderTypeCode;
    @XmlElement(name = "TENDERAMOUNT")
    private BigDecimal tenderAmount;
    @XmlElement(name = "TENDERCURRENCY")
    private String tenderCurrency;
    @XmlElement(name = "TENDERCURRENCY_ISO")
    private String tenderCurrencyISO;
    @XmlElement(name = "TENDERID")
    private String tenderId;
    @XmlElement(name = "ACCOUNTNUMBER")
    private String accountNumber;
    @XmlElement(name = "REFERENCEID")
    private String referenceId;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName())
                .tendersequencenumber(tenderSequenceNumber)
                .tendertypecode(tenderTypeCode).tenderamount(String.valueOf(tenderAmount))
                .tendercurrency(tenderCurrency).tendercurrency_iso(tenderCurrencyISO)
                .tenderid(tenderId).accountnumber(accountNumber).refererenceid(referenceId)
                .build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }

    @Override
    public String getSegmentName(){return "E1BPTENDER";}

    // ======================== PST-логика ========================

    /**
     * Формирует RowData для PST-таблицы bptender_new_pst_2.
     * Читает данные из rawRowData (результат toRowData()), применяет PST бизнес-логику.
     *
     * @param rawRowData   — RowData из toRowData() (raw-слой)
     * @param rawSchema    — схема raw-таблицы (raw_bptender)
     * @param pstSchema    — схема PST-таблицы (bptender_new_pst_2)
     * @param isCertParty  — true если среди TenderExtension есть CERT_PARTY=RU02
     */
    public RowData toRowDataPst(RowData rawRowData, Schema rawSchema, Schema pstSchema, boolean isCertParty) {
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
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "tendercurrency");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "tendercurrency_iso");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "tenderid");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "accountnumber");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "refererenceid");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_date_xml");
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "load_ts_xml");

        // ---- businessdaydate: raw varchar → PST date ----
        Object rawBizDate = extractField(rawRowData, rawSchema, "businessdaydate");
        LocalDate bizDate = extractLocalDate(rawBizDate);
        if (bizDate != null) {
            setPstField(pstRowData, pstSchema, "businessdaydate", (int) bizDate.toEpochDay());
        }

        // ---- tenderamount: raw decimal(32,2) → pst decimal(38,2) ----
        Object rawTenderAmount = extractField(rawRowData, rawSchema, "tenderamount");
        if (rawTenderAmount instanceof DecimalData) {
            BigDecimal amount = ((DecimalData) rawTenderAmount).toBigDecimal();
            setPstField(pstRowData, pstSchema, "tenderamount", DecimalData.fromBigDecimal(amount, 38, 2));
        }

        // ---- tendertypecode: копируем из raw, затем PST-логика 3108 → 3123 ----
        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "tendertypecode");
        Object rawTenderTypeCode = extractField(rawRowData, rawSchema, "tendertypecode");
        String ttc = extractString(rawTenderTypeCode);
        if (isCertParty && "3108".equals(ttc)) {
            setPstField(pstRowData, pstSchema, "tendertypecode", StringData.fromString("3123"));
        }

        copyField(rawRowData, rawSchema, pstRowData, pstSchema, "is_aligned_tran");

        return pstRowData;
    }
}