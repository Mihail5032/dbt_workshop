package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;

import java.time.LocalDate;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@XmlAccessorType(XmlAccessType.FIELD)
public class TenderTotals extends BaseTransactionKey {
    @XmlElement(name = "TENDERTYPECODE")
    private String tenderTypeCode;
    @XmlElement(name = "TENDERAMOUNT")
    private String tenderAmount;
    @XmlElement(name = "TENDERCOUNT")
    private String tenderCount;
    @XmlElement(name = "ACTUALAMOUNT")
    private String actualAmount;
    @XmlElement(name = "SHORTAMOUNT")
    private String shortAmount;
    @XmlElement(name = "OVERAMOUNT")
    private String overAmount;
    @XmlElement(name = "REMOVALAMOUNT")
    private String removalAmount;
    @XmlElement(name = "REMOVALCOUNT")
    private String removalCount;
    @XmlElement(name = "BANKAMOUNT")
    private String bankAmount;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName()).tendertypecode(tenderTypeCode)
                .tenderamount(tenderAmount).tendercount(tenderCount).actualamount(actualAmount)
                .shortamount(shortAmount).overamount(overAmount)
                .removalamount(removalAmount).removalcount(removalCount)
                .bankamount(bankAmount).build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }

    @Override
    public String getSegmentName(){return "E1BPTENDERTOTALS";}
}