package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import org.apache.commons.lang.StringUtils;
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
public class TransactDiscExt extends BaseTransactionKey {
    @XmlElement(name = "DISCOUNTSEQUENCENUMBER")
    private String discountSequenceNumber;
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
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName())
                .discountsequencenumber(discountSequenceNumber).fieldgroup(fieldGroupStr)
                .fieldname(fieldNameStr).fieldvalue(fieldValue)
                .is_aligned_tran(String.valueOf(super.getIs_aligned_tran()))
                .build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }

    @Override
    public String getSegmentName(){return "E1BPTRANSACTDISCEXT";}

    public RowData toRowDataPst(Schema pstSchema, TimestampData timestampDataXml, LocalDate dateXml){
        return toRowData(pstSchema, timestampDataXml, dateXml); //доп. преобразований нет
    }
}