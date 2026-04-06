package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;

import java.time.LocalDate;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class SourceDocument {
    @XmlElement(name = "KEY")
    private String key;

    @XmlElement(name = "TYPE")
    private String type;

    @XmlElement(name = "LOGICALSYSTEM")
    private String logicalSystem;

    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        return RowTablePart.builder().segment_name(getSegmentName())
                .key(key).type(type).logicalsystem(logicalSystem)
                .build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }

    public String getSegmentName(){return "E1BPSOURCEDOCUMENTLI";}
}