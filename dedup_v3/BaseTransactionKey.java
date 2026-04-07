package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Objects;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@XmlAccessorType(XmlAccessType.FIELD)
public class BaseTransactionKey {
    @XmlElement(name = "RETAILSTOREID")
    protected String retailStoreId;
    @XmlElement(name = "BUSINESSDAYDATE")
    protected String businessDayDate;
    @XmlElement(name = "TRANSACTIONTYPECODE")
    protected String transactionTypeCode;
    @XmlElement(name = "WORKSTATIONID")
    protected String workstationId;
    @XmlElement(name = "TRANSACTIONSEQUENCENUMBER")
    protected String transactionSequenceNumber;
    protected Boolean is_aligned_tran;

    public BaseTransactionKey getTransactionKey() {
        return BaseTransactionKey.builder()
                .retailStoreId(retailStoreId)
                .businessDayDate(businessDayDate)
                .transactionTypeCode(transactionTypeCode)
                .workstationId(workstationId)
                .transactionSequenceNumber(transactionSequenceNumber)
                .build();
    }

    protected RowTablePart toRowTablePart() {
        return RowTablePart.builder().retailstoreid(retailStoreId)
                .businessdaydate(businessDayDate)
                .transactiontypecode(transactionTypeCode)
                .workstationid(workstationId)
                .transactionsequencenumber(transactionSequenceNumber)
                .build();
    }

    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        return toRowTablePart().toRowData(icebergSchema, timestampDataXml,dateXml);
    }

    public String getSegmentName(){return "unknown";}

    @Override
    public String toString() {
        return "BaseTransactionKey{" +
                "retailStoreId='" + retailStoreId + '\'' +
                ", businessDayDate='" + businessDayDate + '\'' +
                ", transactionTypeCode='" + transactionTypeCode + '\'' +
                ", workstationId='" + workstationId + '\'' +
                ", transactionSequenceNumber='" + transactionSequenceNumber + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BaseTransactionKey)) return false;
        BaseTransactionKey that = (BaseTransactionKey) o;
        return Objects.equals(retailStoreId, that.retailStoreId)
                && Objects.equals(businessDayDate, that.businessDayDate)
                && Objects.equals(transactionTypeCode, that.transactionTypeCode)
                && Objects.equals(workstationId, that.workstationId)
                && Objects.equals(transactionSequenceNumber, that.transactionSequenceNumber);
    }

    @Override
    public int hashCode() {
        return Objects.hash(retailStoreId, businessDayDate, transactionTypeCode, workstationId, transactionSequenceNumber);
    }

    // ======================== PST-хелперы (общие для всех наследников) ========================

    protected void copyField(RowData rawRowData, Schema rawSchema,
                             GenericRowData pstRowData, Schema pstSchema, String fieldName) {
        Types.NestedField rawField = rawSchema.findField(fieldName);
        Types.NestedField pstField = pstSchema.findField(fieldName);
        if (rawField != null && pstField != null) {
            int rawPos = rawSchema.columns().indexOf(rawField);
            int pstPos = pstSchema.columns().indexOf(pstField);
            Object value = ((GenericRowData) rawRowData).getField(rawPos);
            if (value != null) {
                pstRowData.setField(pstPos, value);
            }
        }
    }

    protected Object extractField(RowData rawRowData, Schema rawSchema, String fieldName) {
        Types.NestedField field = rawSchema.findField(fieldName);
        if (field != null) {
            int pos = rawSchema.columns().indexOf(field);
            return ((GenericRowData) rawRowData).getField(pos);
        }
        return null;
    }

    protected void setPstField(GenericRowData pstRowData, Schema pstSchema, String fieldName, Object value) {
        if (value == null) return;
        Types.NestedField field = pstSchema.findField(fieldName);
        if (field != null) {
            int pos = pstSchema.columns().indexOf(field);
            pstRowData.setField(pos, value);
        }
    }

    protected LocalDate parseRawDate(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        try {
            int year = safeParseInt(raw, 0, 4, 1970);
            int month = safeParseInt(raw, 4, 6, 1);
            int day = safeParseInt(raw, 6, 8, 1);
            return LocalDate.of(year, month, day);
        } catch (Exception e) {
            return null;
        }
    }

    protected LocalDateTime parseRawTimestamp(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        try {
            int year = safeParseInt(raw, 0, 4, 1970);
            int month = safeParseInt(raw, 4, 6, 1);
            int day = safeParseInt(raw, 6, 8, 1);
            int hour = safeParseInt(raw, 8, 10, 0);
            int minute = safeParseInt(raw, 10, 12, 0);
            int second = safeParseInt(raw, 12, 14, 0);
            return LocalDateTime.of(year, month, day, hour, minute, second);
        } catch (Exception e) {
            return null;
        }
    }

    protected BigDecimal parseAmount(String raw) {
        if (raw == null || raw.isEmpty()) return null;
        try {
            return new BigDecimal(raw);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    protected int safeParseInt(String input, int start, int end, int defaultValue) {
        try {
            if (input.length() >= end) {
                return Integer.parseInt(input.substring(start, end));
            }
        } catch (NumberFormatException e) {
            return defaultValue;
        }
        return defaultValue;
    }

    /**
     * Извлекает LocalDateTime из raw-поля (поддерживает TimestampData и StringData).
     */
    protected LocalDateTime extractLocalDateTime(Object rawValue) {
        if (rawValue instanceof TimestampData) {
            return ((TimestampData) rawValue).toLocalDateTime();
        }
        if (rawValue instanceof StringData) {
            return parseRawTimestamp(rawValue.toString());
        }
        return null;
    }

    /**
     * Извлекает LocalDate из raw-поля (поддерживает int/Integer epoch day, StringData).
     */
    protected LocalDate extractLocalDate(Object rawValue) {
        if (rawValue instanceof Integer) {
            return LocalDate.ofEpochDay((int) rawValue);
        }
        if (rawValue instanceof StringData) {
            return parseRawDate(rawValue.toString());
        }
        return null;
    }

    /**
     * Извлекает String из raw-поля (поддерживает StringData).
     */
    protected String extractString(Object rawValue) {
        if (rawValue instanceof StringData) {
            return rawValue.toString();
        }
        return null;
    }
}
