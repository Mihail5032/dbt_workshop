package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.flink.table.data.*;
import org.apache.iceberg.Schema;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@XmlAccessorType(XmlAccessType.FIELD)
public class RetailLineItem extends BaseTransactionKey {
    @XmlElement(name = "RETAILSEQUENCENUMBER")
    private String retailSequenceNumber;
    @XmlElement(name = "RETAILTYPECODE")
    private String retailTypeCode;
    @XmlElement(name = "RETAILREASONCODE")
    private String retailReasonCode;
    @XmlElement(name = "ITEMIDQUALIFIER")
    private String itemIdQualifier;
    @XmlElement(name = "ITEMID")
    private String itemId;
    @XmlElement(name = "RETAILQUANTITY")
    private BigDecimal retailQuantity;
    @XmlElement(name = "SALESUNITOFMEASURE")
    private String salesUnitOfMeasure;
    @XmlElement(name = "SALESUNITOFMEASURE_ISO")
    private String salesUnitOfMeasureISO;
    @XmlElement(name = "SALESAMOUNT")
    private BigDecimal salesAmount;
    @XmlElement(name = "NORMALSALESAMOUNT")
    private String normalSalesAmount;
    @XmlElement(name = "COST")
    private String cost;
    @XmlElement(name = "BATCHID")
    private String batchId;
    @XmlElement(name = "SERIALNUMBER")
    private String serialNumber;
    @XmlElement(name = "PROMOTIONID")
    private String promotionId;
    @XmlElement(name = "ITEMIDENTRYMETHODCODE")
    private String itemIdenTryMethodCode;
    @XmlElement(name = "ACTUALUNITPRICE")
    private String actualUnitPrice;
    @XmlElement(name = "UNITS")
    private String units;
    @XmlElement(name = "NONEXISTENTARTICLEID")
    private String nonExistentArticleId;
    @XmlElement(name = "REGULARSALESAMOUNT")
    private String regularSalesAmount;
    @XmlElement(name = "SCANTIME")
    private String scanTime;
    private String umrezVal;   //поле из itemid
    private String matnrPadded;   //поле из itemid
    private String umrez;   //поле из справочника
    private String matnr;   //поле из справочника
    private String meinh;
    private String lfnum;
    private String ean11;
    private Boolean hasEan;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        if (itemId != null && itemId.contains("UP")) {
            String[] parts = itemId.split("UP", 2);
            if (parts.length == 2) {
                String partBefore = parts[0];
                String partAfter = parts[1];
                matnrPadded = String.format("%18s", partBefore).replace(' ', '0');
                umrezVal = partAfter;
            }
        }
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName()).retailsequencenumber(retailSequenceNumber)
                .retailtypecode(retailTypeCode).retailreasoncode(retailReasonCode)
                .itemidqualifier(itemIdQualifier).itemid(itemId).retailquantity(retailQuantity != null ? retailQuantity.toString() : null)
                .salesunitofmeasure(salesUnitOfMeasure).salesunitofmeasure_iso(salesUnitOfMeasureISO)
                .salesamount(salesAmount != null ? salesAmount.toString() : null).normalsalesamount(normalSalesAmount)
                .cost(cost).batchid(batchId).serialnumber(serialNumber).promotionid(promotionId)
                .itemidentrymethodcode(itemIdenTryMethodCode).actualunitprice(actualUnitPrice).nonexistentarticleid(nonExistentArticleId)
                .regularsalesamount(regularSalesAmount).units(units).scantime(scanTime)
                .matnr_padded(matnrPadded).umrez_val(umrezVal)
                .lfnum(lfnum).ean11(ean11)
                .is_aligned_tran(String.valueOf(super.getIs_aligned_tran()))
                .build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }

    @Override
    public String getSegmentName(){return "E1BPRETAILLINEITEM";}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RetailLineItem)) return false;
        if (!super.equals(o)) return false;
        RetailLineItem that = (RetailLineItem) o;
        return Objects.equals(retailSequenceNumber, that.retailSequenceNumber)
                && Objects.equals(retailTypeCode, that.retailTypeCode)
                && Objects.equals(retailReasonCode, that.retailReasonCode)
                && Objects.equals(itemIdQualifier, that.itemIdQualifier)
                && Objects.equals(itemId, that.itemId)
                && Objects.equals(retailQuantity, that.retailQuantity)
                && Objects.equals(salesUnitOfMeasure, that.salesUnitOfMeasure)
                && Objects.equals(salesUnitOfMeasureISO, that.salesUnitOfMeasureISO)
                && Objects.equals(salesAmount, that.salesAmount)
                && Objects.equals(normalSalesAmount, that.normalSalesAmount)
                && Objects.equals(cost, that.cost)
                && Objects.equals(batchId, that.batchId)
                && Objects.equals(serialNumber, that.serialNumber)
                && Objects.equals(promotionId, that.promotionId)
                && Objects.equals(itemIdenTryMethodCode, that.itemIdenTryMethodCode)
                && Objects.equals(actualUnitPrice, that.actualUnitPrice)
                && Objects.equals(units, that.units)
                && Objects.equals(nonExistentArticleId, that.nonExistentArticleId)
                && Objects.equals(regularSalesAmount, that.regularSalesAmount)
                && Objects.equals(scanTime, that.scanTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), retailSequenceNumber, retailTypeCode, retailReasonCode, itemIdQualifier,
                itemId, retailQuantity, salesUnitOfMeasure, salesUnitOfMeasureISO, salesAmount, normalSalesAmount,
                cost, batchId, serialNumber, promotionId, itemIdenTryMethodCode, actualUnitPrice, units, nonExistentArticleId,
                regularSalesAmount, scanTime
        );
    }
    @Override
    public String toString() {
        return "RetailLineItem{" +
                "retailStoreId='" + retailStoreId + '\'' +
                ", businessDayDate='" + businessDayDate + '\'' +
                ", transactionTypeCode='" + transactionTypeCode + '\'' +
                ", workstationId='" + workstationId + '\'' +
                ", transactionSequenceNumber='" + transactionSequenceNumber + '\'' +
                ", retailSequenceNumber='" + retailSequenceNumber + '\'' +
                ", retailTypeCode='" + retailTypeCode + '\'' +
                ", retailReasonCode='" + retailReasonCode + '\'' +
                ", itemIdQualifier='" + itemIdQualifier + '\'' +
                ", itemId='" + itemId + '\'' +
                ", retailQuantity=" + retailQuantity +
                ", salesUnitOfMeasure='" + salesUnitOfMeasure + '\'' +
                ", salesUnitOfMeasureISO='" + salesUnitOfMeasureISO + '\'' +
                ", salesAmount=" + salesAmount +
                ", normalSalesAmount='" + normalSalesAmount + '\'' +
                ", cost='" + cost + '\'' +
                ", batchId='" + batchId + '\'' +
                ", serialNumber='" + serialNumber + '\'' +
                ", promotionId='" + promotionId + '\'' +
                ", itemIdenTryMethodCode='" + itemIdenTryMethodCode + '\'' +
                ", actualUnitPrice='" + actualUnitPrice + '\'' +
                ", units='" + units + '\'' +
                ", nonExistentArticleId='" + nonExistentArticleId + '\'' +
                ", regularSalesAmount='" + regularSalesAmount + '\'' +
                ", scanTime='" + scanTime + '\'' +
                '}';
    }
    public String createKeyLineItem(){
        return retailStoreId+businessDayDate+transactionTypeCode+workstationId
                +transactionSequenceNumber+retailSequenceNumber;
    }

    public RowData toRowDataPst(Schema pstSchema, TimestampData timestampDataXml, LocalDate dateXml, boolean isAutoMd){
        if (isAutoMd) retailTypeCode="2043";
        //itemid
        String eanItemId = Boolean.TRUE.equals(hasEan) ? ean11 : null;
        String finalItemId = eanItemId != null ? eanItemId : itemId;
        if (Objects.equals(transactionTypeCode,"1014") && finalItemId == null){
            itemId = "0";
        }
        else {
            itemId = finalItemId;
        }
        //itemidqualifier
        if (Objects.equals(transactionTypeCode,"1014") && finalItemId == null) {
            itemIdQualifier = "";
        }
        else if (Boolean.TRUE.equals(hasEan)) {
            itemIdQualifier = "1";
        }
        //salesunitofmeasure
        if (Boolean.TRUE.equals(hasEan) && meinh!=null){
            salesUnitOfMeasure = meinh;
        }
        return toRowData(pstSchema, timestampDataXml, dateXml);
    }

}