package ru.x5.model;

import lombok.Builder;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.data.*;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.*;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

@Builder
public class RowTablePart {
    private static final XXHash64 HASH64 = XXHashFactory.fastestInstance().hash64();
    public static final String DATE_TIME_STRING_FORMAT = "%04d-%02d-%02d %02d:%02d:%02d";
    private String account;
    private String accountassignmentobject;
    private String accountholdername;
    private String accountid;
    private String accountnumber;
    private String actualamount;
    private String actualunitprice;
    private String adjudicationcode;
    private String amount;
    private String arckey;
    private String authorizationcode;
    private String authorizationdatetime;
    private String authorizationmethodcode;
    private String authorizingtermid;
    private String bankaccount;
    private String bankamount;
    private String bankcode;
    private String batchid;
    private String begindatetimestamp;
    private String blacklistdate;
    private String blacklistversion;
    private String bonusbuyid;
    private String businessdaydate;
    private String cardexpirationdate;
    private String cardguid;
    private String cardholdername;
    private String cardinformation;
    private String cardnumber;
    private String cardnumbersuffix;
    private String cardnumberswipedorkeyedcode;
    private String cardtype;
    private String checksoverlimitamount;
    private String checksoverlimitcount;
    private String cimtyp;
    private String commemployeequal;
    private String commisionsequencenumber;
    private String commissionamount;
    private String commissionemployeeid;
    private String cost;
    private String costcenter;
    private String credat;
    private String cretim;
    private String customercardholdername;
    private String customercardnumber;
    private String customercardtype;
    private String customercardvalidfrom;
    private String customercardvalidto;
    private String customerinformationtypecode;
    private String dataelementid;
    private String dataelementvalue;
    private String deliverycompleteflag;
    private String department;
    private String discountcount;
    private String discountid;
    private String discountidqualifier;
    private String discountreasoncode;
    private String discountsequencenumber;
    private String discounttypecode;
    private String docrel;
    private String driverid;
    private String ean11;
    private String ecseperator;
    private String edi_dc40;
    private String eligibleamount;
    private String eligiblequantity;
    private String eligiblequantityuom;
    private String eligiblequantityuom_iso;
    private String enctype;
    private String enddatetimestamp;
    private String expirationdate;
    private String exprss;
    private String externalvaluationamount;
    private String failedlogoncount;
    private String fieldgroup;
    private String fieldname;
    private String fieldvalue;
    private String financialcurrency;
    private String financialcurrency_iso;
    private String financialsequencenumber;
    private String financialtypecode;
    private String forceonline;
    private String goodsmovementreasoncode;
    private String goodsmovementsequencenumber;
    private String goodsmovementtypecode;
    private String hostauthorized;
    private String idletime;
    private String idoc;
    private String immediatevoidamount;
    private String immediatevoidcount;
    private String is_head_bonid;
    private String industrymainkey;
    private String itemid;
    private String itemidentrymethodcode;
    private String itemidqualifier;
    private String key;
    private String keyedofflineflag;
    private String lfnum;
    private String limitamount;
    private String lineitemcount;
    private String lineitemvoidamount;
    private String lineitemvoidcount;
    private String locationid;
    private String locktime;
    private String logicalsystem;
    private String logontime;
    private String loyaltypointsawarded;
    private String loyaltypointsredeemed;
    private String loyaltypointstotal;
    private String loyaltyprogramid;
    private String loyaltysequencenumber;
    private String mandt;
    private String matnr_padded;
    private String mediaissuerid;
    private String mescod;
    private String mesfct;
    private String nonexistentarticleid;
    private String normalsalesamount;
    private String nosaletransactioncount;
    private String odometerreading;
    private String offerid;
    private String opendepartmentamount;
    private String opendepartmentcount;
    private String operatorid;
    private String operatorqualifier;
    private String outmod;
    private String overamount;
    private String partnerid;
    private String partnerqualifier;
    private String paymentcard;
    private String postvoidamount;
    private String postvoidcount;
    private String promotionid;
    private String providerid;
    private String rcvlad;
    private String rcvpfc;
    private String rcvsad;
    private String reactioncode;
    private String reductionamount;
    private String refdocyear;
    private String referencedocumentid;
    private String referencedocumentqualifier;
    private String referencedocumentsequencenr;
    private String referenceid;
    private String refererenceid;
    private String refgrp;
    private String refint;
    private String refmes;
    private String regularsalesamount;
    private String removalamount;
    private String removalcount;
    private String reportcount;
    private String requestedamount;
    private String retailquantity;
    private String retailreasoncode;
    private String retailsequencenumber;
    private String retailstoreid;
    private String retailtypecode;
    private String ringtime;
    private String salesamount;
    private String salesunitofmeasure;
    private String salesunitofmeasure_iso;
    private String scanneditemamount;
    private String scanneditemcount;
    private String scantime;
    private String serial;
    private String serialnumber;
    private String shortamount;
    private String sndlad;
    private String sndpfc;
    private String sndprn;
    private String sndsad;
    private String specialstockcode;
    private String status;
    private String std;
    private String stdmes;
    private String stdvrs;
    private String storefinancialledgeraccountid;
    private String supplierid;
    private String taxamount;
    private String taxcount;
    private String taxsequencenumber;
    private String taxtypecode;
    private String tenderamount;
    private String tendercount;
    private String tendercurrency;
    private String tendercurrency_iso;
    private String tenderid;
    private String tendersequencenumber;
    private String tendertypecode;
    private String terminationamount;
    private String terminationcount;
    private String test;
    private String tofromlocationid;
    private String topartyid;
    private String trainingflag;
    private String transactioncount;
    private String transactioncurrency;
    private String transactioncurrency_iso;
    private String transactionsequencenumber;
    private String transactiontypecode;
    private String transreasoncode;
    private String type;
    private String umrez_val;
    private String unitcount;
    private String units;
    private String unitsshippedcount;
    private String validfromdate;
    private String vehicleid;
    private String voidedbusinessdaydate;
    private String voidedline;
    private String voidedretailstoreid;
    private String voidedtransactionsequencenumbe;
    private String voidedworkstationid;
    private String voidflag;
    private String workstationid;
    private String segment_name;
    private String dataflow_ddtm;
    private String load_ts;
    private String is_aligned_tran;

    public static RowTablePart.RowTablePartBuilder fromBase(RowTablePart base) {
        return RowTablePart.builder()
                .retailstoreid(base.retailstoreid)
                .businessdaydate(base.businessdaydate)
                .transactiontypecode(base.transactiontypecode)
                .workstationid(base.workstationid)
                .transactionsequencenumber(base.transactionsequencenumber);
    }

    public long createKey(List<String> fields) {
        String joined = String.join("|", fields.stream()
                .map(s -> s == null ? "" : s)
                .toArray(String[]::new));
        byte[] bytes = joined.getBytes(StandardCharsets.UTF_8);
        long rk = HASH64.hash(bytes, 0, bytes.length, 0L);
        return rk;
    }
    public void addKeyToRowData(GenericRowData rowData, Schema icebergSchema, String nameKey, List<String> keyFields){
        Types.NestedField keyField = icebergSchema.findField(nameKey);
        if (nonNull(keyField)) {
            int pos = icebergSchema.columns().indexOf(keyField);
            long key = createKey(keyFields);
            byte[] hashBytes = ByteBuffer.allocate(8).putLong(key).flip().array();
            rowData.setField(pos, hashBytes);
        }
    }

    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        int schemaSize = icebergSchema.columns().size();
        GenericRowData rowData = new GenericRowData(schemaSize);
        Map<String, String> currentMap = createMapFieldNameValue();
        for (String key : currentMap.keySet()) {
            Types.NestedField field = icebergSchema.findField(key);
            if (nonNull(field)) {
                String value = currentMap.get(key);
                if (StringUtils.isNotBlank(value)) {
                    int pos = icebergSchema.columns().indexOf(field);
                    Type.TypeID typeID = field.type().typeId();
                    if (typeID == Type.TypeID.DATE) {
                        LocalDate d = stringToDate(value);
                        int a = d == null ? 0 : (int) d.toEpochDay();
                        rowData.setField(pos, a);
                    } else if (typeID == Type.TypeID.DECIMAL) {
                        BigDecimal db = stringToBigDecimal(value);
                        DecimalData data;
                        if (Objects.equals(key, "retailquantity") || Objects.equals(key, "units"))
                            data = db == null ? null : DecimalData.fromBigDecimal(db, 32, 3);
                        else
                            data = db == null ? null : DecimalData.fromBigDecimal(db, 32, 2);
                        rowData.setField(pos, data);
                    } else if (typeID == Type.TypeID.INTEGER) {
                        try {
                            Integer i = Integer.parseInt(value);
                            rowData.setField(pos, i);
                        } catch (NumberFormatException e) {
                            // невалидное значение для INTEGER — пропускаем поле
                        }
                    } else if (typeID == Type.TypeID.TIMESTAMP) {
                        LocalDateTime dateTime = parseToFormattedDate(value);
                        TimestampData td = dateTime == null ? null : TimestampData.fromLocalDateTime(dateTime);
                        rowData.setField(pos, td);
                    } else if (typeID == Type.TypeID.BOOLEAN) {
                        Boolean boolValue = Boolean.parseBoolean(value);
                        rowData.setField(pos, boolValue);
                    } else {
                        if (key.contains("date") || key.contains("timestamp")) {
                            StringData stringDateTime = parseToFormattedDateString(value);
                            rowData.setField(pos, stringDateTime);
                        } else {
                            rowData.setField(pos, StringData.fromString(value));
                        }
                    }
                }
            }
        }
        //добавление ключей
        addKeyToRowData(rowData, icebergSchema, "rtl_txn_rk", Arrays.asList(retailstoreid,businessdaydate,workstationid,
                transactionsequencenumber));
        addKeyToRowData(rowData, icebergSchema, "rtl_txn_fin_rk", Arrays.asList(retailstoreid, businessdaydate, workstationid,
                transactionsequencenumber, financialsequencenumber));
        addKeyToRowData(rowData, icebergSchema, "rtl_txn_item_rk", Arrays.asList(retailstoreid, businessdaydate, workstationid,
                transactionsequencenumber, retailsequencenumber));
        addKeyToRowData(rowData, icebergSchema, "rtl_txn_item_disc_rk", Arrays.asList(retailstoreid, businessdaydate, workstationid,
                transactionsequencenumber, retailsequencenumber, discountsequencenumber));
        addKeyToRowData(rowData, icebergSchema, "rtl_txn_item_tax_rk", Arrays.asList(retailstoreid, businessdaydate, workstationid,
                transactionsequencenumber, retailsequencenumber, taxsequencenumber));
        addKeyToRowData(rowData, icebergSchema, "rtl_txn_tender_rk", Arrays.asList(retailstoreid, businessdaydate, workstationid,
                transactionsequencenumber, tendersequencenumber));
        addKeyToRowData(rowData, icebergSchema, "rtl_txn_disc_rk", Arrays.asList(retailstoreid, businessdaydate, workstationid,
                transactionsequencenumber, discountsequencenumber));
        //добавление значений времени
        Types.NestedField loadTsField = icebergSchema.findField("load_ts");
        if (nonNull(loadTsField)) {
            int pos = icebergSchema.columns().indexOf(loadTsField);
            rowData.setField(pos, TimestampData.fromLocalDateTime(LocalDateTime.now()));
        }
        Types.NestedField loadDateField = icebergSchema.findField("load_date");
        if (nonNull(loadDateField)) {
            int pos = icebergSchema.columns().indexOf(loadDateField);
            rowData.setField(pos, (int) LocalDate.now().toEpochDay());
        }
        Types.NestedField loadTsXmlField = icebergSchema.findField("load_ts_xml");
        if (nonNull(loadTsXmlField)) {
            int pos = icebergSchema.columns().indexOf(loadTsXmlField);
            rowData.setField(pos, timestampDataXml);
        }
        Types.NestedField loadDateXmlField = icebergSchema.findField("load_date_xml");
        if (nonNull(loadDateXmlField)) {
            int pos = icebergSchema.columns().indexOf(loadDateXmlField);
            rowData.setField(pos, (int) dateXml.toEpochDay());
        }
        // is_aligned_tran — всегда true по умолчанию
        Types.NestedField isAlignedField = icebergSchema.findField("is_aligned_tran");
        if (nonNull(isAlignedField)) {
            int pos = icebergSchema.columns().indexOf(isAlignedField);
            rowData.setField(pos, true);
        }
        return rowData;
    }

    private BigDecimal stringToBigDecimal(String input) {
        try {
            double d = Double.parseDouble(input);
            return BigDecimal.valueOf(d);
        } catch (Exception e) {
            return null;
        }
    }

    private Map<String, String> createMapFieldNameValue() {
        Map<String, String> nameValue = new HashMap<>();
        nameValue.put("account", account);
        nameValue.put("accountassignmentobject", accountassignmentobject);
        nameValue.put("accountholdername", accountholdername);
        nameValue.put("accountid", accountid);
        nameValue.put("accountnumber", accountnumber);
        nameValue.put("actualamount", actualamount);
        nameValue.put("actualunitprice", actualunitprice);
        nameValue.put("adjudicationcode", adjudicationcode);
        nameValue.put("amount", amount);
        nameValue.put("arckey", arckey);
        nameValue.put("authorizationcode", authorizationcode);
        nameValue.put("authorizationdatetime", authorizationdatetime);
        nameValue.put("authorizationmethodcode", authorizationmethodcode);
        nameValue.put("authorizingtermid", authorizingtermid);
        nameValue.put("bankaccount", bankaccount);
        nameValue.put("bankamount", bankamount);
        nameValue.put("bankcode", bankcode);
        nameValue.put("batchid", batchid);
        nameValue.put("begindatetimestamp", begindatetimestamp);
        nameValue.put("blacklistdate", blacklistdate);
        nameValue.put("blacklistversion", blacklistversion);
        nameValue.put("bonusbuyid", bonusbuyid);
        nameValue.put("businessdaydate", businessdaydate);
        nameValue.put("cardexpirationdate", cardexpirationdate);
        nameValue.put("cardguid", cardguid);
        nameValue.put("cardholdername", cardholdername);
        nameValue.put("cardinformation", cardinformation);
        nameValue.put("cardnumber", cardnumber);
        nameValue.put("cardnumbersuffix", cardnumbersuffix);
        nameValue.put("cardnumberswipedorkeyedcode", cardnumberswipedorkeyedcode);
        nameValue.put("cardtype", cardtype);
        nameValue.put("checksoverlimitamount", checksoverlimitamount);
        nameValue.put("checksoverlimitcount", checksoverlimitcount);
        nameValue.put("cimtyp", cimtyp);
        nameValue.put("commemployeequal", commemployeequal);
        nameValue.put("commisionsequencenumber", commisionsequencenumber);
        nameValue.put("commissionamount", commissionamount);
        nameValue.put("commissionemployeeid", commissionemployeeid);
        nameValue.put("cost", cost);
        nameValue.put("costcenter", costcenter);
        nameValue.put("credat", credat);
        nameValue.put("cretim", cretim);
        nameValue.put("customercardholdername", customercardholdername);
        nameValue.put("customercardnumber", customercardnumber);
        nameValue.put("customercardtype", customercardtype);
        nameValue.put("customercardvalidfrom", customercardvalidfrom);
        nameValue.put("customercardvalidto", customercardvalidto);
        nameValue.put("customerinformationtypecode", customerinformationtypecode);
        nameValue.put("dataelementid", dataelementid);
        nameValue.put("dataelementvalue", dataelementvalue);
        nameValue.put("deliverycompleteflag", deliverycompleteflag);
        nameValue.put("department", department);
        nameValue.put("discountcount", discountcount);
        nameValue.put("discountid", discountid);
        nameValue.put("discountidqualifier", discountidqualifier);
        nameValue.put("discountreasoncode", discountreasoncode);
        nameValue.put("discountsequencenumber", discountsequencenumber);
        nameValue.put("discounttypecode", discounttypecode);
        nameValue.put("docrel", docrel);
        nameValue.put("driverid", driverid);
        nameValue.put("ean11", ean11);
        nameValue.put("ecseperator", ecseperator);
        nameValue.put("edi_dc40", edi_dc40);
        nameValue.put("eligibleamount", eligibleamount);
        nameValue.put("eligiblequantity", eligiblequantity);
        nameValue.put("eligiblequantityuom", eligiblequantityuom);
        nameValue.put("eligiblequantityuom_iso", eligiblequantityuom_iso);
        nameValue.put("enctype", enctype);
        nameValue.put("enddatetimestamp", enddatetimestamp);
        nameValue.put("expirationdate", expirationdate);
        nameValue.put("exprss", exprss);
        nameValue.put("externalvaluationamount", externalvaluationamount);
        nameValue.put("failedlogoncount", failedlogoncount);
        nameValue.put("fieldgroup", fieldgroup);
        nameValue.put("fieldname", fieldname);
        nameValue.put("fieldvalue", fieldvalue);
        nameValue.put("financialcurrency", financialcurrency);
        nameValue.put("financialcurrency_iso", financialcurrency_iso);
        nameValue.put("financialsequencenumber", financialsequencenumber);
        nameValue.put("financialtypecode", financialtypecode);
        nameValue.put("forceonline", forceonline);
        nameValue.put("goodsmovementreasoncode", goodsmovementreasoncode);
        nameValue.put("goodsmovementsequencenumber", goodsmovementsequencenumber);
        nameValue.put("goodsmovementtypecode", goodsmovementtypecode);
        nameValue.put("hostauthorized", hostauthorized);
        nameValue.put("idletime", idletime);
        nameValue.put("idoc", idoc);
        nameValue.put("immediatevoidamount", immediatevoidamount);
        nameValue.put("immediatevoidcount", immediatevoidcount);
        nameValue.put("industrymainkey", industrymainkey);
        nameValue.put("is_head_bonid", is_head_bonid);
        nameValue.put("itemid", itemid);
        nameValue.put("itemidentrymethodcode", itemidentrymethodcode);
        nameValue.put("itemidqualifier", itemidqualifier);
        nameValue.put("key", key);
        nameValue.put("keyedofflineflag", keyedofflineflag);
        nameValue.put("lfnum", lfnum);
        nameValue.put("limitamount", limitamount);
        nameValue.put("lineitemcount", lineitemcount);
        nameValue.put("lineitemvoidamount", lineitemvoidamount);
        nameValue.put("lineitemvoidcount", lineitemvoidcount);
        nameValue.put("locationid", locationid);
        nameValue.put("locktime", locktime);
        nameValue.put("logicalsystem", logicalsystem);
        nameValue.put("logontime", logontime);
        nameValue.put("loyaltypointsawarded", loyaltypointsawarded);
        nameValue.put("loyaltypointsredeemed", loyaltypointsredeemed);
        nameValue.put("loyaltypointstotal", loyaltypointstotal);
        nameValue.put("loyaltyprogramid", loyaltyprogramid);
        nameValue.put("loyaltysequencenumber", loyaltysequencenumber);
        nameValue.put("mandt", mandt);
        nameValue.put("matnr_padded", matnr_padded);
        nameValue.put("mediaissuerid", mediaissuerid);
        nameValue.put("mescod", mescod);
        nameValue.put("mesfct", mesfct);
        nameValue.put("nonexistentarticleid", nonexistentarticleid);
        nameValue.put("normalsalesamount", normalsalesamount);
        nameValue.put("nosaletransactioncount", nosaletransactioncount);
        nameValue.put("odometerreading", odometerreading);
        nameValue.put("offerid", offerid);
        nameValue.put("opendepartmentamount", opendepartmentamount);
        nameValue.put("opendepartmentcount", opendepartmentcount);
        nameValue.put("operatorid", operatorid);
        nameValue.put("operatorqualifier", operatorqualifier);
        nameValue.put("outmod", outmod);
        nameValue.put("overamount", overamount);
        nameValue.put("partnerid", partnerid);
        nameValue.put("partnerqualifier", partnerqualifier);
        nameValue.put("paymentcard", paymentcard);
        nameValue.put("postvoidamount", postvoidamount);
        nameValue.put("postvoidcount", postvoidcount);
        nameValue.put("promotionid", promotionid);
        nameValue.put("providerid", providerid);
        nameValue.put("rcvlad", rcvlad);
        nameValue.put("rcvpfc", rcvpfc);
        nameValue.put("rcvsad", rcvsad);
        nameValue.put("reactioncode", reactioncode);
        nameValue.put("reductionamount", reductionamount);
        nameValue.put("refdocyear", refdocyear);
        nameValue.put("referencedocumentid", referencedocumentid);
        nameValue.put("referencedocumentqualifier", referencedocumentqualifier);
        nameValue.put("referencedocumentsequencenr", referencedocumentsequencenr);
        nameValue.put("referenceid", referenceid);
        nameValue.put("refererenceid", refererenceid);
        nameValue.put("refgrp", refgrp);
        nameValue.put("refint", refint);
        nameValue.put("refmes", refmes);
        nameValue.put("regularsalesamount", regularsalesamount);
        nameValue.put("removalamount", removalamount);
        nameValue.put("removalcount", removalcount);
        nameValue.put("reportcount", reportcount);
        nameValue.put("requestedamount", requestedamount);
        nameValue.put("retailquantity", retailquantity);
        nameValue.put("retailreasoncode", retailreasoncode);
        nameValue.put("retailsequencenumber", retailsequencenumber);
        nameValue.put("retailstoreid", retailstoreid);
        nameValue.put("retailtypecode", retailtypecode);
        nameValue.put("ringtime", ringtime);
        nameValue.put("salesamount", salesamount);
        nameValue.put("salesunitofmeasure", salesunitofmeasure);
        nameValue.put("salesunitofmeasure_iso", salesunitofmeasure_iso);
        nameValue.put("scanneditemamount", scanneditemamount);
        nameValue.put("scanneditemcount", scanneditemcount);
        nameValue.put("scantime", scantime);
        nameValue.put("serial", serial);
        nameValue.put("serialnumber", serialnumber);
        nameValue.put("shortamount", shortamount);
        nameValue.put("sndlad", sndlad);
        nameValue.put("sndpfc", sndpfc);
        nameValue.put("sndprn", sndprn);
        nameValue.put("sndsad", sndsad);
        nameValue.put("specialstockcode", specialstockcode);
        nameValue.put("status", status);
        nameValue.put("std", std);
        nameValue.put("stdmes", stdmes);
        nameValue.put("stdvrs", stdvrs);
        nameValue.put("storefinancialledgeraccountid", storefinancialledgeraccountid);
        nameValue.put("supplierid", supplierid);
        nameValue.put("taxamount", taxamount);
        nameValue.put("taxcount", taxcount);
        nameValue.put("taxsequencenumber", taxsequencenumber);
        nameValue.put("taxtypecode", taxtypecode);
        nameValue.put("tenderamount", tenderamount);
        nameValue.put("tendercount", tendercount);
        nameValue.put("tendercurrency", tendercurrency);
        nameValue.put("tendercurrency_iso", tendercurrency_iso);
        nameValue.put("tenderid", tenderid);
        nameValue.put("tendersequencenumber", tendersequencenumber);
        nameValue.put("tendertypecode", tendertypecode);
        nameValue.put("terminationamount", terminationamount);
        nameValue.put("terminationcount", terminationcount);
        nameValue.put("test", test);
        nameValue.put("tofromlocationid", tofromlocationid);
        nameValue.put("topartyid", topartyid);
        nameValue.put("trainingflag", trainingflag);
        nameValue.put("transactioncount", transactioncount);
        nameValue.put("transactioncurrency", transactioncurrency);
        nameValue.put("transactioncurrency_iso", transactioncurrency_iso);
        nameValue.put("transactionsequencenumber", transactionsequencenumber);
        nameValue.put("transactiontypecode", transactiontypecode);
        nameValue.put("transreasoncode", transreasoncode);
        nameValue.put("type", type);
        nameValue.put("umrez_val", umrez_val);
        nameValue.put("unitcount", unitcount);
        nameValue.put("units", units);
        nameValue.put("unitsshippedcount", unitsshippedcount);
        nameValue.put("validfromdate", validfromdate);
        nameValue.put("vehicleid", vehicleid);
        nameValue.put("voidedbusinessdaydate", voidedbusinessdaydate);
        nameValue.put("voidedline", voidedline);
        nameValue.put("voidedretailstoreid", voidedretailstoreid);
        nameValue.put("voidedtransactionsequencenumbe", voidedtransactionsequencenumbe);
        nameValue.put("voidedworkstationid", voidedworkstationid);
        nameValue.put("voidflag", voidflag);
        nameValue.put("workstationid", workstationid);
        nameValue.put("segment_name", segment_name);
        nameValue.put("dataflow_ddtm", dataflow_ddtm);
        nameValue.put("is_aligned_tran", is_aligned_tran);
        return nameValue;
    }

    private StringData parseToFormattedDateString(String input) {
        if (isNull(input) || input.isEmpty()) {
            return StringData.fromString(String.valueOf(input));
        }
        int year = substringToIntSafety(input, 0, 4, 1970);
        int month = substringToIntSafety(input, 4, 6, 1);
        int day = substringToIntSafety(input, 6, 8, 1);
        int hour = substringToIntSafety(input, 8, 10, 0);
        int minute = substringToIntSafety(input, 10, 12, 0);
        int second = substringToIntSafety(input, 12, 14, 0);
        return StringData.fromString(String.format(DATE_TIME_STRING_FORMAT, year, month, day, hour, minute, second));
    }

    private LocalDateTime parseToFormattedDate(String input) {
        if (isNull(input) || input.isEmpty()) {
            return null;
        }
        int year = substringToIntSafety(input, 0, 4, 1970);
        int month = substringToIntSafety(input, 4, 6, 1);
        int day = substringToIntSafety(input, 6, 8, 1);
        int hour = substringToIntSafety(input, 8, 10, 0);
        int minute = substringToIntSafety(input, 10, 12, 0);
        int second = substringToIntSafety(input, 12, 14, 0);
        return LocalDateTime.of(year, month, day, hour, minute, second);
    }

    private LocalDate stringToDate(String input) {
        try {
            int year = substringToIntSafety(input, 0, 4, 1970);
            int month = substringToIntSafety(input, 4, 6, 1);
            int day = substringToIntSafety(input, 6, 8, 1);
            int hour = substringToIntSafety(input, 8, 10, 0);
            int minute = substringToIntSafety(input, 10, 12, 0);
            int second = substringToIntSafety(input, 12, 14, 0);
            return LocalDate.of(year, Month.of(month), day);
        } catch (Exception e) {
            return null;
        }
    }

    private int substringToIntSafety(String input, int start, int end, int defaultValue) {
        try {
            if (input.length() >= end) {
                return Integer.parseInt(input.substring(start, end));
            }
        } catch (NumberFormatException e) {
            return defaultValue;
        }
        return defaultValue;
    }
}