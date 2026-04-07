package ru.x5.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.x5.model.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

public class TransactionCorrector implements Serializable {
    private static final Logger log = LogManager.getLogger("ru.x5.parser");
    private static final Set<String> VALID_TRANSACTION_TYPES_ADD = Set.of("1002", "1003");
    private static final Set<String> VALID_TRANSACTION_TYPES_REMOVE = Set.of("1001");

    public List<BaseTransactionKey> performCorrection(IDocWrapper doc) {
        List<BaseTransactionKey> corrBaseTransactionKeyList = new ArrayList<>();
        PostrCreateMultip postrCreateMultip = doc.getIdoc().getPostrCreateMultip();
        updateCancelledRetailLineItem(postrCreateMultip);
        List<Tender> tenders = postrCreateMultip.getTenders();
        List<RetailLineItem> retailLineItems = postrCreateMultip.getRetailLineItems();
        correctionSumTenderAmountSalesAmount(tenders, retailLineItems);
        corrBaseTransactionKeyList = getListBaseTransactionKeys(postrCreateMultip);
        refreshWorkstationIdVprok(postrCreateMultip.getTransactionExtensions(), corrBaseTransactionKeyList);
        setFlagForCanceledReceipts(tenders, retailLineItems, corrBaseTransactionKeyList);
        return corrBaseTransactionKeyList;
    }

    public List<BaseTransactionKey> getListBaseTransactionKeys(PostrCreateMultip postrCreateMultip) {
        List<BaseTransactionKey> baseTransactionKeys = new ArrayList<>();
        baseTransactionKeys.addAll(postrCreateMultip.getFinancialMovemen());
        baseTransactionKeys.addAll(postrCreateMultip.getFinacialMovement());
        baseTransactionKeys.addAll(postrCreateMultip.getLineItemDiscExtensions());
        baseTransactionKeys.addAll(postrCreateMultip.getLineItemDiscounts());
        baseTransactionKeys.addAll(postrCreateMultip.getLineItemExtensions());
        baseTransactionKeys.addAll(postrCreateMultip.getLineItemTaxes());
        baseTransactionKeys.addAll(postrCreateMultip.getRetailLineItems());
        baseTransactionKeys.addAll(postrCreateMultip.getRetailTotals());
        baseTransactionKeys.addAll(postrCreateMultip.getTenders());
        baseTransactionKeys.addAll(postrCreateMultip.getTenderExtensions());
        baseTransactionKeys.addAll(postrCreateMultip.getTenderTotals());
        baseTransactionKeys.addAll(postrCreateMultip.getTransactDiscExts());
        baseTransactionKeys.addAll(postrCreateMultip.getTransactions());
        baseTransactionKeys.addAll(postrCreateMultip.getTransactionDiscounts());
        baseTransactionKeys.addAll(postrCreateMultip.getTransactionExtensions());
        baseTransactionKeys.addAll(postrCreateMultip.getLineItemVoids());
        baseTransactionKeys.addAll(postrCreateMultip.getPostVoidDetails());
        return baseTransactionKeys;
    }

    public void refreshWorkstationIdVprok(List<TransactionExtension> transactionExtensions, List<BaseTransactionKey> baseTransactionKeys) {
        List<String> transactionKeys = new ArrayList<>(); //получение транзакций впрок
        for (TransactionExtension transactionExtension : transactionExtensions) {
            if (transactionExtension.getFieldName()!=null && transactionExtension.getFieldValue()!=null &&
                    transactionExtension.getFieldName().equals("SOURCE") &&
                    transactionExtension.getFieldValue().equals("vprok.express")) {
                transactionKeys.add(transactionExtension.getTransactionKey().toString());
            }
        }
        for (BaseTransactionKey transactionKey : baseTransactionKeys) {
            if (transactionKeys.contains(transactionKey.getTransactionKey().toString())) {
                log.info("Update workstationid 1002 for " + transactionKey.toString());
                transactionKey.setWorkstationId("1002");
            }
        }
    }

    private void eliminationDiscrepancyInAmounts(List<RetailLineItem> retailLineItems, Map<BaseTransactionKey, BigDecimal> tenderAmountSumAdd,
                                                 Map<BaseTransactionKey, BigDecimal> salesAmountSumAdd,
                                                 Map<BaseTransactionKey, Integer> lineItemCount, boolean toRemove) {
        for (Map.Entry<BaseTransactionKey, BigDecimal> entryTenderAmountAdd : tenderAmountSumAdd.entrySet()) {
            BaseTransactionKey key = entryTenderAmountAdd.getKey();
            BigDecimal tenderSum = entryTenderAmountAdd.getValue();
            BigDecimal salesSum = salesAmountSumAdd.get(key);
            if (tenderSum!=null && salesSum!=null) {
                BigDecimal diff = tenderSum.subtract(salesSum);
                if (diff.compareTo(BigDecimal.ZERO) != 0 && diff.abs().compareTo(BigDecimal.ONE) < 0) {
                    if (toRemove) removeLineItem(retailLineItems, key, diff);
                    else addCorrectionLineItem(key, diff, lineItemCount, retailLineItems);
                }
            }
        }
    }

    private void removeLineItem(List<RetailLineItem> retailLineItems, BaseTransactionKey key, BigDecimal diff) {
        boolean isDel = retailLineItems.removeIf(retailLineItem -> retailLineItem.getTransactionKey().equals(key)
                && Objects.equals(retailLineItem.getRetailTypeCode(), "2006") && retailLineItem.getSalesAmount()!=null
                && retailLineItem.getSalesAmount().compareTo(diff.negate()) == 0);
        if (isDel) log.info("Delete retail line item from " + retailLineItems.get(0).getTransactionKey().toString());
    }

    private void addCorrectionLineItem(BaseTransactionKey key, BigDecimal diff,
                                       Map<BaseTransactionKey, Integer> lineItemCount, List<RetailLineItem> retailLineItems) {
        RetailLineItem retailLineItem = RetailLineItem.builder()
                .retailStoreId(key.getRetailStoreId())
                .businessDayDate(key.getBusinessDayDate())
                .transactionTypeCode(key.getTransactionTypeCode())
                .workstationId(key.getWorkstationId())
                .transactionSequenceNumber(key.getTransactionSequenceNumber())
                .retailQuantity(BigDecimal.ONE.setScale(3, RoundingMode.UNNECESSARY))
                .itemIdQualifier("2").itemId("S000004").retailTypeCode("2805")
                .retailSequenceNumber(String.valueOf(lineItemCount.getOrDefault(key, 0) + 1))
                .salesAmount(diff).salesUnitOfMeasure("ST")
                .build();
        retailLineItems.add(retailLineItem);
        log.info("Add line item for " + retailLineItem.getTransactionKey().toString());
    }

    private void accumulateTenderAmounts(List<Tender> tenders, Map<BaseTransactionKey, BigDecimal> tenderAmountSumAdd,
                                         Map<BaseTransactionKey, BigDecimal> tenderAmountSumRemove) {
        for (Tender tender : tenders) {
            BaseTransactionKey key = tender.getTransactionKey();
            BigDecimal amount = tender.getTenderAmount();
            if (amount!=null) {
                if (VALID_TRANSACTION_TYPES_ADD.contains(tender.getTransactionTypeCode())) {
                    tenderAmountSumAdd.merge(key, amount, BigDecimal::add);
                } else if (VALID_TRANSACTION_TYPES_REMOVE.contains(tender.getTransactionTypeCode())) {
                    tenderAmountSumRemove.merge(key, amount, BigDecimal::add);
                }
            }
        }
    }

    private void accumulateSalesAmounts(List<RetailLineItem> retailLineItems,
                                        Map<BaseTransactionKey, BigDecimal> salesAmountSumAdd,
                                        Map<BaseTransactionKey, BigDecimal> salesAmountSumRemove,
                                        Map<BaseTransactionKey, Integer> lineItemCount) {
        for (RetailLineItem item : retailLineItems) {
            BaseTransactionKey key = item.getTransactionKey();
            BigDecimal amount = item.getSalesAmount();
            if (amount!=null) {
                if (VALID_TRANSACTION_TYPES_ADD.contains(item.getTransactionTypeCode())) {
                    salesAmountSumAdd.merge(key, amount, BigDecimal::add);
                    lineItemCount.merge(key, 1, Integer::sum);
                } else if (VALID_TRANSACTION_TYPES_REMOVE.contains(item.getTransactionTypeCode())) {
                    salesAmountSumRemove.merge(key, amount, BigDecimal::add);
                }
            }
        }
    }

    public void correctionSumTenderAmountSalesAmount(List<Tender> tenders, List<RetailLineItem> retailLineItems) {
        Map<BaseTransactionKey, BigDecimal> tenderAmountSumAdd = new HashMap<>();
        Map<BaseTransactionKey, BigDecimal> tenderAmountSumRemove = new HashMap<>();
        Map<BaseTransactionKey, BigDecimal> salesAmountSumAdd = new HashMap<>();
        Map<BaseTransactionKey, BigDecimal> salesAmountSumRemove = new HashMap<>();
        Map<BaseTransactionKey, Integer> lineItemCount = new HashMap<>();
        accumulateTenderAmounts(tenders, tenderAmountSumAdd, tenderAmountSumRemove);
        accumulateSalesAmounts(retailLineItems, salesAmountSumAdd, salesAmountSumRemove, lineItemCount);
        eliminationDiscrepancyInAmounts(retailLineItems, tenderAmountSumAdd, salesAmountSumAdd, lineItemCount, false);
        eliminationDiscrepancyInAmounts(retailLineItems, tenderAmountSumRemove, salesAmountSumRemove, lineItemCount, true);
    }

    private void updateCancelledRetailLineItem(PostrCreateMultip postrCreateMultip) {
        Map<String, List<RetailLineItem>> retailLineItemsCancel = new HashMap<>();
        Map<String, List<RetailLineItem>> retailLineItemsNotCancel = new HashMap<>();
        Map<String, String> lineItemExtTaxMap = new HashMap<>();
        Map<String, String> lineItemExtDataMatrixMap = new HashMap<>();
        for (RetailLineItem retailLineItem : postrCreateMultip.getRetailLineItems()) {
            if (Objects.equals(retailLineItem.getRetailTypeCode(), "2901")){
                if (retailLineItem.getRetailQuantity()!=null &&
                        retailLineItem.getRetailQuantity().compareTo(BigDecimal.ZERO) < 0) {
                    addToMapRetailLineItem(retailLineItemsCancel, retailLineItem); //сбор отмен
                }
                else retailLineItem.setRetailTypeCode("2001");
            }
            if (Objects.equals(retailLineItem.getRetailTypeCode(), "2010")){
                retailLineItem.setItemIdQualifier(null);
            }
            if (!Objects.equals(retailLineItem.getRetailTypeCode(), "2902")) {   //сбор несторнированных позиций
                addToMapRetailLineItem(retailLineItemsNotCancel, retailLineItem);
            }
        }
        if (!retailLineItemsCancel.isEmpty()) {
            for (LineItemExtension lineItemExtension : postrCreateMultip.getLineItemExtensions()) {  //сбор расширений
                if (lineItemExtension.getFieldName() != null && lineItemExtension.getFieldName().equals("DATAMATRIX")) {
                    lineItemExtDataMatrixMap.put(lineItemExtension.createKeyLineItem(), lineItemExtension.getFieldValue());
                }
                if (lineItemExtension.getFieldGroup() != null && lineItemExtension.getFieldGroup().equals("TAX")) {
                    addToMapLineItemExtensionTax(lineItemExtTaxMap, lineItemExtension);
                }
            }
        }
        for (Map.Entry<String, List<RetailLineItem>> retailLineItemCancelEntry : retailLineItemsCancel.entrySet()) {
            String key = retailLineItemCancelEntry.getKey();    //ключ по транзации
            List<RetailLineItem> retailLineItemsCancelList = retailLineItemCancelEntry.getValue();
            for (RetailLineItem retailLineItemCancel : retailLineItemsCancelList) {
                BigDecimal qty = retailLineItemCancel.getRetailQuantity() == null ? null : retailLineItemCancel.getRetailQuantity().negate();
                BigDecimal amnt = retailLineItemCancel.getSalesAmount() == null ? null : retailLineItemCancel.getSalesAmount().negate();
                String cisMarkDel = lineItemExtDataMatrixMap.get(retailLineItemCancel.createKeyLineItem());
                String alcMarkDel = lineItemExtTaxMap.get(retailLineItemCancel.createKeyLineItem());
                boolean flAlc = false;
                boolean flCis = false;
                if (alcMarkDel != null) {   //замена типа позиции продажи по номеру алкомарки
                    flAlc = updateRetailTypeCodeByMark(retailLineItemsNotCancel.get(key), retailLineItemCancel,
                            lineItemExtTaxMap, qty, amnt, alcMarkDel);
                }
                if (cisMarkDel != null) {   //замена типа позиции продажи по номеру КИЗ
                    flCis = updateRetailTypeCodeByMark(retailLineItemsNotCancel.get(key), retailLineItemCancel,
                            lineItemExtDataMatrixMap, qty, amnt, cisMarkDel);
                }
                if (!flAlc && !flCis) { //Товар не алкогольный или же не найден нужный номер алкомарки
                    updateRetailTypeCodeByMark(retailLineItemsNotCancel.get(key), retailLineItemCancel,
                            null, qty, amnt, null); //замена у первого найденного товара
                }
            }
        }
    }

    private boolean updateRetailTypeCodeByMark(List<RetailLineItem> retailLineItemsNotCancel, RetailLineItem retailLineItemCancel,
                                               Map<String, String> lineItemExtMap,
                                               BigDecimal qty, BigDecimal amnt, String markValue) {
        for (RetailLineItem item : retailLineItemsNotCancel) {
            if (Objects.equals(item.getItemId(), retailLineItemCancel.getItemId()) && item.getRetailQuantity() != null && qty!=null
                    && item.getRetailQuantity().compareTo(qty) == 0 && item.getSalesAmount() != null && amnt!=null
                    && item.getSalesAmount().compareTo(amnt) == 0
                    && !Objects.equals(item.getRetailTypeCode(), "2902")) {
                if (markValue != null) {
                    String key = item.createKeyLineItem();
                    String markValueItem = lineItemExtMap.get(key);
                    if (markValue.equals(markValueItem)) {
                        log.info("Update retail line item code to 2902 for sequence number" + item.getRetailSequenceNumber() + " "
                                + retailLineItemCancel.getTransactionKey().toString());
                        item.setRetailTypeCode("2902");
                        return true;
                    }
                } else {
                    log.info("Update retail line item code to 2902 for sequence number" + item.getRetailSequenceNumber() + " "
                            + retailLineItemCancel.getTransactionKey().toString());
                    item.setRetailTypeCode("2902");
                    return true;
                }
            }
        }
        return false;
    }

    private void addToMapRetailLineItem(Map<String, List<RetailLineItem>> retailLineItems, RetailLineItem retailLineItem) {
        String key = retailLineItem.getTransactionKey().toString();
        List<RetailLineItem> list = retailLineItems.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(retailLineItem);
    }

    private void addToMapLineItemExtensionTax(Map<String, String> lineItemExtensions, LineItemExtension lineItemExtension) {
        String key = lineItemExtension.createKeyLineItem();
        String value = lineItemExtension.getFieldValue();
        lineItemExtensions.merge(key, value, (existing, newValue) -> existing + newValue);  //сохранение конкатенации значений TAX
    }

    public void setFlagForCanceledReceipts(List<Tender> tenders,
                                           List<RetailLineItem> retailLineItems,
                                           List<BaseTransactionKey> baseTransactionKeys) {
        Map<BaseTransactionKey, BigDecimal> tenderAmountSum = new HashMap<>();
        Map<BaseTransactionKey, BigDecimal> salesAmountSum = new HashMap<>();
        sumTenderAmounts(tenders, tenderAmountSum);
        sumSalesAmounts(retailLineItems, salesAmountSum);
        comparingSumAmountsLineItemAndTender(tenderAmountSum, salesAmountSum, baseTransactionKeys);
    }

    private void sumTenderAmounts(List<Tender> tenders,
                                  Map<BaseTransactionKey, BigDecimal> tenderAmountSum) {
        for (Tender tender : tenders) {
            if (!isCheckTransaction(tender.getTransactionTypeCode())) {
                continue;
            }
            BaseTransactionKey key = tender.getTransactionKey();
            BigDecimal amount = tender.getTenderAmount();
            if (amount != null) {
                tenderAmountSum.merge(key, amount, BigDecimal::add);
            }
        }
    }

    private void sumSalesAmounts(List<RetailLineItem> retailLineItems,
                                 Map<BaseTransactionKey, BigDecimal> salesAmountSumAdd) {
        for (RetailLineItem item : retailLineItems) {
            if (!isCheckTransaction(item.getTransactionTypeCode())) {
                continue;
            }
            BaseTransactionKey key = item.getTransactionKey();
            BigDecimal amount = item.getSalesAmount();
            if (amount != null) {
                salesAmountSumAdd.merge(key, amount, BigDecimal::add);
            }
        }
    }

    private boolean isCheckTransaction(String transactionTypeCode) {
        return transactionTypeCode != null && transactionTypeCode.startsWith("10");
    }

    private void comparingSumAmountsLineItemAndTender(Map<BaseTransactionKey, BigDecimal> tenderAmountSum,
                                                      Map<BaseTransactionKey, BigDecimal> salesAmountSum,
                                                      List<BaseTransactionKey> baseTransactionKeys) {
        for (BaseTransactionKey key : baseTransactionKeys) {
            BigDecimal tenderSum = tenderAmountSum.get(key.getTransactionKey());
            BigDecimal salesSum = salesAmountSum.get(key.getTransactionKey());
            if (tenderSum != null && salesSum != null) {
                key.setIs_aligned_tran(tenderSum.compareTo(salesSum) == 0);
            } else {
                key.setIs_aligned_tran(null);
            }
        }
    }
}