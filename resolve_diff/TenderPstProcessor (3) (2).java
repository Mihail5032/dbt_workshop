package ru.x5.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TenderPstProcessor {
    private List<BaseTransactionKey> data;

    /**
     * Формирует ключ транзакции для группировки per-transaction.
     */
    private static String txnKey(BaseTransactionKey b) {
        return (b.getRetailStoreId() == null ? "" : b.getRetailStoreId()) + "|"
                + (b.getBusinessDayDate() == null ? "" : b.getBusinessDayDate()) + "|"
                + (b.getWorkstationId() == null ? "" : b.getWorkstationId()) + "|"
                + (b.getTransactionSequenceNumber() == null ? "" : b.getTransactionSequenceNumber());
    }

    public List<Tender> prepareTenderPst() {
        List<Transaction> tx = data.stream()
                .filter(x -> x.getSegmentName().equals("E1BPTRANSACTION"))
                .map(x -> (Transaction) x)
                .collect(Collectors.toList());
        List<Tender> te = data.stream()
                .filter(x -> x.getSegmentName().equals("E1BPTENDER"))
                .map(x -> (Tender) x)
                .collect(Collectors.toList());
        List<TenderExtension> tex = data.stream()
                .filter(x -> x.getSegmentName().equals("E1BPTENDEREXTENSIONS"))
                .map(x -> (TenderExtension) x)
                .collect(Collectors.toList());
        List<RetailLineItem> lineItems = data.stream()
                .filter(x -> x.getSegmentName().equals("E1BPRETAILLINEITEM"))
                .map(x -> (RetailLineItem) x)
                .collect(Collectors.toList());

        // Исключаем retailTypeCode=2020 из расчёта сумм (SAP отбрасывает эти позиции)
        List<RetailLineItem> effectiveLineItems = lineItems.stream()
                .filter(x -> !"2020".equals(x.getRetailTypeCode()))
                .collect(Collectors.toList());

        boolean isVprokExpress = data.stream()
                .filter(x -> x.getSegmentName().equals("E1BPTRANSACTEXTENSIO"))
                .map(x -> (TransactionExtension) x)
                .filter(x -> x.getFieldName() != null && x.getFieldName().contains("SOURCE"))
                .anyMatch(x -> x.getFieldValue() != null && x.getFieldValue().contains("vprok.express"));

        // Суммы per-transaction (без позиций 2020)
        Map<String, BigDecimal> salesByTxn = effectiveLineItems.stream()
                .filter(x -> x.getSalesAmount() != null)
                .collect(Collectors.groupingBy(TenderPstProcessor::txnKey,
                        Collectors.reducing(BigDecimal.ZERO, RetailLineItem::getSalesAmount, BigDecimal::add)));

        Map<String, BigDecimal> tenderByTxn = te.stream()
                .filter(x -> x.getTenderAmount() != null)
                .collect(Collectors.groupingBy(TenderPstProcessor::txnKey,
                        Collectors.reducing(BigDecimal.ZERO, Tender::getTenderAmount, BigDecimal::add)));

        // Ключи транзакций, у которых есть тендеры
        Set<String> txKeysWithTenders = te.stream()
                .map(TenderPstProcessor::txnKey)
                .collect(Collectors.toSet());

        // Ключи транзакций, у которых есть хотя бы одна не-2020 позиция
        Set<String> txKeysWithEffectiveItems = effectiveLineItems.stream()
                .map(TenderPstProcessor::txnKey)
                .collect(Collectors.toSet());

        List<Transaction> tx1014 = tx.stream()
                .filter(x -> "1014".equals(x.getTransactionTypeCode()))
                .collect(Collectors.toList());

        List<Tender> tenders = new ArrayList<>();

        for (Transaction x : tx1014) {
            String key = txnKey(x);
            BigDecimal sumSales = salesByTxn.getOrDefault(key, BigDecimal.ZERO);
            BigDecimal sumTender = tenderByTxn.getOrDefault(key, BigDecimal.ZERO);
            boolean hasTenders = txKeysWithTenders.contains(key);

            if (hasTenders) {
                // Part 1: есть тендеры, но суммы не совпадают → коррекция дельтой
                // ABAP: tendernumber = '9'
                BigDecimal delta = sumSales.subtract(sumTender);
                if (delta.abs().compareTo(BigDecimal.ONE) >= 0) {
                    Tender tender = createSyntheticTender(x, "3101", delta, isVprokExpress, "9");
                    tenders.add(tender);
                }
            } else if (txKeysWithEffectiveItems.contains(key)) {
                // Part 2: нет тендеров, есть не-2020 позиции → создаём оплату на сумму продаж (ABAP: даже если сумма = 0)
                // ABAP: tendernumber = '8'
                Tender tender = createSyntheticTender(x, "3101", sumSales, isVprokExpress, "8");
                tenders.add(tender);
            }
            // Если все позиции 2020 — тендер не создаём (SAP отбрасывает такие позиции)
        }

        // мутация 3108→3123
        applyThirdPartMutation(te, tex);
        return tenders;
    }

    private Tender createSyntheticTender(Transaction x, String tenderTypeCode,
                                          BigDecimal amount, boolean isVprokExpress,
                                          String tenderNumber) {
        Tender tender = new Tender(
                tenderNumber,
                tenderTypeCode,
                amount,
                null,
                null,
                null,
                null,
                null
        );
        tender.setTransactionSequenceNumber(x.transactionSequenceNumber);
        tender.setWorkstationId(isVprokExpress ? "0000001002" : x.workstationId);
        tender.setRetailStoreId(x.retailStoreId);
        tender.setBusinessDayDate(x.businessDayDate);
        tender.setTransactionTypeCode(x.transactionTypeCode);
        tender.setIs_aligned_tran(x.getIs_aligned_tran());  // наследуем флаг от транзакции
        return tender;
    }

    /**
     * Мутирует оригинальные тендеры 3108→3123 при наличии CERT_PARTY=RU02.
     */
    private void applyThirdPartMutation(List<Tender> te, List<TenderExtension> tex) {
        boolean isCertParty = tex.stream()
                .filter(x -> x.getFieldName() != null && x.getFieldName().contains("CERT_PARTY"))
                .anyMatch(x -> x.getFieldValue() != null && x.getFieldValue().contains("RU02"));
        if (isCertParty) {
            te.forEach(x -> {
                if ("3108".equals(x.getTenderTypeCode())) {
                    x.setTenderTypeCode("3123");
                }
            });
        }
    }

}