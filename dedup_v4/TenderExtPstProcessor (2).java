package ru.x5.model;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import lombok.*;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TenderExtPstProcessor {
    private List<BaseTransactionKey> data;

    public List<TenderExtension> prepareTenderExtensionPst() {
        List<TenderExtension> tex = this.data.stream()
                .filter((x) -> x.getSegmentName().contains("E1BPTENDEREXTENSIONS"))
                .map((x) -> (TenderExtension)x).collect(Collectors.toList());
        boolean present = tex.stream().anyMatch((x) -> {
            if (x.getFieldValue() != null && x.getFieldName() != null) {
                return (x.getFieldValue().contains("RU02") || x.getFieldValue().contains(".0")) && (x.getFieldName().contains("CERT_PARTY") || x.getFieldName().contains("CERT_PRICE"));
            } else {
                return false;
            }
        });
        for (TenderExtension tenderExtension : tex) {
            String fieldName = tenderExtension.getFieldName();
            if (fieldName.contains("CERT_PRICE")) {
                tenderExtension.setFieldValue(tenderExtension.getFieldValue().split("\\.")[1]);
            }
        }
        return tex;
    }

}
