package ru.x5.process;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.HashMap;
import java.util.Map;

public class StreamSideOutputTag {
    private static final Map<String, OutputTag<RowData>> TAG_MAP = new HashMap<>();
    static {
        TAG_MAP.put("E1BPSOURCEDOCUMENTLI", new OutputTag<RowData>("raw_bpsourcedocumentli"){});
        TAG_MAP.put("E1BPFINACIALMOVEMENT", new OutputTag<RowData>("raw_bpfinacialmovement"){});
        TAG_MAP.put("E1BPFINANCIALMOVEMEN", new OutputTag<RowData>("raw_bpfinancialmovemen"){});
        TAG_MAP.put("E1BPLINEITEMDISCEXT", new OutputTag<RowData>("raw_bplineitemdiscext"){});
        TAG_MAP.put("E1BPLINEITEMDISCOUNT", new OutputTag<RowData>("raw_bplineitemdiscount"){});
        TAG_MAP.put("E1BPLINEITEMEXTENSIO", new OutputTag<RowData>("raw_bplineitemextensio"){});
//        TAG_MAP.put("E1BPLINEITEMEXTENSIO", new OutputTag<RowData>("bplineitemextensio_pre"){});
        TAG_MAP.put("E1BPLINEITEMTAX", new OutputTag<RowData>("raw_bplineitemtax"){});
        TAG_MAP.put("E1BPRETAILLINEITEM", new OutputTag<RowData>("raw_bpretaillineitem"){});
        TAG_MAP.put("E1BPRETAILTOTALS", new OutputTag<RowData>("raw_bpretailtotals"){});
        TAG_MAP.put("E1BPTENDER", new OutputTag<RowData>("raw_bptender"){});
//        TAG_MAP.put("E1BPTENDER", new OutputTag<RowData>("bptender_pre"){});
        TAG_MAP.put("E1BPTENDEREXTENSIONS", new OutputTag<RowData>("raw_bptenderextensions"){});
//        TAG_MAP.put("E1BPTENDEREXTENSIONS", new OutputTag<RowData>("bptenderextensions_pre"){});
        TAG_MAP.put("E1BPTENDERTOTALS", new OutputTag<RowData>("raw_bptendertotals"){});
        TAG_MAP.put("E1BPTRANSACTDISCEXT", new OutputTag<RowData>("raw_bptransactdiscext"){});
//        TAG_MAP.put("E1BPTRANSACTION", new OutputTag<RowData>("raw_bptransaction"){});
        TAG_MAP.put("E1BPTRANSACTION", new OutputTag<RowData>("raw_bptransaction"){});

        TAG_MAP.put("E1BPTRANSACTIONDISCO", new OutputTag<RowData>("raw_bptransactiondisco"){});
        TAG_MAP.put("E1BPTRANSACTEXTENSIO", new OutputTag<RowData>("raw_bptransactextensio"){});
//        TAG_MAP.put("E1BPTRANSACTEXTENSIO", new OutputTag<RowData>("bptransactextensio_pre"){});
        TAG_MAP.put("E1BPLINEITEMVOID", new OutputTag<RowData>("raw_bplineitemvoid"){});
        TAG_MAP.put("E1BPPOSTVOIDDETAILS", new OutputTag<RowData>("raw_bppostvoiddetails"){});
        // PST side outputs
        TAG_MAP.put("PST_BPTRANSACTION", new OutputTag<RowData>("pst_bptransaction"){});
        TAG_MAP.put("PST_BPFINANCIALMOVEMEN", new OutputTag<RowData>("pst_bpfinancialmovemen"){});
        TAG_MAP.put("PST_BPTRANSACTEXTENSIO", new OutputTag<RowData>("pst_bptransactextensio"){});
        TAG_MAP.put("PST_BPFINANCIALMOVEMENTEXTENSIO", new OutputTag<RowData>("pst_bpfinancialmovementextensio"){});
        TAG_MAP.put("PST_BPRETAILLINEITEM", new OutputTag<RowData>("pst_bpretaillineitem"){});
        TAG_MAP.put("PST_BPLINEITEMEXTENSIO", new OutputTag<RowData>("pst_bplineitemextensio"){});
        TAG_MAP.put("PST_BPLINEITEMDISCOUNT", new OutputTag<RowData>("pst_bplineitemdiscount"){});
        TAG_MAP.put("PST_BPLINEITEMDISCEXT", new OutputTag<RowData>("pst_bplineitemdiscountext"){});
        TAG_MAP.put("PST_BPTRANSACTDISCEXT", new OutputTag<RowData>("pst_bptransactdiscoext"){});
        TAG_MAP.put("PST_BPTENDER", new OutputTag<RowData>("pst_bptender"){});
        TAG_MAP.put("PST_BPTENDEREXTENSIONS", new OutputTag<RowData>("pst_bptenderextensions"){});
    }
    public static OutputTag<RowData> getTag(String segmentName) {
        return TAG_MAP.get(segmentName);
    }
    private StreamSideOutputTag() {}
}