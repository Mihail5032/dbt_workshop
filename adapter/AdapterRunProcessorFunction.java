package ru.Processors;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import ru.Entities.*;
import ru.Utils.EntitiesUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static ru.Utils.EntitiesUtils.*;
import static ru.Utils.SapPIUtils.sendFilesToSAP;

@Slf4j
public class AdapterRunProcessorFunction extends KeyedProcessFunction<String, Row, String> {

    // Key: rtl_txn_fin_rk (один IDOC = одно финансовое движение)
    private transient ValueState<Map<String, WPUFIB01>> wpufib01MapState;
    private transient ValueState<Map<String, E1WPF01>> e1wpf01MapState;
    private transient ValueState<Map<String, E1WPF02>> e1wpf02MapState; // Теперь один E1WPF02 на финансовое движение
    private transient ValueState<Map<String, List<E1WXX01>>> e1wxx01MapState; // Key: rtl_txn_fin_rk -> extensions
    private transient ValueState<Map<String, String>> retailStoreIdMapState; // Key: rtl_txn_fin_rk -> retailstoreid
    private transient ValueState<Long> globalTimerState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        wpufib01MapState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("wpufib01MapState",
                    TypeInformation.of(new TypeHint<Map<String, WPUFIB01>>() {}), new HashMap<>())
        );
        e1wpf01MapState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("e1wpf01MapState",
                    TypeInformation.of(new TypeHint<Map<String, E1WPF01>>() {}), new HashMap<>())
        );
        e1wpf02MapState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("e1wpf02MapState",
                    TypeInformation.of(new TypeHint<Map<String, E1WPF02>>() {}), new HashMap<>())
        );
        e1wxx01MapState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("e1wxx01MapState",
                    TypeInformation.of(new TypeHint<Map<String, List<E1WXX01>>>() {}), new HashMap<>())
        );
        retailStoreIdMapState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("retailStoreIdMapState",
                    TypeInformation.of(new TypeHint<Map<String, String>>() {}), new HashMap<>())
        );
        globalTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("globalTimerState", TypeInformation.of(Long.class), -1L)
        );
    }

    @Override
    public void processElement(Row value, KeyedProcessFunction<String, Row, String>.Context context,
                              Collector<String> out) throws Exception {
        log.info("Starting to create WPUFIB file");

        long currentTimestamp = context.timestamp() != null ? context.timestamp() : System.currentTimeMillis();
        long globalTimeout = globalTimerState.value();

        // Extract fields from SQL result - конвертация в String
        // varbinary → String
        String rtlTxnRk = new String((byte[]) value.getField("rtl_txn_rk"), StandardCharsets.UTF_8);
        String rtlTxnFinRk = new String((byte[]) value.getField("rtl_txn_fin_rk"), StandardCharsets.UTF_8);

        // varchar → String (уже String)
        String retailstoreid = (String) value.getField("retailstoreid");
        String transactiontypecode = (String) value.getField("transactiontypecode");
        String transactionsequencenumber = (String) value.getField("transactionsequencenumber");
        String workstationid = (String) value.getField("workstationid");
        String refererenceid = (String) value.getField("refererenceid");
        String financialtypecode = (String) value.getField("financialtypecode");

        // date → String
        String businessdaydate = value.getField("businessdaydate").toString();

        // integer → String
        String financialsequencenumber = value.getField("financialsequencenumber").toString();

        // decimal → String
        String amount = value.getField("amount").toString();
        String turnover = value.getField("turnover").toString();

        // varchar → String (уже String)
        String financialcurrency = (String) value.getField("financialcurrency");
        String plant = (String) value.getField("plant");
        String fieldgroup = (String) value.getField("fieldgroup");
        String fieldname = (String) value.getField("fieldname");
        String fieldvalue = (String) value.getField("fieldvalue");

        // Create unique key for financial movement grouping (один IDOC = одно финансовое движение rtl_txn_fin_rk)
        String finMovementKey = rtlTxnFinRk;

        // Set timer on first record
        if (globalTimeout == -1L) {
            globalTimeout = currentTimestamp + TimeUnit.SECONDS.toMillis(15);
            globalTimerState.update(globalTimeout);
            context.timerService().registerProcessingTimeTimer(globalTimeout);
            log.info("Timer for transaction set for " + globalTimeout);
        }

        // Store retailstoreid for this financial movement
        Map<String, String> retailStoreIdMap = retailStoreIdMapState.value();
        if (!retailStoreIdMap.containsKey(finMovementKey)) {
            retailStoreIdMap.put(finMovementKey, retailstoreid);
            retailStoreIdMapState.update(retailStoreIdMap);
        }

        // Get or create E1WPF01 header for this financial movement
        Map<String, E1WPF01> e1wpf01Map = e1wpf01MapState.value();
        if (!e1wpf01Map.containsKey(finMovementKey)) {
            E1WPF01 e1wpf01 = EntitiesUtils.parseFlinkTableRowToE1WPF01(
                businessdaydate,
                transactionsequencenumber,
                financialtypecode
            );
            e1wpf01Map.put(finMovementKey, e1wpf01);
            e1wpf01MapState.update(e1wpf01Map);

            // Initialize WPUFIB01
            Map<String, WPUFIB01> wpufib01Map = wpufib01MapState.value();
            wpufib01Map.put(finMovementKey, new WPUFIB01());
            wpufib01MapState.update(wpufib01Map);
        }

        // Process E1WPF02 item (one per financial movement)
        if (financialsequencenumber != null && !financialsequencenumber.isEmpty()) {
            Map<String, E1WPF02> e1wpf02Map = e1wpf02MapState.value();

            // Create E1WPF02 only if not exists for this financial movement
            if (!e1wpf02Map.containsKey(finMovementKey)) {
                E1WPF02 e1wpf02 = EntitiesUtils.parseFlinkTableRowToE1WPF02(
                    financialsequencenumber,
                    refererenceid,
                    amount,
                    financialcurrency
                );
                e1wpf02Map.put(finMovementKey, e1wpf02);
                e1wpf02MapState.update(e1wpf02Map);
            }
        }

        // Process E1WXX01 extensions (multiple per financial movement)
        if (fieldname != null && fieldvalue != null) {
            E1WXX01 e1wxx01 = EntitiesUtils.parseFlinkTableRowToE1WXX01(fieldgroup, fieldname, fieldvalue);
            if (e1wxx01 != null) {
                Map<String, List<E1WXX01>> e1wxx01Map = e1wxx01MapState.value();

                // Extensions are grouped by financial movement key
                List<E1WXX01> extensions = e1wxx01Map.computeIfAbsent(finMovementKey, k -> new ArrayList<>());
                extensions.add(e1wxx01);
                e1wxx01MapState.update(e1wxx01Map);
            }
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        log.info("Timer triggered at " + timestamp);

        List<WPUFIB01> wpufib01List = new ArrayList<>();
        Map<String, WPUFIB01> wpufib01Map = wpufib01MapState.value();
        Map<String, E1WPF01> e1wpf01Map = e1wpf01MapState.value();
        Map<String, E1WPF02> e1wpf02Map = e1wpf02MapState.value();
        Map<String, List<E1WXX01>> e1wxx01Map = e1wxx01MapState.value();
        Map<String, String> retailStoreIdMap = retailStoreIdMapState.value();

        // Build complete WPUFIB01 structures (один IDOC на финансовое движение)
        for (Map.Entry<String, WPUFIB01> entry : wpufib01Map.entrySet()) {
            String finMovementKey = entry.getKey();
            WPUFIB01 wpufib01 = entry.getValue();
            E1WPF01 e1wpf01 = e1wpf01Map.get(finMovementKey);

            if (e1wpf01 != null) {
                // Create IDOC structure
                wpufib01.setIdoc(new IDOC());
                wpufib01.getIdoc().setBegin("1");

                // Get retailstoreid from stored map
                String retailstoreid = retailStoreIdMap.get(finMovementKey);
                if (retailstoreid == null) {
                    retailstoreid = "UNKNOWN";
                }

                wpufib01.getIdoc().setEdiDc40(createEdiDc40(retailstoreid));
                wpufib01.getIdoc().setE1wpf01(e1wpf01);

                // Attach single E1WPF02 line item for this financial movement
                E1WPF02 e1wpf02 = e1wpf02Map.get(finMovementKey);
                if (e1wpf02 != null) {
                    e1wpf01.setE1wpf02List(Arrays.asList(e1wpf02));

                    // Attach E1WXX01 extensions to this line item
                    List<E1WXX01> extensions = e1wxx01Map.get(finMovementKey);
                    if (extensions != null && !extensions.isEmpty()) {
                        e1wpf02.setE1wxx01List(extensions);
                    }
                }

                wpufib01List.add(wpufib01);
            }
        }

        sendFilesToSAP(wpufib01List);
        clearState();
    }

    private void clearState() throws IOException {
        wpufib01MapState.clear();
        e1wpf01MapState.clear();
        e1wpf02MapState.clear();
        e1wxx01MapState.clear();
        retailStoreIdMapState.clear();
        globalTimerState.clear();
    }
}
