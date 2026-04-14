package ru.x5.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.Schema;
import org.apache.log4j.Logger;
import ru.x5.decoder.CustomDecoder;
import ru.x5.http.TriggerHandler;
import ru.x5.xml.ParsingResult;
import ru.x5.xml.XmlParser;

import java.io.InputStream;
import java.util.List;

import static java.util.Objects.nonNull;

public class RawDataProcessFunction extends ProcessFunction<String, RowData> {

    private static final Logger log = Logger.getLogger(RawDataProcessFunction.class);

    private static final String SPLITTER = "split";
    private static final String PARSABLE_MESSAGE = "/POSDW/POSTR_CREATEMULTIPLE02";

    private final XmlParser xmlParser;
    private final TriggerHandler trigger;

    public RawDataProcessFunction(Schema icebergSchema) {
        this.xmlParser = new XmlParser(icebergSchema);
        this.trigger = new TriggerHandler();
    }

    @Override
    public void processElement(String kafkaValue, Context context, Collector<RowData> out) throws Exception {
        log.info("Kafka message: " + kafkaValue);
        String data = null;
        String messageType = null;
        for (String s : kafkaValue.split(System.lineSeparator())) {
            if (s.startsWith("MSGTYPE")) {
                messageType = s.split(SPLITTER)[1];
            }
            if (s.startsWith("DATA")) {
                data = s.split(SPLITTER)[1];
            }
        }
        if (nonNull(messageType) && messageType.equalsIgnoreCase(PARSABLE_MESSAGE)) {
            try (InputStream inputStream = CustomDecoder.decodeAndDecompress(data)) {
                ParsingResult parsingResult = xmlParser.parseExtension(inputStream);
                List<RowData> rowData = parsingResult.getRows();
                for (RowData rd : rowData) {
                    out.collect(rd);
                }
                if (parsingResult.isClosing()) {
                    trigger.triggerOnZeroStation(parsingResult.getStoreId());
                }
            } catch (Exception e) {
                log.warn("Error message: " + kafkaValue);
                log.error("Error on message parsing: ", e);
            }
        } else {
            log.warn("Wrong message type: " + messageType);
        }
    }

}
