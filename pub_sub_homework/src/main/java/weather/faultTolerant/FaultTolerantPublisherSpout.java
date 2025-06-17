package weather.faultTolerant;

// ... (toate importurile rămân la fel)
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import weather.Publication;


public class FaultTolerantPublisherSpout extends BaseRichSpout {
    private static final Logger LOG = LogManager.getLogger(FaultTolerantPublisherSpout.class);
    private SpoutOutputCollector collector;
    private BufferedReader reader;
    private Map<String, Publication> pendingMessages;
    private final String filePath;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("dd.MM.yyyy");

    public FaultTolerantPublisherSpout(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.pendingMessages = new HashMap<>();
        try {
            this.reader = new BufferedReader(new InputStreamReader(new FileInputStream(this.filePath)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Eroare critică: Fișierul de publicații nu a fost găsit: " + this.filePath, e);
        }
        Utils.sleep(2000);
    }

    @Override
    public void nextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) { Utils.sleep(100); return; }
            if (line.trim().isEmpty()) return;

            Map<String, String> valuesMap = new HashMap<>();
            String cleanedLine = line.substring(1, line.length() - 1);
            String[] pairs = cleanedLine.split(";");

            for (String pair : pairs) {
                String trimmedPair = pair.substring(1, pair.length() - 1);
                String[] keyValue = trimmedPair.split(",", 2);
                if (keyValue.length == 2) {
                    valuesMap.put(keyValue[0].trim().toLowerCase(), keyValue[1].trim().replace("\"", ""));
                }
            }

            // Folosim chei cu litere mici în validare, la fel ca la inserare.
            if (!valuesMap.containsKey("stationid") || !valuesMap.containsKey("city") || !valuesMap.containsKey("temp") ||
                    !valuesMap.containsKey("rain") || !valuesMap.containsKey("wind") || !valuesMap.containsKey("direction") ||
                    !valuesMap.containsKey("date")) {
                LOG.warn("Linie invalidă în publications.txt (lipsesc câmpuri): {}", line);
                return;
            }

            int stationId = Integer.parseInt(valuesMap.get("stationid"));
            String city = valuesMap.get("city");
            int temp = Integer.parseInt(valuesMap.get("temp"));
            double rain = Double.parseDouble(valuesMap.get("rain"));
            int wind = Integer.parseInt(valuesMap.get("wind"));
            String direction = valuesMap.get("direction");
            LocalDate date = LocalDate.parse(valuesMap.get("date"), DATE_FORMATTER);

            Publication publication = new Publication(stationId, city, temp, rain, wind, direction, date);

            String msgId = UUID.randomUUID().toString();
            this.pendingMessages.put(msgId, publication);
            this.collector.emit(new Values(publication), msgId);

        } catch (Exception e) {
            LOG.error("Eroare la parsarea liniei: '" + e.getMessage() + "'", e);
        }
    }

    @Override
    public void ack(Object msgId) { this.pendingMessages.remove(msgId.toString()); }
    @Override
    public void fail(Object msgId) {
        LOG.warn("SPOUT FAIL: Se retrimite mesajul {}.", msgId);
        Publication pub = this.pendingMessages.get(msgId.toString());
        if (pub != null) this.collector.emit(new Values(pub), msgId);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("publication")); }
}