package weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BrokerBolt extends BaseRichBolt {
    private OutputCollector collector;
    private int brokerId;
    private final int windowSize;
    private Map<Integer, Subscription> allSubscriptions;
    private Map<Integer, List<Publication>> tumblingWindowBuffers;

    public BrokerBolt(int windowSize) { this.windowSize = windowSize; }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.brokerId = context.getThisTaskId();
        this.allSubscriptions = new HashMap<>();
        this.tumblingWindowBuffers = new HashMap<>();
        System.out.println("BROKER " + brokerId + ": Prepared with window size " + this.windowSize);
    }

    @Override
    public void execute(Tuple input) {
        String sourceComponent = input.getSourceComponent();
        if ("subscription-generator".equals(sourceComponent)) {
            handleSubscription(input);
        } else if ("publication-generator".equals(sourceComponent)) {
            handlePublication(input);
        }
        collector.ack(input);
    }

    private void handleSubscription(Tuple input) {
        Integer subId = input.getIntegerByField("subscriptionId");
        Subscription sub = (Subscription) input.getValueByField("subscription");
        allSubscriptions.put(subId, sub);
        System.out.println("BROKER " + brokerId + ": Registered subscription " + subId + ": " + sub);
        if (sub instanceof WindowedSubscription) {
            tumblingWindowBuffers.put(subId, new ArrayList<>());
        }
    }

    private void handlePublication(Tuple input) {
        Publication pub = (Publication) input.getValueByField("publication");
        for (Map.Entry<Integer, Subscription> entry : allSubscriptions.entrySet()) {
            Integer subId = entry.getKey();
            Subscription sub = entry.getValue();
            if (sub.matches(pub)) {
                if (sub instanceof WindowedSubscription) {
                    //Log pentru a vedea când se face o potrivire cu fereastra
                    System.out.println("BROKER " + brokerId + ": Tumbling window match found for sub " + subId + " with publication: " + pub);
                    processTumblingWindow(subId, (WindowedSubscription) sub, pub);
                } else {
                    //Log pentru a vedea când se face o potrivire
                    System.out.println("BROKER " + brokerId + ": Match found for sub " + subId + " with publication: " + pub);
                    collector.emit("notifications", new Values(subId, sub, pub.toString()));
                }
            }
        }
    }

    // În fișierul BrokerBolt.java

    private void processTumblingWindow(Integer subId, WindowedSubscription sub, Publication pub) {
        List<Publication> buffer = tumblingWindowBuffers.get(subId);
        if (buffer == null) return;

        buffer.add(pub);
        //Log pentru a vedea cum se umple bufferul
        System.out.println("BROKER " + brokerId + " [SubID " + subId + "]: Added to window buffer. Size is now " + buffer.size() + "/" + this.windowSize + ". Pub: " + pub);

        // Verificăm dacă fereastra s-a umplut
        if (buffer.size() == this.windowSize) {
            //Log pentru a anunța evaluarea
            System.out.println("BROKER " + brokerId + " [SubID " + subId + "]: Window is FULL. Evaluating conditions...");

            if (sub.checkWindow(buffer)) {
                // Acest mesaj exista deja
                System.out.println("BROKER " + brokerId + ": Tumbling window match for sub " + subId);
                String city = pub.getCity();
                String metaPublication = "{(city,=," + city + ");(conditions,=,true)}";
                collector.emit("notifications", new Values(subId, sub, metaPublication));
            } else {
                //Log pentru cazul în care fereastra e plină, dar condițiile nu sunt îndeplinite
                System.out.println("BROKER " + brokerId + " [SubID " + subId + "]: Window evaluated. NO match found.");
            }
            
            // Golim bufferul pentru a începe o nouă fereastră (acesta este "tumbling"-ul)
            buffer.clear();
            //Log care confirmă resetarea ferestrei
            System.out.println("BROKER " + brokerId + " [SubID " + subId + "]: Buffer CLEARED. Tumbling to next window.");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("notifications", new Fields("subscriptionId", "subscriptionObject", "matchedContent"));
    }
}