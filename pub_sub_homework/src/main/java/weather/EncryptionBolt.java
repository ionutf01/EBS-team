package weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Un bolt intermediar al cărui singur rol este să primească date în clar
 * și să le emită mai departe sub formă criptată (amprentată).
 */
public class EncryptionBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        // Verificăm dacă am primit o publicație sau o subscripție
        if (input.getSourceComponent().equals("publisher-spout")) {
            Publication originalPub = (Publication) input.getValueByField("publication");
            EncryptedPublication encryptedPub = new EncryptedPublication(originalPub);
            collector.emit("encrypted-publications", new Values(encryptedPub));
        } else if (input.getSourceComponent().equals("subscription-spout")) {
            Subscription originalSub = (Subscription) input.getValueByField("subscription");
            EncryptedSubscription encryptedSub = new EncryptedSubscription(originalSub);
            collector.emit("encrypted-subscriptions", new Values(encryptedSub));
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("encrypted-publications", new Fields("encryptedPublication"));
        declarer.declareStream("encrypted-subscriptions", new Fields("encryptedSubscription"));
    }
}