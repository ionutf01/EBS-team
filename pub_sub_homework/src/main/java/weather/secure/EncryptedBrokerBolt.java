package weather.secure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EncryptedBrokerBolt extends BaseRichBolt {
    private static final Logger LOG = LogManager.getLogger(EncryptedBrokerBolt.class);
    private List<EncryptedSubscription> encryptedSubscriptions;

    // CORECȚIE 1: Declarăm colectorul ca variabilă membră a clasei.
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.encryptedSubscriptions = new ArrayList<>();
        // CORECȚIE 2: Inițializăm variabila membră cu colectorul primit de la Storm.
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals("encrypted-subscriptions")) {
            EncryptedSubscription sub = (EncryptedSubscription) input.getValueByField("encryptedSubscription");
            encryptedSubscriptions.add(sub);
            LOG.info("Broker Criptat: Am primit o nouă subscripție criptată.");
        } else if (input.getSourceStreamId().equals("encrypted-publications")) {
            EncryptedPublication pub = (EncryptedPublication) input.getValueByField("encryptedPublication");
            LOG.info("Broker Criptat: Verific o publicație criptată...");

            for (EncryptedSubscription sub : encryptedSubscriptions) {
                if (matches(pub, sub)) {
                    LOG.warn("POTRIVIRE CRIPTATĂ GĂSITĂ! Notificarea ar fi trimisă aici.");
                    // Aici s-ar emite un nou tuple către un bolt de notificare
                    // ex: this.collector.emit(new Values(...));
                }
            }
        }
        // Acum, 'collector' este recunoscut și poate fi folosit pentru a confirma procesarea.
        this.collector.ack(input);
    }

    private boolean matches(EncryptedPublication pub, EncryptedSubscription sub) {
        if (sub.hashedConditions.isEmpty()) {
            return false;
        }
        for (Map.Entry<String, String> condition : sub.hashedConditions.entrySet()) {
            String hashedField = condition.getKey();
            String hashedValue = condition.getValue();
            if (!pub.hashedFields.containsKey(hashedField) || !pub.hashedFields.get(hashedField).equals(hashedValue)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Nu emitem nimic mai departe în acest exemplu simplificat.
    }
}