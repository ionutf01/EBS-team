package weather.advancedRouting;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import weather.Publication;

import java.util.Map;

/**
 * Acest Bolt este clientul final DEDICAT topologiei cu rutare avansată.
 * Primește doar publicațiile care au trecut cu succes de toate filtrele.
 */
public class AdvancedSubscriberBolt extends BaseRichBolt {
    private static final Logger LOG = LogManager.getLogger(AdvancedSubscriberBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        Publication publication = (Publication) tuple.getValueByField("publication");
        LOG.info(">>> DESTINAȚIE FINALĂ (Mod Avansat): Publicația a trecut de toate filtrele și a fost primită: {}", publication);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields());
    }
}