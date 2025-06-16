package weather;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;

public class StatefulAggregatingBolt extends BaseStatefulBolt<WindowState> {
    private static final Logger LOG = LogManager.getLogger(StatefulAggregatingBolt.class);
    private WindowState state;
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void initState(WindowState state) {
        if (state != null) {
            this.state = state;
            LOG.warn("STATEFUL BOLT: Stare RESTAURATĂ cu succes: {}", this.state);
        } else {
            this.state = new WindowState();
            LOG.info("STATEFUL BOLT: Stare nouă INIȚIALIZATĂ: {}", this.state);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Publication pub = (Publication) tuple.getValueByField("publication");
        state.addTemperature(pub.getTemp());
        LOG.info("STATEFUL BOLT: Primit temp={}. Noua stare este: {}", pub.getTemp(), state);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Capăt de linie
    }
}