package org.example.reliable;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class ReliableCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> wordCounts;
    private Random random;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.wordCounts = new HashMap<>();
        this.random = new Random();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);

        try {
            // Simulăm ocazional o eroare
            if (random.nextInt(50) == 0) { // ~2% șansă de eșec
                throw new RuntimeException("Eroare simulată în ReliableCountBolt");
            }

            Integer count = wordCounts.getOrDefault(word, 0);
            count++;
            wordCounts.put(word, count);

            // Emitem rezultatul ancorat la tuplul de intrare
            collector.emit(input, new Values(word, count));
            System.out.println("Count: '" + word + "' = " + count +
                    " (ancorat la tuplul: " + input.getMessageId() + ")");

            // Confirmăm procesarea cu succes
            collector.ack(input);

        } catch (Exception e) {
            System.err.println("Eroare la numărarea cuvântului '" + word + "': " + e.getMessage());
            // Notificăm eșecul procesării
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}