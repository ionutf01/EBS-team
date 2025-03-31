package org.example.reliable;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class ReliableTerminalBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> finalCounts;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.finalCounts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        try {
            String word = input.getString(0);
            Integer count = input.getInteger(1);

            finalCounts.put(word, count);
            System.out.println("Final: '" + word + "' = " + count);

            // Confirmăm procesarea cu succes
            collector.ack(input);

        } catch (Exception e) {
            System.err.println("Eroare în TerminalBolt: " + e.getMessage());
            // Notificăm eșecul procesării
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Fiind terminal, nu emite nimic
    }

    @Override
    public void cleanup() {
        System.out.println("\n--- REZULTATE FINALE ---");
        finalCounts.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));
    }
}