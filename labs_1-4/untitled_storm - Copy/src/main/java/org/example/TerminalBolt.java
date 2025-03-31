package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class TerminalBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> finalCounts;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.finalCounts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = input.getInteger(1);

        // Stocăm contorul pentru fiecare cuvânt
        finalCounts.put(word, count);
        System.out.println("TerminalBolt: Cuvântul '" + word + "' apare de " + count + " ori");

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Acest bolt nu emite nimic, fiind terminal
    }

    @Override
    public void cleanup() {
        System.out.println("\n--- NUMĂRĂTOARE FINALĂ CUVINTE ---");
        finalCounts.entrySet().stream()
                .sorted((e1, e2) -> e2.getValue().compareTo(e1.getValue()))
                .forEach(entry -> System.out.println(entry.getKey() + ": " + entry.getValue()));
    }
}