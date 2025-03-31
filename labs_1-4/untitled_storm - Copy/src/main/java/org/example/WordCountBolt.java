package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> wordCounts;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.wordCounts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = wordCounts.getOrDefault(word, 0);
        count++;
        wordCounts.put(word, count);
        collector.emit(new Values(word, count));
        System.out.println("WordCountBolt: Cuvântul '" + word + "' apare de " + count + " ori");
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}