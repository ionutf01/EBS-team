package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;

public class SplitTextBolt extends BaseRichBolt {
    private OutputCollector collector;
    private int totalWordCount = 0;
    private List<Integer> statisticsBoltTasks;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        // Obținem listele de task-uri pentru bolt-ul de statistici
        this.statisticsBoltTasks = context.getComponentTasks("statistics-bolt");
        System.out.println("SplitTextBolt: Statistics bolt tasks: " + statisticsBoltTasks);
    }

    @Override
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split("\\s+");

        for (String word : words) {
            word = word.trim().toLowerCase();
            if (!word.isEmpty()) {
                collector.emit(new Values(word));
                totalWordCount++;
            }
        }

        // Emitem numărul total de cuvinte direct către statistics-bolt
        if (!statisticsBoltTasks.isEmpty()) {
            collector.emitDirect(statisticsBoltTasks.get(0), "wordCount", new Values(totalWordCount));
            System.out.println("SplitTextBolt: Emis direct contorul de cuvinte: " + totalWordCount);
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Fluxul de date implicit
        declarer.declare(new Fields("word"));

        // Fluxul direct către bolt-ul de statistici
        declarer.declareStream("wordCount", true, new Fields("count"));
    }
}