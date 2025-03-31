package org.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class StatisticsBolt extends BaseRichBolt {
    private OutputCollector collector;
    private int totalSentences = 0;
    private int totalIndividualWords = 0;
    private int totalTaskId;

    @Override
    public void prepare(Map<String, Object> stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.totalTaskId = context.getThisTaskId();
        System.out.println("StatisticsBolt inițializat cu task ID: " + totalTaskId);
    }

    @Override
    public void execute(Tuple input) {
        String streamId = input.getSourceStreamId();

        if (streamId.equals("sentenceCount")) {
            // Flux direct de la spout cu numărul de propoziții
            int sentenceCount = input.getInteger(0);
            totalSentences = sentenceCount;
            System.out.println("StatisticsBolt: Număr total de propoziții procesate: " + totalSentences);
        }
        else if (streamId.equals("wordCount")) {
            // Flux direct de la split-bolt cu numărul total de cuvinte individuale
            int wordCount = input.getInteger(0);
            totalIndividualWords = wordCount;
            System.out.println("StatisticsBolt: Număr total de cuvinte individuale procesate: " + totalIndividualWords);
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Acest bolt nu emite nimic, fiind terminal
    }

    @Override
    public void cleanup() {
        System.out.println("\n--- STATISTICI FINALE ---");
        System.out.println("Număr total de propoziții procesate: " + totalSentences);
        System.out.println("Număr total de cuvinte individuale procesate: " + totalIndividualWords);

        if (totalSentences > 0) {
            double avgWordsPerSentence = (double) totalIndividualWords / totalSentences;
            System.out.printf("Media cuvintelor per propoziție: %.2f\n", avgWordsPerSentence);
        }
    }
}