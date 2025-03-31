package org.example;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class SourceTextSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;
    private int sentenceCounter = 0;
    private List<Integer> statisticsBoltTasks;

    private String[] sentences = {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature",
            "the early bird catches the worm",
            "a picture is worth a thousand words",
            "all that glitters is not gold",
            "the quick brown fox jumps over the lazy dog",
            "to be or not to be that is the question"
    };

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();

        // Obținem listele de task-uri pentru bolt-ul de statistici
        this.statisticsBoltTasks = context.getComponentTasks("statistics-bolt");
        System.out.println("SourceTextSpout: Statistics bolt tasks: " + statisticsBoltTasks);
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);

        if (sentenceCounter < 20) {  // Limităm la 20 de propoziții pentru exemplu
            String sentence = sentences[random.nextInt(sentences.length)];

            // Emitem propoziția pe fluxul implicit
            collector.emit(new Values(sentence));

            // Incrementăm contorul și emitem valoarea direct către statistics-bolt
            sentenceCounter++;

            if (!statisticsBoltTasks.isEmpty()) {
                collector.emitDirect(statisticsBoltTasks.get(0), "sentenceCount", new Values(sentenceCounter));
                System.out.println("SourceTextSpout: Emis direct contorul de propoziții: " + sentenceCounter);
            }

            System.out.println("SourceTextSpout: Emis propoziția: '" + sentence + "'");
        } else {
            Utils.sleep(1000);  // Pauză lungă după ce am emis toate propozițiile
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Fluxul de date implicit
        declarer.declare(new Fields("sentence"));

        // Fluxul direct către bolt-ul de statistici
        declarer.declareStream("sentenceCount", true, new Fields("count"));
    }
}