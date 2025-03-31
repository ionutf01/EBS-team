package org.example.reliable;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class ReliableSourceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Map<UUID, String> pendingTuples; // Pentru a reține tuple-urile în așteptarea confirmării
    private int emittedCount = 0;
    private int ackedCount = 0;
    private int failedCount = 0;

    private String[] sentences = {
            "the cow jumped over the moon",
            "an apple a day keeps the doctor away",
            "four score and seven years ago",
            "snow white and the seven dwarfs",
            "i am at two with nature"
    };

    private int currentIndex = 0;

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.pendingTuples = new HashMap<>();
        System.out.println("ReliableSourceSpout inițializat");
    }

    @Override
    public void nextTuple() {
        if (currentIndex < sentences.length) {
            String sentence = sentences[currentIndex++];
            UUID msgId = UUID.randomUUID();

            // Stocăm tuplu pentru posibila retrimitere
            pendingTuples.put(msgId, sentence);

            // Emitem tuplu cu ID unic pentru tracking
            collector.emit(new Values(sentence), msgId);
            emittedCount++;

            System.out.println("Emis: '" + sentence + "' cu ID: " + msgId);
            System.out.println("Statistici spout - Emise: " + emittedCount +
                    ", Confirmate: " + ackedCount +
                    ", Eșuate: " + failedCount);

            Utils.sleep(100); // Pauză pentru demonstrație
        } else {
            Utils.sleep(500); // Așteptăm confirmări finale
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void ack(Object msgId) {
        // Tuplu procesat cu succes, îl eliminăm din mapul de așteptare
        pendingTuples.remove(msgId);
        ackedCount++;
        System.out.println("ACK pentru ID: " + msgId + " (Total confirmate: " + ackedCount + ")");
    }

    @Override
    public void fail(Object msgId) {
        // Tuplu a eșuat, îl reemitem
        String sentence = pendingTuples.get(msgId);
        if (sentence != null) {
            failedCount++;
            System.out.println("FAIL pentru ID: " + msgId + " - Reemitem: '" + sentence + "'");

            // Generăm un nou ID și reemitem tuplu
            UUID newMsgId = UUID.randomUUID();
            pendingTuples.put(newMsgId, sentence);
            pendingTuples.remove(msgId); // Eliminăm vechiul ID

            collector.emit(new Values(sentence), newMsgId);
            emittedCount++;
        }
    }
}