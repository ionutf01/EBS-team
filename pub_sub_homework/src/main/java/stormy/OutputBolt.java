package stormy;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class OutputBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final String outputType;
    private PrintWriter writer;
    private AtomicInteger count = new AtomicInteger(0);
    private long startTime;

    public OutputBolt(String outputType) {
        this.outputType = outputType;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.startTime = System.currentTimeMillis();

        try {
            writer = new PrintWriter(new FileWriter(outputType + ".txt"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create output file", e);
        }
    }

    @Override
    public void execute(Tuple input) {
        int id;
        Object data;

        if (outputType.equals("publications")) {
            id = input.getIntegerByField("publicationId");
            data = input.getValueByField("publication");
        } else {
            id = input.getIntegerByField("subscriptionId");
            data = input.getValueByField("subscription");
        }

        // Write to file
        writer.println(id + ": " + data);

        // Log progress periodically
        int current = count.incrementAndGet();
        if (current % 1000 == 0) {
            long elapsedTime = System.currentTimeMillis() - startTime;
            System.out.println("Generated " + current + " " + outputType +
                    " in " + elapsedTime + " ms");
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {
        if (writer != null) {
            writer.close();
        }

        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("Finished generating " + count.get() + " " + outputType +
                " in " + totalTime + " ms");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields as this is a terminal bolt
    }
}