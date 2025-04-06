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

/**
 * OutputBolt serves as a terminal bolt in the Storm topology that writes
 * generated data (publications or subscriptions) to output files.
 * It also tracks performance metrics like generation count and elapsed time.
 */
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

    /**
     * Performs cleanup when the bolt is shutting down.
     * Closes the file writer and logs final performance statistics.
     */
    @Override
    public void cleanup() {
        // Close the writer to ensure all data is flushed to disk
        if (writer != null) {
            writer.close();
        }

        // Calculate final performance metrics
        long totalTime = System.currentTimeMillis() - startTime;
        int totalCount = count.get();
        double itemsPerSecond = (double) totalCount / (totalTime / 1000.0);
        double msPerItem = (double) totalTime / totalCount;

        // Print detailed performance summary
        System.out.println("\n===== PERFORMANCE SUMMARY FOR " + outputType.toUpperCase() + " =====");
        System.out.println("Total items generated: " + totalCount);
        System.out.println("Total processing time: " + totalTime + " ms (" + (totalTime/1000.0) + " seconds)");
        System.out.println("Average processing time: " + String.format("%.2f", msPerItem) + " ms per item");
        System.out.println("Throughput: " + String.format("%.2f", itemsPerSecond) + " items/second");
        System.out.println("=============================================\n");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // No output fields as this is a terminal bolt
    }
}