package stormy;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.example.Publication;

import java.time.LocalDate;
import java.util.Map;
import java.util.Random;

/**
 * PublicationGeneratorBolt generates random weather data publications.
 * This bolt receives publication IDs from upstream components and creates
 * corresponding synthetic weather data with realistic parameters.
 */
public class PublicationGeneratorBolt extends BaseRichBolt {

    private static final java.util.concurrent.atomic.AtomicInteger publishedCount = new java.util.concurrent.atomic.AtomicInteger(0);
    private OutputCollector collector;
    private static final String[] CITIES = {"Bucharest", "Cluj", "Iasi", "Timisoara", "Constanta", "Brasov", "Craiova"};
    private static final String[] DIRECTIONS = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"};

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int publicationId = input.getIntegerByField("publicationId");

        // Generate random publication
        Publication publication = generateRandomPublication();

        // Emit the publication
        collector.emit(new Values(publicationId, publication));

        // Increment the publication counter
        publishedCount.incrementAndGet();

        // Log progress periodically
        collector.ack(input);
    }

    private Publication generateRandomPublication() {
        Random rand = new Random();

        int stationId = rand.nextInt(100) + 1;
        String city = CITIES[rand.nextInt(CITIES.length)];
        int temp = rand.nextInt(41) - 10; // -10 to 30 degrees
        double rain = Math.round(rand.nextDouble() * 50 * 10) / 10.0; // 0 to 50 mm
        int wind = rand.nextInt(101); // 0 to 100 km/h
        String direction = DIRECTIONS[rand.nextInt(DIRECTIONS.length)];

        // Random date between 2023-01-01 and 2025-12-31
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        long days = startDate.toEpochDay();
        long endDays = LocalDate.of(2025, 12, 31).toEpochDay();
        long randomDay = rand.nextInt((int) (endDays - days)) + days;
        LocalDate date = LocalDate.ofEpochDay(randomDay);

        return new Publication(stationId, city, temp, rain, wind, direction, date);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publicationId", "publication"));
    }

    /**
     * Returns the current count of generated publications.
     *
     * @return the number of publications generated so far
     */
    public static int getPublishedCount() {
        return publishedCount.get();
    }

}