package weather;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.time.LocalDate;
import java.util.Map;
import java.util.Random;

public class PublisherSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random rand;
    private static final String[] CITIES = {"Bucharest", "Cluj", "Iasi", "Timisoara", "Constanta"};
    private static final String[] DIRECTIONS = {"N", "E", "S", "W"};

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.rand = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        Publication publication = generateRandomPublication();
        System.out.println("PUBLISHER: Emitting " + publication);
        this.collector.emit(new Values(publication));
    }

    private Publication generateRandomPublication() {
        int stationId = rand.nextInt(10) + 1;
        String city = CITIES[rand.nextInt(CITIES.length)];
        int temp = rand.nextInt(35);
        double rain = Math.round(rand.nextDouble() * 20 * 10) / 10.0;
        int wind = rand.nextInt(80);
        String direction = DIRECTIONS[rand.nextInt(DIRECTIONS.length)];
        LocalDate date = LocalDate.now();
        return new Publication(stationId, city, temp, rain, wind, direction, date);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("publication"));
    }
}