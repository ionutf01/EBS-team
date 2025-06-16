package weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import weather.proto.PublicationProto;

import java.time.LocalDate;
import java.util.Map;
import java.util.Random;

public class PublicationGeneratorBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final String[] CITIES = {"Bucharest", "Cluj", "Iasi", "Timisoara", "Constanta"};
    private static final String[] DIRECTIONS = {"N", "E", "S", "W"};

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) { this.collector = collector; }

    @Override
    public void execute(Tuple input) {
        System.out.println("PUBLICATION_GENERATOR: Received input tuple: " + input);
        int publicationId = input.getIntegerByField("publicationId");
        Publication publication = generateRandomPublication();
        byte[] serialized = PublicationProto.Publication.newBuilder()
                .setStationId(publication.getStationId())
                .setCity(publication.getCity())
                .setTemp(publication.getTemp())
                .setRain(publication.getRain())
                .setWind(publication.getWind())
                .setDirection(publication.getDirection())
                .setDate(publication.getDate().toString())
                .setEmitTimeMillis(publication.getEmitTimeMillis())
                .build()
                .toByteArray();

        collector.emit(new Values(publicationId, serialized));

        collector.ack(input);
    }

    private Publication generateRandomPublication() {
        Random rand = new Random();
        int stationId = rand.nextInt(100) + 1;
        String city = CITIES[rand.nextInt(CITIES.length)];
        int temp = rand.nextInt(41) - 10;
        double rain = Math.round(rand.nextDouble() * 50 * 10) / 10.0;
        int wind = rand.nextInt(101);
        String direction = DIRECTIONS[rand.nextInt(DIRECTIONS.length)];
        LocalDate date = LocalDate.ofEpochDay(LocalDate.of(2023, 1, 1).toEpochDay() + rand.nextInt(365*2));
        return new Publication(stationId, city, temp, rain, wind, direction, date, System.currentTimeMillis());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("publicationId", "publication")); }
}