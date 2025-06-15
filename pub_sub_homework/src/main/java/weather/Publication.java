package weather;

import java.io.Serializable;
import java.time.LocalDate;

public class Publication implements Serializable {
    private static final long serialVersionUID = 1L;
    private final int stationId;
    private final String city;
    private final int temp;
    private final double rain;
    private final int wind;
    private final String direction;
    private final LocalDate date;

    public Publication(int stationId, String city, int temp, double rain, int wind, String direction, LocalDate date) {
        this.stationId = stationId;
        this.city = city;
        this.temp = temp;
        this.rain = rain;
        this.wind = wind;
        this.direction = direction;
        this.date = date;
    }

    public int getStationId() { return stationId; }
    public String getCity() { return city; }
    public int getTemp() { return temp; }
    public double getRain() { return rain; }
    public int getWind() { return wind; }
    public String getDirection() { return direction; }
    public LocalDate getDate() { return date; }

    @Override
    public String toString() {
        return "Publication{city='" + city + "', temp=" + temp + ", rain=" + rain + ", wind=" + wind + '}';
    }
}