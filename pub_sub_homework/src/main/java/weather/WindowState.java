package weather;

import org.apache.storm.state.State;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * REZOLVARE (Partea 2): O clasă ce definește starea salvată de Storm.
 * Trebuie să fie serializabilă pentru a putea fi scrisă și citită de state backend.
 */
public class WindowState implements State, Serializable {
    private final List<Integer> temperatures = new ArrayList<>();

    public void addTemperature(int temp) {
        temperatures.add(temp);
    }

    public double getAverage() {
        if (temperatures.isEmpty()) return 0;
        return temperatures.stream().mapToInt(Integer::intValue).average().getAsDouble();
    }

    @Override
    public void prepareCommit(long txid) {}

    @Override
    public void commit(long txid) {}

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public String toString() {
        return String.format("WindowState{count=%d, avg=%.2f}", temperatures.size(), getAverage());
    }
}