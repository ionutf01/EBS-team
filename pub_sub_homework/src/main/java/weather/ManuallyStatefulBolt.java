package weather;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.Map;

/**
 * O versiune a bolt-ului stateful care SIMULEAZĂ AUTOMAT o cădere
 * pentru a demonstra mecanismul de recuperare a stării.
 */
public class ManuallyStatefulBolt extends BaseRichBolt {
    private static final Logger LOG = LogManager.getLogger(ManuallyStatefulBolt.class);
    private OutputCollector collector;
    private WindowState state;
    private String stateFilePath;

    // COMENTARIU: Folosim un contor transient pentru a număra execuțiile.
    // 'transient' înseamnă că nu va fi salvat împreună cu starea obiectului.
    private transient int executionCount = 0;

    // COMENTARIU: Folosim un flag static pentru a ne asigura că simularea căderii
    // are loc o singură dată pe parcursul rulării topologiei.
    private static boolean hasCrashed = false;

    public ManuallyStatefulBolt(String stateFilePath) {
        this.stateFilePath = stateFilePath;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.state = loadState();
        if (this.state == null) {
            // Log pentru prima pornire
            LOG.info("INITIALIZARE: Nu a fost găsită nicio stare salvată. Se pornește cu o stare nouă.");
            this.state = new WindowState();
        } else {
            // Log pentru momentul recuperării după cădere
            LOG.warn("************************************************************");
            LOG.warn("RECUPERARE: Bolt-ul a fost repornit. Starea a fost RESTAURATĂ cu succes din fișier!");
            LOG.warn("Starea recuperată: {}", this.state);
            LOG.warn("************************************************************");
        }
    }

    @Override
    public void execute(Tuple tuple) {
        executionCount++;
        Publication pub = (Publication) tuple.getValueByField("publication");
        state.addTemperature(pub.getTemp());

        LOG.info("[Execuția #{}] Starea curentă: {}", executionCount, state);

        // Salvăm starea periodic, la fiecare 10 mesaje
        if (executionCount % 10 == 0) {
            saveState();
        }

        // COMENTARIU: SIMULAREA CĂDERII
        // După 15 execuții (și după ce a avut loc cel puțin o salvare), forțăm o cădere.
        if (executionCount > 15 && !hasCrashed) {
            hasCrashed = true; // Setăm flag-ul pentru a nu mai crăpa din nou la repornire
            LOG.fatal("------------------------------------------------------------");
            LOG.fatal("!!! SIMULARE CĂDERE DE NOD: Bolt-ul se va opri brusc... !!!");
            LOG.fatal("------------------------------------------------------------");
            // System.exit(1) simulează o eroare fatală care oprește procesul worker.
            // Storm Supervisor va detecta acest lucru și va reporni worker-ul.
            System.exit(1);
        }

        collector.ack(tuple);
    }

    // ... metodele saveState(), loadState(), cleanup(), declareOutputFields() rămân neschimbate ...
    private void saveState() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(stateFilePath))) {
            oos.writeObject(this.state);
            LOG.info("SALVARE STARE: Starea a fost salvată pe disc. {}", this.state);
        } catch (IOException e) {
            LOG.error("Eroare la salvarea stării!", e);
        }
    }

    private WindowState loadState() {
        File stateFile = new File(stateFilePath);
        if (stateFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(stateFile))) {
                return (WindowState) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                LOG.error("Eroare la încărcarea stării, se va crea una nouă.", e);
                return null;
            }
        }
        return null;
    }

    @Override
    public void cleanup() {
        saveState();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}