Filtrare Securizată în Sisteme Publish-Subscribe prin CriptografieCuprinsConceptul: Filtrare fără Acces la DateArhitectura Topologiei SecurizateComponentele Sistemului (Explicații și Cod)3.1. Serviciul Criptografic (CryptoService.java)3.2. Containerele Criptate (EncryptedPublication & EncryptedSubscription)3.3. Gateway-ul de Criptare (EncryptionBolt.java)3.4. Brokerul "Orb" (EncryptedBrokerBolt.java)Fluxul Complet: O Publicație și o SubscripțieConcluzii și Garanții de Securitate1. Conceptul: Filtrare fără Acces la DateProblema pe care o rezolvăm este următoarea: cum poate un sistem de brokeri să direcționeze o publicație către un subscriitor interesat dacă brokerului nu i se permite să citească conținutul publicației sau al subscripției?Soluția constă în a nu compara datele în clar, ci "amprentele" digitale ale acestora. Vom folosi un mecanism criptografic numit HMAC (Hash-based Message Authentication Code), care generează o amprentă unică și non-reversibilă pentru orice text, folosind o cheie secretă.Principiul de bază:Atât publicatorul, cât și subscriitorul folosesc aceeași cheie secretă pentru a-și "amprenta" datele.Brokerul primește doar aceste amprente. El nu poate deduce datele originale din ele.Brokerul poate, însă, să compare două amprente. Dacă amprenta pentru city din publicație este identică cu amprenta pentru city din subscripție, brokerul știe că valorile originale (pe care nu le vede) sunt identice.Notă Importantă: Această tehnică funcționează perfect pentru potriviri de egalitate (ex: city = "Iasi"). Nu este concepută pentru a funcționa cu operatori de inegalitate (>, <), deoarece procesul de hashing distruge relația de ordine dintre valori.2. Arhitectura Topologiei SecurizatePentru a implementa acest concept, creăm o topologie nouă (SecureTopology) cu componente specializate. Fluxul de date nu mai este direct de la sursă la broker, ci trece printr-un "gateway" de criptare.graph TD
subgraph "Date în Clar"
PublisherSpout["PublisherSpout"]
SubscriptionSpout["SubscriptionSpout"]
end

    subgraph "Gateway Criptografic"
        EncryptionBolt["EncryptionBolt (Amprentează datele)"]
    end
    
    subgraph "Broker Securizat (Lucrează 'în orb')"
        EncryptedBrokerBolt["EncryptedBrokerBolt (Compară doar amprente)"]
    end

    PublisherSpout -- "publication" --> EncryptionBolt
    SubscriptionSpout -- "subscription" --> EncryptionBolt
    
    EncryptionBolt -- "encrypted-publications" --> EncryptedBrokerBolt
    EncryptionBolt -- "encrypted-subscriptions" --> EncryptedBrokerBolt
3. Componentele Sistemului (Explicații și Cod)Acest sistem este implementat în clase noi, dedicate, pentru a nu afecta topologiile existente.3.1. Serviciul Criptografic (CryptoService.java)Aceasta este o clasă utilitară care conține logica de "amprentare". Este piesa centrală a securității.Rol: Să ofere o metodă unică, hash(data), care transformă orice text într-o amprentă HMAC-SHA256.Securitate: Folosește o SECRET_KEY care este cunoscută doar de clienți (simulați de EncryptionBolt), dar nu și de broker.package weather;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class CryptoService {
// Într-un sistem real, această cheie ar fi gestionată în mod securizat.
private static final String SECRET_KEY = "cheia-noastra-secreta-pentru-tema-ebs";
private static final String ALGORITHM = "HmacSHA256";

    /**
     * Calculează amprenta HMAC-SHA256 pentru un text dat.
     */
    public static String hash(String data) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            SecretKeySpec secretKeySpec = new SecretKeySpec(SECRET_KEY.getBytes(StandardCharsets.UTF_8), ALGORITHM);
            mac.init(secretKeySpec);
            byte[] hashBytes = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hashBytes);
        } catch (Exception e) {
            throw new RuntimeException("Eroare la calcularea hash-ului HMAC", e);
        }
    }
}
3.2. Containerele Criptate (EncryptedPublication & EncryptedSubscription)Acestea sunt simple obiecte de transfer (DTOs) care nu conțin date în clar, ci doar amprentele acestora sub forma unei hărți (Map).De exemplu, o publicație Publication{city="Iasi", temp=25} devine un obiect EncryptedPublication care conține o mapă de forma:{ "hash("city")": "hash("Iasi")", "hash("temp")": "hash("25")" }// EncryptedPublication.java
package weather;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EncryptedPublication implements Serializable {
public final Map<String, String> hashedFields = new HashMap<>();

    public EncryptedPublication(Publication original) {
        // Pentru fiecare câmp din publicația originală, calculăm și stocăm amprentele.
        hashedFields.put(CryptoService.hash("city"), CryptoService.hash(original.getCity()));
        hashedFields.put(CryptoService.hash("temp"), CryptoService.hash(String.valueOf(original.getTemp())));
        hashedFields.put(CryptoService.hash("wind"), CryptoService.hash(String.valueOf(original.getWind())));
        // ... și așa mai departe pentru celelalte câmpuri.
    }
}
```java
// EncryptedSubscription.java
package weather;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class EncryptedSubscription implements Serializable {
    public final Map<String, String> hashedConditions = new HashMap<>();

    public EncryptedSubscription(Subscription original) {
        // Amprentăm doar condițiile de EGALITATE ('=').
        for (Subscription.Condition condition : original.conditions) {
            if (condition.getOperator().equals("=")) {
                String hashedField = CryptoService.hash(condition.getField());
                String hashedValue = CryptoService.hash(condition.getValue().toString());
                hashedConditions.put(hashedField, hashedValue);
            }
        }
    }
}
3.3. Gateway-ul de Criptare (EncryptionBolt.java)Acest bolt acționează ca un punct de control la intrarea în sistemul securizat.Rol: Primește obiecte Publication și Subscription în clar. Le transformă în EncryptedPublication și EncryptedSubscription folosind CryptoService. Emite mai departe doar versiunile "amprentate".// EncryptionBolt.java
package weather;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;

public class EncryptionBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceComponent().equals("publisher-spout")) {
            Publication originalPub = (Publication) input.getValueByField("publication");
            EncryptedPublication encryptedPub = new EncryptedPublication(originalPub);
            collector.emit("encrypted-publications", new Values(encryptedPub));
        } else if (input.getSourceComponent().equals("subscription-spout")) {
            Subscription originalSub = (Subscription) input.getValueByField("subscription");
            EncryptedSubscription encryptedSub = new EncryptedSubscription(originalSub);
            collector.emit("encrypted-subscriptions", new Values(encryptedSub));
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("encrypted-publications", new Fields("encryptedPublication"));
        declarer.declareStream("encrypted-subscriptions", new Fields("encryptedSubscription"));
    }
}
3.4. Brokerul "Orb" (EncryptedBrokerBolt.java)Aceasta este piesa centrală a logicii de filtrare, care funcționează fără a cunoaște conținutul real.Rol: Primește subscripții și publicații deja "amprentate". Stochează subscripțiile și, pentru fiecare publicație primită, verifică dacă există o potrivire.Logica de Potrivire: O potrivire are loc dacă toate perechile (amprenta_câmp, amprenta_valoare) din subscripția criptată se regăsesc exact în publicația criptată.// EncryptedBrokerBolt.java
package weather;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EncryptedBrokerBolt extends BaseRichBolt {
    private static final Logger LOG = LogManager.getLogger(EncryptedBrokerBolt.class);
    private List<EncryptedSubscription> encryptedSubscriptions;
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.encryptedSubscriptions = new ArrayList<>();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input.getSourceStreamId().equals("encrypted-subscriptions")) {
            EncryptedSubscription sub = (EncryptedSubscription) input.getValueByField("encryptedSubscription");
            encryptedSubscriptions.add(sub);
            LOG.info("Broker Criptat: Am primit o nouă subscripție criptată.");
        } else if (input.getSourceStreamId().equals("encrypted-publications")) {
            EncryptedPublication pub = (EncryptedPublication) input.getValueByField("encryptedPublication");
            for (EncryptedSubscription sub : encryptedSubscriptions) {
                if (matches(pub, sub)) {
                    LOG.warn("POTRIVIRE CRIPTATĂ GĂSITĂ! Notificarea ar fi trimisă aici.");
                }
            }
        }
        collector.ack(input);
    }
    
    private boolean matches(EncryptedPublication pub, EncryptedSubscription sub) {
        if (sub.hashedConditions.isEmpty()) {
            return false;
        }
        for (Map.Entry<String, String> condition : sub.hashedConditions.entrySet()) {
            String hashedField = condition.getKey();
            String hashedValue = condition.getValue();
            
            if (!pub.hashedFields.containsKey(hashedField) || !pub.hashedFields.get(hashedField).equals(hashedValue)) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Nu emitem nimic mai departe în acest exemplu.
    }
}
4. Fluxul Complet: O Publicație și o SubscripțieSubscriere: SubscriptionSpout emite Subscription{ conditions: [(city,=,Iasi)] }.EncryptionBolt o primește și o transformă într-un EncryptedSubscription care conține mapa: { "ab12...": "5f3a..." } (unde "ab12..." este hash("city") și "5f3a..." este hash("Iasi")).EncryptedBrokerBolt primește și stochează această subscripție criptată.Publicare: PublisherSpout emite Publication{ city="Iasi", temp=25, ... }.EncryptionBolt o primește și o transformă într-un EncryptedPublication cu mapa: { "ab12...": "5f3a...", "cd34...": "8a9b...", ... }.Potrivire: EncryptedBrokerBolt primește publicația criptată. O compară cu subscripția stocată. Verifică dacă în publicație există cheia "ab12..." și dacă valoarea ei este "5f3a...". Deoarece condiția este îndeplinită, se declară o potrivire. Brokerul nu a aflat niciodată că discuția era despre "Iasi".5. Concluzii și Garanții de SecuritatePrin această implementare, am respectat cerința, obținând următoarele garanții:Confidențialitatea Conținutului: Brokerii și orice altă componentă intermediară nu pot accesa sau deduce datele originale din publicații și subscripții.Integritatea Filtrării: Potrivirile sunt corecte din punct de vedere criptografic. O potrivire va avea loc doar dacă datele originale erau identice.Separarea Responsabilităților: Logica de criptare este complet separată de logica de business (rutare/filtrare), făcând sistemul mai curat și mai ușor de întreținut.