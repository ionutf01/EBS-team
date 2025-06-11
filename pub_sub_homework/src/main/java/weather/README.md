# Proiect Publish/Subscribe cu Procesare pe Ferestre

Acest proiect implementează un sistem de tip publish/subscribe, bazat pe conținut, folosind Apache Storm. Sistemul este capabil să filtreze publicații în timp real și să proceseze secvențe de publicații (ferestre temporale).

Arhitectura utilizează o topologie hibridă, definită în **`PublishSubscribeTopology.java`**, care integrează un generator de date complex (capabil să funcționeze pe bază de distribuții statistice) cu un motor de procesare în timp real format dintr-o rețea de brokeri și subscriberi.

## Arhitectura Sistemului

Sistemul este compus din două părți principale care funcționează împreună într-o singură topologie Storm:

1.  **Generatorul de Date**:
    * **`ConfigurationSpout`**: Punctul de plecare, emite configurația inițială (nr. de publicații/subscripții).
    * **`TemplateGeneratorBolt`**: Generează "șabloane" de subscripții și sarcini de generare pentru publicații.
    * **`PublicationGeneratorBolt` / `SubscriptionGeneratorBolt`**: Creează obiectele `Publication` și `Subscription` pe baza sarcinilor primite.

2.  **Motorul Publish/Subscribe**:
    * **`BrokerBolt`**: Inima sistemului. Primește toate publicațiile și subscripțiile. Stochează subscripțiile, filtrează publicațiile pe baza conținutului și evaluează condițiile pe ferestre de evenimente.
    * **`SubscriberBolt`**: Simulează un client final. Primește și afișează notificările trimise de brokeri atunci când o publicație sau o fereastră de publicații satisface o subscripție.

**Fluxul de date:** `Generatori` -> `Brokeri` -> `Subscriberi`.

## Cerințe

Pentru a compila și rula acest proiect, vei avea nevoie de:
* **Java Development Kit (JDK)**: Versiunea 11 sau mai recentă.
* **Apache Maven**: Pentru managementul dependințelor și compilarea proiectului.

## Instalare și Compilare

Proiectul folosește Maven. Pentru a descărca toate dependințele necesare și a compila codul sursă, rulează următoarea comandă din folderul rădăcină al proiectului (acolo unde se află fișierul `pom.xml`):

```bash
mvn clean install
```

Această comandă va crea un folder `target` cu fișierele compilate.

## Rulare Proiect

Punctul de intrare principal al aplicației este clasa **`PublishSubscribeTopology.java`**.

Pentru a rula proiectul, execută metoda `main` din acest fișier, direct din IDE-ul tău (de ex. IntelliJ, Eclipse). Topologia va rula în modul local (`LocalCluster`) pentru o perioadă de timp predefinită și va afișa log-urile în consolă.

## Configurare și Modificare

Poți modifica ușor anumiți parametri pentru a testa diferite scenarii, direct în fișierul **`PublishSubscribeTopology.java`**:

* **Volumul de Date**:
    * `MAX_PUBLICATIONS`: Schimbă numărul total de publicații generate.
    * `MAX_SUBSCRIPTIONS`: Schimbă numărul total de subscripții generate.

    ```java
    private static final int MAX_PUBLICATIONS = 100;
    private static final int MAX_SUBSCRIPTIONS = 50;
    ```

* **Dimensiunea Ferestrei**:
    * `TUMBLING_WINDOW_SIZE`: Definește câte publicații trebuie să conțină o fereastră pentru a declanșa evaluarea subscripțiilor complexe (ex: `avg_temp`).

    ```java
    private static final int TUMBLING_WINDOW_SIZE = 10;
    ```

* **Logica de Generare a Subscripțiilor**:
    * În fișierul `TemplateGeneratorBolt.java`, poți modifica metoda `createRandomTemplate()` pentru a schimba complexitatea subscripțiilor. De exemplu, poți modifica `int conditionCount = rand.nextInt(3) + 1;` pentru a genera subscripții cu mai multe sau mai puține condiții.

* **Datele Aleatorii Generate**:
    * În fișierele `PublicationGeneratorBolt.java` și `SubscriptionGeneratorBolt.java`, poți modifica listele de `CITIES` și `DIRECTIONS` pentru a schimba setul de date din care se generează valori.

## Demonstrarea Funcționalității pe Ferestre (Tumbling Window)

Pentru a testa și demonstra vizibil mecanismul de "tumbling window", în `SubscriptionGeneratorBolt.java` sunt introduse subscripții complexe, predefinite, pentru anumite ID-uri (**9** și **10**).

1.  **Subscripția pentru Temperatură Medie (`ID = 9`)**
    * **Condiția de Bază**: Niciuna. Fereastra acceptă publicații din **orice oraș**.
    * **Condiția de Agregare**: `avg(temp) > 10.0`
    * **Semnificație**: "Anunță-mă dacă media temperaturilor pentru **oricare 10 publicații consecutive** depășește 10 grade."
    * **Scop**: Această subscripție este foarte permisivă și garantează umplerea și evaluarea constantă a ferestrelor, demonstrând clar mecanismul de "tumbling".

2.  **Subscripția pentru Vânt Maxim (`ID = 10`)**
    * **Condiția de Bază**: `city = 'Iasi'`
    * **Condiția de Agregare**: `max(wind) > 95.0`
    * **Semnificație**: "Anunță-mă dacă viteza maximă a vântului înregistrată într-o fereastră de 10 publicații din Iași depășește 95 km/h."
    * **Scop**: Această subscripție demonstrează un caz de utilizare mai specific, cu o condiție de bază.

### Cum să urmărești "Tumbling Window" în Log-uri

Pentru a vedea mecanismul în acțiune (în special pentru subscripția permisivă cu ID 9), trebuie să urmărești în consolă următoarea secvență de log-uri:

1.  **Înregistrarea subscripției:**
    `BROKER ...: Registered subscription 9: ...`

2.  **Umplerea ferestrei** cu publicații:
    `BROKER ... [SubID 9]: Added to window buffer. Size is now X/10...`

3.  **Evaluarea ferestrei** când aceasta este plină (`10/10`):
    `BROKER ... [SubID 9]: Window is FULL. Evaluating conditions...`

4.  **Rezultatul evaluării** (cu sau fără potrivire):
    `BROKER ...: Tumbling window match for sub 9` SAU `BROKER ...: Window evaluated. NO match found.`

5.  **Resetarea ferestrei (acțiunea de "tumbling")**:
    `BROKER ... [SubID 9]: Buffer CLEARED. Tumbling to next window.`

Apariția acestei secvențe complete este dovada funcționării corecte a mecanismului de tumbling window.

## Structura Proiectului

Cele mai importante fișiere din pachetul `weather` sunt:

* **`PublishSubscribeTopology.java`**: Clasa principală care definește, configurează și lansează topologia Storm.
* **`BrokerBolt.java`**: Implementează logica de broker. Primește, stochează, filtrează și notifică. Gestionează atât filtrarea simplă, cât și cea pe ferestre.
* **`SubscriberBolt.java`**: Simulează clientul final. Primește și afișează notificările de la brokeri.
* `Publication.java`, `Subscription.java`, `WindowedSubscription.java`: Clasele model (POJO) care definesc structura datelor.
* `*GeneratorBolt.java` / `*Spout.java`: Componentele care formează mecanismul de generare a datelor.