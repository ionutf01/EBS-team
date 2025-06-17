package weather.secure;

import weather.Publication;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Reprezintă o publicație ale cărei câmpuri și valori au fost "amprentate" (hashed).
 * Brokerul va lucra cu obiecte de acest tip, fără a vedea datele originale.
 */
public class EncryptedPublication implements Serializable {
    // Stochează perechi de tip (hash(nume_câmp), hash(valoare_câmp))
    public final Map<String, String> hashedFields = new HashMap<>();

    public EncryptedPublication(Publication original) {
        // Amprentăm toate câmpurile relevante din publicația originală.
        // CORECȚIE: Am înlocuit "Crypto.Service" cu numele corect al clasei, "CryptoService".
        hashedFields.put(CryptoService.hash("city"), CryptoService.hash(original.getCity()));
        hashedFields.put(CryptoService.hash("temp"), CryptoService.hash(String.valueOf(original.getTemp())));
        hashedFields.put(CryptoService.hash("wind"), CryptoService.hash(String.valueOf(original.getWind())));
        hashedFields.put(CryptoService.hash("rain"), CryptoService.hash(String.valueOf(original.getRain())));
        hashedFields.put(CryptoService.hash("direction"), CryptoService.hash(original.getDirection()));
        // Adăugăm și celelalte câmpuri pentru a fi complet.
        hashedFields.put(CryptoService.hash("stationId"), CryptoService.hash(String.valueOf(original.getStationId())));
        hashedFields.put(CryptoService.hash("date"), CryptoService.hash(original.getDate().toString()));
    }
}