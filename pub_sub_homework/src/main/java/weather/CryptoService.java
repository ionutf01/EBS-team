package weather;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Serviciu centralizat pentru operațiuni criptografice.
 * Folosește HMAC-SHA256 pentru a crea "amprente" digitale ale datelor,
 * folosind o cheie secretă.
 */
public class CryptoService {
    // Într-un sistem real, această cheie ar fi gestionată în mod securizat (ex: vault).
    // Pentru simulare, o hardcodăm. Brokerul NU trebuie să o cunoască.
    private static final String SECRET_KEY = "cheia-noastra-secreta-pentru-tema-ebs";
    private static final String ALGORITHM = "HmacSHA256";

    /**
     * Calculează amprenta HMAC-SHA256 pentru un text dat.
     * @param data Textul de amprentat (ex: "city" sau "Iasi").
     * @return Amprenta digitală sub formă de String Base64.
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