package weather.secure;

import weather.Subscription;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Reprezintă o subscripție ale cărei condiții de egalitate au fost "amprentate".
 */
public class EncryptedSubscription implements Serializable {
    public final Map<String, String> hashedConditions = new HashMap<>();

    public EncryptedSubscription(Subscription original) {
        // Amprentăm doar condițiile de EGALITATE ('=').
        // Celelalte ('>', '<') sunt ignorate în acest model.
        for (Subscription.Condition condition : original.conditions) {
            if (condition.getOperator().equals("=")) {
                String hashedField = CryptoService.hash(condition.getField());
                String hashedValue = CryptoService.hash(condition.getValue().toString());
                hashedConditions.put(hashedField, hashedValue);
            }
        }
    }
}