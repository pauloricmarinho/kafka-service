package  br.com.prmarinho.kafka.producer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewProductOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {


        try (KafkaDispatcher orderDispatcher = new KafkaDispatcher()) {
            try (KafkaDispatcher emailDispatcher = new KafkaDispatcher()) {
                for (int i = 0; i < 10; i++) {

                    String key = UUID.randomUUID().toString();
                    String value = key + ",67523,1234";
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                    String email = "Thank you for your order! We are processing your order!";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
                }
            }
        }
    }


}
