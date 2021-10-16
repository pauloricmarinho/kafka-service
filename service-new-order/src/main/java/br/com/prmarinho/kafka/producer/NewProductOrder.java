package  br.com.prmarinho.kafka.producer;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewProductOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {


        try (KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>()) {
            try (KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<String>()) {
                for (int i = 0; i < 10; i++) {

                    String userId = UUID.randomUUID().toString();
                    String orderId = UUID.randomUUID().toString();
                    BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);

                    Order order = new Order(userId, orderId, amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER_OBJ", userId, order);

                    String email = "Thank you for your order! We are processing your order!";
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL_OBJ", userId, email);
                }
            }
        }
    }


}
