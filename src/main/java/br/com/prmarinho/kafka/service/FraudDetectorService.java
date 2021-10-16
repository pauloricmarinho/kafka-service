package br.com.prmarinho.kafka.service;

import br.com.prmarinho.kafka.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.HashMap;
import java.util.Map;

public class FraudDetectorService {


    public static void main(String[] args) {
        Map<String,String> mapper = new HashMap<String, String> ();
        FraudDetectorService fraudService = new FraudDetectorService();
        try (KafkaService service = new KafkaService(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER_OBJ",
                fraudService::parse,
                Order.class,
                mapper)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> rec) {
             System.err.println ("------------------------------------------------------");
             System.err.println (" Processing new order, checking for fraud.");
             System.err.println ("------------------------------------------------------");
             System.out.println ("Chave     ::: " + rec.key ());
             System.out.println ("Valor     ::: " + rec.value() );
             System.out.println ("Partição  ::: " + rec.partition ());
             System.out.println (rec.offset ());
             System.out.println (rec.timestamp ());
             System.err.println ("------------------------------------------------------");

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // ignoring
                e.printStackTrace();
            }
            System.out.println("Order processed");
         }


}
