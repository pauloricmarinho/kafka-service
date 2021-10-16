package br.com.prmarinho.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        FraudDetectorService fraudService = new FraudDetectorService();
        try (KafkaService service = new KafkaService(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudService::parse)) {
            service.run();
        }
    }



    private void parse(ConsumerRecord<String, String> rec) {
             System.err.println ("------------------------------------------------------");
             System.err.println (" Processing new order, checking for fraud.");
             System.err.println ("------------------------------------------------------");
             System.out.println ("Chave     ::: " + rec.key ());
             System.out.println ("Valor     ::: " + rec.value ());
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
