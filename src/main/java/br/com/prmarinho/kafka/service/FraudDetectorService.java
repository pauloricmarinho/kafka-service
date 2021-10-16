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

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String> (properties());
        consumer.subscribe (Collections.singletonList ("ECOMMERCE_NEW_ORDER"));

        while (true){
             ConsumerRecords<String, String> records = consumer.poll (Duration.ofMillis (100));

             if(!records.isEmpty()) {
                 System.err.println ("Encontrei " + records.count () + " Registros para serem Lidos.");
                for (ConsumerRecord rec : records){
                     System.err.println ("------------------------------------------------------");
                     System.err.println (" Processing new order, checking for fraud.");
                     System.err.println ("------------------------------------------------------");
                     System.out.println ("Chave     ::: " + rec.key ());
                     System.out.println ("Valor     ::: " + rec.value ());
                     System.out.println ("Partição  ::: " + rec.partition ());
                     System.out.println (rec.offset ());
                     System.out.println (rec.timestamp ());
                     System.err.println ("------------------------------------------------------");
                 }
             }
        }


    }

    private static Properties properties(){
        Properties properties =  new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,FraudDetectorService.class.getSimpleName ());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getSimpleName() + "-" + UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        return properties;
    }
}
