package br.com.prmarinho.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class EmailService {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String> (properties());
        consumer.subscribe (Collections.singletonList ("ECOMMERCE_SEND_EMAIL"));

        while (true){
            ConsumerRecords<String, String> records = consumer.poll (Duration.ofMillis (100));

            if(!records.isEmpty()) {
                System.err.println ("Encontrei " + records.count () + " Registros para serem Lidos.");
                for (ConsumerRecord rec : records){
                    System.err.println ("------------------------------------------------------");
                    System.out.println(" Send email ...");
                    System.err.println ("------------------------------------------------------");
                    System.out.println(rec.key());
                    System.out.println(rec.value());
                    System.out.println(rec.partition());
                    System.out.println(rec.offset());
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignoring
                        e.printStackTrace();
                    }
                }
            }
        }


    }

    private static Properties properties(){
        Properties properties =  new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,EmailService.class.getSimpleName ());
        return properties;
    }
}
