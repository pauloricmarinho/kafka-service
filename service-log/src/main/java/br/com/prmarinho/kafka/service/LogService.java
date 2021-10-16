package br.com.prmarinho.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        LogService logService = new LogService();
        Map<String,String> mapper = new HashMap<String, String> ();
        mapper.put (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaService service = new KafkaService(LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
                logService::parse,
                String.class,
                mapper)) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("------------------------------------------");
        System.out.println("LOG: " + record.topic());
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
    }
}
