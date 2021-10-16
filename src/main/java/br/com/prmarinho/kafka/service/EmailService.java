package br.com.prmarinho.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Map;

public class EmailService {

    public static void main(String[] args)  {
        EmailService emailService = new EmailService();
        Map<String,String> mapper = new HashMap<String, String> ();
        try (KafkaService service = new KafkaService(EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL_OBJ",
                emailService::parse,
                String.class,
                mapper )) {
            service.run();
        }
    }

        private void parse(ConsumerRecord<String, String> record) {
            System.out.println("------------------------------------------");
            System.out.println("Send email");
            System.out.println(record.key());
            System.out.println(record.value());
            System.out.println(record.partition());
            System.out.println(record.offset());
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignoring
                e.printStackTrace();
            }
            System.out.println("Email sent");
        }

}
