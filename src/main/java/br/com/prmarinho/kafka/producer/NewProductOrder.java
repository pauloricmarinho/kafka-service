package  br.com.prmarinho.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewProductOrder {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties());
        String valores = "12314,12420,12560";


        Callback callback = ((data, ex) -> {
            if (null != ex) {
                ex.printStackTrace ();
                return;
            }
            System.out.println ("Sucesso Enviado ::: " + data.topic () + " ::: " + data.partition () + " / " + data.offset () + " / " + data.timestamp ());
            });

        String email = "Thank you for your order! We are processing your order!";
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", valores, valores);
        ProducerRecord<String, String> emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", email, email);

        producer.send (record, callback).get ();
        producer.send (emailRecord, callback).get ();


    }

    private static Properties properties(){
        Properties properties =  new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        return properties;
    }


}
