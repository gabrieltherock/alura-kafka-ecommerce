package br.com.gabriel;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var producer = new KafkaProducer<String, String>(properties())) {
            for (var i = 0; i <= 100; i++) {
                var value = "12333, 133332, 182928";
                var key = UUID.randomUUID().toString();

                var producerRecord = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Welcome! We are processing your order.";
                var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

                Callback callback = (data, e) -> {
                    if (e != null) {
                        e.printStackTrace();
                        return;
                    }
                    System.out.println("SUCESSO --> " + data.topic() + ":::partition " + data.partition() + "/ offset "
                            + data.offset() + "/ " + data.timestamp());
                };

                producer.send(producerRecord, callback).get();
                producer.send(emailRecord, callback).get();
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
}