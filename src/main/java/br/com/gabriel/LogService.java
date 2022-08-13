package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) throws InterruptedException {
        try (var kafkaConsumer = new KafkaConsumer<String, String>(properties())) {
            kafkaConsumer.subscribe(Pattern.compile("ECOMMERCE.*"));

            while (true) {
                var newOrderRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                if (newOrderRecords.isEmpty())
                    continue;

                System.out.println("Encontrei " + newOrderRecords.count() + " registros e vou processa-los...");
                for (var newOrderRecord : newOrderRecords) {
                    System.out.println("--------------------------------------------");
                    System.out.println("LOG");
                    System.out.println(newOrderRecord.key());
                    System.out.println(newOrderRecord.value());
                    System.out.println(newOrderRecord.partition());
                    System.out.println(newOrderRecord.offset());
                    System.out.println("--------------------------------------------");
                    Thread.sleep(5000);
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());

        return properties;
    }
}
