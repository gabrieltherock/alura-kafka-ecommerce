package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class KafkaService implements Closeable {

    private final KafkaConsumer<String, String> kafkaConsumer;
    private final ConsumerFunction consumerFunction;
    private boolean isOpen;

    public KafkaService(String groupId, String topic, ConsumerFunction consumerFunction) {
        this.kafkaConsumer = new KafkaConsumer<>(properties(groupId));
        this.kafkaConsumer.subscribe(Collections.singleton(topic));
        this.consumerFunction = consumerFunction;
        this.isOpen = true;
    }

    public void run() {
        while (isOpen) {
            var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

            if (!consumerRecords.isEmpty()) {
                System.out.printf("Encontrei %s registro(s) e vou processá-los.%n", consumerRecords.count());
                consumerRecords.forEach(consumerFunction::consume);
            }
        }
    }

    private Properties properties(String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        return properties;
    }

    @Override
    public void close() {
        isOpen = false;
        kafkaConsumer.close();
    }
}
