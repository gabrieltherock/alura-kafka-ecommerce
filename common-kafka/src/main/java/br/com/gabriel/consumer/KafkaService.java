package br.com.gabriel.consumer;

import br.com.gabriel.Message;
import br.com.gabriel.dispatcher.GsonSerializer;
import br.com.gabriel.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private final KafkaConsumer<String, Message<T>> kafkaConsumer;
    private final ConsumerFunction<T> consumerFunction;

    private boolean isOpen;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> consumerFunction, Map<String, String> additionalProperties) {
        this(groupId, consumerFunction, additionalProperties);
        this.kafkaConsumer.subscribe(Collections.singleton(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> consumerFunction, Map<String, String> additionalProperties) {
        this(groupId, consumerFunction, additionalProperties);
        this.kafkaConsumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction<T> consumerFunction, Map<String, String> additionalProperties) {
        this.kafkaConsumer = new KafkaConsumer<>(properties(groupId, additionalProperties));
        this.consumerFunction = consumerFunction;
        this.isOpen = true;
    }

    public void run() throws ExecutionException, InterruptedException {
        try (var deadLetterDispatcher = new KafkaDispatcher<>()) {
            while (isOpen) {
                var consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));

                if (!consumerRecords.isEmpty()) {
                    System.out.printf("Encontrei %s registro(s) e vou processá-los.%n", consumerRecords.count());
                    for (ConsumerRecord<String, Message<T>> consumerRecord : consumerRecords) {
                        try {
                            consumerFunction.consume(consumerRecord);
                        } catch (Exception e) {
                            e.printStackTrace();
                            var message = consumerRecord.value();
                            try (GsonSerializer<Message<T>> gsonSerializer = new GsonSerializer<>()) {
                                deadLetterDispatcher.send("ECOMMERCE_DEADLETTER", message.getId().toString(), message.getId(),
                                        gsonSerializer.serialize("", message));
                            }
                        }
                    }
                }
            }
        }
    }

    private Properties properties(String groupId, Map<String, String> additionalProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.putAll(additionalProperties);
        return properties;
    }

    @Override
    public void close() {
        isOpen = false;
        kafkaConsumer.close();
    }
}
