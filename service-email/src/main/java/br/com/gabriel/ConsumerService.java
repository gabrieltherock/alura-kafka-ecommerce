package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {

    void parse(ConsumerRecord<String, Message<T>> consumerRecord);
    String getTopic();
    String getConsumerGroup();
}
