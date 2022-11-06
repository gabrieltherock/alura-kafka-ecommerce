package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {

    void consume(ConsumerRecord<String, Message<T>> consumerRecord) throws Exception;
}
