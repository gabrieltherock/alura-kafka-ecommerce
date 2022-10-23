package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction {

    void consume(ConsumerRecord<String, String> consumerRecord);
}
