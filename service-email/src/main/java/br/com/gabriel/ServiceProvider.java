package br.com.gabriel;

import br.com.gabriel.consumer.KafkaService;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ServiceProvider {

    public <T> void run(ServiceFactory<T> factory) throws ExecutionException, InterruptedException {
        var consumerService = factory.create();

        try (var kafkaService = new KafkaService<>(consumerService.getConsumerGroup(),
                consumerService.getTopic(),
                consumerService::parse,
                Map.of())) {
            kafkaService.run();
        }
    }
}
