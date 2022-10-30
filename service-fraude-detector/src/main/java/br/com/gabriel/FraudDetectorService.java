package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService {

    private final KafkaDispatcher<Order> kafkaDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        try (var kafkaService = new KafkaService<>(FraudDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Order.class,
                Map.of())) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> consumerRecord) throws ExecutionException, InterruptedException {
        System.out.println("---------------------------------------------");
        System.out.println("Processando um novo pedido...");
        System.out.println("KEY --> " + consumerRecord.key());
        System.out.println("VALUE --> " + consumerRecord.value());
        System.out.println("PARTITION --> " + consumerRecord.partition());
        System.out.println("OFFSET --> " + consumerRecord.offset());

        var order = consumerRecord.value();
        if (isFraud(order)) {
            System.out.printf("Valor de R$ %s é mais alto que o normal%n", order.getValue());
            kafkaDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getUserId(), order);
        } else {
            System.out.println("Pedido aprovado!");
            kafkaDispatcher.send("ECOMMERCE_ORDER_APROVED", order.getUserId(), order);
        }

        System.out.println("---------------------------------------------");
    }

    private boolean isFraud(Order order) {
        return order.getValue().compareTo(new BigDecimal("4500")) >= 0;
    }
}
