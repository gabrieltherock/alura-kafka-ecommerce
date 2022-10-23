package br.com.gabriel;

import br.com.gabriel.kafka.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderKafkaDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailKafkaDispatcher = new KafkaDispatcher<String>()){
                for (var i = 0; i <= 1; i++) {
                    var key = UUID.randomUUID().toString();

                    var orderValue = Order.builder()
                            .orderId(key)
                            .userId(UUID.randomUUID().toString())
                            .value(BigDecimal.valueOf(Math.random() * 50000 + 1))
                            .build();
                    orderKafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, orderValue);

                    var emailValue = String.format("Welcome! We are processing your order. [%s]", UUID.randomUUID());
                    emailKafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", key, emailValue);
                }
            }
        }
    }
}