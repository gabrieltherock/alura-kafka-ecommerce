package br.com.gabriel;

import br.com.gabriel.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderKafkaDispatcher = new KafkaDispatcher<Order>()) {
            try (var emailKafkaDispatcher = new KafkaDispatcher<String>()){
                for (var i = 0; i <= 100; i++) {
                    var key = UUID.randomUUID().toString();

                    var orderValue = Order.builder()
                            .orderId(key)
                            .userId(UUID.randomUUID().toString())
                            .value(BigDecimal.valueOf(Math.random() * 5000 + 1))
                            .email(Math.random() + "@outlook.com")
                            .build();
                    orderKafkaDispatcher.send("ECOMMERCE_NEW_ORDER", key, new CorrelationId(NewOrderMain.class.getSimpleName()), orderValue);

                    var emailValue = String.format("Welcome! We are processing your order. [%s]", UUID.randomUUID());
                    emailKafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", key, new CorrelationId(NewOrderMain.class.getSimpleName()), emailValue);
                }
            }
        }
    }
}