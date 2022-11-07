package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceProvider().run(EmailService::new);
    }

    public void parse(ConsumerRecord<String, Message<String>> consumerRecord) {
        System.out.println("----------------------------------------------");
        System.out.println("Enviando email...");
        System.out.println("KEY --> " + consumerRecord.key());
        System.out.println("VALUE --> " + consumerRecord.value());
        System.out.println("PARTITION --> " + consumerRecord.partition());
        System.out.println("OFFSET --> " + consumerRecord.offset());
        System.out.println("----------------------------------------------");
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }
}
