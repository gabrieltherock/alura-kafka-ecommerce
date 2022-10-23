package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

    public static void main(String[] args) {
        var fraudDetectorService = new FraudDetectorService();
        var kafkaService = new KafkaService(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_SEND_EMAIL", fraudDetectorService::parse);

        kafkaService.run();
    }

    private void parse(ConsumerRecord<String, String> consumerRecord) {
        System.out.println("---------------------------------------------");
        System.out.println("Processando um novo pedido...");
        System.out.println("KEY --> " + consumerRecord.key());
        System.out.println("VALUE --> " + consumerRecord.value());
        System.out.println("PARTITION --> " + consumerRecord.partition());
        System.out.println("OFFSET --> " + consumerRecord.offset());
        System.out.println("---------------------------------------------");
    }
}
