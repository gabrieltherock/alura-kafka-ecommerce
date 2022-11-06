package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;
    private final KafkaDispatcher<User> userKafkaDispatcher = new KafkaDispatcher<>();

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:users_database.db";
        connection = DriverManager.getConnection(url);
        try (var statemanent = connection.createStatement()) {
            statemanent.execute("create table Users (uuid varchar(200) primary key, email varchar(200))");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws SQLException {
        var batchSendMessageService = new BatchSendMessageService();
        try (var kafkaService = new KafkaService<>(BatchSendMessageService.class.getSimpleName(),
                "SEND_MESSAGE_TO_ALL_USERS",
                batchSendMessageService::parse,
                Map.of())) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> consumerRecord) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("---------------------------------------------");
        System.out.println("Processando um novo batch...");
        System.out.println("TOPIC --> " + consumerRecord.value());

        var message = consumerRecord.value();

        for (User user : getAllUsers()) {
                userKafkaDispatcher.send(message.getPayload(), user.getUuid(), user);
        }
        System.out.println("---------------------------------------------");
    }

    private List<User> getAllUsers() throws SQLException {
        List<User> users;
        try (var resultSet = connection.prepareStatement("select uuid from Users").executeQuery()) {
            users = new ArrayList<>();

            while (resultSet.next()) {
                users.add(User.builder().uuid(resultSet.getString(1)).build());
            }
        }

        return users;
    }
}
