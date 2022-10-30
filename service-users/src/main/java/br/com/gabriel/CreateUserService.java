package br.com.gabriel;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class CreateUserService {

    private final Connection connection;

    CreateUserService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        try (var statemanent = connection.createStatement()) {
            statemanent.execute("create table Users (uuid varchar(200) primary key, email varchar(200))");
        }
    }

    public static void main(String[] args) throws SQLException {
        var createUserService = new CreateUserService();
        try (var kafkaService = new KafkaService<>(CreateUserService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                createUserService::parse,
                Order.class,
                Map.of())) {
            kafkaService.run();
        }
    }

    private void parse(ConsumerRecord<String, Order> consumerRecord) throws SQLException {
        System.out.println("---------------------------------------------");
        System.out.println("Processando um novo pedido... Verificando usuário");
        System.out.println("KEY --> " + consumerRecord.key());
        System.out.println("VALUE --> " + consumerRecord.value());
        System.out.println("PARTITION --> " + consumerRecord.partition());
        System.out.println("OFFSET --> " + consumerRecord.offset());

        var order = consumerRecord.value();
        if (isNewUser(order.getEmail())) {
            insertNewUser(order.getEmail());
        }
        System.out.println("---------------------------------------------");
    }

    private void insertNewUser(String email) throws SQLException {
        try (var insert = connection.prepareStatement("insert into Users (uuid, email) values (?, ?)")) {
            insert.setString(1, "uuid");
            insert.setString(2, "email");
            insert.execute();
        }

        System.out.printf("Usuário [%s] adicionado!", email);
    }

    private boolean isNewUser(String email) throws SQLException {
        try (var select = connection.prepareStatement("select uuid from Users u where u.email = ? limit 1")) {
            select.setString(1, email);
            var results = select.executeQuery();

            return !results.next();
        }
    }

}