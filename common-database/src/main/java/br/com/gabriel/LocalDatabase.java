package br.com.gabriel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDatabase {

    private final Connection connection;

    LocalDatabase(String dbName) throws SQLException {
        String url = "jdbc:sqlite:" + dbName + ".db";
        connection = DriverManager.getConnection(url);
    }

    public void createIfNotExists(String sql) {
        try (var statemanent = connection.createStatement()) {
            statemanent.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void update(String statement, String... params) throws SQLException {
        try (var preparedStatement = prepare(statement, params)){
            preparedStatement.execute();
        }
    }

    private PreparedStatement prepare(String statement, String[] params) throws SQLException {
        var preparedStatement = connection.prepareStatement(statement);
        for (int i = 0; i < params.length; i++) {
            preparedStatement.setString(i + 1, params[i]);
        }
        return preparedStatement;
    }

    public ResultSet query(String query, String... params) throws SQLException {
        return prepare(query, params).executeQuery();
    }
}