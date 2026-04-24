package com.kolesnica.bot.db;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class Database implements AutoCloseable {
    private final Connection connection;

    public Database(String dbPath) throws SQLException, IOException {
        Path file = Path.of(dbPath).toAbsolutePath();
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }

        this.connection = DriverManager.getConnection("jdbc:sqlite:" + file);
        this.connection.setAutoCommit(true);
        initSchema();
    }

    public Connection connection() {
        return connection;
    }

    @Override
    public void close() throws SQLException {
        connection.close();
    }

    private void initSchema() throws IOException, SQLException {
        String sql = loadResource("schema.sql");
        try (Statement st = connection.createStatement()) {
            for (String chunk : sql.split(";\\s*\\n")) {
                String statement = chunk.trim();
                if (!statement.isEmpty()) {
                    st.execute(statement);
                }
            }
        }
    }

    private String loadResource(String name) throws IOException {
        InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
        if (input == null) {
            throw new IOException("Не найден ресурс: " + name);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
            return sb.toString();
        }
    }
}
