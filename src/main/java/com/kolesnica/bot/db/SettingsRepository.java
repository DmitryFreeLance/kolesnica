package com.kolesnica.bot.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class SettingsRepository {
    public static final String KEY_OPERATOR_PHONE = "operator_phone";
    public static final String DEFAULT_OPERATOR_PHONE = "+79005553535";

    private final Connection connection;

    public SettingsRepository(Connection connection) {
        this.connection = connection;
    }

    public String getOperatorPhone() throws SQLException {
        return getValue(KEY_OPERATOR_PHONE, DEFAULT_OPERATOR_PHONE);
    }

    public void setOperatorPhone(String phone) throws SQLException {
        setValue(KEY_OPERATOR_PHONE, phone);
    }

    public String getValue(String key, String fallback) throws SQLException {
        String sql = "SELECT value FROM settings WHERE key = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String value = rs.getString("value");
                    if (value != null && !value.isBlank()) {
                        return value;
                    }
                }
            }
        }
        return fallback;
    }

    public void setValue(String key, String value) throws SQLException {
        String sql = """
                INSERT INTO settings(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.setString(2, value);
            ps.executeUpdate();
        }
    }
}
