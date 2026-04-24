package com.kolesnica.bot.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class RequestRepository {
    private final Connection connection;
    private final ObjectMapper mapper;

    public RequestRepository(Connection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    public void saveRequest(long userId, Long chatId, String type, ObjectNode payload) throws SQLException {
        String sql = """
                INSERT INTO requests(user_id, chat_id, request_type, status, payload_json, created_at, updated_at)
                VALUES (?, ?, ?, 'new', ?, strftime('%s','now'), strftime('%s','now'))
                """;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, userId);
            if (chatId == null) {
                ps.setObject(2, null);
            } else {
                ps.setLong(2, chatId);
            }
            ps.setString(3, type);
            ps.setString(4, toJson(payload));
            ps.executeUpdate();
        }
    }

    private String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Ошибка сериализации JSON", e);
        }
    }
}
