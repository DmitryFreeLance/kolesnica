package com.kolesnica.bot.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kolesnica.bot.model.UserSession;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public final class SessionRepository {
    private final Connection connection;
    private final ObjectMapper mapper;

    public SessionRepository(Connection connection, ObjectMapper mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    public UserSession getOrCreate(long userId, Long chatId) throws SQLException {
        String select = "SELECT * FROM user_sessions WHERE user_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(select)) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new UserSession(
                            rs.getLong("user_id"),
                            rs.getObject("chat_id") == null ? chatId : rs.getLong("chat_id"),
                            rs.getString("scenario"),
                            rs.getString("step"),
                            parseObject(rs.getString("state_json")),
                            parseArray(rs.getString("history_json"))
                    );
                }
            }
        }

        return new UserSession(userId, chatId, null, null, mapper.createObjectNode(), mapper.createArrayNode());
    }

    public void save(UserSession session) throws SQLException {
        String sql = """
                INSERT INTO user_sessions(user_id, chat_id, scenario, step, state_json, history_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, strftime('%s','now'))
                ON CONFLICT(user_id) DO UPDATE SET
                    chat_id = excluded.chat_id,
                    scenario = excluded.scenario,
                    step = excluded.step,
                    state_json = excluded.state_json,
                    history_json = excluded.history_json,
                    updated_at = excluded.updated_at
                """;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, session.userId());
            if (session.chatId() == null) {
                ps.setObject(2, null);
            } else {
                ps.setLong(2, session.chatId());
            }
            ps.setString(3, session.scenario());
            ps.setString(4, session.step());
            ps.setString(5, toJson(session.state()));
            ps.setString(6, toJson(session.history()));
            ps.executeUpdate();
        }
    }

    public void clear(long userId) throws SQLException {
        UserSession session = new UserSession(userId, null, null, null, mapper.createObjectNode(), mapper.createArrayNode());
        save(session);
    }

    private String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Ошибка сериализации JSON", e);
        }
    }

    private ObjectNode parseObject(String json) {
        if (json == null || json.isBlank()) {
            return mapper.createObjectNode();
        }
        try {
            return (ObjectNode) mapper.readTree(json);
        } catch (Exception e) {
            return mapper.createObjectNode();
        }
    }

    private ArrayNode parseArray(String json) {
        if (json == null || json.isBlank()) {
            return mapper.createArrayNode();
        }
        try {
            return (ArrayNode) mapper.readTree(json);
        } catch (Exception e) {
            return mapper.createArrayNode();
        }
    }
}
