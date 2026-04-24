package com.kolesnica.bot.db;

import com.kolesnica.bot.model.BotUser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class UserRepository {
    private final Connection connection;

    public UserRepository(Connection connection) {
        this.connection = connection;
    }

    public void upsertUser(
            long userId,
            String name,
            String firstName,
            String lastName,
            String username,
            boolean isBot,
            Long lastActivityTime
    ) throws SQLException {
        String sql = """
                INSERT INTO users(user_id, name, first_name, last_name, username, is_bot, last_activity_time, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s','now'))
                ON CONFLICT(user_id) DO UPDATE SET
                    name = excluded.name,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    username = excluded.username,
                    is_bot = excluded.is_bot,
                    last_activity_time = excluded.last_activity_time,
                    updated_at = excluded.updated_at
                """;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, userId);
            ps.setString(2, name);
            ps.setString(3, firstName);
            ps.setString(4, lastName);
            ps.setString(5, username);
            ps.setInt(6, isBot ? 1 : 0);
            if (lastActivityTime == null) {
                ps.setObject(7, null);
            } else {
                ps.setLong(7, lastActivityTime);
            }
            ps.executeUpdate();
        }
    }

    public int countHumanUsers() throws SQLException {
        String sql = "SELECT COUNT(*) AS c FROM users WHERE is_bot = 0";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return rs.next() ? rs.getInt("c") : 0;
        }
    }

    public List<BotUser> listHumanUsers(int page, int pageSize) throws SQLException {
        int offset = Math.max(0, page - 1) * pageSize;
        String sql = """
                SELECT user_id, COALESCE(name, first_name, '') AS display_name, username,
                       COALESCE(last_activity_time, 0) AS last_activity_time
                FROM users
                WHERE is_bot = 0
                ORDER BY last_activity_time DESC, user_id DESC
                LIMIT ? OFFSET ?
                """;

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, pageSize);
            ps.setInt(2, offset);
            try (ResultSet rs = ps.executeQuery()) {
                List<BotUser> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(new BotUser(
                            rs.getLong("user_id"),
                            rs.getString("display_name"),
                            rs.getString("username"),
                            rs.getLong("last_activity_time")
                    ));
                }
                return result;
            }
        }
    }
}
