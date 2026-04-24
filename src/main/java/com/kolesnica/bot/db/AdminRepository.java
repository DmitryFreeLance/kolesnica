package com.kolesnica.bot.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class AdminRepository {
    private final Connection connection;

    public AdminRepository(Connection connection) {
        this.connection = connection;
    }

    public boolean hasAnyAdmin() throws SQLException {
        String sql = "SELECT 1 FROM admins LIMIT 1";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return rs.next();
        }
    }

    public boolean isAdmin(long userId) throws SQLException {
        String sql = "SELECT 1 FROM admins WHERE user_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    public void addAdmin(long userId) throws SQLException {
        String sql = "INSERT OR IGNORE INTO admins(user_id) VALUES (?)";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, userId);
            ps.executeUpdate();
        }
    }

    public boolean removeAdmin(long userId) throws SQLException {
        String sql = "DELETE FROM admins WHERE user_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, userId);
            return ps.executeUpdate() > 0;
        }
    }

    public List<Long> listAdmins() throws SQLException {
        String sql = "SELECT user_id FROM admins ORDER BY user_id";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<Long> result = new ArrayList<>();
            while (rs.next()) {
                result.add(rs.getLong("user_id"));
            }
            return result;
        }
    }
}
