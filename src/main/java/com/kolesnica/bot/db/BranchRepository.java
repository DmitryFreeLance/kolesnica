package com.kolesnica.bot.db;

import com.kolesnica.bot.model.Branch;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class BranchRepository {
    private final Connection connection;

    public BranchRepository(Connection connection) {
        this.connection = connection;
    }

    public List<Branch> findAll() throws SQLException {
        String sql = "SELECT * FROM branches ORDER BY city, district, address";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return mapList(rs);
        }
    }

    public Optional<Branch> findById(long id) throws SQLException {
        String sql = "SELECT * FROM branches WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(map(rs));
                }
                return Optional.empty();
            }
        }
    }

    public List<String> listCities() throws SQLException {
        String sql = "SELECT DISTINCT city FROM branches ORDER BY city";
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<String> result = new ArrayList<>();
            while (rs.next()) {
                result.add(rs.getString("city"));
            }
            return result;
        }
    }

    public List<String> listDistricts(String city) throws SQLException {
        String sql = "SELECT DISTINCT district FROM branches WHERE city = ? ORDER BY district";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, city);
            try (ResultSet rs = ps.executeQuery()) {
                List<String> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(rs.getString("district"));
                }
                return result;
            }
        }
    }

    public List<Branch> findByFilters(String city, String district, Boolean only247) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * FROM branches WHERE 1=1");
        List<Object> params = new ArrayList<>();

        if (city != null && !city.isBlank()) {
            sql.append(" AND city = ?");
            params.add(city);
        }
        if (district != null && !district.isBlank()) {
            sql.append(" AND district = ?");
            params.add(district);
        }
        if (only247 != null) {
            sql.append(" AND is_24_7 = ?");
            params.add(only247 ? 1 : 0);
        }

        sql.append(" ORDER BY city, district, address");

        try (PreparedStatement ps = connection.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                return mapList(rs);
            }
        }
    }

    public Optional<Branch> findNearest(double latitude, double longitude, String city) throws SQLException {
        List<Branch> branches = findByFilters(city, null, null);
        Branch nearest = null;
        double minDistance = Double.MAX_VALUE;

        for (Branch branch : branches) {
            if (branch.latitude() == null || branch.longitude() == null) {
                continue;
            }
            double distance = haversine(latitude, longitude, branch.latitude(), branch.longitude());
            if (distance < minDistance) {
                minDistance = distance;
                nearest = branch;
            }
        }

        return Optional.ofNullable(nearest);
    }

    public long addBranch(
            String city,
            String district,
            String address,
            String phone,
            String schedule,
            boolean is24x7,
            Double latitude,
            Double longitude
    ) throws SQLException {
        String sql = """
                INSERT INTO branches(city, district, address, phone, schedule, is_24_7, latitude, longitude)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """;
        try (PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, city);
            ps.setString(2, district);
            ps.setString(3, address);
            ps.setString(4, phone);
            ps.setString(5, schedule);
            ps.setInt(6, is24x7 ? 1 : 0);
            if (latitude == null) {
                ps.setObject(7, null);
            } else {
                ps.setDouble(7, latitude);
            }
            if (longitude == null) {
                ps.setObject(8, null);
            } else {
                ps.setDouble(8, longitude);
            }
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }
        throw new SQLException("Не удалось получить id добавленного филиала");
    }

    public boolean deleteBranch(long branchId) throws SQLException {
        String sql = "DELETE FROM branches WHERE id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, branchId);
            return ps.executeUpdate() > 0;
        }
    }

    private List<Branch> mapList(ResultSet rs) throws SQLException {
        List<Branch> result = new ArrayList<>();
        while (rs.next()) {
            result.add(map(rs));
        }
        return result;
    }

    private Branch map(ResultSet rs) throws SQLException {
        Double lat = rs.getObject("latitude") == null ? null : rs.getDouble("latitude");
        Double lon = rs.getObject("longitude") == null ? null : rs.getDouble("longitude");
        return new Branch(
                rs.getLong("id"),
                rs.getString("city"),
                rs.getString("district"),
                rs.getString("address"),
                rs.getString("phone"),
                rs.getString("schedule"),
                rs.getInt("is_24_7") == 1,
                lat,
                lon
        );
    }

    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        double r = 6371.0;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.pow(Math.sin(dLat / 2), 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.pow(Math.sin(dLon / 2), 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return r * c;
    }
}
