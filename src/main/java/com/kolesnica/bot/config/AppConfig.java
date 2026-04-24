package com.kolesnica.bot.config;

import java.util.Objects;

public final class AppConfig {
    private final String botToken;
    private final String apiBaseUrl;
    private final int pollTimeout;
    private final int pollLimit;
    private final String dbPath;

    private AppConfig(
            String botToken,
            String apiBaseUrl,
            int pollTimeout,
            int pollLimit,
            String dbPath
    ) {
        this.botToken = botToken;
        this.apiBaseUrl = apiBaseUrl;
        this.pollTimeout = pollTimeout;
        this.pollLimit = pollLimit;
        this.dbPath = dbPath;
    }

    public static AppConfig fromEnv() {
        String token = getRequired("BOT_TOKEN");
        String baseUrl = getOrDefault("MAX_API_BASE_URL", "https://platform-api.max.ru");
        int timeout = parseInt("BOT_POLL_TIMEOUT", 25);
        int limit = parseInt("BOT_POLL_LIMIT", 1000);
        String dbPath = getOrDefault("BOT_DB_PATH", "./data/bot.db");

        if (timeout < 0 || timeout > 90) {
            throw new IllegalArgumentException("BOT_POLL_TIMEOUT должен быть в диапазоне 0..90");
        }
        if (limit < 1 || limit > 1000) {
            throw new IllegalArgumentException("BOT_POLL_LIMIT должен быть в диапазоне 1..1000");
        }

        return new AppConfig(token, baseUrl, timeout, limit, dbPath);
    }

    public String botToken() {
        return botToken;
    }

    public String apiBaseUrl() {
        return apiBaseUrl;
    }

    public int pollTimeout() {
        return pollTimeout;
    }

    public int pollLimit() {
        return pollLimit;
    }

    public String dbPath() {
        return dbPath;
    }

    private static String getRequired(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Не задана переменная окружения: " + key);
        }
        return value;
    }

    private static String getOrDefault(String key, String fallback) {
        return Objects.requireNonNullElse(System.getenv(key), fallback);
    }

    private static int parseInt(String key, int fallback) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(raw.trim());
    }
}
