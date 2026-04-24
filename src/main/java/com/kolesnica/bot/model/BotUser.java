package com.kolesnica.bot.model;

public record BotUser(
        long userId,
        String name,
        String username,
        long lastActivityTime
) {
}
