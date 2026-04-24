package com.kolesnica.bot.model;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public record UserSession(
        long userId,
        Long chatId,
        String scenario,
        String step,
        ObjectNode state,
        ArrayNode history
) {
}
