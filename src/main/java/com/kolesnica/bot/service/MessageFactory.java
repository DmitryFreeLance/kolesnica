package com.kolesnica.bot.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

public final class MessageFactory {
    private final ObjectMapper mapper;

    public MessageFactory(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public ObjectNode message(String text, List<List<ObjectNode>> rows, boolean withNavigation) {
        ObjectNode body = mapper.createObjectNode();
        body.put("text", text);
        body.put("format", "markdown");

        List<List<ObjectNode>> allRows = new ArrayList<>(rows);
        if (withNavigation) {
            allRows.add(navigationRow());
        }

        if (!allRows.isEmpty()) {
            ObjectNode attachment = mapper.createObjectNode();
            attachment.put("type", "inline_keyboard");
            ObjectNode payload = mapper.createObjectNode();
            ArrayNode buttons = mapper.createArrayNode();

            for (List<ObjectNode> row : allRows) {
                ArrayNode rowNode = mapper.createArrayNode();
                rowNode.addAll(row);
                buttons.add(rowNode);
            }

            payload.set("buttons", buttons);
            attachment.set("payload", payload);

            ArrayNode attachments = mapper.createArrayNode();
            attachments.add(attachment);
            body.set("attachments", attachments);
        }

        return body;
    }

    public List<ObjectNode> navigationRow() {
        List<ObjectNode> row = new ArrayList<>();
        row.add(callback("⬅️ Назад", "NAV:BACK"));
        row.add(callback("🏠 Главное меню", "NAV:MENU"));
        row.add(callback("👩‍💼 Оператор", "NAV:OPERATOR"));
        return row;
    }

    public ObjectNode callback(String text, String payload) {
        ObjectNode button = mapper.createObjectNode();
        button.put("type", "callback");
        button.put("text", text);
        button.put("payload", payload);
        return button;
    }

    public ObjectNode link(String text, String url) {
        ObjectNode button = mapper.createObjectNode();
        button.put("type", "link");
        button.put("text", text);
        button.put("url", url);
        return button;
    }

    public ObjectNode requestGeo(String text) {
        ObjectNode button = mapper.createObjectNode();
        button.put("type", "request_geo_location");
        button.put("text", text);
        return button;
    }

    public static List<ObjectNode> row(ObjectNode... buttons) {
        return List.of(buttons);
    }
}
