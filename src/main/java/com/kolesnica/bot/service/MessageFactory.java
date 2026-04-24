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
        return buildMessage(text, flattenRows(rows), withNavigation ? userNavigationRows() : List.of());
    }

    public ObjectNode messageKeepRows(String text, List<List<ObjectNode>> rows, boolean withNavigation) {
        return buildMessage(text, rows, withNavigation ? userNavigationRows() : List.of());
    }

    public ObjectNode adminMessage(String text, List<List<ObjectNode>> rows, boolean withNavigation) {
        return buildMessage(text, flattenRows(rows), withNavigation ? adminNavigationRows() : List.of());
    }

    public ObjectNode adminMessageKeepRows(String text, List<List<ObjectNode>> rows, boolean withNavigation) {
        return buildMessage(text, rows, withNavigation ? adminNavigationRows() : List.of());
    }

    private ObjectNode buildMessage(String text, List<List<ObjectNode>> contentRows, List<List<ObjectNode>> navRows) {
        ObjectNode body = mapper.createObjectNode();
        body.put("text", text);
        body.put("format", "markdown");

        List<List<ObjectNode>> allRows = new ArrayList<>(contentRows);
        allRows.addAll(navRows);

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

    private List<List<ObjectNode>> flattenRows(List<List<ObjectNode>> rows) {
        List<List<ObjectNode>> flattened = new ArrayList<>();
        for (List<ObjectNode> row : rows) {
            for (ObjectNode button : row) {
                flattened.add(List.of(button));
            }
        }
        return flattened;
    }

    public List<List<ObjectNode>> userNavigationRows() {
        List<List<ObjectNode>> rows = new ArrayList<>();
        rows.add(List.of(
                callback("⬅️ Назад", "NAV:BACK"),
                callback("👩‍💼 Оператор", "NAV:OPERATOR")
        ));
        rows.add(List.of(callback("🏠 Главное меню", "NAV:MENU")));
        return rows;
    }

    public List<List<ObjectNode>> adminNavigationRows() {
        List<List<ObjectNode>> rows = new ArrayList<>();
        rows.add(List.of(callback("⬅️ Назад", "NAV:BACK")));
        rows.add(List.of(callback("🛠️ Админ панель", "NAV:ADMIN_HOME")));
        return rows;
    }

    public List<ObjectNode> navigationRow() {
        List<ObjectNode> row = new ArrayList<>();
        row.add(callback("⬅️ Назад", "NAV:BACK"));
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
