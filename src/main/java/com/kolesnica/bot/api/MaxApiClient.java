package com.kolesnica.bot.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kolesnica.bot.model.ChatTarget;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public final class MaxApiClient {
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private final String token;
    private final String baseUrl;
    private final int pollTimeout;
    private final int pollLimit;
    private final ObjectMapper mapper;
    private final OkHttpClient http;

    public MaxApiClient(
            String token,
            String baseUrl,
            int pollTimeout,
            int pollLimit,
            ObjectMapper mapper
    ) {
        this.token = token;
        this.baseUrl = baseUrl;
        this.pollTimeout = pollTimeout;
        this.pollLimit = pollLimit;
        this.mapper = mapper;
        long readTimeoutSeconds = Math.max(60L, pollTimeout + 45L);
        this.http = new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(20))
                .readTimeout(Duration.ofSeconds(readTimeoutSeconds))
                .writeTimeout(Duration.ofSeconds(30))
                .callTimeout(Duration.ofSeconds(readTimeoutSeconds + 10L))
                .protocols(List.of(Protocol.HTTP_1_1))
                .build();
    }

    public UpdateBatch getUpdates(Long marker) throws IOException {
        HttpUrl.Builder url = HttpUrl.parse(baseUrl + "/updates").newBuilder()
                .addQueryParameter("timeout", String.valueOf(pollTimeout))
                .addQueryParameter("limit", String.valueOf(pollLimit))
                .addQueryParameter("types", "message_created,message_callback,bot_started");

        if (marker != null) {
            url.addQueryParameter("marker", String.valueOf(marker));
        }

        Request request = new Request.Builder()
                .url(url.build())
                .header("Authorization", token)
                .get()
                .build();

        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Ошибка GET /updates: HTTP " + response.code() + " body=" + readBody(response.body()));
            }

            JsonNode root = mapper.readTree(readBody(response.body()));
            ArrayNode updates = root.path("updates").isArray()
                    ? (ArrayNode) root.path("updates")
                    : mapper.createArrayNode();
            List<JsonNode> list = new ArrayList<>(updates.size());
            updates.forEach(list::add);

            Long nextMarker = root.path("marker").isNull() ? null : root.path("marker").asLong();
            return new UpdateBatch(list, nextMarker);
        } catch (SocketTimeoutException e) {
            return new UpdateBatch(List.of(), marker);
        }
    }

    public void sendMessage(ChatTarget target, ObjectNode body) throws IOException {
        HttpUrl.Builder builder = HttpUrl.parse(baseUrl + "/messages").newBuilder();
        if (target.userId() != null) {
            builder.addQueryParameter("user_id", String.valueOf(target.userId()));
        } else if (target.chatId() != null) {
            builder.addQueryParameter("chat_id", String.valueOf(target.chatId()));
        } else {
            throw new IllegalArgumentException("Не указан ни user_id, ни chat_id");
        }

        Request request = new Request.Builder()
                .url(builder.build())
                .header("Authorization", token)
                .header("Content-Type", "application/json")
                .post(RequestBody.create(body.toString(), JSON))
                .build();

        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Ошибка POST /messages: HTTP " + response.code() + " body=" + readBody(response.body()));
            }
        }
    }

    public void answerCallback(String callbackId, String notification) throws IOException {
        HttpUrl url = HttpUrl.parse(baseUrl + "/answers").newBuilder()
                .addQueryParameter("callback_id", callbackId)
                .build();

        ObjectNode body = mapper.createObjectNode();
        body.put("notification", notification);

        Request request = new Request.Builder()
                .url(url)
                .header("Authorization", token)
                .header("Content-Type", "application/json")
                .post(RequestBody.create(body.toString(), JSON))
                .build();

        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Ошибка POST /answers: HTTP " + response.code() + " body=" + readBody(response.body()));
            }
        }
    }

    public JsonNode me() throws IOException {
        Request request = new Request.Builder()
                .url(baseUrl + "/me")
                .header("Authorization", token)
                .get()
                .build();

        try (Response response = http.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Ошибка GET /me: HTTP " + response.code() + " body=" + readBody(response.body()));
            }
            return mapper.readTree(readBody(response.body()));
        }
    }

    private String readBody(ResponseBody body) throws IOException {
        if (body == null) {
            return "";
        }
        return body.string();
    }

    public record UpdateBatch(List<JsonNode> updates, Long marker) {
    }
}
