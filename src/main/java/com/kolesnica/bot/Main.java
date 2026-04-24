package com.kolesnica.bot;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kolesnica.bot.api.MaxApiClient;
import com.kolesnica.bot.config.AppConfig;
import com.kolesnica.bot.db.AdminRepository;
import com.kolesnica.bot.db.BranchRepository;
import com.kolesnica.bot.db.Database;
import com.kolesnica.bot.db.RequestRepository;
import com.kolesnica.bot.db.SessionRepository;
import com.kolesnica.bot.db.SettingsRepository;
import com.kolesnica.bot.service.BotService;
import com.kolesnica.bot.service.MessageFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnv();
        ObjectMapper mapper = new ObjectMapper();

        try (Database db = new Database(config.dbPath())) {
            SessionRepository sessions = new SessionRepository(db.connection(), mapper);
            BranchRepository branches = new BranchRepository(db.connection());
            AdminRepository admins = new AdminRepository(db.connection());
            SettingsRepository settings = new SettingsRepository(db.connection());
            RequestRepository requests = new RequestRepository(db.connection(), mapper);

            MaxApiClient api = new MaxApiClient(
                    config.botToken(),
                    config.apiBaseUrl(),
                    config.pollTimeout(),
                    config.pollLimit(),
                    mapper
            );

            MessageFactory messageFactory = new MessageFactory(mapper);
            BotService botService = new BotService(
                    api,
                    sessions,
                    branches,
                    admins,
                    settings,
                    requests,
                    messageFactory,
                    mapper
            );

            var me = api.me();
            log.info("Бот запущен: {} (@{})", me.path("name").asText("bot"), me.path("username").asText("unknown"));

            Long marker = null;
            while (true) {
                try {
                    MaxApiClient.UpdateBatch batch = api.getUpdates(marker);
                    for (var update : batch.updates()) {
                        botService.handleUpdate(update);
                    }
                    if (batch.marker() != null) {
                        marker = batch.marker();
                    }
                } catch (Exception e) {
                    log.error("Ошибка polling-цикла", e);
                    Thread.sleep(300L);
                }
            }
        }
    }
}
