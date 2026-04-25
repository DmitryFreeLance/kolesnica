package com.kolesnica.bot.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kolesnica.bot.api.MaxApiClient;
import com.kolesnica.bot.db.AdminRepository;
import com.kolesnica.bot.db.BranchRepository;
import com.kolesnica.bot.db.RequestRepository;
import com.kolesnica.bot.db.SessionRepository;
import com.kolesnica.bot.db.SettingsRepository;
import com.kolesnica.bot.db.UserRepository;
import com.kolesnica.bot.model.Branch;
import com.kolesnica.bot.model.BotUser;
import com.kolesnica.bot.model.ChatTarget;
import com.kolesnica.bot.model.UserSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

public final class BotService {
    private static final Logger log = LoggerFactory.getLogger(BotService.class);

    private static final String SC_BOOKING = "booking";
    private static final String SC_BRANCH = "branch_finder";
    private static final String SC_PRICE = "price";
    private static final String SC_STORAGE = "storage";
    private static final String SC_FEEDBACK = "feedback";
    private static final String SC_OPERATOR = "operator";
    private static final String SC_ADMIN = "admin";
    private static final String KEY_PRICING_CONFIG = "pricing_config";
    private static final int USERS_PAGE_SIZE = 20;
    private static final int BRANCH_NUMBER_ROW_MAX = 4;

    private static final String EX_NONE = "NONE";
    private static final String EX_RUNFLAT = "RUNFLAT";
    private static final String EX_LOW_PROFILE = "LOW_PROFILE";
    private static final String EX_VALVES = "VALVES";
    private static final String EX_SEALANT = "SEALANT";
    private static final String EX_HUB_LUBE = "HUB_LUBE";
    private static final String EX_GLUE_CLEAN = "GLUE_CLEAN";

    private static final String PRICE_CAT_SEDAN = "SEDAN";
    private static final String PRICE_CAT_SUV = "SUV";
    private static final String PRICE_CAT_COMM = "COMM";
    private static final String PRICE_CAT_EXTRA = "EXTRA";
    private static final String PRICE_CAT_SERVICE = "SERVICE";

    private static final Pattern PHONE_PATTERN = Pattern.compile("^[+0-9()\\-\\s]{6,20}$");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("dd.MM.yyyy");

    private final MaxApiClient api;
    private final SessionRepository sessions;
    private final BranchRepository branches;
    private final AdminRepository admins;
    private final SettingsRepository settings;
    private final UserRepository users;
    private final RequestRepository requests;
    private final MessageFactory messages;
    private final ObjectMapper mapper;

    public BotService(
            MaxApiClient api,
            SessionRepository sessions,
            BranchRepository branches,
            AdminRepository admins,
            SettingsRepository settings,
            UserRepository users,
            RequestRepository requests,
            MessageFactory messages,
            ObjectMapper mapper
    ) {
        this.api = api;
        this.sessions = sessions;
        this.branches = branches;
        this.admins = admins;
        this.settings = settings;
        this.users = users;
        this.requests = requests;
        this.messages = messages;
        this.mapper = mapper;
    }

    public void handleUpdate(JsonNode update) {
        try {
            String type = update.path("update_type").asText("");
            switch (type) {
                case "bot_started" -> handleBotStarted(update);
                case "message_created" -> handleMessageCreated(update);
                case "message_callback" -> handleMessageCallback(update);
                default -> log.debug("Пропускаю update_type={}", type);
            }
        } catch (Exception e) {
            log.error("Ошибка обработки update: {}", update, e);
        }
    }

    private void handleBotStarted(JsonNode update) throws SQLException, IOException {
        long userId = update.path("user").path("user_id").asLong(0L);
        Long chatId = optLong(update.path("chat_id"));
        if (userId == 0) {
            return;
        }
        upsertUserFromNode(update.path("user"));

        UserSession session = sessions.getOrCreate(userId, chatId);
        sessions.save(new UserSession(userId, chatId, null, null, mapper.createObjectNode(), mapper.createArrayNode()));
        sendMainMenu(targetFrom(userId, chatId), false);
    }

    private void handleMessageCreated(JsonNode update) throws SQLException, IOException {
        JsonNode message = update.path("message");
        if (message.isMissingNode()) {
            return;
        }

        long userId = message.path("sender").path("user_id").asLong(0L);
        if (userId == 0L) {
            return;
        }
        upsertUserFromNode(message.path("sender"));

        Long chatId = optLong(message.path("recipient").path("chat_id"));
        ChatTarget target = targetFrom(userId, chatId);
        UserSession session = sessions.getOrCreate(userId, chatId);

        String text = message.path("body").path("text").asText("").trim();
        Location location = extractLocation(message.path("body").path("attachments"));

        session = ensureChat(session, chatId);

        if (location != null) {
            session.state().put("last_lat", location.lat());
            session.state().put("last_lon", location.lon());
        }

        if (isStartCommand(text)) {
            session = new UserSession(userId, chatId, null, null, mapper.createObjectNode(), mapper.createArrayNode());
            sessions.save(session);
            sendMainMenu(target, false);
            return;
        }

        if (isAdminCommand(text)) {
            openAdminPanel(userId, session, target);
            return;
        }

        if (handleTypedNavigation(text, session, target)) {
            return;
        }

        if (location != null && tryHandleLocation(session, target, location)) {
            return;
        }

        if (text.isBlank()) {
            sendMessage(target,
                    "🛞 Понял вас. Чтобы двигаться дальше, нажмите кнопку ниже 👇",
                    List.of(),
                    true);
            sessions.save(session);
            return;
        }

        if (session.scenario() == null && session.step() == null) {
            String intent = classifyIntent(text);
            if (intent != null) {
                startScenario(intent, session, target);
            } else {
                sendMainMenu(target, false);
            }
            return;
        }

        boolean handled = handleTextByCurrentStep(session, target, text);
        if (!handled) {
            String intent = classifyIntent(text);
            if (intent != null) {
                startScenario(intent, session, target);
            } else {
                sendMainMenu(target, true);
            }
        }
    }

    private void handleMessageCallback(JsonNode update) throws SQLException, IOException {
        JsonNode callback = update.path("callback");
        String callbackId = callback.path("callback_id").asText("");
        String payload = callback.path("payload").asText("");
        String callbackMessageId = update.path("message").path("body").path("mid").asText("");

        long userId = callback.path("user").path("user_id").asLong(0L);
        if (userId == 0) {
            return;
        }
        upsertUserFromNode(callback.path("user"));

        Long chatId = optLong(update.path("message").path("recipient").path("chat_id"));
        ChatTarget target = targetFrom(userId, chatId);
        UserSession session = ensureChat(sessions.getOrCreate(userId, chatId), chatId);

        if (!callbackId.isBlank()) {
            try {
                api.answerCallback(callbackId, "Принято ✅");
            } catch (Exception e) {
                log.warn("Не удалось отправить callback-answer: {}", e.getMessage());
            }
        }

        if (payload.startsWith("NAV:")) {
            handleNavigationPayload(payload, session, target);
            return;
        }

        if (payload.startsWith("ACT:")) {
            handleActionPayload(payload, session, target);
            return;
        }

        boolean handled = false;
        if (SC_BOOKING.equals(session.scenario())) {
            handled = handleBookingCallback(session, target, payload);
        } else if (SC_BRANCH.equals(session.scenario())) {
            handled = handleBranchCallback(session, target, payload);
        } else if (SC_PRICE.equals(session.scenario())) {
            handled = handlePriceCallback(session, target, payload, callbackMessageId);
        } else if (SC_STORAGE.equals(session.scenario())) {
            handled = handleStorageCallback(session, target, payload);
        } else if (SC_FEEDBACK.equals(session.scenario())) {
            handled = handleFeedbackCallback(session, target, payload);
        } else if (SC_OPERATOR.equals(session.scenario())) {
            handled = handleOperatorCallback(session, target, payload);
        } else if (SC_ADMIN.equals(session.scenario())) {
            handled = handleAdminCallback(session, target, payload);
        }

        if (!handled) {
            sendMainMenu(target, true);
        }
    }

    private boolean handleTypedNavigation(String text, UserSession session, ChatTarget target) throws SQLException, IOException {
        String t = normalize(text);
        if (t.contains("глав") && t.contains("мен")) {
            sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            sendMainMenu(target, false);
            return true;
        }
        if (t.equals("назад") || t.equals("back") || t.equals("⬅️")) {
            goBack(session, target);
            return true;
        }
        if (t.contains("оператор") || t.contains("менеджер")) {
            sendOperatorContact(target);
            sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            return true;
        }
        return false;
    }

    private void handleNavigationPayload(String payload, UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (payload) {
            case "NAV:MENU" -> {
                sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
                sendMainMenu(target, false);
            }
            case "NAV:ADMIN_HOME" -> openAdminPanel(session.userId(), session, target);
            case "NAV:BACK" -> goBack(session, target);
            case "NAV:OPERATOR" -> {
                sendOperatorContact(target);
                sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            }
            default -> sendMainMenu(target, true);
        }
    }

    private void handleActionPayload(String payload, UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (payload) {
            case "ACT:BOOK" -> startScenario(SC_BOOKING, session, target);
            case "ACT:BRANCH" -> startScenario(SC_BRANCH, session, target);
            case "ACT:PRICE" -> startScenario(SC_PRICE, session, target);
            case "ACT:STORAGE" -> startScenario(SC_STORAGE, session, target);
            case "ACT:FEEDBACK" -> startScenario(SC_FEEDBACK, session, target);
            case "ACT:OPERATOR" -> {
                sendOperatorContact(target);
                sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            }
            case "ACT:USER_MENU" -> {
                sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
                sendMainMenu(target, false);
            }
            case "ACT:ADMIN" -> openAdminPanel(session.userId(), session, target);
            case "ACT:RESUME" -> renderCurrentStep(session, target);
            default -> sendMainMenu(target, true);
        }
    }

    private void startScenario(String scenario, UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (scenario) {
            case SC_BOOKING -> {
                ObjectNode state = mapper.createObjectNode();
                if (session.state().has("preferred_branch_id")) {
                    state.set("preferred_branch_id", session.state().get("preferred_branch_id"));
                }
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_BOOKING, "BOOK_CAR", state, mapper.createArrayNode());
                sessions.save(next);
                renderBookingStep(next, target);
            }
            case SC_BRANCH -> {
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_BRANCH, "BR_METHOD", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                renderBranchStep(next, target);
            }
            case SC_PRICE -> {
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_PRICE, "PRICE_SERVICE", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                renderPriceStep(next, target);
            }
            case SC_STORAGE -> {
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_STORAGE, "ST_ACTION", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                renderStorageStep(next, target);
            }
            case SC_FEEDBACK -> {
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_FEEDBACK, "FB_TYPE", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                renderFeedbackStep(next, target);
            }
            case SC_OPERATOR -> {
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_OPERATOR, "OP_INIT", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                renderOperatorStep(next, target);
            }
            case SC_ADMIN -> {
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_ADMIN, "AD_MENU", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                renderAdminStep(next, target);
            }
            default -> sendMainMenu(target, true);
        }
    }

    private void renderCurrentStep(UserSession session, ChatTarget target) throws SQLException, IOException {
        if (session.scenario() == null || session.step() == null) {
            sendMainMenu(target, true);
            return;
        }

        switch (session.scenario()) {
            case SC_BOOKING -> renderBookingStep(session, target);
            case SC_BRANCH -> renderBranchStep(session, target);
            case SC_PRICE -> renderPriceStep(session, target);
            case SC_STORAGE -> renderStorageStep(session, target);
            case SC_FEEDBACK -> renderFeedbackStep(session, target);
            case SC_OPERATOR -> renderOperatorStep(session, target);
            case SC_ADMIN -> renderAdminStep(session, target);
            default -> sendMainMenu(target, true);
        }
    }

    private void goBack(UserSession session, ChatTarget target) throws SQLException, IOException {
        if (session.history().isEmpty()) {
            sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            sendMainMenu(target, true);
            return;
        }

        ArrayNode history = session.history().deepCopy();
        int lastIndex = history.size() - 1;
        String previousStep = history.get(lastIndex).asText();
        history.remove(lastIndex);

        UserSession previous = new UserSession(
                session.userId(),
                session.chatId(),
                session.scenario(),
                previousStep,
                session.state(),
                history
        );
        sessions.save(previous);
        renderCurrentStep(previous, target);
    }

    private boolean tryHandleLocation(UserSession session, ChatTarget target, Location location) throws SQLException, IOException {
        if (SC_BRANCH.equals(session.scenario()) && "BR_GEO_WAIT".equals(session.step())) {
            handleNearestBranch(session, target, location.lat(), location.lon());
            return true;
        }
        if (SC_BOOKING.equals(session.scenario()) && "BOOK_BRANCH_METHOD".equals(session.step())) {
            handleNearestBookingBranch(session, target, location.lat(), location.lon());
            return true;
        }
        return false;
    }

    private boolean handleTextByCurrentStep(UserSession session, ChatTarget target, String text) throws SQLException, IOException {
        if (SC_BOOKING.equals(session.scenario())) {
            return handleBookingText(session, target, text);
        }
        if (SC_BRANCH.equals(session.scenario())) {
            return handleBranchText(session, target, text);
        }
        if (SC_PRICE.equals(session.scenario())) {
            return handlePriceText(session, target, text);
        }
        if (SC_STORAGE.equals(session.scenario())) {
            return handleStorageText(session, target, text);
        }
        if (SC_FEEDBACK.equals(session.scenario())) {
            return handleFeedbackText(session, target, text);
        }
        if (SC_OPERATOR.equals(session.scenario())) {
            return handleOperatorText(session, target, text);
        }
        if (SC_ADMIN.equals(session.scenario())) {
            return handleAdminText(session, target, text);
        }
        return false;
    }

    private void sendMainMenu(ChatTarget target, boolean includeResumeHint) throws IOException {
        String text = includeResumeHint
                ? "🏠 Главное меню\nВыберите, чем помочь прямо сейчас."
                : "👋 Добро пожаловать в *Колесницу*.\nПодберём филиал, цену и запись за пару шагов.";

        List<List<ObjectNode>> rows = List.of(
                MessageFactory.row(messages.callback("✅ Записаться на переобувку", "ACT:BOOK")),
                MessageFactory.row(messages.callback("📍 Найти ближайший филиал", "ACT:BRANCH")),
                MessageFactory.row(messages.callback("💸 Узнать цену", "ACT:PRICE")),
                MessageFactory.row(messages.callback("📦 Хранение шин", "ACT:STORAGE")),
                MessageFactory.row(messages.callback("📝 Отзыв / жалоба", "ACT:FEEDBACK")),
                MessageFactory.row(messages.callback("👩‍💼 Связь с оператором", "ACT:OPERATOR"))
        );

        sendMessage(target, text, rows, false);
    }

    // ---------------- BOOKING ----------------

    private void renderBookingStep(UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (session.step()) {
            case "BOOK_CAR" -> sendMessage(target,
                    "🚗 Выберите тип автомобиля:\nчтобы подобрать точную услугу.",
                    List.of(
                            MessageFactory.row(messages.callback("🚙 Седан", "BOOK:CAR:Седан"), messages.callback("🚘 Кроссовер", "BOOK:CAR:Кроссовер")),
                            MessageFactory.row(messages.callback("🛻 Внедорожник", "BOOK:CAR:Внедорожник"), messages.callback("🚐 Минивен", "BOOK:CAR:Минивен")),
                            MessageFactory.row(messages.callback("🚚 Коммерческий", "BOOK:CAR:Коммерческий"))
                    ), true);
            case "BOOK_WHEELS" -> sendMessage(target,
                    "🚚 Сколько колёс у коммерческого авто?",
                    List.of(
                            MessageFactory.row(messages.callback("4 колеса", "BOOK:WHEELS:4")),
                            MessageFactory.row(messages.callback("6 колёс", "BOOK:WHEELS:6"))
                    ),
                    true);
            case "BOOK_RADIUS" -> sendMessageKeepRows(target,
                    "🛞 Укажите диаметр колёс:",
                    List.of(
                            MessageFactory.row(
                                    messages.callback("R13", "BOOK:RAD:R13"),
                                    messages.callback("R14", "BOOK:RAD:R14"),
                                    messages.callback("R15", "BOOK:RAD:R15"),
                                    messages.callback("R16", "BOOK:RAD:R16")
                            ),
                            MessageFactory.row(
                                    messages.callback("R17", "BOOK:RAD:R17"),
                                    messages.callback("R18", "BOOK:RAD:R18"),
                                    messages.callback("R19+", "BOOK:RAD:R19+")
                            ),
                            MessageFactory.row(messages.callback("🤷 Не знаю", "BOOK:RAD:Не знаю"))
                    ), true);
            case "BOOK_EXTRA" -> sendMessage(target,
                    "🧩 Нужны доп. услуги?\n"
                            + "В стоимость переобувки уже входит:\n"
                            + "• Съём/установка колёс\n"
                            + "• Технологическая мойка колёс\n"
                            + "• Шиномонтаж\n"
                            + "• Балансировка",
                    List.of(
                            MessageFactory.row(messages.callback("🚫 Без допов", "BOOK:EXTRA:Без допов")),
                            MessageFactory.row(messages.callback("⚙️ RunFlat", "BOOK:EXTRA:RunFlat")),
                            MessageFactory.row(messages.callback("🏎️ Низкий профиль (=<50)", "BOOK:EXTRA:Низкий профиль (=<50)")),
                            MessageFactory.row(messages.callback("🛠️ Замена вентилей", "BOOK:EXTRA:Замена вентилей")),
                            MessageFactory.row(messages.callback("🧴 Очистка и нанесение герметика", "BOOK:EXTRA:Очистка и нанесение герметика")),
                            MessageFactory.row(messages.callback("🛞 Смазка ступиц (медь/алюминий)", "BOOK:EXTRA:Медная или алюминиевая смазка ступиц")),
                            MessageFactory.row(messages.callback("🧽 Очистка дисков от старого клея", "BOOK:EXTRA:Очистка дисков от старого клея"))
                    ), true);
            case "BOOK_BRANCH_METHOD" -> {
                List<List<ObjectNode>> rows = new ArrayList<>();
                if (session.state().has("preferred_branch_id")) {
                    rows.add(MessageFactory.row(messages.callback("✅ Использовать выбранный филиал", "BOOK:BRANCH:USE_PREFERRED")));
                }
                rows.add(MessageFactory.row(messages.callback("📍 Ближайший", "BOOK:BRANCH:NEAREST"), messages.callback("🗺️ Выбрать район", "BOOK:BRANCH:DISTRICT")));
                rows.add(MessageFactory.row(messages.callback("📋 Список филиалов", "BOOK:BRANCH:LIST"), messages.requestGeo("📡 Отправить геолокацию")));
                sendMessage(target,
                        "🏢 Выберите филиал:\nможно по району, списком или по геолокации.",
                        rows, true);
            }
            case "BOOK_CITY" -> {
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String city : branches.listCities()) {
                    rows.add(MessageFactory.row(messages.callback("🏙️ " + city, "BOOK:CITY:" + city)));
                }
                sendMessage(target, "🏙️ Выберите город:", rows, true);
            }
            case "BOOK_DISTRICT" -> {
                String city = session.state().path("city").asText("");
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String district : branches.listDistricts(city)) {
                    rows.add(MessageFactory.row(messages.callback("📍 " + district, "BOOK:DIST:" + district)));
                }
                sendMessage(target, "🗺️ Выберите район в городе " + city + ":", rows, true);
            }
            case "BOOK_BRANCH_PICK" -> {
                String city = session.state().path("city").asText("");
                String district = session.state().path("district").asText("");
                List<Branch> list = branches.findByFilters(city.isBlank() ? null : city, district.isBlank() ? null : district, null);
                sendBranchPickerWithNumbers(target, "🏢 Выберите филиал из списка:", list, "BOOK:BRANCH_ID:");
            }
            case "BOOK_DATE" -> sendMessage(target,
                    "📅 Когда удобно приехать?",
                    List.of(
                            MessageFactory.row(messages.callback("🟢 Сегодня", "BOOK:DATE:TODAY"), messages.callback("🟡 Завтра", "BOOK:DATE:TOMORROW")),
                            MessageFactory.row(messages.callback("🗓️ Выбрать дату", "BOOK:DATE:CUSTOM"), messages.callback("⚡ Как можно быстрее", "BOOK:DATE:ASAP"))
                    ), true);
            case "BOOK_DATE_INPUT" -> sendMessage(target,
                    "✍️ Введите дату в формате *дд.мм.гггг*\nНапример: 30.04.2026",
                    List.of(), true);
            case "BOOK_TIME" -> sendMessage(target,
                    "⏰ Выберите время:",
                    List.of(
                            MessageFactory.row(messages.callback("🌅 Утро", "BOOK:TIME:Утро"), messages.callback("☀️ День", "BOOK:TIME:День")),
                            MessageFactory.row(messages.callback("🌇 Вечер", "BOOK:TIME:Вечер"), messages.callback("⚡ Любое ближайшее", "BOOK:TIME:Любое ближайшее"))
                    ), true);
            case "BOOK_NAME" -> sendMessage(target,
                    "🙂 Как к вам обращаться?\nВведите имя:",
                    List.of(), true);
            case "BOOK_PHONE" -> sendMessage(target,
                    "📱 Напишите номер телефона\nв формате +7...",
                    List.of(), true);
            case "BOOK_BRAND" -> sendMessage(target,
                    "🚘 Укажите марку авто:",
                    List.of(), true);
            case "BOOK_PLATE" -> sendMessage(target,
                    "🔢 Госномер (опционально):\nможно ввести или пропустить.",
                    List.of(MessageFactory.row(messages.callback("➖ Пропустить", "BOOK:PLATE:SKIP"))), true);
            case "BOOK_COMMENT_CHOICE" -> sendMessage(target,
                    "💬 Добавим комментарий к заявке?",
                    List.of(
                            MessageFactory.row(messages.callback("🚫 Нет", "BOOK:COMMENT:NONE"), messages.callback("✍️ Добавить", "BOOK:COMMENT:ADD"))
                    ), true);
            case "BOOK_COMMENT_TEXT" -> sendMessage(target,
                    "✍️ Напишите комментарий к заявке:",
                    List.of(), true);
            case "BOOK_EDIT_PICK" -> sendMessage(target,
                    "✏️ Что нужно изменить в заявке?",
                    List.of(
                            MessageFactory.row(messages.callback("🚗 Тип авто", "BOOK:EDIT_FIELD:car_type")),
                            MessageFactory.row(messages.callback("🛞 Диаметр", "BOOK:EDIT_FIELD:radius")),
                            MessageFactory.row(messages.callback("🧩 Доп. услуги", "BOOK:EDIT_FIELD:extra")),
                            MessageFactory.row(messages.callback("🏢 Филиал", "BOOK:EDIT_FIELD:branch")),
                            MessageFactory.row(messages.callback("📅 Дата", "BOOK:EDIT_FIELD:date")),
                            MessageFactory.row(messages.callback("⏰ Время", "BOOK:EDIT_FIELD:time")),
                            MessageFactory.row(messages.callback("🙂 Имя", "BOOK:EDIT_FIELD:name")),
                            MessageFactory.row(messages.callback("📱 Телефон", "BOOK:EDIT_FIELD:phone")),
                            MessageFactory.row(messages.callback("🚙 Марка авто", "BOOK:EDIT_FIELD:car_brand")),
                            MessageFactory.row(messages.callback("🔢 Госномер", "BOOK:EDIT_FIELD:plate")),
                            MessageFactory.row(messages.callback("💬 Комментарий", "BOOK:EDIT_FIELD:comment"))
                    ), true);
            case "BOOK_CONFIRM" -> sendBookingConfirm(session, target);
            default -> sendMainMenu(target, true);
        }
    }

    private boolean handleBookingCallback(UserSession session, ChatTarget target, String payload) throws SQLException, IOException {
        if (payload.startsWith("BOOK:CAR:")) {
            String carType = payload.substring("BOOK:CAR:".length());
            session.state().put("car_type", carType);
            if ("Коммерческий".equals(carType)) {
                session.state().remove("radius");
                session.state().remove("wheel_count");
                UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_WHEELS"));
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
            session.state().remove("wheel_count");
            UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_RADIUS"));
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.startsWith("BOOK:WHEELS:")) {
            String wheels = payload.substring("BOOK:WHEELS:".length());
            if (!"4".equals(wheels) && !"6".equals(wheels)) {
                sendMessage(target, "⚠️ Выберите 4 или 6 колёс кнопкой.", List.of(), true);
                return true;
            }
            session.state().put("wheel_count", wheels);
            session.state().put("radius", "Коммерческий (" + wheels + " колеса)");
            UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_EXTRA"));
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.startsWith("BOOK:RAD:")) {
            return bookingSelect(session, target, "radius", payload.substring("BOOK:RAD:".length()), "BOOK_EXTRA");
        }
        if (payload.startsWith("BOOK:EXTRA:")) {
            return bookingSelect(session, target, "extra", payload.substring("BOOK:EXTRA:".length()), "BOOK_BRANCH_METHOD");
        }
        if (payload.equals("BOOK:BRANCH:USE_PREFERRED")) {
            long id = session.state().path("preferred_branch_id").asLong(0);
            if (id > 0) {
                session.state().put("branch_id", id);
                String title = branches.findById(id).map(Branch::shortLabel).orElse("выбран");
                sendMessage(target, "✅ Филиал выбран: " + title, List.of(), true);
                UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_DATE"));
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
        }
        if (payload.equals("BOOK:BRANCH:NEAREST")) {
            Double lat = session.state().has("last_lat") ? session.state().path("last_lat").asDouble() : null;
            Double lon = session.state().has("last_lon") ? session.state().path("last_lon").asDouble() : null;
            if (lat == null || lon == null) {
                sendMessage(target,
                        "📡 Чтобы найти ближайший филиал, отправьте геолокацию кнопкой ниже.",
                        List.of(MessageFactory.row(messages.requestGeo("📡 Отправить геолокацию"))),
                        true);
                return true;
            }
            handleNearestBookingBranch(session, target, lat, lon);
            return true;
        }
        if (payload.equals("BOOK:BRANCH:DISTRICT")) {
            session.state().put("branch_mode", "district");
            UserSession next = nextStep(session, "BOOK_CITY");
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.equals("BOOK:BRANCH:LIST")) {
            session.state().put("branch_mode", "list");
            UserSession next = nextStep(session, "BOOK_CITY");
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.startsWith("BOOK:CITY:")) {
            session.state().put("city", payload.substring("BOOK:CITY:".length()));
            session.state().remove("district");
            UserSession next = nextStep(session, "BOOK_DISTRICT");
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.startsWith("BOOK:DIST:")) {
            session.state().put("district", payload.substring("BOOK:DIST:".length()));
            UserSession next = nextStep(session, "BOOK_BRANCH_PICK");
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.startsWith("BOOK:BRANCH_ID:")) {
            long id = Long.parseLong(payload.substring("BOOK:BRANCH_ID:".length()));
            session.state().put("branch_id", id);
            UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_DATE"));
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.startsWith("BOOK:DATE:")) {
            String dateType = payload.substring("BOOK:DATE:".length());
            if ("CUSTOM".equals(dateType)) {
                UserSession next = nextStep(session, "BOOK_DATE_INPUT");
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
            String date = switch (dateType) {
                case "TODAY" -> LocalDate.now().format(DATE_FMT);
                case "TOMORROW" -> LocalDate.now().plusDays(1).format(DATE_FMT);
                case "ASAP" -> "Как можно быстрее";
                default -> "Как можно быстрее";
            };
            session.state().put("date", date);
            UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_TIME"));
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.startsWith("BOOK:TIME:")) {
            session.state().put("time", payload.substring("BOOK:TIME:".length()));
            UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_NAME"));
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.equals("BOOK:PLATE:SKIP")) {
            session.state().put("plate", "-");
            UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_COMMENT_CHOICE"));
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.equals("BOOK:COMMENT:NONE")) {
            session.state().put("comment", "-");
            UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_CONFIRM"));
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.equals("BOOK:COMMENT:ADD")) {
            UserSession next = nextStep(session, "BOOK_COMMENT_TEXT");
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.equals("BOOK:EDIT_PICK")) {
            UserSession next = nextStep(session, "BOOK_EDIT_PICK");
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.startsWith("BOOK:EDIT_FIELD:")) {
            String field = payload.substring("BOOK:EDIT_FIELD:".length());
            String step = switch (field) {
                case "car_type" -> "BOOK_CAR";
                case "radius" -> "Коммерческий".equals(session.state().path("car_type").asText("")) ? "BOOK_WHEELS" : "BOOK_RADIUS";
                case "extra" -> "BOOK_EXTRA";
                case "branch" -> "BOOK_BRANCH_METHOD";
                case "date" -> "BOOK_DATE";
                case "time" -> "BOOK_TIME";
                case "name" -> "BOOK_NAME";
                case "phone" -> "BOOK_PHONE";
                case "car_brand" -> "BOOK_BRAND";
                case "plate" -> "BOOK_PLATE";
                case "comment" -> "BOOK_COMMENT_CHOICE";
                default -> null;
            };
            if (step == null) {
                sendMessage(target, "Не удалось выбрать поле для редактирования.", List.of(), true);
                return true;
            }
            session.state().put("editing_field", field);
            UserSession next = nextStep(session, step);
            sessions.save(next);
            renderBookingStep(next, target);
            return true;
        }
        if (payload.equals("BOOK:CONFIRM")) {
            ObjectNode requestPayload = session.state().deepCopy();
            requestPayload.put("scenario", SC_BOOKING);
            requests.saveRequest(session.userId(), session.chatId(), "booking", requestPayload);
            notifyAdmins("🚗 Новая заявка на переобувку\n" + buildBookingAdminSummary(session.state()));

            sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            sendMessage(target,
                    "✅ Заявка создана и передана в обработку.\nСкоро с вами свяжемся для подтверждения.",
                    List.of(MessageFactory.row(messages.callback("🏠 В меню", "NAV:MENU"), messages.callback("👩‍💼 Оператор", "ACT:OPERATOR"))),
                    false);
            return true;
        }

        return false;
    }

    private boolean handleBookingText(UserSession session, ChatTarget target, String text) throws SQLException, IOException {
        switch (session.step()) {
            case "BOOK_DATE_INPUT" -> {
                if (!isValidDate(text)) {
                    sendMessage(target, "📅 Формат даты: *дд.мм.гггг*\nПопробуйте ещё раз.", List.of(), true);
                    return true;
                }
                session.state().put("date", text);
                UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_TIME"));
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
            case "BOOK_NAME" -> {
                session.state().put("name", text);
                UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_PHONE"));
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
            case "BOOK_PHONE" -> {
                if (!isValidPhone(text)) {
                    sendMessage(target, "📱 Номер выглядит странно.\nВведите в формате +7...", List.of(), true);
                    return true;
                }
                session.state().put("phone", text);
                UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_BRAND"));
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
            case "BOOK_BRAND" -> {
                session.state().put("car_brand", text);
                UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_PLATE"));
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
            case "BOOK_PLATE" -> {
                session.state().put("plate", text);
                UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_COMMENT_CHOICE"));
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
            case "BOOK_COMMENT_TEXT" -> {
                session.state().put("comment", text);
                UserSession next = nextStep(session, resolveBookingNextStep(session, "BOOK_CONFIRM"));
                sessions.save(next);
                renderBookingStep(next, target);
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    private void sendBookingConfirm(UserSession session, ChatTarget target) throws IOException, SQLException {
        String branchText = "не выбран";
        long branchId = session.state().path("branch_id").asLong(0);
        if (branchId > 0) {
            Optional<Branch> branch = branches.findById(branchId);
            if (branch.isPresent()) {
                branchText = branch.get().shortLabel();
            }
        }

        String summary = "📋 Проверьте заявку:\n"
                + "🚗 " + safe(session.state(), "car_type") + "\n"
                + "🛞 Диаметр: " + safe(session.state(), "radius") + "\n"
                + ("Коммерческий".equals(safe(session.state(), "car_type"))
                ? "🚚 Колёса: " + safe(session.state(), "wheel_count") + "\n"
                : "")
                + "🏢 " + branchText + "\n"
                + "📅 " + safe(session.state(), "date") + ", " + safe(session.state(), "time") + "\n"
                + "👤 " + safe(session.state(), "name") + " • 📱 " + safe(session.state(), "phone");

        sendMessage(target,
                summary,
                List.of(
                        MessageFactory.row(messages.callback("✅ Подтвердить", "BOOK:CONFIRM"), messages.callback("✏️ Изменить", "BOOK:EDIT_PICK"))
                ), true);
    }

    private void handleNearestBookingBranch(UserSession session, ChatTarget target, double lat, double lon) throws SQLException, IOException {
        Optional<Branch> nearest = branches.findNearest(lat, lon, null);
        if (nearest.isEmpty()) {
            sendMessage(target, "😔 Не удалось найти ближайший филиал.\nВыберите вручную по району.", List.of(), true);
            return;
        }

        Branch b = nearest.get();
        session.state().put("branch_id", b.id());
        String nextStepName = resolveBookingNextStep(session, "BOOK_DATE");
        UserSession next = nextStep(session, nextStepName);
        sessions.save(next);
        if ("BOOK_DATE".equals(nextStepName)) {
            sendMessage(target,
                    "✅ Ближайший филиал выбран:\n" + b.cardText() + "\n\n📅 Когда удобно приехать?",
                    List.of(
                            MessageFactory.row(messages.callback("🟢 Сегодня", "BOOK:DATE:TODAY"), messages.callback("🟡 Завтра", "BOOK:DATE:TOMORROW")),
                            MessageFactory.row(messages.callback("🗓️ Выбрать дату", "BOOK:DATE:CUSTOM"), messages.callback("⚡ Как можно быстрее", "BOOK:DATE:ASAP"))
                    ),
                    true);
            return;
        }
        renderBookingStep(next, target);
    }

    private boolean bookingSelect(UserSession session, ChatTarget target, String key, String value, String step) throws SQLException, IOException {
        session.state().put(key, value);
        UserSession next = nextStep(session, resolveBookingNextStep(session, step));
        sessions.save(next);
        renderBookingStep(next, target);
        return true;
    }

    private String resolveBookingNextStep(UserSession session, String defaultStep) {
        String editing = session.state().path("editing_field").asText("");
        if (!editing.isBlank()) {
            session.state().remove("editing_field");
            return "BOOK_CONFIRM";
        }
        return defaultStep;
    }

    // ---------------- BRANCH FINDER ----------------

    private void renderBranchStep(UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (session.step()) {
            case "BR_METHOD" -> sendMessage(target,
                    "📍 Подберём ближайший филиал.\nКак вам удобнее?",
                    List.of(
                            MessageFactory.row(messages.callback("🗺️ По району", "BR:METHOD:DISTRICT"), messages.callback("📡 По геолокации", "BR:METHOD:GEO")),
                            MessageFactory.row(messages.callback("📋 Показать все", "BR:METHOD:ALL"))
                    ), true);
            case "BR_CITY" -> {
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String city : branches.listCities()) {
                    rows.add(MessageFactory.row(messages.callback("🏙️ " + city, "BR:CITY:" + city)));
                }
                sendMessage(target, "🏙️ Выберите город:", rows, true);
            }
            case "BR_DISTRICT" -> {
                String city = session.state().path("city").asText("");
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String district : branches.listDistricts(city)) {
                    rows.add(MessageFactory.row(messages.callback("📍 " + district, "BR:DIST:" + district)));
                }
                sendMessage(target, "🗺️ Выберите район:", rows, true);
            }
            case "BR_PICK" -> {
                String city = session.state().path("city").asText("");
                String district = session.state().path("district").asText("");
                List<Branch> list = branches.findByFilters(city.isBlank() ? null : city, district.isBlank() ? null : district, null);
                sendBranchPickerWithNumbers(target, "🏢 Выберите филиал:", list, "BR:ID:");
            }
            case "BR_GEO_WAIT" -> sendMessage(target,
                    "📡 Отправьте геолокацию кнопкой ниже,\nи я покажу ближайший филиал.",
                    List.of(MessageFactory.row(messages.requestGeo("📡 Отправить геолокацию"))),
                    true);
            case "BR_CARD" -> {
                long branchId = session.state().path("branch_id").asLong(0);
                Optional<Branch> br = branches.findById(branchId);
                if (br.isEmpty()) {
                    sendMessage(target, "😔 Филиал не найден. Выберите другой.", List.of(), true);
                    return;
                }
                Branch b = br.get();
                sendMessage(target,
                        b.cardText(),
                        List.of(
                                MessageFactory.row(messages.callback("✅ Записаться сюда", "BR:CARD:BOOK:" + b.id())),
                                MessageFactory.row(messages.callback("📞 Позвонить", "BR:CARD:CALL:" + b.id())),
                                MessageFactory.row(messages.callback("🔁 Другой филиал", "BR:CARD:OTHER"))
                        ),
                        true);
            }
            default -> sendMainMenu(target, true);
        }
    }

    private boolean handleBranchCallback(UserSession session, ChatTarget target, String payload) throws SQLException, IOException {
        switch (payload) {
            case "BR:METHOD:DISTRICT" -> {
                session.state().put("method", "district");
                UserSession next = nextStep(session, "BR_CITY");
                sessions.save(next);
                renderBranchStep(next, target);
                return true;
            }
            case "BR:METHOD:ALL" -> {
                session.state().put("method", "all");
                session.state().remove("city");
                session.state().remove("district");
                UserSession next = nextStep(session, "BR_PICK");
                sessions.save(next);
                renderBranchStep(next, target);
                return true;
            }
            case "BR:METHOD:GEO" -> {
                session.state().put("method", "geo");
                UserSession next = nextStep(session, "BR_GEO_WAIT");
                sessions.save(next);
                renderBranchStep(next, target);
                return true;
            }
            case "BR:CARD:OTHER" -> {
                String method = session.state().path("method").asText("all");
                String step = "district".equals(method) ? "BR_DISTRICT" : "BR_PICK";
                UserSession next = nextStep(session, step);
                sessions.save(next);
                renderBranchStep(next, target);
                return true;
            }
            default -> {
                if (payload.startsWith("BR:CITY:")) {
                    session.state().put("city", payload.substring("BR:CITY:".length()));
                    session.state().remove("district");
                    String method = session.state().path("method").asText("district");
                    UserSession next = nextStep(session, "all".equals(method) ? "BR_PICK" : "BR_DISTRICT");
                    sessions.save(next);
                    renderBranchStep(next, target);
                    return true;
                }
                if (payload.startsWith("BR:DIST:")) {
                    session.state().put("district", payload.substring("BR:DIST:".length()));
                    UserSession next = nextStep(session, "BR_PICK");
                    sessions.save(next);
                    renderBranchStep(next, target);
                    return true;
                }
                if (payload.startsWith("BR:ID:")) {
                    session.state().put("branch_id", Long.parseLong(payload.substring("BR:ID:".length())));
                    UserSession next = nextStep(session, "BR_CARD");
                    sessions.save(next);
                    renderBranchStep(next, target);
                    return true;
                }
                if (payload.startsWith("BR:CARD:BOOK:")) {
                    long id = Long.parseLong(payload.substring("BR:CARD:BOOK:".length()));
                    session.state().put("preferred_branch_id", id);
                    UserSession jump = new UserSession(session.userId(), session.chatId(), SC_BOOKING, "BOOK_CAR", session.state(), mapper.createArrayNode());
                    sessions.save(jump);
                    renderBookingStep(jump, target);
                    return true;
                }
                if (payload.startsWith("BR:CARD:CALL:")) {
                    long id = Long.parseLong(payload.substring("BR:CARD:CALL:".length()));
                    Optional<Branch> branch = branches.findById(id);
                    if (branch.isPresent()) {
                        sendMessage(target, "📞 Телефон филиала: " + branch.get().phone(), List.of(), true);
                    } else {
                        sendMessage(target, "😔 Не удалось найти телефон филиала.", List.of(), true);
                    }
                    return true;
                }
                return false;
            }
        }
    }

    private boolean handleBranchText(UserSession session, ChatTarget target, String text) {
        return false;
    }

    private void handleNearestBranch(UserSession session, ChatTarget target, double lat, double lon) throws SQLException, IOException {
        Optional<Branch> nearest = branches.findNearest(lat, lon, null);
        if (nearest.isEmpty()) {
            sendMessage(target, "😔 Не удалось определить ближайший филиал.\nВыберите по району.", List.of(), true);
            return;
        }
        Branch b = nearest.get();
        session.state().put("branch_id", b.id());
        UserSession next = nextStep(session, "BR_CARD");
        sessions.save(next);
        renderBranchStep(next, target);
    }

    // ---------------- PRICE ----------------

    private void renderPriceStep(UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (session.step()) {
            case "PRICE_SERVICE" -> sendMessage(target,
                    "💸 Выберите услугу:\nпосчитаю ориентир " + "*от ... ₽*",
                    List.of(
                            MessageFactory.row(messages.callback("🚗 Переобувка", "PRICE:SERVICE:Переобувка"), messages.callback("🧰 Ремонт", "PRICE:SERVICE:Ремонт")),
                            MessageFactory.row(messages.callback("⚖️ Балансировка", "PRICE:SERVICE:Балансировка"), messages.callback("🔧 Правка", "PRICE:SERVICE:Правка")),
                            MessageFactory.row(messages.callback("📦 Хранение", "PRICE:SERVICE:Хранение"))
                    ), true);
            case "PRICE_CAR" -> sendMessage(target,
                    "🚗 Тип автомобиля:",
                    List.of(
                            MessageFactory.row(messages.callback("🚙 Седан", "PRICE:CAR:Седан"), messages.callback("🚘 Кроссовер", "PRICE:CAR:Кроссовер")),
                            MessageFactory.row(messages.callback("🛻 Внедорожник", "PRICE:CAR:Внедорожник"), messages.callback("🚐 Минивен", "PRICE:CAR:Минивен")),
                            MessageFactory.row(messages.callback("🚚 Коммерческий", "PRICE:CAR:Коммерческий"))
                    ), true);
            case "PRICE_RADIUS" -> sendMessageKeepRows(target,
                    "🛞 Выберите диаметр колёс:",
                    buildDiameterRowsForCar(session.state().path("car_type").asText("")),
                    true);
            case "PRICE_COMM_WHEELS" -> sendMessage(target,
                    "🚚 Сколько колёс в коммерческом авто?",
                    List.of(
                            MessageFactory.row(messages.callback("4 колеса", "PRICE:WHEELS:4")),
                            MessageFactory.row(messages.callback("6 колёс", "PRICE:WHEELS:6"))
                    ),
                    true);
            case "PRICE_PROBLEM_TEXT" -> sendMessage(target,
                    "🛠️ Опишите проблему, чтобы мы рассчитали точную цену.\nНапример: где повреждение, как давно, есть ли фото.",
                    List.of(),
                    true);
            case "PRICE_EXTRA" -> sendPriceExtraStep(session, target);
            default -> sendMainMenu(target, true);
        }
    }

    private boolean handlePriceCallback(UserSession session, ChatTarget target, String payload, String callbackMessageId) throws SQLException, IOException {
        if (payload.startsWith("PRICE:SERVICE:")) {
            String service = payload.substring("PRICE:SERVICE:".length());
            session.state().put("service", service);
            session.state().remove("car_type");
            session.state().remove("wheel_count");
            session.state().remove("radius");
            clearPriceExtras(session.state());

            if ("Ремонт".equals(service) || "Правка".equals(service)) {
                UserSession next = nextStep(session, "PRICE_PROBLEM_TEXT");
                sessions.save(next);
                renderPriceStep(next, target);
                return true;
            }
            if ("Балансировка".equals(service)) {
                return sendBalancingPriceAndFinish(session, target);
            }
            if ("Хранение".equals(service)) {
                return sendStoragePriceAndFinish(session, target);
            }
            UserSession next = nextStep(session, "PRICE_CAR");
            sessions.save(next);
            renderPriceStep(next, target);
            return true;
        }
        if (payload.startsWith("PRICE:CAR:")) {
            String carType = payload.substring("PRICE:CAR:".length());
            session.state().put("car_type", carType);
            session.state().remove("wheel_count");
            session.state().remove("radius");
            clearPriceExtras(session.state());
            String nextStep = "Коммерческий".equals(carType) ? "PRICE_COMM_WHEELS" : "PRICE_RADIUS";
            UserSession next = nextStep(session, nextStep);
            sessions.save(next);
            renderPriceStep(next, target);
            return true;
        }
        if (payload.startsWith("PRICE:WHEELS:")) {
            String wheels = payload.substring("PRICE:WHEELS:".length());
            if (!"4".equals(wheels) && !"6".equals(wheels)) {
                sendMessage(target, "⚠️ Выберите 4 или 6 колёс кнопкой.", List.of(), true);
                return true;
            }
            session.state().put("wheel_count", wheels);
            clearPriceExtras(session.state());
            UserSession next = nextStep(session, "PRICE_EXTRA");
            sessions.save(next);
            renderPriceStep(next, target);
            return true;
        }
        if (payload.startsWith("PRICE:RAD:")) {
            return priceSelect(session, target, "radius", payload.substring("PRICE:RAD:".length()), "PRICE_EXTRA");
        }
        if (payload.equals("PRICE:EXTRA:DONE")) {
            return finishPriceFlow(session, target);
        }
        if (payload.startsWith("PRICE:EXTRA_TOGGLE:")) {
            String code = payload.substring("PRICE:EXTRA_TOGGLE:".length());
            if (EX_NONE.equals(code)) {
                clearPriceExtras(session.state());
                togglePriceExtra(session.state(), EX_NONE);
                sessions.save(session);
                return finishPriceFlow(session, target);
            }

            togglePriceExtra(session.state(), code);
            sessions.save(session);
            if (callbackMessageId != null && !callbackMessageId.isBlank()) {
                editPriceExtraMessage(callbackMessageId, session);
            } else {
                renderPriceStep(session, target);
            }
            return true;
        }
        if (payload.startsWith("PRICE:EXTRA:")) {
            String legacy = payload.substring("PRICE:EXTRA:".length());
            clearPriceExtras(session.state());
            if ("Нет".equals(legacy)) {
                togglePriceExtra(session.state(), EX_NONE);
                sessions.save(session);
                return finishPriceFlow(session, target);
            }
            if ("RunFlat".equalsIgnoreCase(legacy)) {
                togglePriceExtra(session.state(), EX_RUNFLAT);
            } else if ("Низкий профиль".equals(legacy)) {
                togglePriceExtra(session.state(), EX_LOW_PROFILE);
            }
            sessions.save(session);
            if (callbackMessageId != null && !callbackMessageId.isBlank()) {
                editPriceExtraMessage(callbackMessageId, session);
            } else {
                renderPriceStep(session, target);
            }
            return true;
        }

        return false;
    }

    private boolean handlePriceText(UserSession session, ChatTarget target, String text) throws SQLException, IOException {
        if ("PRICE_PROBLEM_TEXT".equals(session.step())) {
            session.state().put("problem", text);
            ObjectNode payload = session.state().deepCopy();
            payload.put("scenario", "price_problem");
            requests.saveRequest(session.userId(), session.chatId(), "price_problem", payload);
            notifyAdmins(
                    "🛠️ Запрос на расчёт цены\n"
                            + "Услуга: " + safe(session.state(), "service") + "\n"
                            + "Описание: " + safe(session.state(), "problem") + "\n"
                            + "Пользователь ID: " + session.userId()
            );
            sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            sendMessage(
                    target,
                    "✅ Спасибо! Передали запрос специалисту.\nСкоро с вами свяжемся с точной оценкой.",
                    List.of(MessageFactory.row(messages.callback("🏠 В меню", "NAV:MENU"))),
                    false
            );
            return true;
        }
        return false;
    }

    private boolean sendBalancingPriceAndFinish(UserSession session, ChatTarget target) throws SQLException, IOException {
        ObjectNode pricing = loadPricingConfig();
        int price = pricing.path("BALANCING_BASE").asInt(500);
        sendMessage(
                target,
                "💸 Балансировка: *" + price + " ₽*",
                List.of(
                        MessageFactory.row(messages.callback("✅ Записаться", "ACT:BOOK"), messages.callback("📍 Филиалы", "ACT:BRANCH")),
                        MessageFactory.row(messages.callback("👩‍💼 Оператор", "ACT:OPERATOR"))
                ),
                true
        );
        sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
        return true;
    }

    private boolean sendStoragePriceAndFinish(UserSession session, ChatTarget target) throws SQLException, IOException {
        ObjectNode pricing = loadPricingConfig();
        int perDay = pricing.path("STORAGE_PER_DAY").asInt(50);
        sendMessage(
                target,
                "💸 Хранение: *" + perDay + " ₽/день*",
                List.of(
                        MessageFactory.row(messages.callback("📦 Оформить хранение", "ACT:STORAGE")),
                        MessageFactory.row(messages.callback("👩‍💼 Оператор", "ACT:OPERATOR"))
                ),
                true
        );
        sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
        return true;
    }

    private boolean priceSelect(UserSession session, ChatTarget target, String key, String value, String nextStep) throws SQLException, IOException {
        session.state().put(key, value);
        UserSession next = nextStep(session, nextStep);
        sessions.save(next);
        renderPriceStep(next, target);
        return true;
    }

    // ---------------- STORAGE ----------------

    private void renderStorageStep(UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (session.step()) {
            case "ST_ACTION" -> sendMessageKeepRows(target,
                    "📦 Хранение шин\nВыберите действие:",
                    List.of(
                            MessageFactory.row(messages.callback("📘 Условия", "ST:ACT:CONDITIONS"), messages.callback("💸 Стоимость", "ST:ACT:PRICE")),
                            MessageFactory.row(messages.callback("📥 Сдать", "ST:ACT:DROP"), messages.callback("📤 Забрать", "ST:ACT:PICK"))
                    ), true);
            case "ST_PRICE_RADIUS" -> sendMessageKeepRows(target,
                    "🛞 Выберите радиус шин:",
                    List.of(
                            MessageFactory.row(
                                    messages.callback("R13-R15", "ST:PRAD:R13-R15"),
                                    messages.callback("R16-R17", "ST:PRAD:R16-R17"),
                                    messages.callback("R18+", "ST:PRAD:R18+")
                            )
                    ), true);
            case "ST_PRICE_TYPE" -> sendMessage(target,
                    "📦 Тип хранения:",
                    List.of(
                            MessageFactory.row(messages.callback("👟 Сезонное", "ST:PTYPE:Сезонное"), messages.callback("🧰 Полный комплект", "ST:PTYPE:Полный комплект"))
                    ), true);
            case "ST_DROP_CITY" -> {
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String city : branches.listCities()) {
                    rows.add(MessageFactory.row(messages.callback("🏙️ " + city, "ST:DCITY:" + city)));
                }
                sendMessage(target, "🏙️ Город сдачи шин:", rows, true);
            }
            case "ST_DROP_DISTRICT" -> {
                String city = session.state().path("drop_city").asText("");
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String district : branches.listDistricts(city)) {
                    rows.add(MessageFactory.row(messages.callback("📍 " + district, "ST:DDIST:" + district)));
                }
                sendMessage(target, "🗺️ Район сдачи шин:", rows, true);
            }
            case "ST_DROP_BRANCH" -> {
                String city = session.state().path("drop_city").asText("");
                String district = session.state().path("drop_district").asText("");
                List<Branch> list = branches.findByFilters(city.isBlank() ? null : city, district.isBlank() ? null : district, null);
                sendBranchPickerWithNumbers(target, "🏢 Выберите филиал для сдачи:", list, "ST:DBRANCH:");
            }
            case "ST_DROP_DATE" -> sendMessage(target,
                    "📅 Когда привезёте шины?",
                    List.of(
                            MessageFactory.row(messages.callback("🟢 Сегодня", "ST:DDATE:TODAY"), messages.callback("🟡 Завтра", "ST:DDATE:TOMORROW")),
                            MessageFactory.row(messages.callback("🗓️ Другая дата", "ST:DDATE:CUSTOM"))
                    ), true);
            case "ST_DROP_DATE_INPUT" -> sendMessage(target, "✍️ Введите дату *дд.мм.гггг*", List.of(), true);
            case "ST_DROP_NAME" -> sendMessage(target, "🙂 Введите имя:", List.of(), true);
            case "ST_DROP_PHONE" -> sendMessage(target, "📱 Введите телефон +7...", List.of(), true);
            case "ST_DROP_COMMENT_CHOICE" -> sendMessage(target,
                    "💬 Добавить комментарий?",
                    List.of(MessageFactory.row(messages.callback("🚫 Нет", "ST:DCOMMENT:NONE"), messages.callback("✍️ Да", "ST:DCOMMENT:ADD"))),
                    true);
            case "ST_DROP_COMMENT_TEXT" -> sendMessage(target, "✍️ Комментарий к сдаче:", List.of(), true);
            case "ST_DROP_CONFIRM" -> sendStorageDropConfirm(session, target);

            case "ST_PICK_FIO" -> sendMessage(target, "🙂 Введите ФИО:", List.of(), true);
            case "ST_PICK_PHONE" -> sendMessage(target, "📱 Введите телефон:", List.of(), true);
            case "ST_PICK_CONTRACT" -> sendMessage(target, "🔢 Номер авто или договора:", List.of(), true);
            case "ST_PICK_CITY" -> {
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String city : branches.listCities()) {
                    rows.add(MessageFactory.row(messages.callback("🏙️ " + city, "ST:PCITY:" + city)));
                }
                sendMessage(target, "🏙️ Город филиала:", rows, true);
            }
            case "ST_PICK_DISTRICT" -> {
                String city = session.state().path("pick_city").asText("");
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String district : branches.listDistricts(city)) {
                    rows.add(MessageFactory.row(messages.callback("📍 " + district, "ST:PDIST:" + district)));
                }
                sendMessage(target, "🗺️ Район филиала:", rows, true);
            }
            case "ST_PICK_BRANCH" -> {
                String city = session.state().path("pick_city").asText("");
                String district = session.state().path("pick_district").asText("");
                List<Branch> list = branches.findByFilters(city.isBlank() ? null : city, district.isBlank() ? null : district, null);
                sendBranchPickerWithNumbers(target, "🏢 Выберите филиал выдачи:", list, "ST:PBRANCH:");
            }
            case "ST_PICK_DATE" -> sendMessage(target,
                    "📅 Когда хотите забрать?",
                    List.of(
                            MessageFactory.row(messages.callback("🟢 Сегодня", "ST:PDATE:TODAY"), messages.callback("🟡 Завтра", "ST:PDATE:TOMORROW")),
                            MessageFactory.row(messages.callback("🗓️ Другая дата", "ST:PDATE:CUSTOM"))
                    ), true);
            case "ST_PICK_DATE_INPUT" -> sendMessage(target, "✍️ Введите дату *дд.мм.гггг*", List.of(), true);
            case "ST_PICK_CONFIRM" -> sendStoragePickConfirm(session, target);
            default -> sendMainMenu(target, true);
        }
    }

    private boolean handleStorageCallback(UserSession session, ChatTarget target, String payload) throws SQLException, IOException {
        if (payload.equals("ST:ACT:CONDITIONS")) {
            sendMessage(target,
                    "📘 Условия хранения:\n"
                            + "• чистые и сухие шины\n"
                            + "• маркировка комплекта\n"
                            + "• выдача по заявке",
                    List.of(MessageFactory.row(messages.callback("↩️ К выбору", "ST:BACK:ACTION"))),
                    true);
            return true;
        }
        if (payload.equals("ST:BACK:ACTION")) {
            UserSession next = nextStep(session, "ST_ACTION");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.equals("ST:ACT:PRICE")) {
            UserSession next = nextStep(session, "ST_PRICE_RADIUS");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.equals("ST:ACT:DROP")) {
            UserSession next = nextStep(session, "ST_DROP_CITY");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.equals("ST:ACT:PICK")) {
            UserSession next = nextStep(session, "ST_PICK_FIO");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }

        if (payload.startsWith("ST:PRAD:")) {
            session.state().put("price_radius", payload.substring("ST:PRAD:".length()));
            UserSession next = nextStep(session, "ST_PRICE_TYPE");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.startsWith("ST:PTYPE:")) {
            String type = payload.substring("ST:PTYPE:".length());
            String radius = safe(session.state(), "price_radius");
            int value = estimateStoragePrice(radius, type);
            sendMessage(target,
                    "💸 Хранение " + type.toLowerCase(Locale.ROOT) + ": *от " + value + " ₽*",
                    List.of(
                            MessageFactory.row(messages.callback("📥 Сдать шины", "ST:ACT:DROP"), messages.callback("📤 Забрать шины", "ST:ACT:PICK"))
                    ),
                    true);
            return true;
        }

        if (payload.startsWith("ST:DCITY:")) {
            session.state().put("drop_city", payload.substring("ST:DCITY:".length()));
            session.state().remove("drop_district");
            UserSession next = nextStep(session, "ST_DROP_DISTRICT");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.startsWith("ST:DDIST:")) {
            session.state().put("drop_district", payload.substring("ST:DDIST:".length()));
            UserSession next = nextStep(session, "ST_DROP_BRANCH");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.startsWith("ST:DBRANCH:")) {
            session.state().put("drop_branch", payload.substring("ST:DBRANCH:".length()));
            UserSession next = nextStep(session, "ST_DROP_DATE");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.startsWith("ST:DDATE:")) {
            String value = payload.substring("ST:DDATE:".length());
            if ("CUSTOM".equals(value)) {
                UserSession next = nextStep(session, "ST_DROP_DATE_INPUT");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            session.state().put("drop_date", pickDateValue(value));
            UserSession next = nextStep(session, "ST_DROP_NAME");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.equals("ST:DCOMMENT:NONE")) {
            session.state().put("drop_comment", "-");
            UserSession next = nextStep(session, "ST_DROP_CONFIRM");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.equals("ST:DCOMMENT:ADD")) {
            UserSession next = nextStep(session, "ST_DROP_COMMENT_TEXT");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.equals("ST:DROP:CONFIRM")) {
            ObjectNode payloadJson = session.state().deepCopy();
            payloadJson.put("scenario", "storage_drop");
            requests.saveRequest(session.userId(), session.chatId(), "storage_drop", payloadJson);
            notifyAdmins("📥 Новая заявка на сдачу шин\n" + buildStorageDropAdminSummary(session.state()));
            sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            sendMessage(target, "✅ Заявка на сдачу шин зарегистрирована.", List.of(MessageFactory.row(messages.callback("🏠 В меню", "NAV:MENU"))), false);
            return true;
        }

        if (payload.startsWith("ST:PCITY:")) {
            session.state().put("pick_city", payload.substring("ST:PCITY:".length()));
            session.state().remove("pick_district");
            UserSession next = nextStep(session, "ST_PICK_DISTRICT");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.startsWith("ST:PDIST:")) {
            session.state().put("pick_district", payload.substring("ST:PDIST:".length()));
            UserSession next = nextStep(session, "ST_PICK_BRANCH");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.startsWith("ST:PBRANCH:")) {
            session.state().put("pick_branch", payload.substring("ST:PBRANCH:".length()));
            UserSession next = nextStep(session, "ST_PICK_DATE");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.startsWith("ST:PDATE:")) {
            String value = payload.substring("ST:PDATE:".length());
            if ("CUSTOM".equals(value)) {
                UserSession next = nextStep(session, "ST_PICK_DATE_INPUT");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            session.state().put("pick_date", pickDateValue(value));
            UserSession next = nextStep(session, "ST_PICK_CONFIRM");
            sessions.save(next);
            renderStorageStep(next, target);
            return true;
        }
        if (payload.equals("ST:PICK:CONFIRM")) {
            ObjectNode payloadJson = session.state().deepCopy();
            payloadJson.put("scenario", "storage_pick");
            requests.saveRequest(session.userId(), session.chatId(), "storage_pick", payloadJson);
            notifyAdmins("📤 Новая заявка на выдачу шин\n" + buildStoragePickAdminSummary(session.state()));
            sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
            sendMessage(target, "✅ Заявка на выдачу шин зарегистрирована.", List.of(MessageFactory.row(messages.callback("🏠 В меню", "NAV:MENU"))), false);
            return true;
        }

        return false;
    }

    private boolean handleStorageText(UserSession session, ChatTarget target, String text) throws SQLException, IOException {
        switch (session.step()) {
            case "ST_DROP_DATE_INPUT" -> {
                if (!isValidDate(text)) {
                    sendMessage(target, "📅 Нужен формат *дд.мм.гггг*", List.of(), true);
                    return true;
                }
                session.state().put("drop_date", text);
                UserSession next = nextStep(session, "ST_DROP_NAME");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            case "ST_DROP_NAME" -> {
                session.state().put("drop_name", text);
                UserSession next = nextStep(session, "ST_DROP_PHONE");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            case "ST_DROP_PHONE" -> {
                if (!isValidPhone(text)) {
                    sendMessage(target, "📱 Проверьте номер +7...", List.of(), true);
                    return true;
                }
                session.state().put("drop_phone", text);
                UserSession next = nextStep(session, "ST_DROP_COMMENT_CHOICE");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            case "ST_DROP_COMMENT_TEXT" -> {
                session.state().put("drop_comment", text);
                UserSession next = nextStep(session, "ST_DROP_CONFIRM");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            case "ST_PICK_FIO" -> {
                session.state().put("pick_fio", text);
                UserSession next = nextStep(session, "ST_PICK_PHONE");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            case "ST_PICK_PHONE" -> {
                if (!isValidPhone(text)) {
                    sendMessage(target, "📱 Проверьте номер +7...", List.of(), true);
                    return true;
                }
                session.state().put("pick_phone", text);
                UserSession next = nextStep(session, "ST_PICK_CONTRACT");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            case "ST_PICK_CONTRACT" -> {
                session.state().put("pick_contract", text);
                UserSession next = nextStep(session, "ST_PICK_CITY");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            case "ST_PICK_DATE_INPUT" -> {
                if (!isValidDate(text)) {
                    sendMessage(target, "📅 Нужен формат *дд.мм.гггг*", List.of(), true);
                    return true;
                }
                session.state().put("pick_date", text);
                UserSession next = nextStep(session, "ST_PICK_CONFIRM");
                sessions.save(next);
                renderStorageStep(next, target);
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    private void sendStorageDropConfirm(UserSession session, ChatTarget target) throws IOException {
        String msg = "📋 Проверьте заявку на сдачу:\n"
                + "🏙️ " + safe(session.state(), "drop_city") + "\n"
                + "📅 " + safe(session.state(), "drop_date") + "\n"
                + "👤 " + safe(session.state(), "drop_name") + " • 📱 " + safe(session.state(), "drop_phone");

        sendMessage(target,
                msg,
                List.of(MessageFactory.row(messages.callback("✅ Подтвердить", "ST:DROP:CONFIRM"))),
                true);
    }

    private void sendStoragePickConfirm(UserSession session, ChatTarget target) throws IOException {
        String msg = "📋 Проверьте заявку на выдачу:\n"
                + "👤 " + safe(session.state(), "pick_fio") + "\n"
                + "📱 " + safe(session.state(), "pick_phone") + "\n"
                + "🔢 " + safe(session.state(), "pick_contract") + "\n"
                + "📅 " + safe(session.state(), "pick_date");

        sendMessage(target,
                msg,
                List.of(MessageFactory.row(messages.callback("✅ Подтвердить", "ST:PICK:CONFIRM"))),
                true);
    }

    // ---------------- FEEDBACK ----------------

    private void renderFeedbackStep(UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (session.step()) {
            case "FB_TYPE" -> sendMessage(target,
                    "📝 Выберите тип обращения:",
                    List.of(
                            MessageFactory.row(messages.callback("🌟 Отзыв", "FB:TYPE:Отзыв"), messages.callback("⚠️ Жалоба по качеству", "FB:TYPE:Жалоба по качеству")),
                            MessageFactory.row(messages.callback("🙍 Жалоба на персонал", "FB:TYPE:Жалоба на персонал"), messages.callback("💡 Предложение", "FB:TYPE:Предложение"))
                    ), true);
            case "FB_CITY" -> {
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String city : branches.listCities()) {
                    rows.add(MessageFactory.row(messages.callback("🏙️ " + city, "FB:CITY:" + city)));
                }
                sendMessage(target, "🏙️ Город филиала:", rows, true);
            }
            case "FB_DISTRICT" -> {
                String city = session.state().path("city").asText("");
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (String district : branches.listDistricts(city)) {
                    rows.add(MessageFactory.row(messages.callback("📍 " + district, "FB:DIST:" + district)));
                }
                sendMessage(target, "🗺️ Район филиала:", rows, true);
            }
            case "FB_BRANCH" -> {
                String city = session.state().path("city").asText("");
                String district = session.state().path("district").asText("");
                List<Branch> list = branches.findByFilters(city.isBlank() ? null : city, district.isBlank() ? null : district, null);
                sendBranchPickerWithNumbers(target, "🏢 Выберите филиал:", list, "FB:BRANCH:");
            }
            case "FB_DATE" -> sendMessage(target,
                    "📅 Когда произошла ситуация?",
                    List.of(
                            MessageFactory.row(messages.callback("🟢 Сегодня", "FB:DATE:TODAY"), messages.callback("🟡 Вчера", "FB:DATE:YESTERDAY")),
                            MessageFactory.row(messages.callback("🗓️ Другая дата", "FB:DATE:CUSTOM"))
                    ), true);
            case "FB_DATE_INPUT" -> sendMessage(target, "✍️ Введите дату *дд.мм.гггг*", List.of(), true);
            case "FB_DESC" -> sendMessage(target, "✍️ Опишите ситуацию в 2-3 предложениях:", List.of(), true);
            case "FB_CONTACT" -> sendMessage(target, "📱 Оставьте контакт для ответа:", List.of(), true);
            default -> sendMainMenu(target, true);
        }
    }

    private boolean handleFeedbackCallback(UserSession session, ChatTarget target, String payload) throws SQLException, IOException {
        if (payload.startsWith("FB:TYPE:")) {
            session.state().put("type", payload.substring("FB:TYPE:".length()));
            UserSession next = nextStep(session, "FB_CITY");
            sessions.save(next);
            renderFeedbackStep(next, target);
            return true;
        }
        if (payload.startsWith("FB:CITY:")) {
            session.state().put("city", payload.substring("FB:CITY:".length()));
            session.state().remove("district");
            UserSession next = nextStep(session, "FB_DISTRICT");
            sessions.save(next);
            renderFeedbackStep(next, target);
            return true;
        }
        if (payload.startsWith("FB:DIST:")) {
            session.state().put("district", payload.substring("FB:DIST:".length()));
            UserSession next = nextStep(session, "FB_BRANCH");
            sessions.save(next);
            renderFeedbackStep(next, target);
            return true;
        }
        if (payload.startsWith("FB:BRANCH:")) {
            session.state().put("branch_id", payload.substring("FB:BRANCH:".length()));
            UserSession next = nextStep(session, "FB_DATE");
            sessions.save(next);
            renderFeedbackStep(next, target);
            return true;
        }
        if (payload.startsWith("FB:DATE:")) {
            String value = payload.substring("FB:DATE:".length());
            if ("CUSTOM".equals(value)) {
                UserSession next = nextStep(session, "FB_DATE_INPUT");
                sessions.save(next);
                renderFeedbackStep(next, target);
                return true;
            }
            if ("TODAY".equals(value)) {
                session.state().put("date", LocalDate.now().format(DATE_FMT));
            } else {
                session.state().put("date", LocalDate.now().minusDays(1).format(DATE_FMT));
            }
            UserSession next = nextStep(session, "FB_DESC");
            sessions.save(next);
            renderFeedbackStep(next, target);
            return true;
        }
        return false;
    }

    private boolean handleFeedbackText(UserSession session, ChatTarget target, String text) throws SQLException, IOException {
        switch (session.step()) {
            case "FB_DATE_INPUT" -> {
                if (!isValidDate(text)) {
                    sendMessage(target, "📅 Нужен формат *дд.мм.гггг*", List.of(), true);
                    return true;
                }
                session.state().put("date", text);
                UserSession next = nextStep(session, "FB_DESC");
                sessions.save(next);
                renderFeedbackStep(next, target);
                return true;
            }
            case "FB_DESC" -> {
                session.state().put("description", text);
                UserSession next = nextStep(session, "FB_CONTACT");
                sessions.save(next);
                renderFeedbackStep(next, target);
                return true;
            }
            case "FB_CONTACT" -> {
                session.state().put("contact", text);
                ObjectNode payload = session.state().deepCopy();
                payload.put("scenario", "feedback");
                requests.saveRequest(session.userId(), session.chatId(), "feedback", payload);
                notifyAdmins("📝 Новое обращение (отзыв/жалоба)\n" + buildFeedbackAdminSummary(session.state()));
                sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
                sendMessage(target,
                        "✅ Обращение зарегистрировано.\nСпасибо, что помогаете нам стать лучше.",
                        List.of(MessageFactory.row(messages.callback("🏠 В меню", "NAV:MENU"))),
                        false);
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    // ---------------- OPERATOR ----------------

    private void renderOperatorStep(UserSession session, ChatTarget target) throws IOException, SQLException {
        sendOperatorContact(target);
        sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
    }

    private boolean handleOperatorCallback(UserSession session, ChatTarget target, String payload) throws SQLException, IOException {
        sendOperatorContact(target);
        sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
        return true;
    }

    private boolean handleOperatorText(UserSession session, ChatTarget target, String text) throws SQLException, IOException {
        sendOperatorContact(target);
        sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
        return true;
    }

    // ---------------- ADMIN ----------------

    private void openAdminPanel(long userId, UserSession session, ChatTarget target) throws SQLException, IOException {
        if (!admins.hasAnyAdmin()) {
            admins.addAdmin(userId);
            sendAdminMessage(target,
                    "🔐 Вы зарегистрированы как первый администратор.\nДобро пожаловать в админ-панель.",
                    List.of(),
                    true);
        }

        if (!admins.isAdmin(userId)) {
            sendAdminMessage(target,
                    "⛔ Доступ запрещён.\nАдмин-панель доступна только администраторам.",
                    List.of(),
                    true);
            return;
        }

        UserSession next = new UserSession(session.userId(), session.chatId(), SC_ADMIN, "AD_MENU", mapper.createObjectNode(), mapper.createArrayNode());
        sessions.save(next);
        renderAdminStep(next, target);
    }

    private void renderAdminStep(UserSession session, ChatTarget target) throws SQLException, IOException {
        switch (session.step()) {
            case "AD_MENU" -> {
                String phone = settings.getOperatorPhone();
                sendAdminMessage(target,
                        "🛠️ *Админ-панель*\n"
                                + "📞 Оператор: " + phone + "\n"
                                + "Выберите действие:",
                        List.of(
                                MessageFactory.row(messages.callback("📞 Телефон оператора", "ADM:PHONE:EDIT")),
                                MessageFactory.row(messages.callback("👥 Список админов", "ADM:ADMINS:LIST")),
                                MessageFactory.row(messages.callback("➕ Добавить админа", "ADM:ADMINS:ADD")),
                                MessageFactory.row(messages.callback("➖ Удалить админа", "ADM:ADMINS:REMOVE")),
                                MessageFactory.row(messages.callback("👥 Все пользователи", "ADM:USERS:PAGE:1")),
                                MessageFactory.row(messages.callback("🏢 Добавить филиал", "ADM:BRANCH:ADD")),
                                MessageFactory.row(messages.callback("🗑️ Удалить филиал", "ADM:BRANCH:REMOVE")),
                                MessageFactory.row(messages.callback("💸 Изменить цены", "ADM:PRICE:EDIT")),
                                MessageFactory.row(messages.callback("🏠 Пользовательское меню", "ACT:USER_MENU"))
                        ),
                        true);
            }
            case "AD_PHONE_INPUT" -> sendAdminMessage(target,
                    "📞 Введите новый номер оператора\nв формате +79005553535",
                    List.of(),
                    true);
            case "AD_ADMIN_ADD_INPUT" -> sendAdminMessage(target,
                    "👤 Введите `user_id` нового администратора:",
                    List.of(),
                    true);
            case "AD_ADMIN_REMOVE_PICK" -> {
                List<Long> ids = admins.listAdmins();
                List<List<ObjectNode>> rows = new ArrayList<>();
                for (Long id : ids) {
                    rows.add(MessageFactory.row(messages.callback("➖ Удалить " + id, "ADM:ADMIN:DEL:" + id)));
                }
                if (rows.isEmpty()) {
                    rows = List.of(MessageFactory.row(messages.callback("🔄 Обновить", "ADM:REFRESH")));
                }
                sendAdminMessage(target, "👥 Выберите администратора для удаления:", rows, true);
            }
            case "AD_BRANCH_ADD_INPUT" -> sendAdminMessage(target,
                    "🏢 Введите филиал в одну строку:\n"
                            + "`Город;Район;Адрес;Телефон;График;24/7(да/нет);Широта(опц);Долгота(опц)`",
                    List.of(),
                    true);
            case "AD_BRANCH_REMOVE_PICK" -> {
                List<Branch> list = branches.findAll();
                sendAdminBranchRemovePicker(target, list);
            }
            case "AD_PRICE_CATEGORY" -> sendAdminMessage(target,
                    "💸 Изменение цен\nВыберите раздел:",
                    List.of(
                            MessageFactory.row(messages.callback("🚙 Седан", "ADM:PRICE:CAT:" + PRICE_CAT_SEDAN)),
                            MessageFactory.row(messages.callback("🚘 Кроссовер / Внедорожник / Минивен", "ADM:PRICE:CAT:" + PRICE_CAT_SUV)),
                            MessageFactory.row(messages.callback("🚚 Коммерческий", "ADM:PRICE:CAT:" + PRICE_CAT_COMM)),
                            MessageFactory.row(messages.callback("⚙️ Прочие услуги", "ADM:PRICE:CAT:" + PRICE_CAT_SERVICE)),
                            MessageFactory.row(messages.callback("🧩 Доп. услуги", "ADM:PRICE:CAT:" + PRICE_CAT_EXTRA)),
                            MessageFactory.row(messages.callback("⬅️ Назад", "ADM:REFRESH"))
                    ),
                    true);
            case "AD_PRICE_ITEM_PICK" -> {
                ObjectNode pricing = loadPricingConfig();
                String category = session.state().path("price_category").asText("");
                sendAdminMessage(target,
                        "💸 Выберите позицию:",
                        buildPriceItemRows(category, pricing),
                        true);
            }
            case "AD_PRICE_VALUE_INPUT" -> {
                ObjectNode pricing = loadPricingConfig();
                String key = session.state().path("price_key").asText("");
                int oldPrice = pricing.path(key).asInt(0);
                String label = priceItemLabel(key);
                sendAdminMessage(target,
                        "🧾 Позиция: *" + label + "*\n"
                                + "Текущая цена: *" + oldPrice + " ₽*\n"
                                + "Введите новую цену (только число):",
                        List.of(
                                MessageFactory.row(messages.callback("⬅️ Назад", "ADM:PRICE:BACK:ITEMS"))
                        ),
                        true);
            }
            default -> renderAdminStep(new UserSession(session.userId(), session.chatId(), SC_ADMIN, "AD_MENU", session.state(), session.history()), target);
        }
    }

    private boolean handleAdminCallback(UserSession session, ChatTarget target, String payload) throws SQLException, IOException {
        if (!admins.isAdmin(session.userId())) {
            sendAdminMessage(target, "⛔ Доступ запрещён.", List.of(), true);
            return true;
        }

        if ("ADM:PHONE:EDIT".equals(payload)) {
            UserSession next = nextStep(session, "AD_PHONE_INPUT");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if ("ADM:ADMINS:LIST".equals(payload)) {
            List<Long> ids = admins.listAdmins();
            StringBuilder sb = new StringBuilder("👥 Текущие администраторы:\n");
            if (ids.isEmpty()) {
                sb.append("— список пуст");
            } else {
                for (Long id : ids) {
                    sb.append("• ").append(id).append('\n');
                }
            }
            sendAdminMessage(target, sb.toString().trim(), List.of(MessageFactory.row(messages.callback("↩️ Назад в панель", "ADM:REFRESH"))), true);
            return true;
        }
        if ("ADM:ADMINS:ADD".equals(payload)) {
            UserSession next = nextStep(session, "AD_ADMIN_ADD_INPUT");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if ("ADM:ADMINS:REMOVE".equals(payload)) {
            UserSession next = nextStep(session, "AD_ADMIN_REMOVE_PICK");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if ("ADM:BRANCH:ADD".equals(payload)) {
            UserSession next = nextStep(session, "AD_BRANCH_ADD_INPUT");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if ("ADM:BRANCH:REMOVE".equals(payload)) {
            UserSession next = nextStep(session, "AD_BRANCH_REMOVE_PICK");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if ("ADM:PRICE:EDIT".equals(payload)) {
            session.state().remove("price_category");
            session.state().remove("price_key");
            UserSession next = nextStep(session, "AD_PRICE_CATEGORY");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if (payload.startsWith("ADM:PRICE:CAT:")) {
            String category = payload.substring("ADM:PRICE:CAT:".length());
            if (!Set.of(PRICE_CAT_SEDAN, PRICE_CAT_SUV, PRICE_CAT_COMM, PRICE_CAT_SERVICE, PRICE_CAT_EXTRA).contains(category)) {
                sendAdminMessage(target, "⚠️ Неизвестный раздел цен.", List.of(), true);
                return true;
            }
            session.state().put("price_category", category);
            session.state().remove("price_key");
            UserSession next = nextStep(session, "AD_PRICE_ITEM_PICK");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if (payload.startsWith("ADM:PRICE:ITEM:")) {
            String key = payload.substring("ADM:PRICE:ITEM:".length());
            if (!defaultPricingKeys().contains(key)) {
                sendAdminMessage(target, "⚠️ Неизвестная позиция цены.", List.of(), true);
                return true;
            }
            session.state().put("price_key", key);
            UserSession next = nextStep(session, "AD_PRICE_VALUE_INPUT");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if ("ADM:PRICE:BACK:CATEGORY".equals(payload)) {
            session.state().remove("price_key");
            UserSession next = nextStep(session, "AD_PRICE_CATEGORY");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if ("ADM:PRICE:BACK:ITEMS".equals(payload)) {
            session.state().remove("price_key");
            UserSession next = nextStep(session, "AD_PRICE_ITEM_PICK");
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if ("ADM:REFRESH".equals(payload)) {
            UserSession next = new UserSession(session.userId(), session.chatId(), SC_ADMIN, "AD_MENU", mapper.createObjectNode(), mapper.createArrayNode());
            sessions.save(next);
            renderAdminStep(next, target);
            return true;
        }
        if (payload.startsWith("ADM:USERS:PAGE:")) {
            Integer page = tryParseInt(payload.substring("ADM:USERS:PAGE:".length()));
            if (page == null || page < 1) {
                page = 1;
            }
            renderUsersPage(target, page);
            return true;
        }
        if (payload.startsWith("ADM:ADMIN:DEL:")) {
            long adminId = Long.parseLong(payload.substring("ADM:ADMIN:DEL:".length()));
            List<Long> ids = admins.listAdmins();
            if (ids.size() <= 1) {
                sendAdminMessage(target, "⚠️ Нельзя удалить последнего администратора.", List.of(), true);
                return true;
            }
            if (adminId == session.userId()) {
                sendAdminMessage(target, "⚠️ Нельзя удалить самого себя из панели.", List.of(), true);
                return true;
            }
            boolean deleted = admins.removeAdmin(adminId);
            sendAdminMessage(target,
                    deleted ? "✅ Администратор удалён: " + adminId : "ℹ️ Администратор не найден.",
                    List.of(MessageFactory.row(messages.callback("↩️ К списку", "ADM:ADMINS:REMOVE"))),
                    true);
            return true;
        }
        if (payload.startsWith("ADM:BRANCH:DEL:")) {
            long branchId = Long.parseLong(payload.substring("ADM:BRANCH:DEL:".length()));
            boolean deleted = branches.deleteBranch(branchId);
            sendAdminMessage(target,
                    deleted ? "✅ Филиал удалён: #" + branchId : "ℹ️ Филиал не найден.",
                    List.of(MessageFactory.row(messages.callback("↩️ К списку", "ADM:BRANCH:REMOVE"))),
                    true);
            return true;
        }

        return false;
    }

    private boolean handleAdminText(UserSession session, ChatTarget target, String text) throws SQLException, IOException {
        if (!admins.isAdmin(session.userId())) {
            sendAdminMessage(target, "⛔ Доступ запрещён.", List.of(), true);
            return true;
        }

        switch (session.step()) {
            case "AD_PHONE_INPUT" -> {
                if (!isValidPhone(text)) {
                    sendAdminMessage(target, "📱 Неверный формат. Пример: +79005553535", List.of(), true);
                    return true;
                }
                settings.setOperatorPhone(text.trim());
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_ADMIN, "AD_MENU", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                sendAdminMessage(target, "✅ Телефон оператора обновлён: " + text.trim(), List.of(), true);
                renderAdminStep(next, target);
                return true;
            }
            case "AD_ADMIN_ADD_INPUT" -> {
                Long id = tryParseLong(text.trim());
                if (id == null || id <= 0) {
                    sendAdminMessage(target, "⚠️ Нужен числовой `user_id`.", List.of(), true);
                    return true;
                }
                admins.addAdmin(id);
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_ADMIN, "AD_MENU", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                sendAdminMessage(target, "✅ Админ добавлен: " + id, List.of(), true);
                renderAdminStep(next, target);
                return true;
            }
            case "AD_BRANCH_ADD_INPUT" -> {
                BranchDraft draft = parseBranchDraft(text);
                if (draft == null) {
                    sendAdminMessage(target,
                            "⚠️ Не удалось разобрать филиал.\n"
                                    + "Формат: `Город;Район;Адрес;Телефон;График;24/7(да/нет);Широта;Долгота`",
                            List.of(),
                            true);
                    return true;
                }
                long id = branches.addBranch(
                        draft.city(),
                        draft.district(),
                        draft.address(),
                        draft.phone(),
                        draft.schedule(),
                        draft.is24x7(),
                        draft.latitude(),
                        draft.longitude()
                );
                UserSession next = new UserSession(session.userId(), session.chatId(), SC_ADMIN, "AD_MENU", mapper.createObjectNode(), mapper.createArrayNode());
                sessions.save(next);
                sendAdminMessage(target, "✅ Филиал добавлен: #" + id, List.of(), true);
                renderAdminStep(next, target);
                return true;
            }
            case "AD_PRICE_VALUE_INPUT" -> {
                Integer newPrice = tryParseInt(text.trim());
                if (newPrice == null || newPrice < 0) {
                    sendAdminMessage(target, "⚠️ Нужна цена числом (например: 2420).", List.of(), true);
                    return true;
                }
                String key = session.state().path("price_key").asText("");
                if (!defaultPricingKeys().contains(key)) {
                    sendAdminMessage(target, "⚠️ Не выбрана позиция цены.", List.of(), true);
                    UserSession next = nextStep(session, "AD_PRICE_CATEGORY");
                    sessions.save(next);
                    renderAdminStep(next, target);
                    return true;
                }
                ObjectNode config = loadPricingConfig();
                config.put(key, newPrice);
                settings.setValue(KEY_PRICING_CONFIG, config.toString());
                session.state().remove("price_key");
                UserSession next = nextStep(session, "AD_PRICE_ITEM_PICK");
                sessions.save(next);
                sendAdminMessage(target, "✅ Цена обновлена: " + priceItemLabel(key) + " = " + newPrice + " ₽", List.of(), true);
                renderAdminStep(next, target);
                return true;
            }
            default -> {
                return false;
            }
        }
    }

    // ---------------- HELPERS ----------------

    private UserSession ensureChat(UserSession session, Long chatId) {
        if (chatId == null || session.chatId() != null) {
            return session;
        }
        return new UserSession(session.userId(), chatId, session.scenario(), session.step(), session.state(), session.history());
    }

    private UserSession nextStep(UserSession session, String nextStep) {
        ArrayNode history = session.history().deepCopy();
        if (session.step() != null) {
            history.add(session.step());
        }
        return new UserSession(session.userId(), session.chatId(), session.scenario(), nextStep, session.state(), history);
    }

    private void sendMessage(ChatTarget target, String text, List<List<ObjectNode>> rows, boolean withNavigation) throws IOException {
        api.sendMessage(target, messages.message(text, rows, withNavigation));
    }

    private void sendMessageKeepRows(ChatTarget target, String text, List<List<ObjectNode>> rows, boolean withNavigation) throws IOException {
        api.sendMessage(target, messages.messageKeepRows(text, rows, withNavigation));
    }

    private void sendAdminMessage(ChatTarget target, String text, List<List<ObjectNode>> rows, boolean withNavigation) throws IOException {
        api.sendMessage(target, messages.adminMessage(text, rows, withNavigation));
    }

    private void sendAdminMessageKeepRows(ChatTarget target, String text, List<List<ObjectNode>> rows, boolean withNavigation) throws IOException {
        api.sendMessage(target, messages.adminMessageKeepRows(text, rows, withNavigation));
    }

    private ChatTarget targetFrom(long userId, Long chatId) {
        if (chatId != null && chatId > 0) {
            return new ChatTarget(null, chatId);
        }
        return new ChatTarget(userId, null);
    }

    private void upsertUserFromNode(JsonNode user) {
        if (user == null || user.isMissingNode() || user.isNull()) {
            return;
        }
        long userId = user.path("user_id").asLong(0L);
        if (userId == 0L) {
            return;
        }
        try {
            users.upsertUser(
                    userId,
                    user.path("name").asText(null),
                    user.path("first_name").asText(null),
                    user.path("last_name").asText(null),
                    user.path("username").asText(null),
                    user.path("is_bot").asBoolean(false),
                    user.path("last_activity_time").isMissingNode() ? null : user.path("last_activity_time").asLong()
            );
        } catch (Exception e) {
            log.warn("Не удалось обновить пользователя {}: {}", userId, e.getMessage());
        }
    }

    private void sendOperatorContact(ChatTarget target) throws IOException, SQLException {
        String phone = settings.getOperatorPhone();
        sendMessage(
                target,
                "👩‍💼 Связь с оператором:\n📞 " + phone,
                List.of(MessageFactory.row(messages.callback("🏠 Главное меню", "NAV:MENU"))),
                false
        );
    }

    private void notifyAdmins(String text) throws SQLException {
        List<Long> adminIds = admins.listAdmins();
        for (Long adminId : adminIds) {
            boolean delivered = false;
            Long chatId = sessions.findChatIdByUserId(adminId);
            if (chatId != null && chatId > 0) {
                try {
                    api.sendMessage(new ChatTarget(null, chatId), messages.adminMessage(text, List.of(), false));
                    delivered = true;
                } catch (Exception e) {
                    log.warn("Не удалось отправить уведомление админу {} в чат {}: {}", adminId, chatId, e.getMessage());
                }
            }
            try {
                if (!delivered) {
                    api.sendMessage(new ChatTarget(adminId, null), messages.adminMessage(text, List.of(), false));
                }
            } catch (Exception e) {
                log.warn("Не удалось отправить уведомление админу {}: {}", adminId, e.getMessage());
            }
        }
    }

    private void sendBranchPickerWithNumbers(ChatTarget target, String title, List<Branch> list, String payloadPrefix) throws IOException {
        if (list.isEmpty()) {
            sendMessage(target, "😔 Филиалы не найдены. Попробуйте другой фильтр.", List.of(), true);
            return;
        }
        StringBuilder text = new StringBuilder(title).append('\n');
        List<List<ObjectNode>> rows = new ArrayList<>();
        List<ObjectNode> currentRow = new ArrayList<>();

        int i = 1;
        for (Branch branch : list) {
            text.append(i).append(". ")
                    .append(branch.city()).append(", ")
                    .append(branch.district()).append(", ")
                    .append(branch.address())
                    .append('\n');

            currentRow.add(messages.callback(String.valueOf(i), payloadPrefix + branch.id()));
            if (currentRow.size() == BRANCH_NUMBER_ROW_MAX) {
                rows.add(List.copyOf(currentRow));
                currentRow.clear();
            }
            i++;
        }
        if (!currentRow.isEmpty()) {
            rows.add(List.copyOf(currentRow));
        }

        sendMessageKeepRows(target, text.toString().trim(), rows, true);
    }

    private void sendAdminBranchRemovePicker(ChatTarget target, List<Branch> list) throws IOException {
        if (list.isEmpty()) {
            sendAdminMessage(target,
                    "😔 Нет филиалов для удаления.",
                    List.of(MessageFactory.row(messages.callback("↩️ Назад в панель", "ADM:REFRESH"))),
                    true);
            return;
        }

        StringBuilder text = new StringBuilder("🏢 Выберите филиал для удаления:\n");
        List<List<ObjectNode>> rows = new ArrayList<>();
        List<ObjectNode> currentRow = new ArrayList<>();

        int i = 1;
        for (Branch branch : list) {
            text.append(i).append(". ")
                    .append(branch.city()).append(", ")
                    .append(branch.district()).append(", ")
                    .append(branch.address())
                    .append('\n');

            currentRow.add(messages.callback(String.valueOf(i), "ADM:BRANCH:DEL:" + branch.id()));
            if (currentRow.size() == BRANCH_NUMBER_ROW_MAX) {
                rows.add(List.copyOf(currentRow));
                currentRow.clear();
            }
            i++;
        }
        if (!currentRow.isEmpty()) {
            rows.add(List.copyOf(currentRow));
        }
        rows.add(MessageFactory.row(messages.callback("↩️ Назад в панель", "ADM:REFRESH")));

        sendAdminMessageKeepRows(target, text.toString().trim(), rows, true);
    }

    private void renderUsersPage(ChatTarget target, int page) throws SQLException, IOException {
        int totalUsers = users.countHumanUsers();
        int totalPages = Math.max(1, (int) Math.ceil(totalUsers / (double) USERS_PAGE_SIZE));
        int safePage = Math.max(1, Math.min(page, totalPages));

        List<BotUser> list = users.listHumanUsers(safePage, USERS_PAGE_SIZE);
        StringBuilder text = new StringBuilder("👥 Пользователи\n");
        text.append("Страница ").append(safePage).append(" из ").append(totalPages).append("\n\n");

        if (list.isEmpty()) {
            text.append("Список пуст.");
        } else {
            int idx = (safePage - 1) * USERS_PAGE_SIZE + 1;
            for (BotUser user : list) {
                text.append(idx++).append(". ")
                        .append(user.name() == null || user.name().isBlank() ? "Без имени" : user.name())
                        .append(" (id: ").append(user.userId()).append(")");
                if (user.username() != null && !user.username().isBlank()) {
                    text.append(" @").append(user.username());
                }
                text.append('\n');
            }
        }

        List<List<ObjectNode>> rows = new ArrayList<>();
        if (safePage > 1) {
            rows.add(MessageFactory.row(messages.callback("◀️ Предыдущая", "ADM:USERS:PAGE:" + (safePage - 1))));
        }
        if (safePage < totalPages) {
            rows.add(MessageFactory.row(messages.callback("▶️ Следующая", "ADM:USERS:PAGE:" + (safePage + 1))));
        }
        rows.add(MessageFactory.row(messages.callback("↩️ Назад в панель", "ADM:REFRESH")));

        sendAdminMessage(target, text.toString().trim(), rows, true);
    }

    private boolean isStartCommand(String text) {
        String t = normalize(text);
        return "/start".equalsIgnoreCase(text)
                || "start".equals(t)
                || "начать".equals(t)
                || "начать работу".equals(t);
    }

    private boolean isAdminCommand(String text) {
        String t = normalize(text);
        return "/admin".equalsIgnoreCase(text)
                || "админ".equals(t)
                || "админ панель".equals(t)
                || "админ-панель".equals(t);
    }

    private String classifyIntent(String text) {
        String t = normalize(text);
        if (containsAny(t, "запис", "переобув", "переобуть", "смена шин", "шиномонтаж")) {
            return SC_BOOKING;
        }
        if (containsAny(t, "адрес", "ближай", "филиал", "точк", "где")) {
            return SC_BRANCH;
        }
        if (containsAny(t, "цена", "стоим", "сколько", "прайс")) {
            return SC_PRICE;
        }
        if (containsAny(t, "хранен", "сдать шины", "забрать шины", "склад")) {
            return SC_STORAGE;
        }
        if (containsAny(t, "жалоб", "отзыв", "предложен")) {
            return SC_FEEDBACK;
        }
        if (containsAny(t, "оператор", "менеджер", "человек")) {
            return SC_OPERATOR;
        }
        return null;
    }

    private boolean containsAny(String value, String... parts) {
        for (String part : parts) {
            if (value.contains(part)) {
                return true;
            }
        }
        return false;
    }

    private String normalize(String text) {
        return text.toLowerCase(Locale.ROOT).trim();
    }

    private Long optLong(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        return node.asLong();
    }

    private Location extractLocation(JsonNode attachments) {
        if (!attachments.isArray()) {
            return null;
        }
        for (JsonNode att : attachments) {
            if ("location".equals(att.path("type").asText(""))) {
                double lat = att.path("latitude").asDouble(Double.NaN);
                double lon = att.path("longitude").asDouble(Double.NaN);
                if (!Double.isNaN(lat) && !Double.isNaN(lon)) {
                    return new Location(lat, lon);
                }
            }
        }
        return null;
    }

    private boolean isValidPhone(String phone) {
        return PHONE_PATTERN.matcher(phone).matches();
    }

    private boolean isValidDate(String date) {
        try {
            LocalDate.parse(date, DATE_FMT);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String pickDateValue(String code) {
        return switch (code) {
            case "TODAY" -> LocalDate.now().format(DATE_FMT);
            case "TOMORROW" -> LocalDate.now().plusDays(1).format(DATE_FMT);
            case "YESTERDAY" -> LocalDate.now().minusDays(1).format(DATE_FMT);
            default -> LocalDate.now().format(DATE_FMT);
        };
    }

    private String safe(ObjectNode node, String key) {
        JsonNode value = node.get(key);
        if (value == null || value.isNull() || value.asText().isBlank()) {
            return "—";
        }
        return value.asText();
    }

    private String branchNameByIdSafe(String rawId) {
        Long id = tryParseLong(rawId);
        if (id == null || id <= 0) {
            return "—";
        }
        try {
            Optional<Branch> b = branches.findById(id);
            if (b.isPresent()) {
                Branch br = b.get();
                return br.city() + ", " + br.district() + ", " + br.address();
            }
        } catch (Exception ignored) {
            // nothing
        }
        return "#" + rawId;
    }

    private String buildFeedbackAdminSummary(ObjectNode state) {
        return "🧾 Тип: " + safe(state, "type") + "\n"
                + "🏢 Филиал: " + branchNameByIdSafe(safe(state, "branch_id")) + "\n"
                + "📅 Дата: " + safe(state, "date") + "\n"
                + "📝 Описание: " + safe(state, "description") + "\n"
                + "📱 Контакт: " + safe(state, "contact");
    }

    private String buildBookingAdminSummary(ObjectNode state) {
        return "🏢 Филиал: " + branchNameByIdSafe(safe(state, "branch_id")) + "\n"
                + "🚗 Авто: " + safe(state, "car_type") + ", " + safe(state, "car_brand") + "\n"
                + "🛞 Диаметр: " + safe(state, "radius") + "\n"
                + ("Коммерческий".equals(safe(state, "car_type")) ? "🚚 Колёса: " + safe(state, "wheel_count") + "\n" : "")
                + "🧩 Доп. услуги: " + safe(state, "extra") + "\n"
                + "📅 Дата/время: " + safe(state, "date") + ", " + safe(state, "time") + "\n"
                + "👤 Клиент: " + safe(state, "name") + ", " + safe(state, "phone") + "\n"
                + "🔢 Госномер: " + safe(state, "plate") + "\n"
                + "💬 Комментарий: " + safe(state, "comment");
    }

    private String buildStorageDropAdminSummary(ObjectNode state) {
        return "🏢 Филиал: " + branchNameByIdSafe(safe(state, "drop_branch")) + "\n"
                + "📅 Дата: " + safe(state, "drop_date") + "\n"
                + "👤 Имя: " + safe(state, "drop_name") + "\n"
                + "📱 Телефон: " + safe(state, "drop_phone") + "\n"
                + "💬 Комментарий: " + safe(state, "drop_comment");
    }

    private String buildStoragePickAdminSummary(ObjectNode state) {
        return "🏢 Филиал: " + branchNameByIdSafe(safe(state, "pick_branch")) + "\n"
                + "📅 Дата: " + safe(state, "pick_date") + "\n"
                + "👤 ФИО: " + safe(state, "pick_fio") + "\n"
                + "📱 Телефон: " + safe(state, "pick_phone") + "\n"
                + "🔢 Номер авто/договор: " + safe(state, "pick_contract");
    }

    private void sendPriceExtraStep(UserSession session, ChatTarget target) throws IOException {
        sendMessage(target, buildPriceExtraText(session.state()), buildPriceExtraRows(getPriceExtras(session.state())), true);
    }

    private void editPriceExtraMessage(String messageId, UserSession session) throws IOException {
        ObjectNode message = messages.message(buildPriceExtraText(session.state()), buildPriceExtraRows(getPriceExtras(session.state())), true);
        api.editMessage(messageId, message);
    }

    private String buildPriceExtraText(ObjectNode state) {
        Set<String> selected = getPriceExtras(state);
        StringBuilder text = new StringBuilder("🧩 Выберите доп. услуги:\n");
        text.append("Можно выбрать несколько вариантов, затем нажмите *Готово*.");

        if (!selected.isEmpty()) {
            text.append("\n\nВыбрано:\n");
            for (String code : selected) {
                text.append("• ").append(extraDisplayName(code)).append('\n');
            }
        }
        return text.toString().trim();
    }

    private boolean finishPriceFlow(UserSession session, ChatTarget target) throws SQLException, IOException {
        ObjectNode pricing = loadPricingConfig();
        int basePrice = estimatePriceBase(session.state(), pricing);
        int extraPrice = estimateExtrasPrice(session.state(), pricing);
        int total = basePrice + extraPrice;
        String details = buildPriceResultText(session.state(), pricing, basePrice, extraPrice, total);

        sendMessage(target,
                details,
                List.of(
                        MessageFactory.row(messages.callback("✅ Записаться", "ACT:BOOK"), messages.callback("📍 Филиалы", "ACT:BRANCH")),
                        MessageFactory.row(messages.callback("👩‍💼 Оператор", "ACT:OPERATOR"))
                ),
                true);
        sessions.save(new UserSession(session.userId(), session.chatId(), null, null, mapper.createObjectNode(), mapper.createArrayNode()));
        return true;
    }

    private List<List<ObjectNode>> buildDiameterRowsForCar(String carType) {
        List<String> diameters;
        if (isSedan(carType)) {
            diameters = List.of("R12-R14", "R15", "R16", "R17", "R18", "R19", "R20", "R21", "R22+");
        } else {
            diameters = List.of("R13-R14", "R15", "R16", "R17", "R18", "R19", "R20", "R21", "R22+");
        }

        List<List<ObjectNode>> rows = new ArrayList<>();
        List<ObjectNode> current = new ArrayList<>();
        for (String diameter : diameters) {
            current.add(messages.callback(diameter, "PRICE:RAD:" + diameter));
            if (current.size() == BRANCH_NUMBER_ROW_MAX) {
                rows.add(List.copyOf(current));
                current.clear();
            }
        }
        if (!current.isEmpty()) {
            rows.add(List.copyOf(current));
        }
        rows.add(MessageFactory.row(messages.callback("🤷 Не знаю", "PRICE:RAD:Не знаю")));
        return rows;
    }

    private List<List<ObjectNode>> buildPriceExtraRows(Set<String> selected) {
        List<List<ObjectNode>> rows = new ArrayList<>();
        rows.add(MessageFactory.row(messages.callback(extraButtonText(EX_RUNFLAT, selected), "PRICE:EXTRA_TOGGLE:" + EX_RUNFLAT)));
        rows.add(MessageFactory.row(messages.callback(extraButtonText(EX_LOW_PROFILE, selected), "PRICE:EXTRA_TOGGLE:" + EX_LOW_PROFILE)));
        rows.add(MessageFactory.row(messages.callback(extraButtonText(EX_VALVES, selected), "PRICE:EXTRA_TOGGLE:" + EX_VALVES)));
        rows.add(MessageFactory.row(messages.callback(extraButtonText(EX_SEALANT, selected), "PRICE:EXTRA_TOGGLE:" + EX_SEALANT)));
        rows.add(MessageFactory.row(messages.callback(extraButtonText(EX_HUB_LUBE, selected), "PRICE:EXTRA_TOGGLE:" + EX_HUB_LUBE)));
        rows.add(MessageFactory.row(messages.callback(extraButtonText(EX_GLUE_CLEAN, selected), "PRICE:EXTRA_TOGGLE:" + EX_GLUE_CLEAN)));
        rows.add(MessageFactory.row(messages.callback(extraButtonText(EX_NONE, selected), "PRICE:EXTRA_TOGGLE:" + EX_NONE)));
        rows.add(MessageFactory.row(messages.callback("✅ Готово", "PRICE:EXTRA:DONE")));
        return rows;
    }

    private String extraButtonText(String code, Set<String> selected) {
        String marker = selected.contains(code) ? "✅ " : "⬜ ";
        return marker + extraDisplayName(code);
    }

    private void togglePriceExtra(ObjectNode state, String code) {
        if (!Set.of(EX_NONE, EX_RUNFLAT, EX_LOW_PROFILE, EX_VALVES, EX_SEALANT, EX_HUB_LUBE, EX_GLUE_CLEAN).contains(code)) {
            return;
        }

        Set<String> selected = getPriceExtras(state);
        if (EX_NONE.equals(code)) {
            if (selected.contains(EX_NONE)) {
                selected.clear();
            } else {
                selected.clear();
                selected.add(EX_NONE);
            }
        } else {
            selected.remove(EX_NONE);
            if (selected.contains(code)) {
                selected.remove(code);
            } else {
                selected.add(code);
            }
        }
        savePriceExtras(state, selected);
    }

    private void clearPriceExtras(ObjectNode state) {
        state.remove("price_extras");
    }

    private Set<String> getPriceExtras(ObjectNode state) {
        Set<String> selected = new java.util.LinkedHashSet<>();
        JsonNode node = state.path("price_extras");
        if (node.isArray()) {
            for (JsonNode item : node) {
                String code = item.asText("");
                if (!code.isBlank()) {
                    selected.add(code);
                }
            }
        }
        return selected;
    }

    private void savePriceExtras(ObjectNode state, Set<String> selected) {
        ArrayNode arr = mapper.createArrayNode();
        for (String code : selected) {
            arr.add(code);
        }
        state.set("price_extras", arr);
    }

    private int estimatePriceBase(ObjectNode state, ObjectNode pricing) {
        String service = safe(state, "service");
        String carType = safe(state, "car_type");
        String diameter = safe(state, "radius");
        String wheelCount = safe(state, "wheel_count");

        if ("Переобувка".equals(service)) {
            if ("Коммерческий".equals(carType)) {
                if ("6".equals(wheelCount)) {
                    return pricing.path("COMMERCIAL_6").asInt(5380);
                }
                return pricing.path("COMMERCIAL_4").asInt(3920);
            }
            String key = basePriceKey(carType, diameter);
            int value = pricing.path(key).asInt(0);
            if (value > 0) {
                return value;
            }
            return isSedan(carType) ? pricing.path("SEDAN_R16").asInt(2420) : pricing.path("SUV_R16").asInt(2660);
        }

        int base = switch (service) {
            case "Ремонт" -> 900;
            case "Балансировка" -> 1100;
            case "Правка" -> 1600;
            case "Хранение" -> 2200;
            default -> 1500;
        };
        if (isSuvGroup(carType)) {
            base += 300;
        } else if ("Коммерческий".equals(carType)) {
            base += 700;
        }
        String legacyDiameter = normalizeLegacyDiameter(diameter);
        if ("R16".equals(legacyDiameter)) {
            base += 200;
        } else if ("R17".equals(legacyDiameter)) {
            base += 350;
        } else if ("R18".equals(legacyDiameter)) {
            base += 550;
        } else if ("R19+".equals(legacyDiameter)) {
            base += 800;
        }
        return base;
    }

    private int estimateExtrasPrice(ObjectNode state, ObjectNode pricing) {
        Set<String> selected = getPriceExtras(state);
        if (selected.isEmpty() || selected.contains(EX_NONE)) {
            return 0;
        }

        int sum = 0;
        for (String code : selected) {
            String key = extraPriceKey(code);
            if (key != null) {
                sum += pricing.path(key).asInt(0);
            }
        }
        return sum;
    }

    private String buildPriceResultText(ObjectNode state, ObjectNode pricing, int basePrice, int extrasPrice, int total) {
        String service = safe(state, "service");
        StringBuilder text = new StringBuilder();
        text.append("💸 Итоговая стоимость: *").append(total).append(" ₽*");
        if (extrasPrice > 0) {
            text.append("\nБазовая стоимость: ").append(basePrice).append(" ₽");
            text.append("\nДоп. услуги: +").append(extrasPrice).append(" ₽");
        }

        if ("Переобувка".equals(service)) {
            text.append("\n\nВ стоимость входит:\n");
            text.append("• Съём/установка колёс\n");
            text.append("• Технологическая мойка колёс\n");
            text.append("• Шиномонтаж\n");
            text.append("• Балансировка");
        }

        Set<String> selected = getPriceExtras(state);
        text.append("\n\nДоп. услуги:\n");
        if (selected.isEmpty() || selected.contains(EX_NONE)) {
            text.append("• Без допов");
        } else {
            for (String code : selected) {
                String key = extraPriceKey(code);
                int value = key == null ? 0 : pricing.path(key).asInt(0);
                text.append("• ").append(extraDisplayName(code)).append(" (+").append(value).append(" ₽)").append('\n');
            }
        }

        return text.toString().trim();
    }

    private ObjectNode loadPricingConfig() throws SQLException {
        ObjectNode config = defaultPricingConfig();
        String raw = settings.getValue(KEY_PRICING_CONFIG, "");
        if (raw == null || raw.isBlank()) {
            return config;
        }

        try {
            JsonNode node = mapper.readTree(raw);
            if (!node.isObject()) {
                return config;
            }
            for (String key : defaultPricingKeys()) {
                if (node.has(key) && node.path(key).canConvertToInt()) {
                    int value = node.path(key).asInt();
                    if (value >= 0) {
                        config.put(key, value);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Не удалось прочитать pricing_config: {}", e.getMessage());
        }
        return config;
    }

    private ObjectNode defaultPricingConfig() {
        ObjectNode cfg = mapper.createObjectNode();

        cfg.put("SEDAN_R12_14", 2260);
        cfg.put("SEDAN_R15", 2320);
        cfg.put("SEDAN_R16", 2420);
        cfg.put("SEDAN_R17", 2600);
        cfg.put("SEDAN_R18", 2860);
        cfg.put("SEDAN_R19", 3220);
        cfg.put("SEDAN_R20", 3800);
        cfg.put("SEDAN_R21", 3980);
        cfg.put("SEDAN_R22_PLUS", 4240);

        cfg.put("SUV_R13_14", 2460);
        cfg.put("SUV_R15", 2560);
        cfg.put("SUV_R16", 2660);
        cfg.put("SUV_R17", 2980);
        cfg.put("SUV_R18", 3420);
        cfg.put("SUV_R19", 3880);
        cfg.put("SUV_R20", 4180);
        cfg.put("SUV_R21", 4320);
        cfg.put("SUV_R22_PLUS", 4520);

        cfg.put("COMMERCIAL_4", 3920);
        cfg.put("COMMERCIAL_6", 5380);
        cfg.put("BALANCING_BASE", 500);
        cfg.put("STORAGE_PER_DAY", 50);

        cfg.put("EXTRA_RUNFLAT", 400);
        cfg.put("EXTRA_LOW_PROFILE", 400);
        cfg.put("EXTRA_VALVES", 400);
        cfg.put("EXTRA_SEALANT", 1160);
        cfg.put("EXTRA_HUB_LUBE", 280);
        cfg.put("EXTRA_GLUE_CLEAN", 800);
        return cfg;
    }

    private List<String> defaultPricingKeys() {
        return List.of(
                "SEDAN_R12_14", "SEDAN_R15", "SEDAN_R16", "SEDAN_R17", "SEDAN_R18", "SEDAN_R19", "SEDAN_R20", "SEDAN_R21", "SEDAN_R22_PLUS",
                "SUV_R13_14", "SUV_R15", "SUV_R16", "SUV_R17", "SUV_R18", "SUV_R19", "SUV_R20", "SUV_R21", "SUV_R22_PLUS",
                "COMMERCIAL_4", "COMMERCIAL_6",
                "BALANCING_BASE", "STORAGE_PER_DAY",
                "EXTRA_RUNFLAT", "EXTRA_LOW_PROFILE", "EXTRA_VALVES", "EXTRA_SEALANT", "EXTRA_HUB_LUBE", "EXTRA_GLUE_CLEAN"
        );
    }

    private List<List<ObjectNode>> buildPriceItemRows(String category, ObjectNode cfg) {
        List<String> keys = switch (category) {
            case PRICE_CAT_SEDAN -> List.of("SEDAN_R12_14", "SEDAN_R15", "SEDAN_R16", "SEDAN_R17", "SEDAN_R18", "SEDAN_R19", "SEDAN_R20", "SEDAN_R21", "SEDAN_R22_PLUS");
            case PRICE_CAT_SUV -> List.of("SUV_R13_14", "SUV_R15", "SUV_R16", "SUV_R17", "SUV_R18", "SUV_R19", "SUV_R20", "SUV_R21", "SUV_R22_PLUS");
            case PRICE_CAT_COMM -> List.of("COMMERCIAL_4", "COMMERCIAL_6");
            case PRICE_CAT_SERVICE -> List.of("BALANCING_BASE", "STORAGE_PER_DAY");
            case PRICE_CAT_EXTRA -> List.of("EXTRA_RUNFLAT", "EXTRA_LOW_PROFILE", "EXTRA_VALVES", "EXTRA_SEALANT", "EXTRA_HUB_LUBE", "EXTRA_GLUE_CLEAN");
            default -> List.of();
        };

        List<List<ObjectNode>> rows = new ArrayList<>();
        for (String key : keys) {
            String label = priceItemLabel(key) + " — " + cfg.path(key).asInt(0) + " ₽";
            rows.add(MessageFactory.row(messages.callback(label, "ADM:PRICE:ITEM:" + key)));
        }
        rows.add(MessageFactory.row(messages.callback("⬅️ Назад", "ADM:PRICE:BACK:CATEGORY")));
        return rows;
    }

    private String priceItemLabel(String key) {
        return switch (key) {
            case "SEDAN_R12_14" -> "Седан R12-R14";
            case "SEDAN_R15" -> "Седан R15";
            case "SEDAN_R16" -> "Седан R16";
            case "SEDAN_R17" -> "Седан R17";
            case "SEDAN_R18" -> "Седан R18";
            case "SEDAN_R19" -> "Седан R19";
            case "SEDAN_R20" -> "Седан R20";
            case "SEDAN_R21" -> "Седан R21";
            case "SEDAN_R22_PLUS" -> "Седан R22+";

            case "SUV_R13_14" -> "Кроссовер/SUV R13-R14";
            case "SUV_R15" -> "Кроссовер/SUV R15";
            case "SUV_R16" -> "Кроссовер/SUV R16";
            case "SUV_R17" -> "Кроссовер/SUV R17";
            case "SUV_R18" -> "Кроссовер/SUV R18";
            case "SUV_R19" -> "Кроссовер/SUV R19";
            case "SUV_R20" -> "Кроссовер/SUV R20";
            case "SUV_R21" -> "Кроссовер/SUV R21";
            case "SUV_R22_PLUS" -> "Кроссовер/SUV R22+";

            case "COMMERCIAL_4" -> "Коммерческий (4 колеса)";
            case "COMMERCIAL_6" -> "Коммерческий (6 колёс)";
            case "BALANCING_BASE" -> "Балансировка (базовая)";
            case "STORAGE_PER_DAY" -> "Хранение (за день)";

            case "EXTRA_RUNFLAT" -> "RunFlat";
            case "EXTRA_LOW_PROFILE" -> "Низкий профиль (=<50)";
            case "EXTRA_VALVES" -> "Замена вентилей";
            case "EXTRA_SEALANT" -> "Очистка и нанесение герметика";
            case "EXTRA_HUB_LUBE" -> "Медная/алюминиевая смазка ступиц";
            case "EXTRA_GLUE_CLEAN" -> "Очистка дисков от старого клея";
            default -> key;
        };
    }

    private String buildPricingAdminHelp(ObjectNode cfg) {
        StringBuilder text = new StringBuilder("💸 Изменение цен\n");
        text.append("Отправьте строки формата `КЛЮЧ=ЦЕНА`.\n");
        text.append("Можно прислать только изменяемые позиции.\n\n");
        text.append("Текущие значения:\n");
        for (String key : defaultPricingKeys()) {
            text.append("`").append(key).append("=").append(cfg.path(key).asInt(0)).append("`\n");
        }
        return text.toString().trim();
    }

    private PricingUpdateResult applyPricingUpdates(ObjectNode config, String raw) {
        List<String> errors = new ArrayList<>();
        int updated = 0;

        String[] lines = raw.split("\\r?\\n");
        for (String line : lines) {
            String src = line.trim();
            if (src.isBlank()) {
                continue;
            }

            String keyPart;
            String valuePart;
            String prepared = src.replace('—', '=').replace('–', '=').replace(':', '=');
            int eq = prepared.indexOf('=');
            if (eq >= 0) {
                keyPart = prepared.substring(0, eq).trim();
                valuePart = prepared.substring(eq + 1).trim();
            } else {
                String[] parts = src.split("\\s+");
                if (parts.length < 2) {
                    errors.add("• `" + src + "` — не удалось распознать строку");
                    continue;
                }
                valuePart = parts[parts.length - 1].trim();
                keyPart = src.substring(0, src.length() - valuePart.length()).trim();
            }

            Integer value = tryParseInt(valuePart);
            if (value == null || value < 0) {
                errors.add("• `" + src + "` — цена должна быть числом >= 0");
                continue;
            }

            String key = resolvePricingKey(keyPart);
            if (key == null) {
                errors.add("• `" + src + "` — неизвестный ключ");
                continue;
            }

            config.put(key, value);
            updated++;
        }
        return new PricingUpdateResult(updated, errors);
    }

    private String resolvePricingKey(String rawKey) {
        String k = rawKey.toUpperCase(Locale.ROOT)
                .replace('Ё', 'Е')
                .replace('-', '_')
                .replace("+", "_PLUS")
                .replace(' ', '_')
                .replace("/", "_")
                .replace("__", "_")
                .trim();

        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("SEDAN_R12_14", "SEDAN_R12_14");
        aliases.put("SEDAN_R15", "SEDAN_R15");
        aliases.put("SEDAN_R16", "SEDAN_R16");
        aliases.put("SEDAN_R17", "SEDAN_R17");
        aliases.put("SEDAN_R18", "SEDAN_R18");
        aliases.put("SEDAN_R19", "SEDAN_R19");
        aliases.put("SEDAN_R20", "SEDAN_R20");
        aliases.put("SEDAN_R21", "SEDAN_R21");
        aliases.put("SEDAN_R22_PLUS", "SEDAN_R22_PLUS");
        aliases.put("SUV_R13_14", "SUV_R13_14");
        aliases.put("SUV_R15", "SUV_R15");
        aliases.put("SUV_R16", "SUV_R16");
        aliases.put("SUV_R17", "SUV_R17");
        aliases.put("SUV_R18", "SUV_R18");
        aliases.put("SUV_R19", "SUV_R19");
        aliases.put("SUV_R20", "SUV_R20");
        aliases.put("SUV_R21", "SUV_R21");
        aliases.put("SUV_R22_PLUS", "SUV_R22_PLUS");
        aliases.put("COMMERCIAL_4", "COMMERCIAL_4");
        aliases.put("COMMERCIAL_6", "COMMERCIAL_6");
        aliases.put("EXTRA_RUNFLAT", "EXTRA_RUNFLAT");
        aliases.put("EXTRA_LOW_PROFILE", "EXTRA_LOW_PROFILE");
        aliases.put("EXTRA_VALVES", "EXTRA_VALVES");
        aliases.put("EXTRA_SEALANT", "EXTRA_SEALANT");
        aliases.put("EXTRA_HUB_LUBE", "EXTRA_HUB_LUBE");
        aliases.put("EXTRA_GLUE_CLEAN", "EXTRA_GLUE_CLEAN");

        aliases.put("СЕДАН_R12_14", "SEDAN_R12_14");
        aliases.put("СЕДАН_R15", "SEDAN_R15");
        aliases.put("СЕДАН_R16", "SEDAN_R16");
        aliases.put("СЕДАН_R17", "SEDAN_R17");
        aliases.put("СЕДАН_R18", "SEDAN_R18");
        aliases.put("СЕДАН_R19", "SEDAN_R19");
        aliases.put("СЕДАН_R20", "SEDAN_R20");
        aliases.put("СЕДАН_R21", "SEDAN_R21");
        aliases.put("СЕДАН_R22_PLUS", "SEDAN_R22_PLUS");
        aliases.put("КРОССОВЕР_R13_14", "SUV_R13_14");
        aliases.put("КРОССОВЕР_R15", "SUV_R15");
        aliases.put("КРОССОВЕР_R16", "SUV_R16");
        aliases.put("КРОССОВЕР_R17", "SUV_R17");
        aliases.put("КРОССОВЕР_R18", "SUV_R18");
        aliases.put("КРОССОВЕР_R19", "SUV_R19");
        aliases.put("КРОССОВЕР_R20", "SUV_R20");
        aliases.put("КРОССОВЕР_R21", "SUV_R21");
        aliases.put("КРОССОВЕР_R22_PLUS", "SUV_R22_PLUS");
        aliases.put("КОММЕРЧЕСКИЙ_4", "COMMERCIAL_4");
        aliases.put("КОММЕРЧЕСКИЙ_6", "COMMERCIAL_6");
        aliases.put("RUNFLAT", "EXTRA_RUNFLAT");
        aliases.put("НИЗКИЙ_ПРОФИЛЬ", "EXTRA_LOW_PROFILE");
        aliases.put("ЗАМЕНА_ВЕНТИЛЕЙ", "EXTRA_VALVES");
        aliases.put("ОЧИСТКА_И_НАНЕСЕНИЕ_ГЕРМЕТИКА", "EXTRA_SEALANT");
        aliases.put("МЕДНАЯ_ИЛИ_АЛЮМИНИЕВАЯ_СМАЗКА_СТУПИЦ", "EXTRA_HUB_LUBE");
        aliases.put("ОЧИСТКА_ДИСКОВ_ОТ_СТАРОГО_КЛЕЯ", "EXTRA_GLUE_CLEAN");

        return aliases.get(k);
    }

    private String basePriceKey(String carType, String diameter) {
        String d = normalizeDiameterForKey(diameter, carType);
        if (isSedan(carType)) {
            return "SEDAN_" + d;
        }
        return "SUV_" + d;
    }

    private String normalizeDiameterForKey(String diameter, String carType) {
        String d = diameter.toUpperCase(Locale.ROOT).replace(" ", "");
        if ("НЕЗНАЮ".equals(d) || "НЕ_ЗНАЮ".equals(d) || "—".equals(d) || d.isBlank()) {
            return isSedan(carType) ? "R16" : "R16";
        }
        if ("R12-R14".equals(d) || "R12_14".equals(d)) {
            return "R12_14";
        }
        if ("R13-R14".equals(d) || "R13_14".equals(d)) {
            return "R13_14";
        }
        if ("R22+".equals(d) || "R22PLUS".equals(d)) {
            return "R22_PLUS";
        }
        return d.replace("+", "").replace("-", "_");
    }

    private String normalizeLegacyDiameter(String diameter) {
        String d = diameter.toUpperCase(Locale.ROOT).replace(" ", "");
        return switch (d) {
            case "R16" -> "R16";
            case "R17" -> "R17";
            case "R18" -> "R18";
            case "R19", "R19+", "R20", "R21", "R22+" -> "R19+";
            default -> "";
        };
    }

    private String extraPriceKey(String code) {
        return switch (code) {
            case EX_RUNFLAT -> "EXTRA_RUNFLAT";
            case EX_LOW_PROFILE -> "EXTRA_LOW_PROFILE";
            case EX_VALVES -> "EXTRA_VALVES";
            case EX_SEALANT -> "EXTRA_SEALANT";
            case EX_HUB_LUBE -> "EXTRA_HUB_LUBE";
            case EX_GLUE_CLEAN -> "EXTRA_GLUE_CLEAN";
            default -> null;
        };
    }

    private String extraDisplayName(String code) {
        return switch (code) {
            case EX_RUNFLAT -> "RunFlat";
            case EX_LOW_PROFILE -> "Низкий профиль (=<50)";
            case EX_VALVES -> "Замена вентилей";
            case EX_SEALANT -> "Очистка и нанесение герметика";
            case EX_HUB_LUBE -> "Медная или алюминиевая смазка ступиц";
            case EX_GLUE_CLEAN -> "Очистка дисков от старого клея";
            case EX_NONE -> "Без допов";
            default -> code;
        };
    }

    private boolean isSedan(String carType) {
        return "Седан".equals(carType) || "Легковой".equals(carType);
    }

    private boolean isSuvGroup(String carType) {
        return "Кроссовер".equals(carType) || "Внедорожник".equals(carType) || "Минивен".equals(carType);
    }

    private int estimateStoragePrice(String radius, String type) {
        int base = switch (radius) {
            case "R13-R15" -> 1800;
            case "R16-R17" -> 2300;
            case "R18+" -> 2800;
            default -> 2200;
        };
        if ("Полный комплект".equals(type)) {
            base += 600;
        }
        return base;
    }

    private Long tryParseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (Exception e) {
            return null;
        }
    }

    private Integer tryParseInt(String value) {
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return null;
        }
    }

    private String shortBranchLabel(Branch branch) {
        String base = branch.city() + ", " + branch.district() + ", " + branch.address();
        if (base.length() <= 48) {
            return base;
        }
        return base.substring(0, 45) + "...";
    }

    private BranchDraft parseBranchDraft(String raw) {
        String[] parts = raw.split(";");
        if (parts.length < 6) {
            return null;
        }
        String city = parts[0].trim();
        String district = parts[1].trim();
        String address = parts[2].trim();
        String phone = parts[3].trim();
        String schedule = parts[4].trim();
        String is247Raw = parts[5].trim().toLowerCase(Locale.ROOT);

        if (city.isBlank() || district.isBlank() || address.isBlank() || phone.isBlank() || schedule.isBlank()) {
            return null;
        }
        if (!isValidPhone(phone)) {
            return null;
        }

        boolean is247 = "да".equals(is247Raw)
                || "yes".equals(is247Raw)
                || "true".equals(is247Raw)
                || "1".equals(is247Raw)
                || "24/7".equals(is247Raw);

        Double lat = null;
        Double lon = null;
        if (parts.length > 6 && !parts[6].trim().isBlank()) {
            try {
                lat = Double.parseDouble(parts[6].trim().replace(',', '.'));
            } catch (Exception ignored) {
                return null;
            }
        }
        if (parts.length > 7 && !parts[7].trim().isBlank()) {
            try {
                lon = Double.parseDouble(parts[7].trim().replace(',', '.'));
            } catch (Exception ignored) {
                return null;
            }
        }

        return new BranchDraft(city, district, address, phone, schedule, is247, lat, lon);
    }

    private record Location(double lat, double lon) {
    }

    private record BranchDraft(
            String city,
            String district,
            String address,
            String phone,
            String schedule,
            boolean is24x7,
            Double latitude,
            Double longitude
    ) {
    }

    private record PricingUpdateResult(
            int updatedCount,
            List<String> errors
    ) {
    }
}
