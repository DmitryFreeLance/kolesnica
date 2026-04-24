CREATE TABLE IF NOT EXISTS branches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city TEXT NOT NULL,
    district TEXT NOT NULL,
    address TEXT NOT NULL UNIQUE,
    phone TEXT NOT NULL,
    schedule TEXT NOT NULL,
    is_24_7 INTEGER NOT NULL,
    latitude REAL,
    longitude REAL,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS user_sessions (
    user_id INTEGER PRIMARY KEY,
    chat_id INTEGER,
    scenario TEXT,
    step TEXT,
    state_json TEXT NOT NULL,
    history_json TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS requests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    chat_id INTEGER,
    request_type TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS admins (
    user_id INTEGER PRIMARY KEY,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    name TEXT,
    first_name TEXT,
    last_name TEXT,
    username TEXT,
    is_bot INTEGER NOT NULL DEFAULT 0,
    last_activity_time INTEGER,
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);

CREATE INDEX IF NOT EXISTS idx_requests_user ON requests(user_id);
CREATE INDEX IF NOT EXISTS idx_requests_type ON requests(request_type);

INSERT OR IGNORE INTO settings(key, value) VALUES
('operator_phone', '+79005553535');

INSERT OR IGNORE INTO branches(city, district, address, phone, schedule, is_24_7, latitude, longitude) VALUES
('Волгоград', 'Кировский', 'ул. 64 Армии, 50А/1', '+7-904-413-84-08', 'Круглосуточно', 1, 48.6311240, 44.4283523),
('Волгоград', 'Центральный', 'ул. Пархоменко, 57А', '+7-927-067-38-76', 'Круглосуточно', 1, 48.7235710, 44.5262303),
('Волгоград', 'Красноармейский', 'ул. Удмуртская, 103', '+7-937-696-43-81', '08:00–21:00', 0, 48.5087639, 44.5643159),
('Волгоград', 'Советский', 'проспект Университетский, 81', '+7-904-750-47-39', '08:00–21:00', 0, 48.6542991, 44.4373833),
('Волгоград', 'Центральный', 'ул. Ткачёва, 20Г/1', '+7-904-750-17-85', '08:00–21:00', 0, 48.7235369, 44.5153850),
('Волгоград', 'Краснооктябрьский', 'ул. Маршала Еременко, 49А', '+7-995-400-47-30', '08:00–21:00', 0, 48.7885342, 44.5723604),
('Волгоград', 'Краснооктябрьский', 'ул. Вершинина, 5/1', '+7-937-566-90-32', '08:00–21:00', 0, 48.7679049, 44.5478295),
('Волгоград', 'Краснооктябрьский', 'ул. Маршала Еременко, 5Д', '+7-937-086-51-28', '08:00–21:00', 0, 48.7675001, 44.5405063),
('Волгоград', 'Краснооктябрьский', 'ул. Генерала Штеменко, 52А', '+7-927-067-24-71', '08:00–21:00', 0, 48.7781379, 44.5502062),
('Волгоград', 'Дзержинский', 'ул. Космонавтов, 16Г/1', '+7-937-086-45-97', '08:00–21:00', 0, 48.7701543, 44.5040980),
('Волгоград', 'Дзержинский', 'бульвар 30-летия Победы, 15/1', '+7-927-067-63-32', '08:00–21:00', 0, 48.7452300, 44.5023319),
('Волгоград', 'Дзержинский', 'ул. 51-й Гвардейской Дивизии, 22', '+7-904-418-29-43', '08:00–21:00', 0, 48.7680388, 44.4892752),
('Волгоград', 'Тракторозаводский', 'ул. Менжинского, 15А/1', '+7-904-407-61-67', '08:00–21:00', 0, 48.8178769, 44.6214819),
('Волжский', 'Волжский', 'ул. Оломоуцкая, 9', '+7-927-068-29-81', '08:00–21:00', 0, 48.7598005, 44.7979178),
('Волжский', 'Волжский', 'ул. Александрова, 18А', '+7-900-182-12-30', '08:00–21:00', 0, 48.7724880, 44.8001808),
('Волжский', 'Волжский', 'ул. Карбышева, 47В', '+7-995-425-39-25', '08:00–21:00', 0, 48.7799982, 44.7687775),
('Волжский', 'Волжский', 'ул. Медведева, 37', '+7-992-156-34-96', '08:00–21:00', 0, 48.7527800, 44.8305048);
