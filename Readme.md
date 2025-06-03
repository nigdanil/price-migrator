# 🛠 price-migrator

**price-migrator** — это утилита на Go для миграции данных из локальной SQLite базы (`scraper.db`) в PostgreSQL, с поддержкой синхронизации, автоматической вставки недостающих категорий и соблюдением внешних ключей.

---

## 📦 Назначение

Проект используется для переноса и синхронизации таблиц:

- `gallery_categories`
- `category_urls`
- `gallery_products`
- `gallery_product_prices`

Из SQLite → в PostgreSQL с минимальными накладными расходами и контролем ссылочной целостности.

---

## 📁 Структура проекта

```text
price-migrator/
├── db/                 # Логика миграции, подключение к БД
│   ├── connect.go
│   ├── migrate.go
│   └── helpers.go
├── utils/              # Генерация SQL-запросов (UPSERT и т.д.)
│   └── query.go
├── .env                # Конфигурация подключения к PostgreSQL
├── main.go             # Точка входа
├── go.mod              # Модули Go
├── .gitignore
└── README.md
````

---

## ⚙️ Как использовать

1. Установите Go ≥ 1.18
2. Подготовьте `.env` файл (или напрямую настройте переменные окружения):

```env
PG_HOST=localhost
PG_PORT=5432
PG_USER=root
PG_PASSWORD=yourpassword
PG_DB=price_monitor
PG_SCHEMA=price_data
```

3. Убедитесь, что в корне проекта лежит `scraper.db` (SQLite).
4. Запустите миграцию:

```bash
go run main.go
```

---

## 💡 Особенности

* 🔄 **Синхронизация**: удаляет лишние записи, добавляет новые, обновляет изменённые
* 🔗 **Проверка внешних ключей**: миграция с учётом `FOREIGN KEY`
* ✅ **Автодобавление категорий**: если `category_id` есть в других таблицах, но нет в `gallery_categories` — они будут добавлены
* 📊 **Поддержка deferred constraints**: выполнение `SET CONSTRAINTS ALL DEFERRED`

---

## 🧪 Пример вывода

```text
🔍 Проверка ссылок в category_urls...
🚨 ВНИМАНИЕ: category_id 'halal_meats' есть в category_urls, но отсутствует в gallery_categories
➕ Добавлена категория: halal_meats
➡ Начало миграции таблицы: gallery_products
📥 Извлечено из SQLite: 13625 строк
✅ Миграция таблицы gallery_products завершена
```

---

## 🔐 Безопасность

* Не коммитьте `scraper.db` или `.env` в публичные репозитории
* Для доступа к GitHub используйте SSH или Personal Access Tokens

---

## 🧑‍💻 Автор

**nigdanil** — [github.com/nigdanil](https://github.com/nigdanil)

---

## 📄 Лицензия

MIT License — свободное использование с сохранением авторства

```