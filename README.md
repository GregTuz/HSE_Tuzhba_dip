# HSE_Tuzhba_dip
Репозиторий для магистерского дипломного проекта

Демонстрирует полный цикл обработки финансовых транзакций — от генерации потока данных через REST API до аналитических витрин и визуализации.

---

## О проекте

Система реализует NRT-конвейер (Near Real-Time) поставки платёжных данных с сохранением истории транзакций. Архитектура построена на принципе event-driven: каждая транзакция немедленно публикуется в брокер сообщений и становится доступной для аналитики в течение нескольких секунд.

Система решает две задачи:

- **Антифрод** — обнаружение подозрительных паттернов: дробление переводов, превышение лимитов, аномальные снятия наличных
- **Бизнес-аналитика** — оборот по категориям мерчантов, географии, уровням аккаунтов и временным паттернам

**Датасет:** ~37 500 финансовых транзакций за 2016–2025 годы с полями `transaction_id`, `account_id`, `timestamp`, `amount`, `currency`, `merchant_category`, `country_code`, `account_level` и др. Датасет обогащён дополнительными атрибутами (категория мерчанта, страна, уровень аккаунта, валюта) из открытого источника [Kaggle](https://www.kaggle.com/datasets/mdhossanr/financial-transactions-dataset-for-analysis).

**Производительность:** задержка обработки ~3 секунды при интенсивности 500 транзакций/сек.

---

## Архитектура

```
CSV (Financial_Transactions_Enriched.csv)
        │
        ▼
   FastAPI Producer
   (валидация Pydantic: сумма, валюта, timestamp)
        │
        ├──► Kafka: transactions.valid   (3 партиции, retention 3 GB)
        └──► Kafka: transactions.invalid (3 партиции, retention 3 GB)
                │
                ▼
         ClickHouse Kafka Engine
         (вычитка каждые 3 секунды, kafka_max_block_size = 10 000)
                │
                ▼
         Materialized View
         (обогащение через словари, конвертация суммы в RUB)
                │
                ▼
         ReplacingMergeTree
         transactions_valid / transactions_invalid
         (Partition: toYYYYMM, ORDER BY (account_id, transaction_id))
                │
                ▼
           Airflow DAGs
      (витрины: антифрод, лимиты, аналитика)
      (паттерн: DROP PARTITION → INSERT)
                │
                ▼
            Superset
         (дашборды и визуализация)
```

---

## Компоненты

| Сервис | Образ | Версия | Порт |
|--------|-------|--------|------|
| Apache Kafka | apache/kafka | 3.7.0 (KRaft mode) | 9094 (external) |
| Kafka UI | provectuslabs/kafka-ui | latest | 8085 |
| ClickHouse | clickhouse/clickhouse-server | 23.8 | 8123 (HTTP), 9000 (native) |
| Apache Airflow | custom build | 2.x | 8081 |
| Apache Superset | apache/superset | 3.1.0 | 8088 |
| Jupyter Notebook | jupyter/scipy-notebook | latest | 8888 |
| FastAPI | custom build | — | 8000 |
| PostgreSQL (Airflow DB) | postgres | 15 | — |

---

## Структура проекта

```
HSE_Tuzhba_dip/
├── clickhouse/
│   ├── config/
│   │   └── user_settings.xml              # max_partitions_per_insert_block = 500
│   └── init/                              # DDL скрипты, выполняются при старте
│       ├── 01_init_account_levels.sql     # справочник уровней аккаунтов
│       ├── 01_init_countries.sql          # справочник стран
│       ├── 01_init_currencies.sql         # таблица курсов валют
│       ├── 01_init_merchant_categories.sql # справочник категорий мерчантов
│       ├── 02_init_valid_transactions.sql  # Kafka Engine: transactions.valid
│       ├── 02_init_invalid_transactions.sql # Kafka Engine: transactions.invalid
│       ├── 03_init_valid_merge.sql         # ReplacingMergeTree: transactions_valid
│       ├── 03_init_invalid_megre.sql       # ReplacingMergeTree: transactions_invalid
│       ├── 04_init_final_valid.sql         # Materialized View: mv_transactions_valid
│       ├── 04_init_invalid_transactions.sql # Materialized View: mv_transactions_invalid
│       ├── 05_antifraud.sql               # витрина dm_antifraud_daily
│       ├── 05_limits_daily.sql            # витрина dm_daily_limits
│       ├── 05_limits_monthly.sql          # витрина dm_monthly_limits
│       └── 05_analitics_daily.sql         # витрина dm_analytics_daily
├── dags/
│   ├── load_currency_rates.py             # загрузка курсов ЦБ РФ (ежедневно 09:05)
│   ├── antifraud_daily.py                 # витрина антифрода (ежедневно 01:00)
│   ├── limits_daily.py                    # витрина дневных лимитов (ежедневно 01:00)
│   ├── limits_monthly.py                  # витрина месячных лимитов (2-е число 01:00)
│   └── analitics_daily.py                 # аналитическая витрина (ежедневно 01:00)
├── fastapi_app/
│   ├── main.py                            # эндпоинты и логика отправки в Kafka
│   ├── models.py                          # Pydantic модели валидации
│   ├── config.py                          # настройки через переменные окружения
│   ├── Dockerfile
│   └── requirements.txt
├── notebooks/
│   ├── Financial_Transactions.csv          # исходный датасет (Kaggle)
│   ├── Financial_Transactions_Enriched.csv # обогащённый датасет
│   ├── Enrichment.ipynb                    # обогащение и подготовка датасета
│   ├── ClickHouse_filling.ipynb            # наполнение справочников
│   ├── Dumping.ipynb                       # дамп таблиц в CSV
│   ├── Restoration.ipynb                   # восстановление из дампа
│   ├── dumps/                              # дампы таблиц для быстрого старта
│   └── requirements.txt                    # зависимости для Jupyter
├── docker-compose.yaml
├── Dockerfile                              # образ для Airflow
├── .env                                    # переменные окружения
├── .env.example                            # шаблон переменных
└── README.md
```

---

## Быстрый старт

### 1. Клонировать репозиторий

```bash
git clone https://github.com/your-username/HSE_Tuzhba_dip.git
cd HSE_Tuzhba_dip
```

Скопировать `.env.example` в `.env` и заполнить секреты (пароли, ключи).

### 2. Поднять все сервисы

```bash
docker-compose up -d --build
```

Порядок старта управляется через `depends_on` — сервисы поднимаются в правильном порядке автоматически.

### 3. Дождаться инициализации ClickHouse

При первом старте ClickHouse выполняет все DDL скрипты из `clickhouse/init/`. Занимает ~5 минут. Проверить готовность:

```bash
docker logs clickhouse --tail 20
```

### 4. Наполнить справочники

Открыть Jupyter на `http://localhost:8888` (токен из `.env`), запустить:

```bash
# Инициализация ядра
init_kernel.sh

# Заполнение словарей ClickHouse
ClickHouse_filling.ipynb → запустить все ячейки
```

### 5. Восстановить данные из дампа (опционально)

В Jupyter запустить `Restoration.ipynb` — загрузит курсы валют и накопленные транзакции без ожидания стрима.

### 6. Запустить стриминг транзакций

FastAPI доступен на `http://localhost:8000/docs`.

```bash
# Запустить непрерывный стриминг (до 500 транзакций/сек)
POST /stream/start

# Остановить стриминг
POST /stream/stop

# Проверить статус
GET /stream/status
```

Отправка одной транзакции вручную:
```bash
POST /transaction/send
```

### 7. Запустить DAG'и в Airflow

Airflow доступен на `http://localhost:8081` (admin/admin).

| DAG | Расписание | Назначение |
|-----|-----------|------------|
| `load_currency_rates` | `5 9 * * *` | Курсы валют ЦБ РФ |
| `antifraud_daily` | `0 1 * * *` | Антифрод витрина |
| `limits_daily` | `0 1 * * *` | Дневные лимиты |
| `limits_monthly` | `0 1 2 * *` | Месячные лимиты (2-е число) |
| `analitics_daily` | `0 1 * * *` | Аналитическая витрина |

Для расчёта полного исторического периода использовать бэкфил:

```bash
docker exec -it airflow-scheduler airflow dags backfill \
    <dag_id> \
    --start-date <YYYY-MM-DD> \
    --end-date <YYYY-MM-DD> \
    -y
```


### 8. Подключить ClickHouse в Superset

Superset доступен на `http://localhost:8088`.

**Settings → Database Connections → + Database → ClickHouse Connect (SuperSet)**

```
Host:     clickhouse
Port:     8123
Database: default
Username: default
Password: (пусто)
```

---

## Эндпоинты FastAPI

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/transaction` | Получить случайную валидную транзакцию из CSV |
| GET | `/transaction/broken` | Получить случайную невалидную транзакцию из CSV |
| POST | `/transaction/send` | Валидировать и отправить одну транзакцию в Kafka |
| POST | `/stream/start` | Запустить непрерывный стриминг (до 500 сообщ/сек) |
| POST | `/stream/stop` | Остановить стриминг |
| GET | `/stream/status` | Получить статус стриминга |

---

## Валидация транзакций (Pydantic)

FastAPI валидирует каждую транзакцию перед отправкой в Kafka по трём правилам:

- `0 < amount < account_balance`
- `currency` ∈ `[USD, EUR, RUB, GBP]`
- `timestamp` ≤ текущее время (не в будущем)

Транзакции, прошедшие валидацию → `transactions.valid` → `transactions_valid`  
Транзакции с нарушениями → `transactions.invalid` → `transactions_invalid`

---

## Топики Kafka

| Топик | Партиции | Retention | Ключ сообщения |
|-------|----------|-----------|----------------|
| `transactions.valid` | 3 | 3 GB | `account_id` |
| `transactions.invalid` | 3 | 3 GB | `account_id` |

Kafka работает в режиме **KRaft** (без ZooKeeper), начиная с версии 3.7.0.

Параметры ClickHouse Kafka Engine:
- `kafka_flush_interval_ms = 3000` — вычитка каждые 3 секунды (максимум)
- `kafka_max_block_size = 10000` — максимальный размер блока

---

## Таблицы ClickHouse

| Таблица | Движок | Назначение |
|---------|--------|------------|
| `currency_rates` | ReplacingMergeTree | Курсы валют ЦБ РФ (USD, EUR, GBP) |
| `kafka_transactions_valid` | Kafka Engine | Буфер чтения из `transactions.valid` |
| `kafka_transactions_invalid` | Kafka Engine | Буфер чтения из `transactions.invalid` |
| `transactions_valid` | ReplacingMergeTree | Обогащённые валидные транзакции |
| `transactions_invalid` | ReplacingMergeTree | Невалидные транзакции |
| `dm_antifraud_daily` | ReplacingMergeTree | Витрина антифрода |
| `dm_daily_limits` | ReplacingMergeTree | Витрина дневных лимитов |
| `dm_monthly_limits` | ReplacingMergeTree | Витрина месячных лимитов |
| `dm_analytics_daily` | ReplacingMergeTree | Аналитическая витрина |

Все таблицы хранения используют `PARTITION BY toYYYYMM(transaction_date)` и `ORDER BY (account_id, transaction_id)`.

---

## Справочники ClickHouse

| Справочник | Ключ | Атрибуты |
|------------|------|----------|
| `dict_merchant_category` | `category_code` | `category_name`, `risk_score`, `is_online` |
| `dict_country` | `country_code` | `country_name`, `region`, `risk_level` |
| `dict_account_level` | `level_code` | `level_name`, `daily_limit_rub`, `monthly_limit_rub` |

Все словари используют layout `COMPLEX_KEY_HASHED` и загружаются в оперативную память. Обращение через `dictGet(dict_name, attr, tuple(key))`.

---

## Витрины данных

| Витрина | Расписание | Гранулярность | Ключевые метрики |
|---------|-----------|---------------|-----------------|
| `dm_analytics_daily` | Ежедневно 01:00 | День, категория, страна, уровень, час | Количество транзакций, оборот в RUB |
| `dm_daily_limits` | Ежедневно 01:00 | Счёт, день | Дневной оборот vs лимит, признак превышения |
| `dm_monthly_limits` | 2-е число 01:00 | Счёт, месяц | Месячный оборот vs лимит, признак превышения |
| `dm_antifraud_daily` | Ежедневно 01:00 | Счёт, день | Причина срабатывания (`ATM`, `repeated_transfers`, `high_frequency`) |

Все витрины обновляются по идемпотентному паттерну `ALTER TABLE DROP PARTITION → INSERT`.

**Правила антифрода:**
- ≥ 2 снятий наличных через банкомат за день (`ATM_THRESHOLD = 2`)
- ≥ 2 переводов одинаковой суммы за день (`TRANSFER_THRESHOLD = 2`)
- ≥ 5 транзакций от одного счёта за день (`HIGH_FREQUENCY_THRESHOLD = 5`)

---

## Дашборды Superset

| Дашборд | Источник | Элементы |
|---------|----------|----------|
| Аналитика транзакций | `dm_analytics_daily` | Карта мира, динамика по месяцам, распределение по категориям и валютам, счётчики оборота |
| Мониторинг подозрительной активности | `dm_antifraud_daily` | Счётчики срабатываний, распределение по причинам, таблица подозрительных счетов |
| Лимитный контроль | `dm_daily_limits`, `dm_monthly_limits` | Счётчики превышений, таблица с детализацией по счетам |

---

## Пересборка с нуля

Если нужно полностью пересобрать систему с сохранением курсов валют:

```bash
# 1. Сделать дамп курсов валют
docker exec clickhouse clickhouse-client --query \
  "SELECT * FROM currency_rates FORMAT CSV" > currency_rates_backup.csv

# 2. Остановить сервисы и удалить volumes
docker-compose down
docker volume rm $(docker volume ls -q | grep -E "kafka_data|clickhouse_data|superset_data")

# 3. Поднять заново
docker-compose up -d --build

# 4. Восстановить курсы валют
docker exec -i clickhouse clickhouse-client --query \
  "INSERT INTO currency_rates FORMAT CSV" < currency_rates_backup.csv
```

---

## Зависимости

**FastAPI** (`fastapi_app/requirements.txt`):
```
fastapi==0.111.0
uvicorn[standard]==0.29.0
pydantic==2.7.1
pydantic-settings==2.2.1
pandas==2.2.2
confluent-kafka==2.4.0
```

**Jupyter** (`notebooks/requirements.txt`):
```
clickhouse-driver==0.2.7
clickhouse-connect==0.7.0
confluent-kafka==2.3.0
pydantic==2.6.0
pandas==2.2.0
matplotlib==3.8.0
seaborn==0.13.2
requests==2.31.0
```
