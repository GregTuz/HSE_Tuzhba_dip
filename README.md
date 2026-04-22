# HSE_Tuzhba_dip
Репозиторий для магистрского дипломного проекта

Демонстрирует полный цикл обработки финансовых транзакций — от генерации потока данных через REST API до аналитических витрин и визуализации.

---

## О проекте

Система моделирует аналитическое хранилище для малого бизнеса, работающее в паре с основной транзакционной БД. Выполняет две функции:

- **Антифрод** — обнаружение подозрительных паттернов: дробление переводов, превышение лимитов, аномальные снятия наличных
- **Бизнес-аналитика** — оборот по категориям, географии, уровням аккаунтов и временным паттернам

Датасет: ~37 500 финансовых транзакций за 2016–2024 годы с полями `transaction_id`, `account_id`, `timestamp`, `amount`, `currency`, `merchant_category`, `country_code`, `account_level` и др.

---

## Архитектура

```
CSV (Financial_Transactions_Enriched.csv)
        │
        ▼
   FastAPI Producer
   (валидация Pydantic)
        │
        ├──► Kafka: transactions.valid
        └──► Kafka: transactions.invalid
                │
                ▼
         ClickHouse Kafka Engine
         (батч каждые 5 минут)
                │
                ▼
         Materialized View
         (обогащение через словари)
                │
                ▼
         ReplacingMergeTree
         transactions_valid / transactions_invalid
                │
                ▼
           Airflow DAGs
      (витрины: антифрод, лимиты, аналитика)
                │
                ▼
            Superset
         (дашборды и визуализация)
```

---

## Компоненты

| Сервис | Образ | Версия | Порт |
|--------|-------|--------|------|
| Apache Kafka | apache/kafka | 3.7.0 | 9094 (external) |
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
│   ├── load_currency_rates.py             # загрузка курсов ЦБ РФ (ежедневно)
│   ├── antifraud_daily.py                 # витрина антифрода (ежедневно)
│   ├── limits_daily.py                    # витрина дневных лимитов (ежедневно)
│   ├── limits_monthly.py                  # витрина месячных лимитов (2-е число)
│   └── analitics_daily.py                 # аналитическая витрина (ежедневно)
├── fastapi_app/
│   ├── main.py                            # эндпоинты и логика отправки в Kafka
│   ├── models.py                          # Pydantic модели валидации
│   ├── config.py                          # настройки через переменные окружения
│   ├── Dockerfile
│   └── requirements.txt
├── notebooks/
│   ├── Financial_Transactions.csv          # исходный датасет
│   ├── Financial_Transactions_Enriched.csv # обогащённый датасет (~37 500 строк)
│   ├── Enrichment.ipynb                    # обогащение и подготовка датасета
│   ├── ClickHouse_filling.ipynb            # наполнение справочников
│   ├── Dumping.ipynb                       # дамп таблиц в CSV
│   ├── Restoration.ipynb                   # восстановление из дампа
│   ├── dumps/                              # дампы таблиц для быстрого старта
│   └── requirements.txt                    # зависимости для Jupyter
├── docker-compose.yaml
├── Dockerfile                              # образ для Airflow
├── .env                                    # переменные окружения (не в git)
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

Заполнить секреты в `.env` (пароли, ключи).

### 2. Поднять все сервисы

```bash
docker-compose up -d
```

Порядок старта управляется через `depends_on` — сервисы поднимаются в правильном порядке автоматически.

### 3. Дождаться инициализации ClickHouse

При первом старте ClickHouse выполняет все DDL скрипты из `clickhouse/init/`. Занимает ~5 минут. Проверить готовность:

```bash
docker logs clickhouse --tail 20
```

### 4. Наполнить справочники

Открыть Jupyter на `http://localhost:8888` (токен из `.env`), запустить:

Скрипт инициализация ядра
```
init_kernel.sh
```

Скрипт заполнения словарей
```
ClickHouse_filling.ipynb → запустить все ячейки
```

### 5. Восстановить данные из дампа

В Jupyter запустить `Restoration.ipynb` — загрузит курсы валют и накопленные транзакции без ожидания стрима.


### 6. Запустить DAG'и в Airflow

Airflow доступен на `http://localhost:8081` (admin/admin).

Запустить DAG'и вручную для первоначального наполнения витрин:

| DAG | Расписание | Назначение |
|-----|-----------|------------|
| `load_currency_rates` | `5 9 * * *` | Курсы валют ЦБ РФ |
| `antifraud_daily` | `0 1 * * *` | Антифрод витрина |
| `limits_daily` | `0 1 * * *` | Дневные лимиты |
| `limits_monthly` | `0 1 2 * *` | Месячные лимиты (2-е число) |
| `analitics_daily` | `0 1 * * *` | Аналитическая витрина |

Для получения расчета полного периода рекомендуется прогнать расчет через бэкфил
```
docker exec -it airflow-scheduler airflow dags backfill \
    <Интересующий вас проект> \
    --start-date <Дата с> \
    --end-date <Дата по>
```

### 7. Подключить ClickHouse в Superset

Superset доступен на `http://localhost:8088`.

**Settings → Database Connections → + Database → ClickHouse Connect**

```
Host:     clickhouse
Port:     8123
Database: default
Username: default
Password: (пусто)
```

---

## Топики Kafka

| Топик | Партиции | Retention |
|-------|----------|-----------|
| `transactions.valid` | 3 | 3 GB |
| `transactions.invalid` | 3 | 3 GB |

---

## Таблицы ClickHouse

| Таблица | Движок | Назначение |
|---------|--------|------------|
| `currency_rates` | ReplacingMergeTree | Курсы валют ЦБ РФ |
| `kafka_transactions_valid` | Kafka Engine | Буфер чтения из `transactions.valid` |
| `kafka_transactions_invalid` | Kafka Engine | Буфер чтения из `transactions.invalid` |
| `transactions_valid` | ReplacingMergeTree | Обогащённые валидные транзакции |
| `transactions_invalid` | ReplacingMergeTree | Невалидные транзакции |
| `dm_antifraud_daily` | ReplacingMergeTree | Витрина антифрода |
| `dm_daily_limits` | ReplacingMergeTree | Витрина дневных лимитов |
| `dm_monthly_limits` | ReplacingMergeTree | Витрина месячных лимитов |
| `dm_analytics_daily` | ReplacingMergeTree | Аналитическая витрина |

---

## Справочники ClickHouse

| Справочник | Описание |
|------------|----------|
| `dict_merchant_category` | Категории мерчантов с `risk_score` и флагом `is_online` |
| `dict_country` | Страны с регионом и `risk_level` |
| `dict_account_level` | Уровни аккаунтов с лимитами в рублях (`daily_limit_rub`, `monthly_limit_rub`) |

---

## Валидация транзакций (Pydantic)

FastAPI валидирует каждую транзакцию перед отправкой в Kafka:

- `0 < amount < account_balance`
- `currency` ∈ `[USD, EUR, RUB, GBP]`
- `timestamp` ≤ текущее время

Валидные → `transactions.valid` → `transactions_valid`
Невалидные → `transactions.invalid` → `transactions_invalid`

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
