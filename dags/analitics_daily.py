import logging
from datetime import datetime, date, timedelta

from airflow.decorators import dag, task
from clickhouse_driver import Client


log = logging.getLogger(__name__)

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000


def get_clickhouse_client() -> Client:
	return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


@dag(
	dag_id="dm_analytics_daily",
	schedule="0 1 * * *",
	start_date=datetime(2016, 1, 2),
	catchup=True,
	max_active_runs=50,
	tags=["mart", "analytics"],
)
def dm_analytics_daily():

	@task
	def compute(**context) -> None:
		dt: date = context["data_interval_end"].date() - timedelta(days=1)
		log.info(f"Считаем аналитику за: {dt}")

		client = get_clickhouse_client()

		client.execute(
			"ALTER TABLE dm_analytics_daily DROP PARTITION %(partition)s",
			{"partition": dt.strftime("%Y%m%d")}
		)

		query = """
			SELECT
				account_level,
				merchant_category,
				category_name,
				country_code,
				country_name,
				region,
				transaction_date,
				toHour(timestamp) AS hour,
				toDayOfWeek(timestamp) AS day_of_week,
				count() AS transaction_count,
				sum(amount_rub) AS total_amount_rub,
				avg(amount_rub) AS avg_amount_rub
			FROM transactions_valid
			WHERE transaction_date = %(dt)s
			GROUP BY
				account_level,
				merchant_category,
				category_name,
				country_code,
				country_name,
				region,
				transaction_date,
				hour,
				day_of_week
			ORDER BY transaction_date, hour
			"""

		print("Скрипт - ", query)

		rows = client.execute(
			query,
			{"dt": dt}
		)

		client.execute(
			"""
			INSERT INTO dm_analytics_daily (
				account_level, merchant_category, category_name,
				country_code, country_name, region,
				transaction_date, hour, day_of_week,
				transaction_count, total_amount_rub, avg_amount_rub
			) VALUES
			""",
			rows
		)
		log.info(f"Записано строк: {len(rows)}")

	@task
	def verify(**context) -> None:
		dt: date = context["data_interval_end"].date() - timedelta(days=1)
		client = get_clickhouse_client()

		result = client.execute(
			"""
			SELECT
				count() AS rows,
				sum(transaction_count) AS total_transactions,
				round(sum(total_amount_rub), 2) AS total_turnover
			FROM dm_analytics_daily
			WHERE transaction_date = %(dt)s
			""",
			{"dt": dt}
		)
		rows, transactions, turnover = result[0]
		log.info(f"За {dt}: {rows} строк, {transactions} транзакций, оборот {turnover} руб")

	compute() >> verify()


dm_analytics_daily()