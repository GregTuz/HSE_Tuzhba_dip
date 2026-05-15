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
	dag_id="dm_daily_limits",
	schedule="0 1 * * *",
	start_date=datetime(2015, 12, 31),
	catchup=True,
	max_active_runs=50,
	tags=["mart", "limits"],
)
def dm_daily_limits():

	@task
	def compute(**context) -> None:
		print(f"CONTEXT KEYS: {list(context.keys())}")
		print(f"data_interval_end: {context.get('data_interval_end')}")
		print(f"data_interval_start: {context.get('data_interval_start')}")
		dt: date = context["data_interval_end"].date() - timedelta(days=1)
		print(f"DT = {dt}")
		log.info(f"Считаем дневные лимиты за: {dt}")

		client = get_clickhouse_client()

		client.execute(
			"ALTER TABLE dm_daily_limits DROP PARTITION %(partition)s",
			{"partition": dt.strftime("%Y%m%d")}
		)

		rows = client.execute(
			"""
			WITH daily AS (
				SELECT
					account_id,
					transaction_date AS dt,
					account_level,
					sum(amount_rub) AS daily_turnover_rub,
					any(daily_limit_rub) AS daily_limit_rub
				FROM transactions_valid
				WHERE transaction_date = %(dt)s
				GROUP BY account_id, transaction_date, account_level
			)
			SELECT
				account_id,
				dt,
				account_level,
				daily_turnover_rub,
				daily_limit_rub,
				1 AS is_exceeded
			FROM daily
			WHERE daily_turnover_rub > toFloat64(daily_limit_rub)
			""",
			{"dt": dt}
		)

		client.execute(
			"""
			INSERT INTO dm_daily_limits
				(account_id, dt, account_level, daily_turnover_rub, daily_limit_rub, is_exceeded)
			VALUES
			""",
			rows
		)
		log.info(f"Записано строк: {len(rows)}")

	@task
	def verify(**context) -> None:
		dt: date = context["data_interval_end"].date() - timedelta(days=1)
		client = get_clickhouse_client()

		result = client.execute(
			"SELECT count() FROM dm_daily_limits WHERE dt = %(dt)s",
			{"dt": dt}
		)
		log.info(f"Превышений дневного лимита за {dt}: {result[0][0]}")

	compute() >> verify()


dm_daily_limits()