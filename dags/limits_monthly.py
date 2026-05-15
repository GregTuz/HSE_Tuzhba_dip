import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

from airflow.decorators import dag, task
from clickhouse_driver import Client


log = logging.getLogger(__name__)

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000


def get_clickhouse_client() -> Client:
	return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


@dag(
	dag_id="dm_monthly_limits",
	schedule="0 1 2 * *",
	start_date=datetime(2015, 12, 31),
	catchup=True,
	max_active_runs=5,
	tags=["mart", "limits"],
)
def dm_monthly_limits():

	@task
	def compute(**context) -> None:
		run_date: date = context["data_interval_end"].date()
		month_end: date = run_date.replace(day=1)
		month_start: date = (month_end - relativedelta(months=1))

		log.info(f"data_interval_end: {context['data_interval_end']}")
		log.info(f"run_date: {run_date}")
		log.info(f"month_end: {month_end}")
		log.info(f"month_start: {month_start}")

		log.info(f"Ищем подозрительные аккаунты за: {month_start} — {month_end}")

		client = get_clickhouse_client()

		client.execute(
			"ALTER TABLE dm_monthly_limits DROP PARTITION %(partition)s",
			{"partition": month_start.strftime("%Y%m")}
		)

		rows = client.execute(
			"""
			WITH monthly AS (
				SELECT
					account_id,
					toStartOfMonth(transaction_date) AS month,
					account_level,
					sum(amount_rub) AS monthly_turnover_rub,
					any(monthly_limit_rub) AS monthly_limit_rub
				FROM transactions_valid
				WHERE transaction_date >= %(month_start)s
				  AND transaction_date < %(month_end)s
				GROUP BY account_id, month, account_level
			)
			SELECT
				account_id,
				month,
				account_level,
				monthly_turnover_rub,
				monthly_limit_rub,
				1 AS is_exceeded
			FROM monthly
			WHERE monthly_turnover_rub > toFloat64(monthly_limit_rub)
			""",
			{"month_start": month_start, "month_end": month_end}
		)

		client.execute(
			"""
			INSERT INTO dm_monthly_limits
				(account_id, month, account_level, monthly_turnover_rub, monthly_limit_rub, is_exceeded)
			VALUES
			""",
			rows
		)
		log.info(f"Записано строк: {len(rows)}")

	@task
	def verify(**context) -> None:
		run_date: date = context["data_interval_end"].date()
		month_start: date = (run_date - relativedelta(months=1)).replace(day=1)
		client = get_clickhouse_client()

		result = client.execute(
			"SELECT count() FROM dm_monthly_limits WHERE month = %(month)s",
			{"month": month_start}
		)
		log.info(f"Превышений месячного лимита за {month_start.strftime('%Y-%m')}: {result[0][0]}")

	compute() >> verify()


dm_monthly_limits()