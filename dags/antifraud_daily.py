import logging
from datetime import datetime, date, timedelta

from airflow.decorators import dag, task
from clickhouse_driver import Client


log = logging.getLogger(__name__)

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000

ATM_THRESHOLD = 2
TRANSFER_THRESHOLD = 2
HIGH_FREQUENCY_THRESHOLD = 5


def get_clickhouse_client() -> Client:
	return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


@dag(
	dag_id="dm_antifraud_daily",
	schedule="0 1 * * *",
	start_date=datetime(2015, 12, 31),
	catchup=True,
	max_active_runs=50,
	tags=["mart", "antifraud"],
	params={"dt": ""}
)
def dm_antifraud_daily():

	@task
	def compute_antifraud(**context) -> None:
		conf = context.get("params", {})
		if conf.get("dt"):  # проверяем что строка непустая
			dt = date.fromisoformat(conf["dt"])
		else:
			dt = context["data_interval_end"].date() - timedelta(days=1)
		log.info(f"Ищем подозрительные аккаунты за дату: {dt}")

		client = get_clickhouse_client()

		client.execute(
			"ALTER TABLE dm_antifraud_daily DROP PARTITION %(partition)s",
			{"partition": dt.strftime("%Y%m%d")}
		)

		query = """
		    WITH atm AS (
			    SELECT account_id, 'Слишком много снятий наличных' AS reason, transaction_date
			    FROM transactions_valid
			    WHERE transaction_date = %(dt)s
			      AND merchant_category = 'ATM'
			    GROUP BY account_id, transaction_date
			    HAVING count() >= %(ATM_THRESHOLD)s
			),
			transfer AS (
			    SELECT account_id, 'Слишком много повторяющихся переводов' AS reason, transaction_date
			    FROM transactions_valid
			    WHERE transaction_date = %(dt)s
			      AND merchant_category = 'personal_transfer'
			    GROUP BY account_id, amount, transaction_date
			    HAVING count() >= %(TRANSFER_THRESHOLD)s
			),
			high_frequency AS (
			    SELECT account_id, 'Подозрительно высокая частота транзакций' AS reason, transaction_date
			    FROM transactions_valid
			    WHERE transaction_date = %(dt)s
			    GROUP BY account_id, transaction_date
			    HAVING count() >= %(HIGH_FREQUENCY_THRESHOLD)s
			),
			all_suspects AS (
			    SELECT * FROM atm
			    UNION ALL
			    SELECT * FROM transfer
			    UNION ALL
			    SELECT * FROM high_frequency
			)
			SELECT
			    account_id,
			    transaction_date AS dt,
			    1 AS is_suspicious,
			    reason
			FROM all_suspects
		"""

		result = client.execute(
			query,
			{
				"dt": dt,
				"ATM_THRESHOLD": ATM_THRESHOLD,
				"TRANSFER_THRESHOLD": TRANSFER_THRESHOLD,
				"HIGH_FREQUENCY_THRESHOLD": HIGH_FREQUENCY_THRESHOLD
			}
		)
		print(f"Найдено {len(result)} подозрительных аккаунтов")


		client.execute(
			"INSERT INTO dm_antifraud_daily (account_id, dt, is_suspicious, reason) VALUES",
			result
		)
		log.info(f"Записано строк: {len(result)}")

	@task
	def verify(**context) -> None:
		dt: date = context["data_interval_end"].date() - timedelta(days=1)
		print('Проверяем за ', dt)
		client = get_clickhouse_client()

		result = client.execute(
			"SELECT count() FROM dm_antifraud_daily WHERE dt = %(dt)s",
			{"dt": dt.strftime("%Y%m%d")}
		)
		count = result[0][0]
		log.info(f"Строк в витрине за {dt}: {count}")

		if count == 0:
			log.info("Подозрительных аккаунтов за дату не найдено")

		sample = client.execute(
			"""
			SELECT account_id, reason
			FROM dm_antifraud_daily
			WHERE dt = %(dt)s
			LIMIT 5
			""",
			{"dt": dt.strftime("%Y%m%d")}
		)
		for account_id, reason in sample:
			log.info(f"  account_id={account_id}: {reason}")

	compute_antifraud() >> verify()


dm_antifraud_daily()