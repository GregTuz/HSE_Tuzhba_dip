import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date

from airflow.decorators import dag, task
from clickhouse_driver import Client
from airflow.models.param import Param


log = logging.getLogger(__name__)

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000

TARGET_CURRENCIES = {"USD", "EUR", "GBP", "RUB"}


def get_clickhouse_client() -> Client:
	return Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)


@dag(
	dag_id="load_currency_rates",
	schedule="5 9 * * *",
	start_date=datetime(2025, 1, 1),
	catchup=True,
	params={
		"logical_date": Param(
			default=None,
			type=["null", "string"],
			description="Дата для ретро-запуска в формате YYYY-MM-DD. Если не указана — берётся дата рана.",
		)
	},
	tags=["reference", "cbr"],
)
def load_currency_rates():

	@task
	def fetch_rates_from_cbr(**context) -> list[dict]:
		dag_run = context["dag_run"]
		conf = dag_run.conf or {}

		if "logical_date" in conf:
			logical_date = date.fromisoformat(conf["logical_date"])
		else:
			logical_date = context["data_interval_end"].date()

		url = "https://www.cbr.ru/scripts/XML_daily.asp"
		params = {"date_req": logical_date.strftime("%d/%m/%Y")}
		response = requests.get(url, params=params, timeout=15)
		response.encoding = "windows-1251"

		root = ET.fromstring(response.text)
		dt = logical_date.strftime("%Y-%m-%d")
		result = [{
			"dt": dt,
			"currency": "RUB",
			"rate_to_rub": 1.0,
			"nominal": 1,
		}]

		for valute in root.findall("Valute"):
			char_code = valute.find("CharCode").text

			if char_code not in TARGET_CURRENCIES:
				continue

			nominal = int(valute.find("Nominal").text)
			value   = float(valute.find("Value").text.replace(",", "."))

			result.append({
				"dt": dt,
				"currency": char_code,
				"rate_to_rub": round(value / nominal, 6),
				"nominal": nominal,
			})

		log.info("Получено курсов: ", len(result))
		for r in result:
			log.info(f"  {r['currency']}: {r['rate_to_rub']} р",)

		return result

	@task
	def save_rates_to_clickhouse(rates: list[dict]) -> None:
		client = get_clickhouse_client()

		rows = [
			(date.fromisoformat(r["dt"]), r["currency"], r["rate_to_rub"], r["nominal"])
			for r in rates
		]

		client.execute(
			"""
			INSERT INTO currency_rates
				(dt, currency, rate_to_rub, nominal)
			VALUES
			""",
			rows,
		)

		log.info("Записано строк ", len(rows))

	@task
	def verify_rates(**context) -> None:
		logical_date = context["logical_date"].date()
		client = get_clickhouse_client()

		result = client.execute("""
            SELECT
                dt,
                currency,
                rate_to_rub
            FROM currency_rates
            WHERE dt = today()
            ORDER BY currency
        """, {"dt": logical_date})

		if not result:
			raise ValueError("Сегоднящняя партиция пуста")

		log.info("Курсы за сегодня в ClickHouse:")
		for dt, currency, rate in result:
			log.info(f"  {currency}: {rate} р",)

	rates = fetch_rates_from_cbr()
	save_rates_to_clickhouse(rates) >> verify_rates()


load_currency_rates()