import logging

import pendulum
from airflow.decorators import dag, task

from cdm.settlement_loader import SettlementLoader
from cdm.cdm_courier_ledger import DmCourierLoader

from lib import ConnectionBuilder
log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True,  # Остановлен/запущен при появлении. Сразу запущен.

)
def project5_cdm_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="settlement_load_task")
    def load_settlement_task():
        rest_loader = SettlementLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="courierleder_load_task")
    def load_courierleder_task():
        rest_loader = DmCourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываю функцию, которая перельет данные.

    # Инициализируем объявленные таски.

    res_settlement = load_settlement_task()
    res_courierleder = load_courierleder_task()


    res_settlement >> res_courierleder


cdm_load_dag = project5_cdm_load_dag()