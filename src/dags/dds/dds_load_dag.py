import logging

import pendulum
from airflow.decorators import dag, task

from dds.dm_users_loader import DmUserLoader
from dds.dm_restaurants_loader import DmRestaurantLoader
from dds.dm_timestamps_loader import DmTimestampLoader
from dds.dm_products_loader import DmProductLoader
from dds.dm_orders_loader import DmOrderLoader
from dds.dm_couriers_loader import DmCourierLoader
from dds.dm_deliveries_loader import DmDeliveriesLoader
from dds.fct_product_sales_loader import ProductSalesLoader
from dds.fct_deliveries_loader import FctDeliveriesLoader

from lib import ConnectionBuilder
log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 7, 1, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True,  # Остановлен/запущен при появлении. Сразу запущен.

)
def project5_dds_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_dm_users_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmUserLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load("dm_users_load")  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_restaurants_load")
    def load_dm_restaurant_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmRestaurantLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_restaurant_data("dm_restaurants_load")  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_timestamps_load")
    def load_dm_timestamp_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmTimestampLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_products_load")
    def load_dm_product_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmProductLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_orders_load")
    def load_dm_order_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmOrderLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_couriers_load")
    def load_dm_courier_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmCourierLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries_data():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.


    @task(task_id="fct_product_sales")
    def load_fct_product_sales():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = ProductSalesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_data()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="fct_deliveries")
    def load_fct_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = FctDeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.data_load()  # Вызываем функцию, которая перельет данные.
    # Инициализируем объявленные таски.

    dm_user_dict = load_dm_users_data()
    dm_restaurant_dict = load_dm_restaurant_data()
    dm_timestamp_dict = load_dm_timestamp_data()
    dm_product_dict = load_dm_product_data()
    dm_order_dict = load_dm_order_data()
    dm_couriers = load_dm_courier_data()
    dm_deliveries_dict = load_dm_deliveries_data()
    fct_product_sales = load_fct_product_sales()
    fct_deliveries = load_fct_deliveries()

    (
       [dm_user_dict, dm_restaurant_dict, dm_timestamp_dict, dm_couriers, dm_deliveries_dict] >>
       dm_product_dict >>
       dm_order_dict >>
       [fct_product_sales, fct_deliveries]
    )

dds_load_dag = project5_dds_load_dag()