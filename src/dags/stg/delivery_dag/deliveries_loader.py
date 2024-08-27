from logging import Logger
from typing import List
from datetime import datetime

from psycopg import Connection
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str

from stg.delivery_dag.api_reader import ApiConnect
from lib.settings_repository import EtlSetting, EtlSettingsRepository
import datetime as dt
from airflow.operators.python import get_current_context

class DeliveryObj(BaseModel):
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: int
    sum: float
    tip_sum: float


class DeliverysOriginRepository:
    def __init__(self) -> None:
        pass

    def list_deliverys(self, sort: str, threshold: int, limit: int,  from_ts: dt.datetime) -> List[DeliveryObj]:
        x = ApiConnect('deliveries', sort, limit, threshold,from_ts)

        return x.list_data()


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_deliveries
                        (order_id, 
                        order_ts, 
                        delivery_id, 
                        courier_id, 
                        address, 
                        delivery_ts, 
                        rate, 
                        sum, 
                        tip_sum)
                    VALUES (%(order_id)s, 
                            %(order_ts)s, 
                            %(delivery_id)s, 
                            %(courier_id)s, 
                            %(address)s, 
                            %(delivery_ts)s, 
                            %(rate)s, 
                            %(sum)s, 
                            %(tip_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "order_id": delivery["order_id"],
                    "order_ts": delivery["order_ts"],
                    "delivery_id": delivery["delivery_id"],
                    "courier_id": delivery["courier_id"],
                    "address": delivery["address"],
                    "delivery_ts": delivery["delivery_ts"],
                    "rate": delivery["rate"],
                    "sum": delivery["sum"],
                    "tip_sum": delivery["tip_sum"]
                },
            )


class DeliverysLoader:
    WF_KEY = "stg_deliverys_system_delivery_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    LAST_LOADED_ID = "last_loaded_id"
    BATCH_LIMIT = 50 # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.
    SHEMA_TABLE = 'stg.srv_wf_settings'

    def __init__(self,pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliverysOriginRepository()
        self.stg = DeliveryDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log
        self.context = get_current_context()
        self.current_ts = self.context["logical_date"]  # Определяем дату для выгрузки данных за isoformat

        self.log.info(f"current_ts= {self.current_ts} ")

    def load_deliverys(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:
            #self.log.info(f"self.current_ts ={self.current_ts }")
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)

            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID:0, self.LAST_LOADED_TS_KEY: "2022-07-19 18:30:00"})

            # Вычитываем очередную пачку объектов.
           # current_ts = dt.datetime.fromisoformat(self.current_ts).replace(tzinfo=None)
            from_current_ts = (self.current_ts + dt.timedelta(days=-7))
            from_datetime = from_current_ts.strftime('%Y-%m-%d %H:%M:%S%Z')

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID]
            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = dt.datetime.fromisoformat(last_loaded_ts_str).replace(tzinfo=None)
            last_loaded_datetime = last_loaded_ts.strftime('%Y-%m-%d %H:%M:%S%Z')

            if (last_loaded_datetime < from_datetime):
                load_queue = self.origin.list_deliverys(sort="order_ts", limit=self.BATCH_LIMIT,threshold=last_loaded,
                                                        from_ts=from_datetime)
            else:
                load_queue = self.origin.list_deliverys(sort="order_ts", limit=self.BATCH_LIMIT,threshold=last_loaded,
                                                        from_ts=last_loaded_datetime)

            self.log.info(f"Found {len(load_queue)} deliveries to load.")


            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.

            for delivery in load_queue:
                    self.stg.insert_delivery(conn, delivery)


            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.

            ts = max([t['order_ts'] for t in load_queue])
            datetimeObj = dt.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = dt.datetime.strftime(datetimeObj,
                                                                                         '%Y-%m-%d %H:%M:%S')

            wf_setting.workflow_settings[self.LAST_LOADED_ID] = last_loaded + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.

            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")