from logging import Logger
from typing import List, Union
from datetime import datetime

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str

from lib.settings_repository import EtlSetting, EtlSettingsRepository


class DmDeliveryObj(BaseModel):
    id: int
    courier_id: str
    delivery_id: str
    order_id: str
    order_ts: datetime
    rate: float
    delivery_sum: float
    tip_sum: float

class DmDeliveriesRepository:
    def __init__(self, pg: PgConnect,log: Logger) -> None:
        self._db = pg
        self.log = log

    def list_deliveries(self, _threshold: str, limit: int) ->List[DmDeliveryObj]:
        self.log.info(f"limit= {limit}")
        with self._db.client().cursor(row_factory=class_row(DmDeliveryObj)) as cur:
            cur.execute(
                """
                    WITH dmord AS (
                                    SELECT id, order_key
                                    FROM dds.dm_orders
                                    ),
                        dmcour AS (
                                    SELECT id, courier_id
                                    FROM dds.dm_couriers dc 
                                    )
                    SELECT 
                        dd.id, 
                         (select id from dds.dm_deliveries dd2 where dd2.delivery_id=dd.delivery_id)delivery_id,  
                        dmord.id AS order_id,
                        order_ts,
                        dmcour.id AS courier_id,
                        rate,
                        sum as delivery_sum,
                        tip_sum
                    FROM stg.deliverysystem_deliveries dd
                    LEFT JOIN dmord ON dd.order_id = dmord.order_key
                    LEFT JOIN dmcour ON dd.courier_id = dmcour.courier_id
                    WHERE dd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                            and dmord.id is NOT null
                    ORDER BY dd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": _threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        self.log.info(f"len= {len(objs)}")
        return objs


class DmDeliveriesDestRepository:
    def insert_object(self, conn: Connection, delivery: DmDeliveryObj) -> None:
        # Сюда данные попадают уже в формате DmDeliveryDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(courier_id, delivery_id, order_id, order_ts, rate, delivery_sum, tip_sum)
                    VALUES (
                        %(courier_id)s,
                        %(delivery_id)s, 
                        %(order_id)s,
                        %(order_ts)s,
                        %(rate)s,
                        %(delivery_sum)s,
                        %(tip_sum)s                       
                    )
                    ON CONFLICT (id) DO NOTHING;
                """,
                {
                    "courier_id": delivery.courier_id,
                    "delivery_id": delivery.delivery_id,
                    "order_id": delivery.order_id,
                    "order_ts": delivery.order_ts,
                    "rate": delivery.rate,
                    "delivery_sum": delivery.delivery_sum,
                    "tip_sum": delivery. tip_sum
                },
            )


class FctDeliveriesLoader:
    WF_KEY = "dds_fct_deliveries_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 50  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmDeliveriesRepository(pg_origin,log)
        self.dds = DmDeliveriesDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log


    def data_load(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            self.log.info(f"wf_setting= {wf_setting}")
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"last_loaded= {last_loaded}")
            self.log.info(f"Found {len(load_queue)} fct_deliveries to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                self.dds.insert_object(conn, row)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


