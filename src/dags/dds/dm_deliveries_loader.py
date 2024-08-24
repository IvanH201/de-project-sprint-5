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
    delivery_id: str
    order_id: int
    address: str
    delivery_ts: datetime

class DmDeliveriesRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, _threshold: int, limit: int) -> List[DmDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DmDeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        t.id, 
                        t.delivery_id, 
                        (select id from dds.dm_orders where dds.dm_orders.order_key = t.order_id) order_id,
                        t.address,
                        t.delivery_ts
                    FROM stg.deliverysystem_deliveries t
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                        AND exists ( select id from dds.dm_orders where dds.dm_orders.order_key = t.order_id )
                    ORDER BY t.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем пачку объектов.
                """, {
                    "threshold": _threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmDeliveriesDestRepository:

    def insert_dm_deliveries(self, conn: Connection, dm_deliveries: DmDeliveryObj) -> None:
        # Сюда данные попадают уже в формате DmDeliveryDestObj
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, delivery_ts, order_id, address)
                    VALUES (%(delivery_id)s, %(delivery_ts)s, %(order_id)s, %(address)s )
                    ON CONFLICT (id) DO NOTHING;

                """,
                {
                    "delivery_id": dm_deliveries.delivery_id,
                    "delivery_ts": dm_deliveries.delivery_ts,
                    "order_id": dm_deliveries.order_id,
                    "address": dm_deliveries.address
                },
            )


class DmDeliveriesLoader:
    WF_KEY = "dds_dm_deliveries_workflow"
    LAST_LOADED_ID_KEY = "last_load_id"
    BATCH_LIMIT = 50  # Загружаем пачками
    SHEMA_TABLE = 'dds.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmDeliveriesRepository(pg_origin)
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
            self.log.info(f"Found {len(load_queue)} dm_deliveries to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for row in load_queue:
                try:
                    self.dds.insert_dm_deliveries(conn, row)
                except Exception as err:
                    print(row)
                    print("Error = ", err)
                    raise

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


