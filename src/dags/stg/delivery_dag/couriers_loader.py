from logging import Logger
from typing import List

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str

from stg.delivery_dag.api_reader import ApiConnect
from lib.settings_repository import EtlSetting, EtlSettingsRepository


class CourierObj(BaseModel):
    courier_id: str
    name: str


class CouriersOriginRepository:
    def __init__(self) -> None:
        pass

    def list_couriers(self, sort: str, threshold: int, limit: int) -> List[CourierObj]:
        x = ApiConnect('couriers', sort, limit, threshold)
        x.client()

        return x.client()


class CourierDestRepository:
    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliverysystem_couriers(courier_id, name)
                    VALUES (%(_id)s, %(name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        name = EXCLUDED.name;
                """,
                {
                    "_id": courier['_id'],
                    "name": courier['name']
                },
            )


class CouriersLoader:
    WF_KEY = "stg_deliverys_system_couriers_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 50  # инкрементальная загрузка рангов.
    SHEMA_TABLE = 'stg.srv_wf_settings'

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersOriginRepository()
        self.stg = CourierDestRepository()
        self.settings_repository = EtlSettingsRepository(self.SHEMA_TABLE)
        self.log = log

    def load_couriers(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            # Вычитываем очередную пачку объектов.

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_couriers(sort="id", threshold=last_loaded, limit=self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                self.stg.insert_courier(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded + len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.

            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")