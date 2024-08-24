Проектная работа по DWH для нескольких источников

1. Цели и задачи проекта

Цель проекта: Формирование многослойного хранилища данных по заказам из ресторанов и работе курьеров по доставке данных заказов.

Назначение проекта: Проект предназначен для модификации текущего хранилища данных в части добавления нового источника данных и новой витрины.

Задачи проекта:

Усовершенствовать хранилище данных, добавив новый источник в слой stg.
Связать данные из нового источника с данными в хранилище.
Реализовать витрину для расчётов с курьерами.
2. Требования к проекту 2.1. Общие требования

Данные должны собираться из трех источников:
данные о заказах из документоориентированной БД Mongodb;
данные об оплате заказов бонусами из реляционной БД Postgresql;
данные о работе курьерской службы (API).
DWH хранилище должно состоять из следующих слоев:
stg (слой сырых данных);
dds (слой детальных данных) модель данных "снежинка";
cdm (слой витрин).
Должен быть реализован механизм инкрементальной загрузки данных при помощи технических таблиц srv_wf_settings для каждого слоя DWH.
Должен быть реализован механизм загрузки данных по средствам Airflow, путем создания нескольких DAG.
Учесть возможное появление дубликатов.
Учесть изменение формата данных.
2.2. Требования к витрине

Витрина, содержащая информацию о выплатах курьерам должна отвечать следующим требованиям:

Состав витрины:
id — идентификатор записи.
courier_id — ID курьера, которому перечисляем.
courier_name — Ф. И. О. курьера.
settlement_year — год отчёта.
settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
orders_count — количество заказов за период (месяц).
orders_total_sum — общая стоимость заказов.
rate_avg — средний рейтинг курьера по оценкам пользователей.
order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).
Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:
r < 4 — 5% от заказа, но не менее 100 р.;
4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
4.9 <= r — 10% от заказа, но не менее 200 р.
Отчёт собирается по дате заказа. Если заказ был сделан ночью и даты заказа и доставки не совпадают, в отчёте стоит ориентироваться на дату заказа, а не дату доставки. Так как заказы, сделанные ночью до 23:59, доставляют на следующий день: дата заказа и доставки не совпадёт. При этом такие случаи могут выпадать в том числе и на последний день месяца. Тогда начисление курьеру относите к дате заказа, а не доставки.
3. Реализация проекта

Спроектирован механиз инкрементальной загрузки данных при помощи технических таблиц srv_wf_settings для каждого слоя DWH. Каждая следующая загрузка осуществляется исходя из метки предыдущей загрузки

3.1. Выполнен анализ API системы доставки заказов

Выполнен анализ API и текущего состава таблиц. Результат приведен в файле api_entities.md.

Спецификация API

GET /restaurants Метод /restaurants возвращает список доступных ресторанов (_id, name). curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}'
--header 'X-Nickname: {{ your_nickname }}'
--header 'X-Cohort: {{ your_cohort_number }}'
--header 'X-API-KEY: {{ api_key }}'

В качестве переменных заголовка, указанных в {{ }}, передайте ваши актуальные данные.

Описание параметров запроса, указанных в {{ }} :

• sort_field — необязательный параметр. Возможные значения: id, name. Значение по умолчанию: id.

Параметр определяет поле, к которому будет применяться сортировка, переданная в параметре sort_direction.

• sort_direction — необязательный параметр. Возможные значения: asc, desc.

Параметр определяет порядок сортировки для поля, переданного в sort_field:

• asc — сортировка по возрастанию,

• desc — сортировка по убыванию.

• limit — необязательный параметр. Значение по умолчанию: 50. Возможные значения: целое число в интервале от 0 до 50 включительно.

Параметр определяет максимальное количество записей, которые будут возвращены в ответе.

• offset — необязательный параметр. Значение по умолчанию: 0.

Параметр определяет количество возвращаемых элементов результирующей выборки, когда формируется ответ.

Метод возвращает список ресторанов. Каждый ресторан списка содержит:

• _id — ID задачи;

• name — название ресторана.

GET /couriers

Метод /couriers используется для того, чтобы получить список курьеров с учётом фильтров, переданных в запросе.

curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ offset }}'
--header 'X-Nickname: {{ your_nickname }}'
--header 'X-Cohort: {{ your_cohort_number }}'
--header 'X-API-KEY: {{ api_key }}'

Параметры /couriers аналогичны параметрам в предыдущем методе: sort_field, sort_direction, limit, offset.

Метод возвращает список курьеров. Каждый элемент списка содержит:

• _id — ID курьера в БД;

• name — имя курьера.

GET / deliveries

Метод /deliveries используется для того, чтобы получить список совершённых доставок с учётом фильтров, переданных в запросе.

curl --location --request GET 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id={{ restaurant_id }}&from={{ from }}&to={{ to }}&sort_field={{ sort_field }}&sort_direction={{ sort_direction }}&limit={{ limit }}&offset={{ limit }}'
--header 'X-Nickname: {{ your_nickname }}'
--header 'X-Cohort: {{ your_cohort_number }}'
--header 'X-API-KEY: {{ api_key }}'

Параметры метода также включают sort_direction, limit, offset и несколько дополнительных полей:

• sort_field — необязательный параметр. Возможные значения: _id, date. Если указать _id, то будет применена сортировка по ID заказа (order_id). Если указать date, то будет применена сортировка по дате создания заказа (order_ts).

• restaurant_id — ID ресторана. Если значение не указано, то метод вернёт данные по всем доступным в БД ресторанам.

• from — параметр фильтрации. В выборку попадают заказы с датой доставки, которая больше или равна значению from. Дата должна быть в формате '%Y-%m-%d %H:%M:%S’, например, 2022-01-01 00:00:00.

• to — параметр фильтрации. В выборку попадают заказы с датой доставки меньше значения to.

Метод возвращает список совершённых доставок с учётом фильтров в запросе. Каждый элемент списка содержит:

• order_id — ID заказа;

• order_ts — дата и время создания заказа;

• delivery_id — ID доставки;

• courier_id — ID курьера;

• address — адрес доставки;

• delivery_ts — дата и время совершения доставки;

• rate — рейтинг доставки, который выставляет покупатель: целочисленное значение от 1 до 5;

• tip_sum — сумма чаевых, которые оставил покупатель курьеру (в руб.).

Результат анализа

Используем только GET /couriers и GET / deliveries. Информация из GET /restaurants в БД уже загружена.

3.2. Обеспечен доступ к источникам данных содержащих информацию:

- о заказах. Подключение к документоориентированной БД Mongodb для закачки необходимых данных из таблиц: orders, restaurants, users;
- об оплате заказов бонусами (подключение к БД PostgreSQL для закачки необходимых данных), а именно схема public, таблицы: outbox, ranks, users;
- о работе курьерской службы (подключение к API для закачки необходимых данных).
3.3. Структура витрины расчётов с курьерами

На основе изученных данных и вводных о составе витрины составлен DDL-запрос для создания витрины cdm.dm_courier_ledger.

CREATE TABLE cdm.dm_courier_ledger (

id serial4 NOT NULL,

courier_id int4 NOT NULL,

courier_name varchar NOT NULL,

settlement_year int4 NOT NULL,

settlement_month int4 NOT NULL,

orders_count int4 DEFAULT 0 NOT NULL,

orders_total_sum numeric(14, 2) DEFAULT 0 NOT NULL,

rate_avg float8 NOT NULL,

order_processing_fee numeric DEFAULT 0 NOT NULL,

courier_order_sum numeric(14, 2) DEFAULT 0 NOT NULL,

courier_tips_sum numeric(14, 2) DEFAULT 0 NOT NULL,

courier_reward_sum numeric(14, 2) DEFAULT 0 NOT NULL,

CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK ((courier_order_sum >= (0)::numeric)),

CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK ((courier_reward_sum >= (0)::numeric)),

CONSTRAINT dm_courier_ledger_courier_settlement_year_month_unique UNIQUE (courier_id, settlement_year, settlement_month),

CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK ((courier_tips_sum >= (0)::numeric)),

CONSTRAINT dm_courier_ledger_orders_count_check CHECK ((orders_count >= 0)),

CONSTRAINT dm_courier_ledger_orders_count_check1 CHECK ((orders_count >= 0)),

CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK ((orders_total_sum >= (0)::numeric)),

CONSTRAINT dm_courier_ledger_pk PRIMARY KEY (id),

CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (((rate_avg >= (1)::double precision) AND (rate_avg <= (5)::double precision))),

CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12)))
);

3.4. Структура DDS- слоя

Создаем таблицы dds.dm_couriers, dds.dm_deliveries, dds.fct_deliveries.

CREATE TABLE dds.dm_couriers (

id serial4 NOT NULL,

courier_id varchar NOT NULL,

courier_name varchar NOT NULL,

CONSTRAINT dm_couriers_courier_id_uindex UNIQUE (courier_id),

CONSTRAINT dm_couriers_pk PRIMARY KEY (id)
);

CREATE TABLE dds.dm_deliveries (

id serial4 NOT NULL,

delivery_id varchar NOT NULL,

delivery_ts timestamp NOT NULL,

order_id int4 NOT NULL,

address varchar NULL,

CONSTRAINT dm_deliveries_delivery_id_key UNIQUE (delivery_id),

CONSTRAINT dm_deliveries_pk PRIMARY KEY (id)
);

-- dds.dm_deliveries внешние включи

ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);

CREATE TABLE dds.fct_deliveries (

id serial4 NOT NULL,

courier_id int4 NOT NULL,

delivery_id int4 NOT NULL,

order_id int4 NOT NULL,

order_ts timestamp NOT NULL,

rate int4 NOT NULL,

delivery_sum numeric(14, 2) DEFAULT 0 NOT NULL,

tip_sum numeric(14, 2) DEFAULT 0 NOT NULL,

CONSTRAINT fct_deliveries_pk PRIMARY KEY (id)
);

-- dds.fct_deliveries внешние включи

ALTER TABLE dds.fct_deliveries ADD CONSTRAINT fct_deliveries_id1_fkey FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id);

ALTER TABLE dds.fct_deliveries ADD CONSTRAINT fct_deliveries_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);

Ранее созданные таблицы: dds.dm_orders, dds.dm_products, dds.dm_restaurants, dds.dm_timestamps, dds.dm_users, dds.fct_product_sales, dds.srv_wf_settings.

3.5. Структура STG- слоя

Создание таблиц для данных из API по доставке

stg.deliverysystem_couriers

stg.deliverysystem_deliveries
CREATE TABLE stg.deliverysystem_couriers (

id serial4 NOT NULL,

courier_id varchar NOT NULL,

"name" varchar NOT NULL,

CONSTRAINT deliverysystem_couriers_courier_id_uindex UNIQUE (courier_id),

CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY (id)
);

CREATE TABLE stg.deliverysystem_deliveries (

id serial4 NOT NULL,

order_id varchar NOT NULL,

order_ts timestamp NOT NULL,

delivery_id varchar NOT NULL,

courier_id varchar NOT NULL,

address varchar NOT NULL,

delivery_ts varchar NOT NULL,

rate int4 NOT NULL,

sum numeric(14, 2) DEFAULT 0 NOT NULL,

tip_sum numeric(14, 2) DEFAULT 0 NOT NULL,

CONSTRAINT deliverysystem_deliveries_delivery_id_uindex UNIQUE (delivery_id),

CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY (id),

CONSTRAINT deliverysystem_deliveries_rate_check CHECK (((rate >= 1) AND (rate <= 5)))
);

Ранее созданные таблицы: stg bonussystem_events, stg.bonussystem_ranks, stg.bonussystem_users, stg.ordersystem_orders, stg.ordersystem_restaurants, stg.ordersystem_users, stg. srv_wf_setting.

3.6. Реализованные DAG

Реализован механизм загрузки данных в указанные таблицы по средствам Airflow, путем создания нескольких DAG:

- для создания схем и таблиц:
• init_schema_dag.py - запускается однократно

- для заполнения слоя stg:
• bonus_system_ranks_dag.py - запускается раз в 15 минут

• order_system_orders_dag.py - запускается раз в 15 минут

• order_system_restaurants_dag.py - запускается раз в 15 минут

• order_system_users_dag.py - запускается раз в 15 минут

• deliverys_system_dag.py - запускается раз в 15 минут

- для заполнения слоя dds:
• dds_load_dag.py - запускается раз в 15 минут

- для заполнения слоя cdm:
• cdm_load_dag.py

Код для DAG:

Слой STG

dags\stg\delivery_dag\delivery_dag\deliverys_system_dag.py

dags\stg\bonus_system_ranks_dag\ bonus_system_ranks_dag.py

dags\stg\init_schema_dag\init_schema_dag.py

dags\stg\order_system_dag\order_system_orders_dag.py

dags\stg\order_system_dag\order_system_restaurants_dag.py

dags\stg\order_system_dag\order_system_users_dag.py

Слой DDS

dags\dds\dds_load_dag.py

Слой CDM

dags\cdm\cdm_load_dag.py
