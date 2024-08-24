DROP TABLE if exists dds.dm_deliveries;

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