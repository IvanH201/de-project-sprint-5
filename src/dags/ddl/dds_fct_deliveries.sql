DROP TABLE if exists dds.fct_deliveries;

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