DROP TABLE if exists stg.deliverysystem_deliveries;

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