DROP TABLE if exists stg.deliverysystem_couriers;

CREATE TABLE stg.deliverysystem_couriers (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	"name" varchar NOT NULL,
	CONSTRAINT deliverysystem_couriers_courier_id_uindex UNIQUE (courier_id),
	CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY (id)
);