CREATE TABLE stg.ordersystem_restaurants (
	id serial NOT NULL,
	object_id varchar NOT NULL UNIQUE,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_restaurants_pkey PRIMARY KEY (id)
);