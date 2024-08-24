CREATE TABLE stg.ordersystem_users (
	id serial NOT NULL,
	object_id varchar NOT null UNIQUE,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL,
	CONSTRAINT ordersystem_users_pkey PRIMARY KEY (id)
);