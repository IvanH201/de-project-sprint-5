drop table if exists cdm.dm_courier_ledger ;


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