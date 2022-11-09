--liquibase formatted sql

--changeset 60098727:create:table:rms_trns_sales

CREATE TABLE rms_trns_sales(
    lm_code bigint,
    opened_date date,
    store_num int4,
    line_type varchar(50),
    qty_sold numeric,
    price numeric,
    unit_cost numeric,
    ca_ttc numeric,
    created_dttm TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    updated_dttm TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP()
)
WITH (
	appendonly=true,
	compresslevel=1,
	orientation=column,
	compresstype=zstd
)
DISTRIBUTED BY (lm_code)
PARTITION BY RANGE (opened_date) (DEFAULT PARTITION other);
