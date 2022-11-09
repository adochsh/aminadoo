--liquibase formatted sql

--changeset 60098727:create:table:gold_stores_supply_chain_setting

CREATE TABLE gold_stores_supply_chain_setting (
	store_num int,
	dep_code text,
	supp_code text,
	ARACCIN int,
	SPSCFIN int,
	supp_name text,
	ORDER_TYPE text,
	DKK text,
	order_days text,
	SROK text,
	srok_dostavki_v_magazin int,
	DELAY_VALUE int,
	R_SROK_CHECK_D text,
	THEOR_OPTIM text,
	MIN_MAX_DIFF int,
	scheme_type text,
	FRANCO int,
	AFRANKO int,
	TYPP text,
	ADD_CONDITIONS_to_FRANCO text,
	FRAN_QTYSHARE int
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (supp_code);

--changeset 60098727:alter:table:gold_stores_supply_chain_setting
ALTER TABLE gold_stores_supply_chain_setting ADD updated_dttm timestamptz NOT NULL DEFAULT CLOCK_TIMESTAMP() encoding (compresslevel=1,compresstype=zstd,blocksize=32768);

