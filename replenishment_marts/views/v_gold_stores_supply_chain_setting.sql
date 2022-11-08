--liquibase formatted sql

--changeset 60098727:create:view:v_gold_stores_supply_chain_setting

CREATE OR REPLACE VIEW v_gold_stores_supply_chain_setting as
select
	store_num,
	dep_code,
	supp_code,
	ARACCIN,
	SPSCFIN,
	supp_name,
	ORDER_TYPE,
	DKK,
	order_days,
	SROK,
	srok_dostavki_v_magazin,
	DELAY_VALUE,
	R_SROK_CHECK_D,
	THEOR_OPTIM,
	MIN_MAX_DIFF,
	scheme_type,
	FRANCO,
	AFRANKO,
	TYPP,
	ADD_CONDITIONS_to_FRANCO,
	FRAN_QTYSHARE,
    updated_dttm
from gold_stores_supply_chain_setting
