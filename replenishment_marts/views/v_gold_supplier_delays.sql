--liquibase formatted sql

--changeset 60098727:create:view:v_gold_supplier_delays

CREATE OR REPLACE VIEW v_gold_supplier_delays as
select
    ord_tsf_no,
	shipment_id,
	supplier_id,
	from_loc_type,
	from_loc,
	to_loc,
	to_loc_type,
	item,
	ord_tsf_date,
	plan_receive_date,
	receive_date1_to_wh,
	receive_date,
	delay,
	flow_type,
	order_qty,
	received_qty,
	not_received_qty,
	updated_dttm
from gold_supplier_delays
