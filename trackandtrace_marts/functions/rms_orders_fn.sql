--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.rms_orders_fn
-- xxlm_* таблицы интерфейсные, могут умереть

CREATE OR REPLACE FUNCTION rms_orders_fn()
RETURNS boolean
LANGUAGE plpgsql
AS $function$
begin

drop table if exists tmp_ries_orders;
drop table if exists tmp_warehouse_names;
drop table if exists tmp_dpac; -- Цена dpac

create temp table tmp_ries_orders as (
	select "number" as order_id
	from (
		SELECT "number", row_number() over(partition by pk_id  order by version desc) as rn
		FROM ries_portal_ods.v_order
	) t where t.rn=1
);

CREATE TEMPORARY TABLE tmp_warehouse_names (code INTEGER, name_rus TEXT, name_eng TEXT);
DO $$ BEGIN
    PERFORM public.rdm('ries_warehouse_names', 'tmp_warehouse_names');
END $$;

create temp table tmp_dpac as (
	 select item as lm_code
	      , loc::int
	      , sum(dp.value)::numeric as dpac_item
	 from rms_p009qtzb_rms_ods.v_xxlm_rms_item_loc_comp_dpac dp
	 where dp.loc_type = 'W' AND dp.is_actual = '1'
	 group by 1,2
);

truncate table rms_orders;
insert into rms_orders(order_id, lm_code, supplier_code, supplier_name, v_loc, loc, loc_name
                      , order_date, status, unit_price, dpac_item, order_qty, received_qty
                      , order_dpac_amount, received_dpac_amount, order_amount, received_amount, status_oc, dbf_rms_orders
)
select oh.order_no::text as order_id
         , ol.item::text as lm_code
         , oh.supplier as supplier_code
     , sups.sup_name as supplier_name
     , ol.location as v_loc --- Номер виртуального склада
     , substr(ol.location::text,1,3)::int as loc  -- номер склада
         , wn.code || ' ' || wn.name_rus as loc_name
     , oh.written_date::date as order_date
         , CASE
                                     WHEN oh.status = 'W' THEN 'Worksheet'
                                     WHEN oh.status = 'A' THEN 'Approved'
                                     WHEN oh.status = 'S' THEN 'Sent'
                                     WHEN oh.status = 'C' THEN 'Closed'
    END as status
     , ol.unit_cost as unit_price
     , tmp_dpac.dpac_item
     , ol.QTY_ORDERED as order_qty
     , ol.QTY_RECEIVED as received_qty
     , ol.QTY_ORDERED * tmp_dpac.dpac_item as order_dpac_amount
     , ol.QTY_RECEIVED * tmp_dpac.dpac_item as received_dpac_amount
     , ol.QTY_ORDERED * ol.unit_cost as order_amount
     , ol.QTY_RECEIVED * ol.unit_cost as received_amount
     , sup_info.attribute1 as status_oc
     , true as dbf_rms_orders
FROM rms_p009qtzb_rms_ods.v_ORDHEAD oh -- заказ
         LEFT JOIN rms_p009qtzb_rms_ods.v_ORDLOC ol -- заказ-артикул
             on ol.ORDER_NO =oh.ORDER_NO and ol.is_actual='1'
         LEFT JOIN rms_p009qtzb_rms_ods.v_sups sups -- справочник
             ON sups.supplier = oh.supplier and sups.is_actual='1'
         LEFT join tmp_dpac on tmp_dpac.lm_code =ol.item::text and ol.location =tmp_dpac.loc
         LEFT join tmp_warehouse_names wn on wn.code =substr(ol.location::text,1,3)::int
         LEFT join rms_p009qtzb_rms_ods.v_XXLM_RMS_SUP_INFORM sup_info
                   on sup_info.order_no = oh.order_no
                       and sup_info.pi_no = oh.vendor_order_no
                       and sup_info.is_actual = '1'
where oh.is_actual='1' and oh.order_no::text in (select order_id from tmp_ries_orders);

return 0;
end;
$function$;
