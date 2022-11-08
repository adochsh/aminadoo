--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.rms_orders_shipments_fn

CREATE OR REPLACE FUNCTION rms_orders_shipments_fn()
    RETURNS boolean
    LANGUAGE plpgsql
AS $function$
begin

    drop table if exists tmp_ries_orders;
    drop table if exists tmp_item_info_adeo;

    create temp table tmp_ries_orders as (
        select "number" as order_id
        from (
                 SELECT "number", row_number() over(partition by pk_id  order by version desc) as rn
                 FROM ries_portal_ods.v_order
             ) t where t.rn=1
    );

    CREATE TEMP TABLE tmp_item_info_adeo as (
		SELECT item, commodity as adeo_code
		FROM rms_p009qtzb_rms_ods.v_item_import_attr
		where commodity is not null
	);

truncate table rms_orders_shipments;
insert into rms_orders_shipments (
    order_id, invoice_id, lm_code, adeo_code, invoice_upload, receiving_date1, receiving_date2, to_loc, ship_expected
    , ship_received, dbf_rms_orders_shipments)
SELECT s.order_no::text as order_id
         , s.ASN as invoice_id --номер инвойса
         , sku.item::text as lm_code
         , im.adeo_code
         , xx.first_receiving_date::date as invoice_upload
         , xx.lm_wh_arrival::date  as receiving_date1 -- Первая приемка
         , s.receive_date::date as receiving_date2  --Вторая приемка
         , s.to_loc::int as to_loc
         , sum(sku.qty_expected) as ship_expected
         , sum(sku.qty_received) as ship_received
         , true as dbf_rms_orders_shipments
FROM rms_p009qtzb_rms_ods.v_shipment s
         left join rms_p009qtzb_rms_ods.v_shipsku sku ON s.shipment = sku.shipment and to_loc_type ='W' and sku.is_actual ='1'
         left join rms_p009qtzb_rms_ods.v_xxlm_rms_first_receiving xx ON s.shipment=xx.shipment and xx.is_actual ='1'
         left join tmp_item_info_adeo im on im.item = sku.item::text
where s.is_actual ='1' and s.to_loc_type ='W' and s.from_loc_type is null
  and s.order_no::text in (select order_id from tmp_ries_orders)
group by order_id, invoice_id, lm_code, im.adeo_code, invoice_upload, receiving_date1, receiving_date2, to_loc;

return 0;
end;
$function$;
