--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_supplier_delays

CREATE OR REPLACE FUNCTION fn_load_gold_supplier_delays(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin

    create temp table tmp_gold_supplier_delays (ord_tsf_no text, shipment_id int, supplier_id int, from_loc_type text,
                    from_loc int, to_loc int, to_loc_type text, item text, ord_tsf_date date, plan_receive_date date,
                    receive_date1_to_wh date, receive_date date, delay int, flow_type text, order_qty numeric,
                    received_qty numeric, not_received_qty numeric)
	with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (ord_tsf_no);

    raise notice '==================================== START =====================================';
    raise notice '[%] Inserting into replenishment_marts.gold_supplier_delays (transfers)' , date_trunc('second' , clock_timestamp())::text;

    insert into tmp_gold_supplier_delays (ord_tsf_no, shipment_id, supplier_id, from_loc_type,
                        from_loc, to_loc, to_loc_type, item, ord_tsf_date, plan_receive_date,
                        receive_date1_to_wh, receive_date, delay, flow_type, order_qty,
                        received_qty, not_received_qty)
	select th.tsf_no::text
	    , s.shipment as shipment_id
	    , NULL as supplier_id
	    , th.from_loc_type
	    , th.from_loc
	    , th.to_loc
	    , th.to_loc_type
	    , sku.item::text
	    , th.create_date::date as request_date   -- дата созадния трансфера
	    , th.delivery_date::date  as plan_receive_date  --Планируемая дата доставки
	    , xx.lm_wh_arrival::date  as receive_date1_to_wh    --Первая приемка
		, s.receive_date::date   													   --Вторая приемка
	    , DATE_PART('day', s.receive_date - th.delivery_date) as delay
	    , case when th.from_loc_type = 'W' and th.to_loc_type = 'S' then 'STOCK WH->S' -- WH->S
	            when th.from_loc_type = 'W' and th.to_loc_type = 'W' then 'STOCK WH->WH' -- WH->WH
	            when th.from_loc_type = 'S' and th.to_loc_type = 'S' then 'STOCK S->S' -- S->S
	      end as flow_type  --вид потока трансфера
	    , coalesce(td.tsf_qty,0) as order_qty           -- заказано, шт
	    , coalesce(td.received_qty,0) as received_qty       -- получено, шт
	    , case when coalesce(td.tsf_qty,0) -coalesce(td.received_qty,0)<0 then 0
	    		else coalesce(td.tsf_qty,0) -coalesce(td.received_qty,0) end as not_received_qty  -- Недопоставка, шт
	from rms_p009qtzb_rms_ods.v_tsfhead th
	join rms_p009qtzb_rms_ods.v_tsfdetail td on td.tsf_no = th.tsf_no and td.is_actual = '1'
	join rms_p009qtzb_rms_ods.v_shipsku sku on sku.distro_no = th.tsf_no and td.item = sku.item and sku.is_actual = '1'
	join rms_p009qtzb_rms_ods.v_shipment s on s.shipment = sku.shipment and s.receive_date is not null and s.is_actual = '1'
	left join rms_p009qtzb_rms_ods.v_xxlm_rms_first_receiving xx on s.shipment=xx.shipment and xx.lm_wh_arrival is not null and xx.is_actual ='1'
	where th.is_actual='1' and th.delivery_date is not null and th.from_loc_type is not null and th.to_loc_type is not null and th.status ='C'
	and cast(th.updated_dttm as date) between period_start and period_end;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into replenishment_marts.gold_supplier_delays (transfers)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    raise notice '[%] Inserting into replenishment_marts.gold_supplier_delays (orders)' , date_trunc('second' , clock_timestamp())::text;

    insert into tmp_gold_supplier_delays (ord_tsf_no, shipment_id, supplier_id, from_loc_type,
                        from_loc, to_loc, to_loc_type, item, ord_tsf_date, plan_receive_date,
                        receive_date1_to_wh, receive_date, delay, flow_type, order_qty,
                        received_qty, not_received_qty)
	select oh.order_no::text
		, s.shipment as shipment_id
		, oh.supplier as supplier_id
		, s.from_loc_type
		, s.from_loc
	    , s.to_loc
	    , s.to_loc_type
		, ol.item::text
	    , oh.written_date::date as order_date --дата заказа
	    , oh.not_after_date 	 as plan_receive_date  --Планируемая дата доставки
		, xx.lm_wh_arrival::date as receive_date1_to_wh 	   --Первая приемка
		, s.receive_date::date  					   		   --Вторая приемка
		, case when oh.import_id::text like '%999%' then DATE_PART('day', s.receive_date - oh.not_after_date)
			   else DATE_PART('day', xx.lm_wh_arrival - oh.not_after_date)
		  end as delay
		, case when oh.import_id is null then 'DSD'
			   when oh.import_id::text like '%999%' then 'XDOCK' --- вторая приемка, а по всем остальным первую приемку
			   when oh.import_id::text like '%060%' or oh.import_id::text like '%063%' then 'BBXD'
		  end as flow_type   --вид потока доставки

	    , coalesce(ol.QTY_ORDERED,0) as order_qty           -- заказано, шт
	    , coalesce(ol.QTY_RECEIVED,0) as received_qty       -- получено, шт
	    , case when coalesce(ol.QTY_ORDERED,0) - coalesce(ol.QTY_RECEIVED,0) < 0 then 0
	    		else coalesce(ol.QTY_ORDERED,0) - coalesce(ol.QTY_RECEIVED,0) end as not_received_qty  -- Недопоставка, шт
	from rms_p009qtzb_rms_ods.v_ORDHEAD oh
	join rms_p009qtzb_rms_ods.v_ORDLOC ol on ol.ORDER_NO =oh.ORDER_NO and ol.is_actual='1'
	join rms_p009qtzb_rms_ods.v_shipment s on oh.order_no =s.order_no and s.receive_date is not null and s.is_actual ='1'
	join rms_p009qtzb_rms_ods.v_shipsku sku ON s.shipment =sku.shipment and ol.item =sku.item and sku.inv_status=8 and sku.is_actual ='1'
	left join rms_p009qtzb_rms_ods.v_xxlm_rms_first_receiving xx ON s.shipment=xx.shipment and xx.lm_wh_arrival is not null and xx.is_actual ='1'
	where coalesce(oh.po_type, '-') != 'CC' AND oh.INCLUDE_ON_ORDER_IND = 'Y' and oh.is_actual='1' and oh.not_after_date is not null and oh.status ='C'
	and cast(oh.updated_dttm as date) between period_start and period_end;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into replenishment_marts.gold_supplier_delays (orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    delete from gold_supplier_delays t
    where exists (select 1
                  from tmp_gold_supplier_delays s
                  where s.ord_tsf_no = t.ord_tsf_no);

    insert into gold_supplier_delays (ord_tsf_no, shipment_id, supplier_id, from_loc_type,
                        from_loc, to_loc, to_loc_type, item, ord_tsf_date, plan_receive_date,
                        receive_date1_to_wh, receive_date, delay, flow_type, order_qty,
                        received_qty, not_received_qty)
    select ord_tsf_no, shipment_id, supplier_id, from_loc_type,
           from_loc, to_loc, to_loc_type, item, ord_tsf_date, plan_receive_date,
           receive_date1_to_wh, receive_date, delay, flow_type, order_qty,
           received_qty, not_received_qty
    from tmp_gold_supplier_delays;

    perform public.fn_analyze_table('replenishment_marts','gold_supplier_delays');
    raise notice '[%] Function finished.' , date_trunc('second' , clock_timestamp())::text;
    raise notice '==================================== FINISH =====================================';
    return 0;
end;
$function$
;
