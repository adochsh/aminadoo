--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_specifics_cc_sales

  CREATE OR REPLACE FUNCTION fn_load_gold_specifics_cc_sales(period_start date, period_end date, external_server text)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    v_sql text;
    v_cnt bigint;
    week_start int;
    week_end int;
    period_start_actual date;
    period_end_actual date;
begin

	select semddeb::date, semnsem
	into period_start_actual, week_start
	from gold_refgwr_ods.v_fctsem sem
	where period_start::date between sem.semddeb::date and sem.semdfin::date and sem.is_actual = '1';

	select semdfin::date, semnsem
	into period_end_actual, week_end
	from gold_refgwr_ods.v_fctsem sem
	where period_end::date between sem.semddeb::date and sem.semdfin::date and sem.is_actual = '1';

	raise notice 'period_start_actual = %' , period_start_actual::text;
	raise notice 'period_end_actual = %' , period_end_actual::text;

	drop table if exists tmp_future_stock;

    create temp table tmp_future_stock
    	(item text,
    	 store int8,
    	 create_date timestamp,
    	 close_date timestamp,
    	 qty_ordered numeric);

    insert into tmp_future_stock (item, store, create_date, close_date, qty_ordered)
    select l.item, l."location", hh.written_date, hh.close_date, l.qty_ordered
    from rms_p009qtzb_rms_ods.v_ordhead hh
    join rms_p009qtzb_rms_ods.v_ordloc l ON l.ORDER_NO=hh.ORDER_NO
    where hh.PO_TYPE='CC'
    and hh.written_date::date  between period_start_actual::date - interval '2 week' and period_end_actual::date
    and l.cancel_date is null and l.qty_received is not null;


    insert into tmp_future_stock (item, store, create_date, close_date, qty_ordered)
    select d.item, h.to_loc, h.create_date, h.close_date, d.tsf_qty
    from rms_p009qtzb_rms_ods.v_tsfhead h
    join rms_p009qtzb_rms_ods.v_tsfdetail d on d.tsf_no=h.tsf_no
    join rms_p009qtzb_rms_ods.v_tsfhead_cfa_ext ext on d.tsf_no=ext.tsf_no and ext.group_id='11'AND ext.VARCHAR2_1='CC'
    where h.from_loc_type = 'W'
    and h.create_date::date between period_start_actual::date - interval '2 week' and period_end_actual::date
    and d.cancelled_qty is null;

	drop table if exists tmp_cc_sales;

    create temp table tmp_cc_sales(nk text,
                                   product_id text,
                                   store_id int4,
                                   given_away_quantity numeric,
                                   updated timestamp);

    insert into tmp_cc_sales(nk, product_id, store_id, given_away_quantity, updated)
    select
         cast(ff.nk as text) as nk
        ,ff.product_id
        ,ff.store_id
        ,ff.given_away_quantity
        ,ff.updated
    from (select vftlo.product_id, ft.store_id, vftlo.given_away_quantity, pt.updated,
        vftlo.ext_order_line_id as nk, ft.created,
    	row_number() over (partition by ft.fulfillment_task_id, vftlo.line_number
    	        order by vftlo.document_version desc, pt.document_version desc) as rn
    	from fulfillment_ods.v_fulfillment_task ft
    	join fulfillment_ods.v_picking_task pt on ft.ext_order_id = pt.ext_order_id
    	join fulfillment_ods.v_fulfillment_task_line vftlo on vftlo.fulfillment_task_id = ft.fulfillment_task_id
    	    where ft.task_status='GIVEN_AWAY'
    	    and vftlo.line_status ='GIVEN_AWAY'
    	    and pt.picking_zone ='FUTURE_STOCK'
    	    and pt.task_status='PICKED'
    	    and pt.updated::date between period_start_actual::date and period_end_actual::date) ff
    where ff.rn = 1
    and exists (select 1
                from tmp_future_stock tfs
                where tfs.item = ff.product_id and tfs.store = ff.store_id
                and tfs.create_date::date between ff.created::date and ff.updated::date
                and tfs.close_date::date <= ff.updated::date
                and tfs.qty_ordered >= ff.given_away_quantity);

	drop table if exists tmp_cc_sales_aggregate;

    create temp table tmp_cc_sales_aggregate(product_id text,
                                   store_id int4,
                                   week_num int,
                                   correction numeric);
    insert into tmp_cc_sales_aggregate(product_id, store_id, week_num, correction)
    select ts.product_id as item, ts.store_id as site, sem.semnsem as week_num, sum(ts.given_away_quantity) as correction
    from tmp_cc_sales ts
    join gold_refgwr_ods.v_fctsem sem on sem.is_actual = '1' and ts.updated::date between sem.semddeb::date and sem.semdfin::date
    group by ts.product_id, ts.store_id, sem.semnsem;

    delete from gold_specifics_agg_detailed agg
    where exists (select 1
                  from tmp_cc_sales ts
                  join gold_refgwr_ods.v_fctsem sem on sem.is_actual = '1' and ts.updated::date between sem.semddeb::date and sem.semdfin::date
                  where agg.item = ts.product_id and agg.site = ts.store_id and agg.week_num = sem.semnsem
                  and agg.id_spec = 1 and agg.nk = ts.nk and ts.given_away_quantity <> agg.correction_sum);

    insert into gold_specifics_agg_detailed(nk, item, site, week_num, correction_sum,
                                                sale_date, id_spec)
    select ts.nk as nk, ts.product_id as item, ts.store_id as site, sem.semnsem as week_num,
        ts.given_away_quantity as correction_sum, ts.updated::date as sale_date, 1 as id_spec
    from tmp_cc_sales ts
    join gold_refgwr_ods.v_fctsem sem on sem.is_actual = '1' and ts.updated::date between sem.semddeb::date and sem.semdfin::date
    left join gold_specifics_agg_detailed agg on agg.item = ts.product_id and agg.site = ts.store_id and agg.week_num = sem.semnsem
                  and agg.id_spec = 1 and agg.nk = ts.nk and ts.given_away_quantity = agg.correction_sum
    where agg.item is null;

   drop table if exists tmp_gold_sales;
   create temp table tmp_gold_sales (product_id text, store_id int4, week_num int4, sale numeric(14, 3));

   insert into tmp_gold_sales (product_id, store_id, week_num, sale)
   select ara.aracexr, ara.arasite, s.SMSSEMAINE, s.SMSSAIU
   from gold_refcesh_ods.v_artuc ara
    join gold_refcesh_ods.v_stomvsemaine s
        on ara.ARACINL=s.smscinl and s.SMSSITE=ara.arasite and s.is_actual = '1'
    join gold_refgwr_ods.v_fctsem sem on s.smssemaine = sem.semnsem
   where sem.semddeb::date between araddeb::date and aradfin::date
   		and ara.is_actual = '1'
   		and s.SMSSEMAINE between week_start and week_end;

    drop table if exists tmp_cc_corrections;

    create temp table tmp_cc_corrections(item_res varchar(13), site_res int4, week_num_res int4,
                                             sale numeric(14, 3), correction_res numeric(14, 3));

    insert into tmp_cc_corrections(item_res, site_res, week_num_res, sale, correction_res)
    select tsa.product_id, tsa.store_id, tsa.week_num, tgs.sale as sale, tgs.sale - tsa.correction correction_res
    from tmp_cc_sales_aggregate tsa
    join tmp_gold_sales tgs on tsa.product_id = tgs.product_id
    	and tsa.store_id = tgs.store_id and tsa.week_num = tgs.week_num
    where tgs.sale >= tsa.correction;


    truncate table gold_specifics_agg_delta;

    insert into gold_specifics_agg_delta(item_res, site_res, week_num_res, sale, correction_res)
    select tcc.item_res, tcc.site_res, tcc.week_num_res, null as sale, tcc.correction_res
    from tmp_cc_corrections tcc
    left join gold_specifics_agg gsa on tcc.item_res = gsa.item_res and tcc.site_res = gsa.site_res
    	and tcc.week_num_res = gsa.week_num_res and gsa.id_spec = 1 --(?)
    where tcc.sale <> gsa.sale or tcc.correction_res <> gsa.correction_res or gsa.item_res is null;

    delete from gold_specifics_agg gsa
    where exists (select 1
    			  from tmp_cc_corrections tcc
    			  where tcc.item_res = gsa.item_res and tcc.site_res = gsa.site_res
    			  	and tcc.week_num_res = gsa.week_num_res and gsa.id_spec = 1
    			  and (tcc.sale <> gsa.sale or tcc.correction_res <> gsa.correction_res));

    insert into gold_specifics_agg (item_res, site_res, week_num_res, sale, id_spec, correction_res, cre_date)
	select tcc.item_res, tcc.site_res, tcc.week_num_res, tcc.sale, 1 as id_spec, tcc.correction_res, current_timestamp as cre_date
	from tmp_cc_corrections tcc
	left join gold_specifics_agg gsa on tcc.item_res = gsa.item_res and tcc.site_res = gsa.site_res
    		and tcc.week_num_res = gsa.week_num_res and gsa.id_spec = 1
    		and tcc.sale = gsa.sale and tcc.correction_res = gsa.correction_res
    where gsa.item_res is null;

	perform fn_dump_gold_specifics('replenishment_marts', 'gold_specifics_agg_delta',
    	'item_res, site_res, week_num_res, sale, correction_res', 'refgwr',
    	'LMV_ITFHISTOH',
    	'HHFCEXR varchar,HHFSITE NUMERIC,HHFSEMAINE NUMERIC,HHFQTV NUMERIC,HHFTRT NUMERIC,HHFDTRT DATE,HHFDCRE DATE,HHFDMAJ DATE,HHFUTIL varchar,HHFNLIG NUMERIC,HHFFICH varchar,HHFERR NUMERIC,HHFMESS varchar,HHFQTVCORR NUMERIC',
    	'HHFCEXR,HHFSITE,HHFSEMAINE,HHFQTV,HHFQTVCORR',
    	'1=1', external_server);

    return 0;
end;
$function$
;
