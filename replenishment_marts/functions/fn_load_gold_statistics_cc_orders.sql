--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_statistics_cc_orders

CREATE OR REPLACE FUNCTION fn_load_gold_statistics_cc_orders(period_start date, period_end date)
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
	where period_start between sem.semddeb and sem.semdfin and sem.is_actual = '1';

	select semdfin::date, semnsem
	into period_end_actual, week_end
	from gold_refgwr_ods.v_fctsem sem
	where period_end between sem.semddeb and sem.semdfin and sem.is_actual = '1';

	raise notice 'period_start_actual = %' , period_start_actual::text;
	raise notice 'period_end_actual = %' , period_end_actual::text;

 	drop table if exists tmp_cc_sales;

    create temp table tmp_cc_sales(nk text,
                                   product_id text,
                                   store_id int4,
                                   given_away_quantity numeric,
                                   updated timestamp,
                                   created timestamp);

    insert into tmp_cc_sales(nk, product_id, store_id, given_away_quantity, updated,created)
    select
         cast(ff.nk as text) as nk
        ,ff.product_id
        ,ff.store_id
        ,ff.given_away_quantity
        ,ff.updated
        ,ff.created
    from (select vftlo.product_id, ft.store_id, vftlo.given_away_quantity, pt.updated,
        vftlo.ext_order_line_id as nk, ft.created,
      row_number() over (partition by ft.fulfillment_task_id, vftlo.line_number order by vftlo.document_version desc) as rn
      from fulfillment_ods.v_fulfillment_task ft
      join fulfillment_ods.v_picking_task pt on ft.ext_order_id = pt.ext_order_id
      join fulfillment_ods.v_fulfillment_task_line vftlo on vftlo.fulfillment_task_id = ft.fulfillment_task_id
          where ft.task_status='GIVEN_AWAY'
          and vftlo.line_status ='GIVEN_AWAY'
          and pt.picking_zone ='FUTURE_STOCK'
          and pt.task_status='PICKED'
          and pt.updated::date between period_start_actual and period_end_actual
          ) ff
    where ff.rn = 1;

   drop table if exists tmp_cc_corrections_detailed;

   CREATE temp TABLE tmp_cc_corrections_detailed (
        reg int2 NULL,
	    store int4 NULL,
	    dep varchar(8) NULL,
	    prop_date timestamp(0) NULL,
	    prop float8 NULL,
	    ord_ls varchar(4000) NULL,
	    ord_em varchar(4000) NULL,
	    ord_rd varchar(4000) NULL,
	    ord_rm varchar(4000) NULL,
	    ord_cc varchar(4000) NULL,
	    supp varchar(4000) NULL,
	    suppname varchar(4000) NULL,
	    prop_price float8 NULL,
	    cover float8 NULL,
	    franco float8 NULL,
	    ord_price float8 NULL,
	    item varchar(13) NULL,
	    order_item_qty float8 NULL,
	    prop_item_qty float8 NULL,
	    corrected float8 NULL,
	    full_prop_price float8 NULL,
	    full_order_price float8 NULL,
	    updated_dttm timestamp NOT NULL DEFAULT clock_timestamp(),
	    order_date timestamp(0) NULL,
	    delivery_date timestamp(0) NULL,
	    cover_end_date timestamp(0) NULL,
	    bbxd_type int4 null,
	    rms_reject int2 null,
	    reject_reason int2 null,
	    ord_tsf char(1)
    )
    WITH (
	    appendonly=true,
	    compresslevel=1,
	    orientation=column,
	    compresstype=zstd
    );

    truncate table tmp_cc_corrections_detailed;


    -- orders
    insert into tmp_cc_corrections_detailed (reg, store, dep, order_date, prop_date, prop, ord_ls, ord_em, ord_rd,
                            ord_rm, ord_cc, supp, suppname, prop_price, cover, franco, ord_price, item, order_item_qty,
                            prop_item_qty, corrected, full_prop_price, full_order_price, updated_dttm, delivery_date,
                            cover_end_date,bbxd_type, rms_reject, reject_reason, ord_tsf)
    select cast(sat.satvaln as int) as reg,
        hh.location as store,
        hh.dept as dep,
        hh.written_date::date as order_date,
        null as prop_date, null as prop, null as ord_ls, null as ord_em, null as ord_rd, null as ord_rm,
        hh.order_no as ord_cc,
        hh.supplier as supp,
        s.sup_name as suppname,
        null as prop_price, null as cover,
        pv.franco_sum as franco,
        l.unit_cost as ord_price, l.item as item,
        l.qty_ordered as order_item_qty, null as prop_item_qty,
        l.qty_ordered as corrected, null as full_prop_price,
        sum(l.unit_cost*l.qty_ordered) over(partition by l.order_no, l.item) as full_order_price,
        current_timestamp as  updated_dttm, null as delivery_date, null as cover_end_date,
        null as bbxd_type, 0 as rms_reject, 0 as reject_reason,
        'o' as ord_tsf
    from rms_p009qtzb_rms_ods.v_ordhead hh
    left join rms_p009qtzb_rms_ods.v_ordloc l ON l.ORDER_NO=hh.ORDER_NO
    left join rms_p009qtzb_rms_ods.v_sups s on hh.supplier = s.supplier
    left join tmp_cc_sales cc
    	on l.item = cc.product_id and l."location" = cc.store_id
    	and hh.written_date between cc.created::date and cc.updated::date
    	and hh.close_date::date <= cc.updated::date
    	and l.qty_ordered >= cc.given_away_quantity
    left join rms_p009qtzb_rms_ods.v_xxlm_path_header ph on ph.supplier = hh.supplier and hh."location" = ph.loc
    left join rms_p009qtzb_rms_ods.v_xxlm_path_version pv on ph.id = pv.id and pv.activate_date is null
    left join rms_p009qtzb_rms_ods.v_store vs on hh.location = vs.store
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG' and sat.satsite=hh.location and period_start between sat.satddeb and sat.satdfin
    where hh.PO_TYPE='CC'
    and cc.product_id is null
    and hh.written_date::date between period_start_actual and period_end_actual;

    -- transfers
    insert into tmp_cc_corrections_detailed (reg, store, dep, order_date, prop_date, prop, ord_ls, ord_em, ord_rd,
                            ord_rm, ord_cc, supp, suppname, prop_price, cover, franco, ord_price, item, order_item_qty,
                            prop_item_qty, corrected, full_prop_price, full_order_price, updated_dttm, delivery_date,
                            cover_end_date, bbxd_type, rms_reject, reject_reason, ord_tsf)
    select cast(sat.satvaln as int) as reg,
        h.to_loc as store,
        h.dept as dep,
        h.create_date::date as order_date,
        null as prop_date, null as prop, null as ord_ls, null as ord_em, null as ord_rd, null as ord_rm,
        h.tsf_no as ord_cc,
        il.primary_supp supp, s.sup_name as suppname,
        null as prop_price, null as cover,
        null as franco,
        d.tsf_cost as ord_price, d.item as item, d.tsf_qty as order_item_qty, null as prop_item_qty,
        d.tsf_qty as corrected, null as full_prop_price,
        sum(d.tsf_cost*d.tsf_qty) over(partition by h.tsf_no, d.item) as full_order_price,
        current_timestamp as  updated_dttm, null as delivery_date,
        null as cover_end_date,
        null as bbxd_type, 0 as rms_reject, 0 as reject_reason,
        't' as ord_tsf
    from rms_p009qtzb_rms_ods.v_tsfhead h
    join rms_p009qtzb_rms_ods.v_tsfdetail d on d.tsf_no=h.tsf_no
    join rms_p009qtzb_rms_ods.v_tsfhead_cfa_ext ext on d.tsf_no=ext.tsf_no and ext.group_id='11'AND ext.VARCHAR2_1='CC'
    left join rms_p009qtzb_rms_ods.v_item_loc il on il.item = d.item and il.loc = h.to_loc
    left join rms_p009qtzb_rms_ods.v_sups s on il.primary_supp = s.supplier
    left join tmp_cc_sales cc
    	on d.item = cc.product_id and h.to_loc = cc.store_id
    	and h.create_date between cc.created::date and cc.updated::date
    	and h.close_date::date <= cc.updated::date
    	and d.tsf_qty >= cc.given_away_quantity
    left join rms_p009qtzb_rms_ods.v_xxlm_path_header ph on ph.supplier = s.supplier and h.to_loc = ph.loc
    left join rms_p009qtzb_rms_ods.v_xxlm_path_version pv on ph.id = pv.id and pv.activate_date is null
    left join rms_p009qtzb_rms_ods.v_store vs on h.to_loc = vs.store
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG' and sat.satsite=h.to_loc and period_start between sat.satddeb and sat.satdfin
    where h.to_loc_type = 'S'
    and cc.product_id is null
    and h.create_date::date between period_start_actual and period_end_actual;

    delete from gold_corrections_detailed
    where order_date::date between period_start_actual and period_end_actual
    and ord_cc is not null;

    insert into gold_corrections_detailed(reg, store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, ord_cc,
                                          supp, suppname, prop_price, cover, franco, ord_price, item, order_item_qty,
                                          prop_item_qty, corrected, full_prop_price, full_order_price, updated_dttm,
                                          order_date, delivery_date, cover_end_date, bbxd_type, rms_reject, reject_reason)
    select reg, store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, ord_cc,
           supp, suppname, prop_price, cover, franco, ord_price, item, order_item_qty,
           prop_item_qty, corrected, full_prop_price, full_order_price, updated_dttm,
           order_date, delivery_date, cover_end_date, bbxd_type, rms_reject, reject_reason
    from tmp_cc_corrections_detailed;

    delete from gold_statistics
    where prop_date::date between period_start_actual and period_end_actual
    and ord_cc is not null;

    insert into gold_statistics(store, dep, prop_date, prop, ord_ls,
    ord_em, ord_rd, ord_rm, ord_cc, "type",
    supp, suppname, prop_price, cover, franco,
    ord_price, qty, changed_qty, util,
    dmaj, reg, negative_correction,
    positive_correction, bbxd_lines_qty, bbxd_qty, bbxd_amount,
    updated_dttm, target_dep, target_reg, target_store, supplier_type,
    rms_reject_items)
    SELECT tcd.store, tcd.dep, tcd.order_date as prop_date, null as prop, null as ord_ls,
    null as ord_em, null as ord_rd, null as ord_rm, tcd.ord_cc as ord_cc, null as "type",
    tcd.supp, tcd.suppname, null as prop_price, null as cover, sum(tcd.franco) as franco,
    avg(tcd.full_order_price) as ord_price, sum(1) as qty, sum(1) as changed_qty, 'CC' as util,
    null as dmaj, reg, null as negative_correction,
    sum(tcd.ord_price) as positive_correction, 0 as bbxd_lines_qty, 0 as bbxd_qty, 0 as bbxd_amount,
    current_timestamp as updated_dttm, avg(par_dep.parvan1/100) as target_dep,
    avg(par_reg.parvan1/100) as target_reg,
    avg(par_store.parvan1/100) as target_store, null as supplier_type,
    null as rms_reject_items
    from tmp_cc_corrections_detailed tcd
    left join gold_refcesh_ods.v_parpostes par_dep on par_dep.parcmag=10 and par_dep.partabl=9005 and par_dep.parpost = cast(tcd.dep as int)
    left join gold_refcesh_ods.v_parpostes par_store on par_store.parcmag=10 and par_store.partabl=9005 and par_store.parpost = 16
    left join gold_refcesh_ods.v_parpostes par_reg on par_reg.parcmag=10 and par_reg.partabl=9005 and par_reg.parpost = 17
    where tcd.ord_tsf = 'o'
    group by store, dep, order_date, ord_cc, supp, suppname, reg;

    insert into gold_statistics(store, dep, prop_date, prop, ord_ls,
    ord_em, ord_rd, ord_rm, ord_cc, "type",
    supp, suppname, prop_price, cover, franco,
    ord_price, qty, changed_qty, util,
    dmaj, reg, negative_correction,
    positive_correction, bbxd_lines_qty, bbxd_qty, bbxd_amount,
    updated_dttm, target_dep, target_reg, target_store, supplier_type,
    rms_reject_items)
    SELECT tcd.store, tcd.dep, tcd.order_date as prop_date, null as prop, null as ord_ls,
    null as ord_em, null as ord_rd, null as ord_rm, tcd.ord_cc as ord_cc, null as "type",
    null as supp, null as suppname, null as prop_price, null as cover, null as franco,
    avg(tcd.full_order_price) as ord_price, sum(1) as qty, sum(1) as changed_qty, 'CC' as util,
    null as dmaj, reg, null as negative_correction,
    sum(tcd.ord_price) as positive_correction, 0 as bbxd_lines_qty, 0 as bbxd_qty, 0 as bbxd_amount,
    current_timestamp as updated_dttm, avg(par_dep.parvan1/100) as target_dep,
    avg(par_reg.parvan1/100) as target_reg,
    avg(par_store.parvan1/100) as target_store, null as supplier_type,
    null as rms_reject_items
    from tmp_cc_corrections_detailed tcd
    left join gold_refcesh_ods.v_parpostes par_dep on par_dep.parcmag=10 and par_dep.partabl=9005 and par_dep.parpost = cast(tcd.dep as int)
    left join gold_refcesh_ods.v_parpostes par_store on par_store.parcmag=10 and par_store.partabl=9005 and par_store.parpost = 16
    left join gold_refcesh_ods.v_parpostes par_reg on par_reg.parcmag=10 and par_reg.partabl=9005 and par_reg.parpost = 17
    where tcd.ord_tsf = 't'
    group by store, dep, order_date, ord_cc, reg;

    return 0;
end;
$function$
;
