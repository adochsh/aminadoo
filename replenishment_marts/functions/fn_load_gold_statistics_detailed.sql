--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_statistics_detailed_incremental

CREATE OR REPLACE FUNCTION fn_load_gold_statistics_detailed(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin

    raise notice '[%] Creation of detailed mart start' , date_trunc('second' , clock_timestamp())::text;

    delete
    from gold_corrections_detailed s
    where exists(
    	select 1
    	from tmp_corr_detailed t
    	where s.prop = t.prop_number
    		and t.tech_case_no = 1
    		and coalesce(t.prop_date, t.order_date) between period_start and period_end);

    insert into gold_corrections_detailed (
    	reg, store, dep, order_date, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, supp, suppname,
        prop_price, delivery_date, cover_end_date, cover, franco, ord_price, item, order_item_qty, prop_item_qty,
    	corrected, rms_reject, reject_reason, full_prop_price, full_order_price, bbxd_type)
    select
    	cast(sat.satvaln as smallint) as reg,
    	tcd.site as store,
    	substring(fcc.fccnum, 7, 2) dep,
    	tcd.order_date as order_date,
    	tcd.prop_date as prop_date,
    	tcd.prop_number as prop,
    	tcd.contract_numbers -> 'LS' as ORD_ls,
    	tcd.contract_numbers -> 'EM' as ORD_EM,
    	tcd.contract_numbers -> 'RD' as ORD_RD,
    	tcd.contract_numbers -> 'RM' as ORD_RM,
    	fou.foucnuf as supp,
        fou.foulibl as suppname,
        tcd.pdcvapar as prop_price,
        tcd.delivery_date as delivery_date,
        tcd.cover_end_date as cover_end_date,
        cast(tcd.cover_end_date::date - tcd.delivery_date::date as numeric(22,5)) as cover,
        fra.franco as franco,
        tcd.ord_price as ord_price,
        tcd.item as item,
        tcd.order_item_qty as order_item_qty,
    	tcd.prop_item_qty as prop_item_qty,
    	coalesce(tcd.order_item_qty, 0) - coalesce(tcd.prop_item_qty, 0) as corrected,
    	tcd.rms_reject,
    	tcd.reject_reason,
    	sum(tcd.prop_item_qty * prop_coeff_euro) over(partition by tcd.prop_number) as full_prop_price,
    	sum(tcd.ord_price) over(partition by tcd.prop_number) as full_order_price,
    	0 as bbxd_type
    from tmp_corr_detailed tcd
    left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
    left join gold_refcesh_ods.v_foudgene fou on tcd.supp_int_code = fou.foucfin  and fou.is_actual = '1'
    left join tmp_statistics_franco fra on fra.key_number = cast(tcd.prop_number as varchar(13)) and fra.tech_case_no = 1
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
            AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and tcd.site = sat.satsite and sat.is_actual = '1'
    where tcd.tech_case_no = 1
        and coalesce(tcd.prop_date, tcd.order_date) between period_start and period_end;

    raise notice '[%] Inserted % rows into gold_corrections_detailed (proposals with orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    delete
    from gold_corrections_detailed s
    where exists(
    	select 1
    	from tmp_corr_detailed t
    	where coalesce(t.contract_numbers -> 'LS', t.contract_numbers -> 'EM',
    		  		   t.contract_numbers -> 'RD', t.contract_numbers -> 'RM') =
    		  coalesce(s.ord_ls, s.ord_em, s.ord_rd, s.ord_rm)
    		and t.tech_case_no = 2
    		and coalesce(t.prop_date, t.order_date) between period_start and period_end);

    insert into gold_corrections_detailed (
    	reg, store, dep, order_date, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, supp, suppname,
        prop_price, delivery_date, cover_end_date, cover, franco, ord_price, item, order_item_qty, prop_item_qty,
    	corrected, rms_reject, reject_reason, full_prop_price, full_order_price, bbxd_type)
    select
    	cast(sat.satvaln as smallint) as reg,
    	tcd.site as store,
    	substring(fcc.fccnum, 7, 2) dep,
    	tcd.order_date as order_date,
    	tcd.prop_date as prop_date,
    	tcd.prop_number as prop,
    	tcd.contract_numbers -> 'LS' as ORD_ls,
    	tcd.contract_numbers -> 'EM' as ORD_EM,
    	tcd.contract_numbers -> 'RD' as ORD_RD,
    	tcd.contract_numbers -> 'RM' as ORD_RM,
    	fou.foucnuf as supp,
        fou.foulibl as suppname,
        NULL as prop_price,
        NULL as delivery_date,
        NULL as cover_end_date,
        NULL as cover,
        fra.franco as franco,
        tcd.ord_price as ord_price,
        tcd.item as item,
        tcd.order_item_qty as order_item_qty,
    	0 as prop_item_qty,
    	coalesce(tcd.order_item_qty, 0) - coalesce(tcd.prop_item_qty, 0) as corrected,
    	tcd.rms_reject,
    	tcd.reject_reason,
    	0 as full_prop_price,
    	sum(tcd.ord_price) over(partition by tcd.contract_numbers) as full_order_price,
    	case when bbxd.order_number is not null then 1 else 0 end as bbxd_type
    from tmp_corr_detailed tcd
    left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
    left join gold_refcesh_ods.v_foudgene fou on tcd.supp_int_code = fou.foucfin  and fou.is_actual = '1'
--    left join tmp_statistics_gold_statistics_cover_detailed cov on cov.nprop = tcd.prop_number and cov.item = cov.item_internal
    left join tmp_statistics_franco fra on fra.key_number = coalesce(tcd.contract_numbers -> 'LS', tcd.contract_numbers -> 'EM',
    												  tcd.contract_numbers -> 'RD', tcd.contract_numbers -> 'RM') and fra.tech_case_no = 2
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
            AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and tcd.site = sat.satsite and sat.is_actual = '1'
    left join tmp_statistics_bbxd bbxd on bbxd.order_number = coalesce(tcd.contract_numbers -> 'LS', tcd.contract_numbers -> 'EM',
                           												  tcd.contract_numbers -> 'RD', tcd.contract_numbers -> 'RM')
    where tcd.tech_case_no = 2
        and coalesce(tcd.prop_date, tcd.order_date) between period_start and period_end;

    raise notice '[%] Inserted % rows into gold_corrections_detailed (orders without proposals)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    delete
    from gold_corrections_detailed s
    where exists(
    	select 1
    	from tmp_corr_detailed t
    	where s.prop = t.prop_number
    		and t.tech_case_no = 3
    		and coalesce(t.prop_date, t.order_date) between period_start and period_end);

    insert into gold_corrections_detailed (
    	reg, store, dep, order_date, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, supp, suppname,
        prop_price, delivery_date, cover_end_date, cover, franco, ord_price, item, order_item_qty, prop_item_qty,
    	corrected, rms_reject, reject_reason, full_prop_price, full_order_price, bbxd_type)
    select
    	cast(sat.satvaln as smallint) as reg,
    	tcd.site as store,
    	substring(fcc.fccnum, 7, 2) dep,
    	tcd.order_date as order_date,
    	tcd.prop_date as prop_date,
    	tcd.prop_number as prop,
    	tcd.contract_numbers -> 'LS' as ORD_ls,
    	tcd.contract_numbers -> 'EM' as ORD_EM,
    	tcd.contract_numbers -> 'RD' as ORD_RD,
    	tcd.contract_numbers -> 'RM' as ORD_RM,
    	fou.foucnuf as supp,
        fou.foulibl as suppname,
        tcd.pdcvapar as prop_price,
        tcd.delivery_date,
        tcd.cover_end_date,
        cast(tcd.cover_end_date::date - tcd.delivery_date::date as numeric(22,5)) as cover,
        fra.franco as franco,
        tcd.ord_price as ord_price,
        tcd.item as item,
        tcd.order_item_qty as order_item_qty,
    	tcd.prop_item_qty as prop_item_qty,
    	coalesce(tcd.order_item_qty, 0) - coalesce(tcd.prop_item_qty, 0) as corrected,
    	tcd.rms_reject,
    	tcd.reject_reason,
    	sum(tcd.prop_item_qty * prop_coeff_euro) over(partition by tcd.prop_number) as full_prop_price,
    	0 as full_order_price,
    	0 as bbxd_type
    from tmp_corr_detailed tcd
    left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
    left join gold_refcesh_ods.v_foudgene fou on tcd.supp_int_code = fou.foucfin  and fou.is_actual = '1'
    left join tmp_statistics_franco fra on fra.key_number = cast(tcd.prop_number as varchar(13)) and fra.tech_case_no = 1
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
            AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and tcd.site = sat.satsite and sat.is_actual = '1'
    where tcd.tech_case_no = 3
        and coalesce(tcd.prop_date, tcd.order_date) between period_start and period_end;

    raise notice '[%] Inserted % rows into gold_corrections_detailed (proposals without orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    raise notice '[%] Creation of detailed mart end' , date_trunc('second' , clock_timestamp())::text;

    return 0;
end;
$function$
;
