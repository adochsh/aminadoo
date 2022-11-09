--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_statistics_common
CREATE OR REPLACE FUNCTION fn_load_gold_statistics_common(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin

	raise notice '==================================== START =====================================';
    raise notice '[%] Creation of temp precalculated tables start' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_statistics_orders_link;

    insert into tmp_statistics_orders_link (key_number, h, tech_case_no)
    select cast(ecdcomm1 as bigint) as key_number, public.hstore(array_agg(flow order by flow), array_agg(orders order by flow)) as h, 1 as tech_case_no
    from (
    select ecdcomm1, substring(fc.fccnum, 4, 2) as flow, string_agg(ecdcexcde, ', ') as orders
    from v_cdeentcde_historical ecd
        join gold_refcesh_ods.v_fouccom fc on ecd.ecdccin = fc.fccccin and fc.is_actual = '1'
        where substring(fc.fccnum, 4, 2) in ('LS', 'EM', 'RD', 'RM')
        and  (ecdcomm1 ~ E'^\\d+$')
    group by ecdcomm1, substring(fc.fccnum, 4, 2)) tmp
    group by ecdcomm1;

    raise notice '[%] Inserted % rows into tmp_statistics_orders_link (proposals)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_statistics_orders_link (key_number, h, tech_case_no)
    select ecd.ecdcincde, public.hstore(substring(fc.fccnum, 4, 2), string_agg(ecdcexcde, ', ')) as h, 2 as tech_case_no
    from v_cdeentcde_historical ecd
        join gold_refcesh_ods.v_fouccom fc on ecd.ecdccin = fc.fccccin and fc.is_actual = '1'
        join gold_refgwr_ods.v_lmv_siteinteg lsi on ecd.ecdsite = lsi.site and lsi.is_actual = '1'
        where substring(fc.fccnum, 4, 2) in ('LS', 'EM', 'RD', 'RM')
        and  (not (ecdcomm1 ~ E'^\\d+$') or ecdcomm1 is null)
    group by ecd.ecdcincde, substring(fc.fccnum, 4, 2);

    perform public.fn_analyze_table('replenishment_marts','tmp_statistics_orders_link');

    raise notice '[%] Inserted % rows into tmp_statistics_orders_link (orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    truncate table tmp_statistics_users;

    insert into tmp_statistics_users (putnprop, ausextuser, putdmaj)
    select putnprop, ausextuser, putdmaj
    from (select putnprop, ausextuser, putdmaj, row_number() over(partition by putnprop order by putdmaj desc) rn
          from gold_refgwr_ods.v_SPE_KLT_PRPTRACEUSERS skp, gold_refgwr_ods.v_adm_users adu
          where pututil = aususer and skp.is_actual = '1' and adu.is_actual = '1') a
    where rn = 1;

    perform public.fn_analyze_table('replenishment_marts','tmp_statistics_users');

    raise notice '[%] Inserted % rows into tmp_statistics_users' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    truncate table tmp_next_proposal;

    insert into tmp_next_proposal(prop_id, next_delivery_date, srok)
    select pdc.pdcnprop as prop_id,
           lead(pdc.pdcdliv) over(partition by pdc.pdcsite order by ecd.ecddcom)::date as next_delivery_date,
           date_part('day', lead(pdc.pdcdliv) over(partition by pdc.pdcsite order by ecd.ecddcom) - pdc.pdcdliv) as srok
    from v_prpentprop_historical pdc
    join v_cdeentcde_historical ecd on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    where ecd.ecdcomm1 ~ E'^\\d+$';

    truncate table tmp_statistics_franco;

    insert into tmp_statistics_franco (tech_case_no, key_number, franco)
    select 1 as tech_case_no,
    	cast(pdc.pdcnprop as varchar(13)) as key_number,
    	cast(case when coalesce(substring(fcc.fccnum, 4, 2), '0') = '0' then lre1.lremini else lre2.lremini end  as double precision) franco
    from v_prpentprop_historical pdc
    LEFT join gold_refgwr_ods.v_lienfou lif ON  lif.lifsitef = pdc.pdcsite
    	and lif.lifsitep = pdc.pdcsite
    	and lif.lifccinf = pdc.pdcccin
    	and lif.lifcfinf = pdc.PDCCFIN
    	and cast(pdc.PDCDCDE as date) between cast(lif.lifddeb as date) AND cast(lif.lifdfin as date)
    	and lif.is_actual = '1'
    LEFT JOIN gold_refgwr_ods.v_lienreap lre1 on lre1.lrecfin = pdc.PDCCFIN
        and lre1.lrenfilf = pdc.pdcnfilf
        and lre1.lreccin = pdc.pdcccin
        and lre1.lresite = pdc.pdcsite
        and lre1.is_actual = '1'
    LEFT JOIN gold_refgwr_ods.v_lienreap lre2 ON lre2.lrecfin = pdc.PDCCFIN
    	and lre2.lrenfilf = pdc.pdcnfilf
    	and lre2.lreccin = lif.lifccinp
    	and lre2.lresite = pdc.pdcsite
    	and lre2.is_actual = '1'
    LEFT JOIN gold_refcesh_ods.v_fouccom fcc ON lif.lifccinp = fcc.fccccin and fcc.is_actual = '1';

    raise notice '[%] Inserted % rows into tmp_statistics_franco (proposals)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_statistics_franco (tech_case_no, key_number, franco)
    select 2 as tech_case_no,
    	cast(ecd.ecdcexcde as varchar(13)) as key_number,
    	cast(case when coalesce(substring(fcc.fccnum, 4, 2), '0') = '0' then lre1.lremini else lre2.lremini end  as double precision) franco
    from v_cdeentcde_historical ecd
    LEFT join gold_refgwr_ods.v_lienfou lif ON  lif.lifsitef = ecd.ecdsite
    	and lif.lifsitep = ecd.ecdsite
    	and lif.lifccinf = ecd.ecdccin
    	and lif.lifcfinf = ecd.ecdcfin
    	and cast(ecd.ecddcom as date) between cast(lif.lifddeb as date) AND cast(lif.lifdfin as date)
    	and lif.is_actual = '1'
    LEFT JOIN gold_refgwr_ods.v_lienreap lre1 on lre1.lrecfin = ecd.ecdcfin
        and lre1.lrenfilf = ecd.ecdnfilf
        and lre1.lreccin = ecd.ecdccin
        and lre1.lresite = ecd.ecdsite
        and lre1.is_actual = '1'
    LEFT JOIN gold_refgwr_ods.v_lienreap lre2 ON lre2.lrecfin = ecd.ecdcfin
    	and lre2.lrenfilf = ecd.ecdnfilf
    	and lre2.lreccin = lif.lifccinp
    	and lre2.lresite = ecd.ecdsite
    	and lre2.is_actual = '1'
    LEFT JOIN gold_refcesh_ods.v_fouccom fcc ON lif.lifccinp = fcc.fccccin and fcc.is_actual = '1'
    left join tmp_statistics_franco tf on tf.tech_case_no = 1 and tf.key_number = ecd.ecdcomm1
    where tf.key_number is null;

    perform public.fn_analyze_table('replenishment_marts','tmp_statistics_franco');

    raise notice '[%] Inserted % rows into tmp_statistics_franco (orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    drop table if exists tmp_rms_rejects_tsf;

    create temp table tmp_rms_rejects_tsf (nprop bigint, item text);

    insert into tmp_rms_rejects_tsf (nprop, item)
    select pdc.pdcnprop, art.artcexr
    from v_prpentprop_historical pdc
    join v_cdeentcde_historical ecd on ecd.ecdnvalo = pdc.pdcnprop
    join v_cdedetcde_historical dcd on dcd.dcdcexcde  = ecd.ecdcexcde
    left join gold_refcesh_ods.v_artrac art ON dcd.dcdcinr = art.artcinr
    join rms_p009qtzb_rms_ods.v_xxlm_rms_gold_doc_no rmdoc on ecd.ecdcexcde = rmdoc.gold_doc_no
    join rms_p009qtzb_rms_ods.v_tsfhead th on rmdoc.rms_doc_no = th.tsf_no and th.create_id = 'RMS'
    left join rms_p009qtzb_rms_ods.v_tsfdetail_hist td on td.TSF_NO = rmdoc.rms_doc_no AND td.ITEM = art.artcexr
    where cast(pdc.pdcdcde as date) between period_start and period_end
    and td.ITEM is null;

    drop table if exists tmp_rms_rejects_ord;

    create temp table tmp_rms_rejects_ord (nprop bigint, item text);

    insert into tmp_rms_rejects_ord (nprop, item)
    select pdc.pdcnprop, art.artcexr
    from v_prpentprop_historical pdc
    join v_cdeentcde_historical ecd on ecd.ecdnvalo = pdc.pdcnprop
    join v_cdedetcde_historical dcd on dcd.dcdcexcde  = ecd.ecdcexcde
    left join gold_refcesh_ods.v_artrac art ON dcd.dcdcinr = art.artcinr
    join rms_p009qtzb_rms_ods.v_xxlm_rms_gold_doc_no rmdoc on ecd.ecdcexcde = rmdoc.gold_doc_no
    join rms_p009qtzb_rms_ods.v_ordhead oh on rmdoc.rms_doc_no = oh.order_no and oh.orig_approval_id = 'RMS'
    left join rms_p009qtzb_rms_ods.v_ordloc_hist ol on ol.order_no = rmdoc.rms_doc_no AND ol.ITEM = art.artcexr
    where cast(pdc.pdcdcde as date) between period_start and period_end
    and ol.ITEM is null;

	drop table if exists tmp_rms_rejects;

    create temp table tmp_rms_rejects (nprop bigint, item text);

	insert into tmp_rms_rejects (nprop, item)
	select nprop, item
	from tmp_rms_rejects_tsf;

	insert into tmp_rms_rejects (nprop, item)
	select ord.nprop, ord.item
	from tmp_rms_rejects_ord ord
	left join tmp_rms_rejects_tsf tsf on ord.nprop = tsf.nprop and ord.item = tsf.item
	where tsf.nprop is null;

    raise notice '[%] Creation of temp precalculated tables end' , date_trunc('second' , clock_timestamp())::text;

    raise notice '[%] Creation of aggregated mart start' , date_trunc('second' , clock_timestamp())::text;

    delete
    from tmp_corr_detailed
    where coalesce(prop_date, order_date) between period_start and period_end;

    insert into tmp_corr_detailed (site, code_internal, prop_date, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, prop_item_qty,
        prop_coeff_euro, prop_line_code, contract_numbers, ord_price, order_item_qty, ord_item_price, item_price, item, tech_case_no, prp_amt, item_internal,
        cov_period, stock_qty, intransit_qty, lpcarrcde, lpccumbes, lpccumprev, sdpp, lpcarrref, pdcvapar, cover_delivery, delivery_date, cover_end_date,
        rms_reject)
    select pdc.pdcsite as site,
    	   pdc.pdcccin as code_internal,
    	   pdc.pdcdcde as prop_date,
    	   min(ecd.ecddcom) as order_date,
    	   pdc.pdcnprop as prop_number,
    	   pdc.pdctpro as prop_type,
    	   pdc.pdccfin as supp_int_code,
    	   pdc.pdcvalide as prop_is_valid,
    	   avg(lpc.lpcqtec) as prop_item_qty,
    	   avg(lpc.lpccoefeuro) prop_coeff_euro,
    	   lpc.lpccinr as prop_line_code,
    	   tol.h as contract_numbers,
    	   sum(dcd.dcdpbac) ord_price,
    	   sum(dcd.dcdqtec) order_item_qty,
    	   avg(dcd.dcdprix) ord_item_price,
    	   max(lpc.LPCAPAB) as item_price,
    	   art.artcexr as item,
    	   1 as tech_case_no,
    	   null as prp_amt,
    	   lpc.lpccinl as item_internal,
    	   DATE_PART('day', max(pdc.pdcdfin) - max(ecd.ecddcom)) as cov_period,
    	   avg(coalesce(lpc.LPCQSTR,0)) as stock_qty,
    	   avg(coalesce(lpc.LPCQRAL,0)) as intransit_qty,
    	   avg(coalesce(lpc.LPCARRCDE,0)) as lpcarrcde,
    	   avg(coalesce(lpc.LPCCUMBES,0)) as lpccumbes,
    	   avg(coalesce(lpc.LPCCUMPREV,0)) as lpccumprev,
    	   avg(coalesce(lpc.LPCCUMPREV,0))/nullif(DATE_PART('day', max(pdc.pdcdfin) - max(ecd.ecddcom)), 0) as sdpp,
    	   avg(coalesce(lpc.LPCARRREF,0)) as lpcarrref,
    	   cast(round(avg(lpc.lpcqtec * lpc.LPCAPNH),1) as numeric(22,5)) as pdcvapar,
    	   cast(avg(pdc.pdcdfin::date - pdc.pdcdliv::date) as numeric(22,5)) as cover_delivery,
    	   max(pdc.pdcdliv) as delivery_date,
           max(pdc.pdcdfin) as cover_end_date,
           max(case when rj.nprop is not null then 1 else 0 end) as rms_reject
    from v_prpentprop_historical pdc
    join v_prpdetprop_historical lpc on lpc.lpcnprop = pdc.pdcnprop
    join v_cdeentcde_historical ecd on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    left join v_cdedetcde_historical dcd on ecd.ecdcexcde = dcd.dcdcexcde and lpc.lpccinr = dcd.dcdcinr
    left join tmp_statistics_orders_link tol on pdc.pdcnprop = tol.key_number and tol.tech_case_no = 1
    LEFT JOIN gold_refcesh_ods.v_artrac art ON lpc.lpccinr = art.artcinr and art.is_actual = '1'
    left join tmp_rms_rejects rj on pdc.pdcnprop = rj.nprop and art.artcexr = rj.item
    where cast(pdc.pdcdcde as date) >= period_start and cast(pdc.pdcdcde as date) <= period_end
    group by pdc.pdcsite, pdc.pdcccin, pdc.pdcdcde, pdc.pdcnprop, pdc.pdctpro, pdc.pdccfin, pdc.pdcvalide,
    	lpc.lpccinr, tol.h, art.artcexr, lpc.lpccinl;

    raise notice '[%] Inserted % rows into tmp_corr_detailed (proposals with orders 1)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_corr_detailed (site, code_internal, prop_date, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, prop_item_qty,
        prop_coeff_euro, prop_line_code, contract_numbers, ord_price, order_item_qty, ord_item_price, item_price, item, tech_case_no, prp_amt, item_internal,
        cov_period, stock_qty, intransit_qty, lpcarrcde, lpccumbes, lpccumprev, sdpp, lpcarrref, pdcvapar, cover_delivery, delivery_date, cover_end_date,
        rms_reject)
    select pdc.pdcsite as site,
    	   pdc.pdcccin as code_internal,
    	   pdc.pdcdcde  as prop_date,
    	   min(ecd.ecddcom) as order_date,
    	   pdc.pdcnprop as prop_number,
    	   pdc.pdctpro as prop_type,
    	   pdc.pdccfin as supp_int_code,
    	   pdc.pdcvalide as prop_is_valid,
    	   avg(lpc.lpcqtec) as prop_item_qty,
    	   avg(lpc.lpccoefeuro) prop_coeff_euro,
    	   null prop_line_code,
    	   tol.h as contract_numbers,
    	   sum(dcd.dcdpbac) ord_price,
    	   sum(dcd.dcdqtec) order_item_qty,
    	   sum(dcd.dcdprix) ord_item_price,
    	   max(lpc.LPCAPAB) as item_price,
    	   art.artcexr as item,
    	   1 as tech_case_no,
    	   null as prp_amt,
    	   NULL as item_internal,
    	   DATE_PART('day', max(pdc.pdcdfin) - max(ecd.ecddcom)) as cov_period,
    	   avg(coalesce(lpc.LPCQSTR,0)) as stock_qty,
    	   avg(coalesce(lpc.LPCQRAL,0)) as intransit_qty,
    	   avg(coalesce(lpc.LPCARRCDE,0)) as lpcarrcde,
    	   avg(coalesce(lpc.LPCCUMBES,0)) as lpccumbes,
    	   avg(coalesce(lpc.LPCCUMPREV,0)) as lpccumprev,
    	   avg(coalesce(lpc.LPCCUMPREV,0))/nullif(DATE_PART('day', max(pdc.pdcdfin) - max(ecd.ecddcom)), 0) as sdpp,
    	   avg(coalesce(lpc.LPCARRREF,0)) as lpcarrref,
           cast(round(avg(lpc.lpcqtec * lpc.LPCAPNH),1) as numeric(22,5)) as pdcvapar,
           cast(avg(pdc.pdcdfin::date - pdc.pdcdliv::date) as numeric(22,5)) as cover_delivery,
           max(pdc.pdcdliv) as delivery_date,
           max(pdc.pdcdfin) as cover_end_date,
           max(case when rj.nprop is not null then 1 else 0 end) as rms_reject
    from v_cdedetcde_historical dcd
    	join v_cdeentcde_historical ecd on ecd.ecdcexcde = dcd.dcdcexcde
    	join v_prpentprop_historical pdc on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    	left join v_prpdetprop_historical lpc on lpc.lpcnprop = pdc.pdcnprop and lpc.lpccinr = dcd.dcdcinr
    	left join tmp_statistics_orders_link tol on pdc.pdcnprop = tol.key_number and tol.tech_case_no = 1
    	LEFT JOIN gold_refcesh_ods.v_artrac art ON dcd.DCDCINR = art.artcinr and art.is_actual = '1'
    	left join tmp_rms_rejects rj on pdc.pdcnprop = rj.nprop and art.artcexr = rj.item
    where lpc.lpccinr is null
    	and cast(pdc.pdcdcde as date) >= period_start and cast(pdc.pdcdcde as date) <= period_end
    group by pdc.pdcsite, pdc.pdcccin, pdc.pdcdcde, pdc.pdcnprop, pdc.pdctpro, pdc.pdccfin, pdc.pdcvalide,
    	dcd.dcdcinr, tol.h, art.artcexr, lpc.lpccinl;

    raise notice '[%] Inserted % rows into tmp_corr_detailed (proposals with orders 2)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    drop table if exists tmp_manual_reject;

    create temp table tmp_manual_reject(ord_no int8, reject_reason int2);

    insert into tmp_manual_reject(ord_no, reject_reason)
    select tsf_no, 1 as reject_reason
    from rms_p009qtzb_rms_ods.v_tsfhead th
    left join rms_p009qtzb_rms_ods.v_xxlm_rms_gold_doc_no gold on gold.rms_doc_no = th.tsf_no
    join gold_refcesh_ods.v_cdeentcde vc on vc.ecdcexcde = cast(th.tsf_no as text)
    where create_id = 'RMS' and gold.rms_doc_no is null;

    insert into tmp_manual_reject(ord_no, reject_reason)
    select oh.order_no, 2 as reject_reason
    from rms_p009qtzb_rms_ods.v_ordhead oh
    left join rms_p009qtzb_rms_ods.v_xxlm_rms_gold_doc_no gold on gold.RMS_DOC_NO = oh.order_no
    join gold_refcesh_ods.v_cdeentcde vc on vc.ecdcexcde = cast(oh.order_no as text)
    left join tmp_manual_reject tmr on oh.order_no = tmr.ord_no
    where orig_approval_id = 'RMS' and gold.RMS_DOC_NO is null and tmr.ord_no is null;

    insert into tmp_manual_reject(ord_no, reject_reason)
    select tsf_no, 3 as reject_reason
    from rms_p009qtzb_rms_ods.v_tsfhead th
    join boss_marts.v_emp_obj emp on th.CREATE_ID = emp.ldap
    left join tmp_manual_reject tmr on th.tsf_no = tmr.ord_no
    where not(lower(shop_name) like '%магазин%' or
		  lower(shop_name) like '%циз%' or
		  lower(shop_name) like '%центр исполнения заказов%')
    and th.from_loc_type = 'W' and th.to_loc_type = 'S' and tmr.ord_no is null;

    insert into tmp_manual_reject(ord_no, reject_reason)
    select oh.order_no, 4 as reject_reason
    from rms_p009qtzb_rms_ods.v_ordhead oh
    join boss_marts.v_emp_obj emp on oh.orig_approval_id = emp.ldap
    left join tmp_manual_reject tmr on oh.order_no = tmr.ord_no
    where not(lower(shop_name) like '%магазин%' or
		  lower(shop_name) like '%циз%' or
		  lower(shop_name) like '%центр исполнения заказов%')
	    and tmr.ord_no is null;

    insert into tmp_corr_detailed (site, code_internal, prop_date, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, prop_item_qty,
        prop_coeff_euro, prop_line_code, contract_numbers, ord_price, order_item_qty, ord_item_price, item_price, item, tech_case_no, prp_amt, item_internal,
        cov_period, stock_qty, intransit_qty, lpcarrcde, lpccumbes, lpccumprev, sdpp, lpcarrref, pdcvapar, cover_delivery, delivery_date, cover_end_date,
        rms_reject, reject_reason)
    select ecd.ecdsite as site,
    	   ecd.ecdccin as code_internal,
    	   null as prop_date,
    	   ecd.ecddcom as order_date,
    	   null as prop_number,
    	   null as prop_type,
    	   ecd.ecdcfin as supp_int_code,
    	   null as prop_is_valid,
    	   null as prop_item_qty,
    	   null as prop_coeff_euro,
    	   null as prop_line_code,
    	   tol.h as contract_numbers,
    	   dcd.dcdpbac as ord_price,
    	   dcd.dcdqtec as order_item_qty,
    	   dcd.dcdprix as ord_item_price,
    	   null as item_price,
    	   art.artcexr as item,
    	   2 as tech_case_no,
    	   null as prp_amt,
    	   dcd.dcdcinl as item_internal,
    	   null as cov_period,
    	   null as stock_qty,
    	   null as intransit_qty,
    	   null as lpcarrcde,
    	   null as lpccumbes,
    	   null as lpccumprev,
    	   null as sdpp,
    	   null as lpcarrref,
           null as pdcvapar,
           null as cover_delivery,
           null as delivery_date,
           null as cover_end_date,
           case when tmr.ord_no is not null then 1 else 0 end as rms_reject,
           coalesce(tmr.reject_reason, 0) as reject_reason
    from v_cdedetcde_historical dcd
    join v_cdeentcde_historical ecd on ecd.ecdcexcde = dcd.dcdcexcde
    left join v_prpentprop_historical pdc on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    join tmp_statistics_orders_link tol on ecd.ecdcincde = tol.key_number and tol.tech_case_no = 2
    LEFT JOIN gold_refcesh_ods.v_artrac art ON dcd.DCDCINR = art.artcinr and art.is_actual = '1'
    left join tmp_manual_reject tmr on ecd.ecdcexcde = cast(tmr.ord_no as text)
    where pdc.pdcnprop is null and ecd.ecdsite <> ecd.ecdcincde and ecd.ecdnvalo is null
    and cast(ecd.ecddcom as date) >= period_start and cast(ecd.ecddcom as date) <= period_end;

    raise notice '[%] Inserted % rows into tmp_corr_detailed (orders without proposals)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_corr_detailed (site, code_internal, prop_date, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, prop_item_qty,
        prop_coeff_euro, prop_line_code, contract_numbers, ord_price, order_item_qty, ord_item_price, item_price, item, tech_case_no, prp_amt, item_internal,
        cov_period, stock_qty, intransit_qty, lpcarrcde, lpccumbes, lpccumprev, sdpp, lpcarrref, pdcvapar, cover_delivery, delivery_date, cover_end_date,
        rms_reject)
    select pdc.pdcsite as site,
    	   pdc.pdcccin as code_internal,
    	   pdc.pdcdcde as prop_date,
    	   null as order_date,
    	   pdc.pdcnprop as prop_number,
    	   pdc.pdctpro as prop_type,
    	   pdc.pdccfin as supp_int_code,
    	   1 as prop_is_valid,
    	   case when lpc.lpcqtec > 0 or (lpc.lpcqtei is not null and lpc.lpcqtec = 0) then 1 else 0 end as prop_item_qty,
    	   lpc.lpccoefeuro as prop_coeff_euro,
    	   lpc.lpccinr as prop_line_code,
    	   null as contract_numbers,
    	   null as ord_price,
    	   null as order_item_qty,
    	   null as ord_item_price,
    	   lpc.LPCAPAB as item_price,
    	   art.artcexr as item,
    	   3 as tech_case_no,
    	   lpc.lpccoefeuro * lpc.lpcqtec as prp_amt,
    	   lpc.lpccinl as item_internal,
    	   null as cov_period,
    	   null as stock_qty,
    	   null as intransit_qty,
    	   null as lpcarrcde,
    	   null as lpccumbes,
    	   null as lpccumprev,
    	   null as sdpp,
    	   null as lpcarrref,
           cast(round(lpc.lpcqtec * lpc.LPCAPNH,1) as numeric(22,5)) as pdcvapar,
           cast(pdc.pdcdfin::date - pdc.pdcdliv::date as numeric(22,5)) as cover_delivery,
           pdc.pdcdliv as delivery_date,
           pdc.pdcdfin as cover_end_date,
           0 as rms_reject
    from v_prpentprop_historical pdc
    left join v_prpdetprop_historical lpc on lpc.lpcnprop = pdc.pdcnprop
    left join v_cdeentcde_historical ecd on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    left join tmp_corr_detailed t on pdc.pdcnprop = t.prop_number
    LEFT JOIN gold_refcesh_ods.v_artrac art ON lpc.LPCCINR = art.artcinr and art.is_actual = '1'
    where ecd.ecdcomm1 is null and t.prop_number is null
    and pdc.pdcvalide = 1
    and cast(pdc.pdcdcde as date) >= period_start and cast(pdc.pdcdcde as date) <= period_end;

    perform public.fn_analyze_table('replenishment_marts','tmp_corr_detailed');

    truncate table tmp_statistics_bbxd;

    insert into tmp_statistics_bbxd (order_number, bbxd_lines_qty,	bbxd_qty, bbxd_amount)
    select dcd.dcdcexcde as order_number, sum(1) as bbxd_lines_qty, sum(dcdqtec) as bbxd_qty, sum(dcdprix*dcdqtec) as bbxd_amount
    from tmp_corr_detailed tcd
    join v_cdedetcde_historical dcd on coalesce(tcd.contract_numbers -> 'LS', tcd.contract_numbers -> 'EM',
    												  tcd.contract_numbers -> 'RD', tcd.contract_numbers -> 'RM') = dcd.dcdcexcde
    join gold_refcesh_ods.v_artuc ara on cast(dcd.dcddcom as date) between ara.araddeb and ara.aradfin and ara.arasite = dcd.dcdsite and dcd.dcdcinr = ara.aracinr
    join gold_refgwr_ods.v_lmv_fouscheme sps on sps.spssite = arasite
                  and sps.spscfin = ara.aracfin
                  and sps.spsxddc like '%060%'
                  and dcd.dcddcom between sps.spsddeb and sps.spsdfin
    join gold_refcesh_ods.v_foudgene fou on tcd.supp_int_code = fou.foucfin  and fou.is_actual = '1'
    where tcd.tech_case_no = 2 and ara.is_actual = '1' and sps.is_actual = '1'
    and fou.foucnuf LIKE 'DC%'
    group by dcd.dcdcexcde;

    perform public.fn_analyze_table('replenishment_marts','tmp_statistics_bbxd');

    raise notice '[%] Inserted % rows into tmp_corr_detailed (proposals without orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;


    return 0;
end;
$function$
;
