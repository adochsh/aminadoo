--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_new_items
CREATE OR REPLACE FUNCTION fn_load_gold_new_items()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin

    raise notice '==================================== START =====================================';
    raise notice '[%] Function started.' , date_trunc('second' , clock_timestamp())::text;

    truncate table gold_new_items;

    drop table if exists tmp_gamma;

    create temp table tmp_gamma (item text, uda_id int, uda_value_desc text)
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (item);

    insert into tmp_gamma (item, uda_id, uda_value_desc)
    select item, uda_id, uda_value_desc
    from (
    	select uil.item, uil.uda_id, uv.uda_value_desc,
    		row_number() OVER(PARTITION BY uil.item, uil.uda_id ORDER BY uil.last_update_datetime DESC) AS rn
        	   FROM rms_p009qtzb_rms_ods.v_uda_item_lov uil
        	   JOIN rms_p009qtzb_rms_ods.v_uda_values uv ON uil.uda_id = uv.uda_id
        	   AND uil.uda_value = uv.uda_value AND uil.is_actual='1' AND uv.is_actual='1'
        	   WHERE uil.uda_id = 5) a
    where rn = 1;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_gamma' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    drop table if exists tmp_store_articles;

    create temp table tmp_store_articles (store bigint, item text, item_desc text, gamma text, supplier bigint, wh_name text, status bpchar(1))
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (item);

    insert into tmp_store_articles (store, item, item_desc, gamma, supplier, wh_name, status)
    select il.loc as STORE,
    	   il.item as ITEM,
    	   il.local_item_desc as ITEM_DESC,
    	   tg.uda_value_desc as GAMMA,
    	   case when cast (il.source_wh as text) like '%000' or cast (il.source_wh as text) like '%001' then il.source_wh else il.primary_supp end as SUPPLIER,
    	   case when cast (il.source_wh as text) like '%000' or cast (il.source_wh as text) like '%001' then wh.wh_name else null end as wh_name,
    	   il.status as STATUS
    from  rms_p009qtzb_rms_ods.v_item_loc il
    left join rms_p009qtzb_rms_ods.v_wh wh on wh.wh = il.source_wh
    join tmp_gamma tg on il.item = tg.item
    where il.is_actual = '1' and loc_type ='S'
    and tg.uda_value_desc <> 'P' and tg.uda_value_desc <>  'T' and tg.uda_value_desc <>  'S'; --Updated Rows	20416803

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_store_articles' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    drop table if exists tmp_articles_report;

    create temp table tmp_articles_report (store bigint, dep text, item bigint, item_desc text, gamma text,
    		supplier bigint, status bpchar(1), internal_code bigint, in_store_market_basket text, sup_name text, zapas numeric)
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (item);

    insert into tmp_articles_report (store, dep, item, item_desc, gamma, supplier, status, internal_code, in_store_market_basket, sup_name, zapas)
    select tsa.store as STORE,
    	   left(cast(im.dept as text), -2) as DEP,
    	   cast(tsa.item as bigint) as ITEM,
    	   tsa.item_desc as ITEM_DESC,
    	   tsa.gamma as GAMMA,
    	   tsa.supplier as SUPPLIER,
    	   tsa.status as STATUS,
    	   aru.arucinl as internal_code,
    	   ilt.in_store_market_basket as in_store_market_basket,
    	   case when tsa.wh_name is null then s.sup_name else tsa.wh_name end as sup_name,
    	   ils.stock_on_hand as zapas
    from tmp_store_articles tsa
    join rms_p009qtzb_rms_ods.v_item_master im on im.item = tsa.item and im.is_actual = '1'
    JOIN rms_p009qtzb_rms_ods.v_item_supp_country isc ON isc.item = im.item and isc.is_actual='1'
    left join gold_refcesh_ods.v_artrac art on tsa.item = art.artcexr and art.is_actual = '1'
    left join gold_refcesh_ods.v_artul aru on aru.arucinr = art.artcinr and aru.arutypul = 1 and aru.is_actual = '1'
    left join rms_p009qtzb_rms_ods.v_item_loc_traits ilt on ilt.item = tsa.item and ilt.loc = tsa.store and ilt.is_actual = '1'
    left join rms_p009qtzb_rms_ods.v_sups s on s.supplier = tsa.supplier and s.is_actual = '1'
    left join rms_p009qtzb_rms_ods.v_item_loc_soh ils on ils.item = tsa.item and ils.loc = tsa.store
    		and ils.loc_type = 'S' AND ils.is_actual = '1' AND ils.stock_on_hand > 0
    where isc.primary_supp_ind = 'Y'
    	AND isc.primary_country_ind = 'Y'
    	and im.item_number_type = 'ITEM';

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_articles_report' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    drop table if exists tmp_reserve;

    create temp table tmp_reserve (appcinl bigint, appsite int, novinki int, pm int)
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (appcinl);

    insert into tmp_reserve (appcinl, appsite, novinki, pm)
    select appcinl, appsite,
    sum(case when apptype = 1 then appqte else null end) as novinki,
    sum(case when apptype = 4 then appqte else null end) as pm
      from gold_refgwr_ods.v_artparpre
      where is_actual = '1' and apptype in (1, 4)
      and current_date BETWEEN appddeb and appdfin
    group by appcinl, appsite;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_reserve' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    drop table if exists tmp_items_en_route;

    create temp table tmp_items_en_route (item bigint, loc int, qty bigint)
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (item);

    insert into tmp_items_en_route (item, loc, qty)
    select cast(dcdcode as bigint) as item, dcdsite as loc, sum(dcdqtec) as qty
    from v_cdedetcde_historical
    where dcdetat= 5
    group by dcdcode, dcdsite;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_items_en_route' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    drop table if exists tmp_cover;

    create temp table tmp_cover (store bigint, supp varchar, cover bigint)
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
        distributed randomly;

    insert into tmp_cover (store, supp, cover)
    select lmv.SPSSITE AS store
    	, f.FOUCNUF AS supp
    	, max(1 + coalesce(lmv.SPSDP,0) + coalesce(lmv.SPSDR,0) + coalesce(lmv.SPSXDDP,0) + coalesce(lmv.SPSXDDR,0)
    		- coalesce((lmv2.DELIVERY_EXC),0) + coalesce(lmv1.R_SROK,0)) AS cover
    from gold_refgwr_ods.v_lmv_fouscheme lmv
    JOIN gold_refcesh_ods.v_foudgene f ON f.FOUCFIN =lmv.SPSCFIN and f.is_actual = '1'
    JOIN gold_refgwr_ods.v_lmv_supfrancorep lmv1 ON lmv1.R_SUPP =f.FOUCNUF AND lmv1.R_STORE = lmv.SPSSITE and lmv1.is_actual = '1'
    LEFT JOIN gold_refgwr_ods.v_lmv_intfouscheme_exception lmv2 ON cast (lmv2.SUPPLIER as text) = f.FOUCNUF AND lmv2.site =lmv.SPSSITE and lmv2.is_actual = '1'
    where current_date between lmv.SPSDDEB and lmv.SPSDFIN
    and lmv.is_actual = '1'
    group by lmv.SPSSITE, f.FOUCNUF;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_cover' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;


    drop table if exists tmp_alert;
    create temp table tmp_alert (item bigint, loc int)
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (item);

    insert into tmp_alert (item, loc)
    SELECT ALEARG1 as item, alesite as loc
    from gold_refgwr_ods.v_fctalert
    where is_actual = '1' and alemess in ('New article', 'Starting') and aledcre >= current_timestamp - interval '28 days';

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_alert' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    drop table if exists tmp_cur_forecast ;

    create temp table tmp_cur_forecast (item bigint, loc int, forecast numeric(10,3))
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (item);

    insert into tmp_cur_forecast (item, loc, forecast)
    select pve.pvecinl as item, pve.pvesite as loc, pvd.pvdcalc as forecast
    from v_fctentpvh_historical pve
    join v_fctdetpvh_historical pvd on pve.pveid=pvd.pvdeid
    join gold_refgwr_ods.v_fctsem sem on pvd.pvdnsem = sem.semnsem and sem.is_actual = '1' and current_date BETWEEN sem.semddeb AND sem.semdfin;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_cur_forecast' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;


   drop table if exists tmp_sales_avg;

    create temp table tmp_sales_avg (item bigint, store int, sales numeric(14, 3));

    insert into tmp_sales_avg (item, store, sales)
    select  ss.smscinl as item,
    	smssite as store,
    	avg(coalesce (SMSCOIU + SMSSAIU, 0)) as median_sales
    from gold_refgwr_ods.v_fctsem f
    JOIN gold_refcesh_ods.v_stomvsemaine ss on f.semnsem = ss.smssemaine and ss.is_actual = '1'
    where f.is_actual = '1'
    and current_date between f.semddeb and f.semdfin
    group by ss.smscinl, ss.SMSSITE;


    drop table if exists tmp_sales_median;

    create temp table tmp_sales_median (item bigint, sales numeric(14, 3));

    insert into tmp_sales_median(item, sales)
    select  ss.smscinl as item,
    	median (coalesce (SMSCOIU + SMSSAIU, 0)) as sales
    from gold_refgwr_ods.v_fctsem f
    JOIN gold_refcesh_ods.v_stomvsemaine ss on f.semnsem = ss.smssemaine and ss.is_actual = '1'
    where f.is_actual = '1'
    and f.semddeb >= (current_date -90) and f.semddeb < current_date+7
    group by ss.smscinl;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_sales' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;


    --new
    drop table if exists tmp_new_items_splitted;

    create temp table tmp_new_items_splitted(store bigint, dep text, item bigint, item_desc text, gamma text,
    		supplier bigint, status bpchar(1), internal_code bigint, in_store_market_basket text,
    		sup_name text, zapas numeric, novinki int, pm int, new_item_type text);

    truncate table tmp_new_items_splitted;

    insert into tmp_new_items_splitted(store , dep, item, item_desc, gamma,
    		supplier, status, internal_code, in_store_market_basket,
    		sup_name, zapas, novinki, pm, new_item_type)
    select tar.store, tar.dep, tar.item, tar.item_desc, tar.gamma,
    		tar.supplier, tar.status, tar.internal_code, tar.in_store_market_basket,
    		tar.sup_name, tar.zapas, res.novinki, res.pm, 'NEW' as new_item_type
    from tmp_articles_report tar
    join tmp_alert ale on tar.internal_code = ale.item and tar.store = ale.loc
    left join tmp_reserve res on res.appcinl = tar.internal_code and res.appsite = tar.store
    where res.novinki <> 0 or res.novinki is null;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_new_items_splitted (NEW)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;


    --renew

    insert into tmp_new_items_splitted(store , dep, item, item_desc, gamma,
    		supplier, status, internal_code, in_store_market_basket,
    		sup_name, zapas, novinki, pm, new_item_type)
    select tar.store, tar.dep, tar.item, tar.item_desc, tar.gamma,
    		tar.supplier, tar.status, tar.internal_code, tar.in_store_market_basket,
    		tar.sup_name, tar.zapas, res.novinki, res.pm, 'RENEW' as new_item_type
    from tmp_articles_report tar
    left join tmp_alert ale on tar.internal_code = ale.item and tar.store = ale.loc
    left join tmp_reserve res on res.appcinl = tar.internal_code and res.appsite = tar.store
    join tmp_cur_forecast fct on fct.item = tar.internal_code and fct.loc = tar.store
    left join gold_refcesh_ods.v_artrac art on cast(tar.item as varchar)=art.artcexr and art.is_actual = '1'
    join gold_refcesh_ods.v_artvalres avr on avr.avrcinr = art.artcinr and avr.avrsite = tar.store and avr.avrcarac = '1'
    												and current_date BETWEEN avr.avrddeb and avr.avrdfin and avr.is_actual = '1'
    where ale.item is null and tar.in_store_market_basket <> 'TOP0' and coalesce(fct.forecast, 0) <=0.01
    	and avr.avrddeb BETWEEN current_date -90 AND current_date;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_new_items_splitted (RENEW)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    --sleep

    insert into tmp_new_items_splitted(store , dep, item, item_desc, gamma,
    		supplier, status, internal_code, in_store_market_basket,
    		sup_name, zapas, novinki, pm, new_item_type)
    select tar.store, tar.dep, tar.item, tar.item_desc, tar.gamma,
    		tar.supplier, tar.status, tar.internal_code, tar.in_store_market_basket,
    		tar.sup_name, tar.zapas, res.novinki, res.pm, 'SLEEP' as new_item_type
    from tmp_articles_report tar
    left join tmp_alert ale on tar.internal_code = ale.item and tar.store = ale.loc
    left join tmp_reserve res on res.appcinl = tar.internal_code and res.appsite = tar.store
    join tmp_cur_forecast fct on fct.item = tar.internal_code and fct.loc = tar.store
    left join gold_refcesh_ods.v_artrac art on cast(tar.item as varchar)=art.artcexr and art.is_actual = '1'
    join gold_refcesh_ods.v_artvalres avr on avr.avrcinr = art.artcinr and avr.avrsite = tar.store and avr.avrval = '1' and avr.avrcarac = '1'
    												and current_date BETWEEN avr.avrddeb and avr.avrdfin and avr.is_actual = '1'
    where ale.item is null and coalesce(fct.forecast, 0) <=0.01
    	AND avr.avrddeb <= current_date - 140;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into tmp_new_items_splitted (SLEEP)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;


    insert into gold_new_items (store, dep, item, item_desc, type, gamma, supplier, suplier_name, top,
        status, zapas, v_puti, pm, reserv_novinki, average_sales, median_sales, cover_period, updated_dttm)
	select nw.store, nw.dep, nw.item, nw.item_desc, nw.new_item_type as type, nw.gamma, nw.supplier, nw.sup_name as suplier_name, nw.in_store_market_basket as top,
        nw.status, coalesce(nw.zapas, 0) as zapas, coalesce(er.qty, 0) as v_puti, coalesce(nw.pm, 0) as pm,
        coalesce(nw.novinki, 0) as reserv_novinki, coalesce(savg.sales), coalesce(smed.sales), cov.cover as cover_period,
        current_timestamp as updated_dttm
    from tmp_new_items_splitted nw
    left join tmp_items_en_route er on nw.item = er.item and nw.store = er.loc
    left join tmp_cover cov on nw.store = cov.store and cast(nw.supplier as text) = cov.supp
    left join tmp_sales_avg savg on nw.internal_code = savg.item and nw.store = savg.store
    left join tmp_sales_median smed on nw.internal_code = smed.item;


    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into replenishment_marts.gold_new_items' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','gold_new_items');

    raise notice '[%] Function finished.' , date_trunc('second' , clock_timestamp())::text;
    raise notice '==================================== FINISH =====================================';
    return 0;
end;
$function$
;
