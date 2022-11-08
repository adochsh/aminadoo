--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_reserve

CREATE OR REPLACE FUNCTION fn_load_gold_reserve(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin
    raise notice '==================================== START =====================================';
    raise notice '[%] Inserting into replenishment_marts.gold_reserve' , date_trunc('second' , clock_timestamp())::text;

    drop table if exists tmp_gold_reserve_raw;

    create temp table tmp_gold_reserve_raw (
        code_lm varchar(13),
        code_logistics bigint,
        reserve_type varchar(20),
        reserve_code int2,
        qty int,
        start_date timestamp without time zone,
        end_date timestamp without time zone,
        qty_type VARCHAR(255),
        store_num integer,
        beginning_dt timestamp(0)
    );

    drop table if exists tmp_gold_reserve;

    create temp table tmp_gold_reserve (
    	code_lm varchar(13) NULL,
    	code_logistics int8 NULL,
    	reserve_type varchar(20) NULL,
    	reserve_code int2,
    	qty numeric(12,2) NULL,
    	start_date timestamp NULL,
    	end_date timestamp NULL,
    	store_num int4 NULL,
    	qty_per_day numeric(22,5) null,
    	updated_dttm timestamp(0) null
    	)
    	WITH (
    		appendonly=true,
    		compresslevel=1,
    		orientation=column,
    		compresstype=zstd
    	);

    INSERT INTO tmp_gold_reserve_raw (code_lm, code_logistics, reserve_type, reserve_code, qty, start_date, end_date, qty_type, store_num, beginning_dt)
    SELECT atc.ARTCEXR AS lm_code,
           atl.ARUCINL AS code_logistics,
           tp1743.TPARLIBL AS reserve_type,
           app.APPTYPE AS reserve_code,
           app.APPQTE AS qty,
           app.APPDDEB AS start_date,
           app.APPDFIN AS end_date,
           tp1737.TPARLIBL AS qty_type,
           app.APPSITE AS store_num,
           CASE when current_date between cast(app.APPDDEB as date) and cast(app.APPDFIN as date) THEN current_date else app.APPDDEB END AS beginning_dt
    FROM gold_refgwr_ods.v_artparpre app
    left JOIN gold_refcesh_ods.v_artul atl ON app.APPCINL = atl.ARUCINL
    left JOIN gold_refcesh_ods.v_artrac atc ON atl.ARUCINR = atc.ARTCINR
    left JOIN gold_refcesh_ods.v_tra_parpostes tp1737 ON tp1737.TPARPOST = app.APPUPRE AND tp1737.tpartabl=1737 AND tp1737.LANGUE='RU' AND tp1737.tparcmag=0
    left  JOIN gold_refcesh_ods.v_tra_parpostes tp1743 ON tp1743.TPARPOST = app.APPTYPE AND tp1743.tpartabl=1743 AND tp1743.LANGUE='RU' AND tp1743.tparcmag=0
    WHERE coalesce(app.APPAUTO,0) <> 1 and cast(app.appdmaj as date) between period_start and period_end
    and app.is_actual = '1' and atl.is_actual = '1' and atc.is_actual = '1' and tp1737.is_actual = '1' and tp1743.is_actual = '1';

    INSERT INTO tmp_gold_reserve (code_lm, code_logistics, reserve_type, reserve_code, qty, qty_per_day, start_date, end_date, store_num, updated_dttm)
    SELECT code_lm, code_logistics, reserve_type, reserve_code, qty, null, start_date, end_date, store_num, current_timestamp
    FROM tmp_gold_reserve_raw
    WHERE qty_type = 'Units';

    raise notice '[%] Inserted % rows with reserve type measured in Units' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    INSERT INTO tmp_gold_reserve
    (code_lm, code_logistics, reserve_type, reserve_code, qty, start_date, end_date, store_num, qty_per_day, updated_dttm)
    SELECT tgr.code_lm,
	   tgr.code_logistics,
	   tgr.reserve_type,
	   tgr.reserve_code,
	   case when count(pvdnsem) = 0 then null
            else round(((sum(coalesce(fdp.pvdcorr, coalesce(fdp.pvdcalc, 0))) / (count(fdp.pvdnsem) * 7) ) * tgr.qty),2) end as qty_per_day,
	   tgr.start_date,
	   tgr.end_date,
	   tgr.store_num,
	   tgr.qty,
	   current_timestamp
    FROM tmp_gold_reserve_raw tgr
    LEFT JOIN gold_refgwr_ods.v_fctsem fcs1 on tgr.beginning_dt  BETWEEN fcs1.SEMDDEB AND fcs1.SEMDFIN  and fcs1.is_actual = '1'
    LEFT JOIN gold_refgwr_ods.v_fctsem fcs2 on tgr.beginning_dt + tgr.qty * interval '1 days' BETWEEN fcs2.SEMDDEB AND fcs2.SEMDFIN and fcs2.is_actual = '1'
    LEFT JOIN v_fctentpvh_historical fep ON pvesite = tgr.store_num AND pvecinl = tgr.code_logistics
    LEFT JOIN v_fctdetpvh_historical fdp on fep.pveid = fdp.pvdeid AND fdp.PVDNSEM BETWEEN fcs1.SEMNSEM AND fcs2.SEMNSEM
    WHERE tgr.qty_type = 'Days'
    GROUP BY tgr.code_lm,
	   tgr.code_logistics,
	   tgr.reserve_type,
	   tgr.reserve_code,
	   tgr.qty,
	   tgr.start_date,
	   tgr.end_date,
	   tgr.store_num;

    raise notice '[%] Inserted % rows with reserve type measured in Days' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

	truncate table tmp_gold_reserve_raw;

    INSERT INTO tmp_gold_reserve_raw (code_lm, code_logistics, reserve_type, reserve_code, qty, start_date, end_date, qty_type, store_num, beginning_dt)
    SELECT atc.ARTCEXR AS code_lm,
           atl.ARUCINL AS code_logistics,
           tp1743.TPARLIBL AS reserve_type,
           app.APPTYPE AS reserve_code,
           app.APPQTE AS qty,
           app.APPDDEB AS start_date,
         	   app.APPDFIN AS end_date,
         	   tp1737.TPARLIBL AS qty_type,
         	   app.APPSITE AS store_num,
         	   CASE when current_date between cast(app.APPDDEB as date) and cast(app.APPDFIN as date) THEN current_date else app.APPDDEB END AS beginning_dt
    FROM gold_refgwr_ods.v_artparpre app
    left JOIN gold_refcesh_ods.v_artul atl ON app.APPCINL = atl.ARUCINL and atl.is_actual = '1'
    left JOIN gold_refcesh_ods.v_artrac atc ON atl.ARUCINR = atc.ARTCINR and atc.is_actual = '1'
    left JOIN gold_refcesh_ods.v_tra_parpostes tp1737 ON tp1737.TPARPOST = app.APPUPRE AND tp1737.tpartabl=1737 AND tp1737.LANGUE='RU' AND tp1737.tparcmag=0 and tp1737.is_actual = '1'
    left  JOIN gold_refcesh_ods.v_tra_parpostes tp1743 ON tp1743.TPARPOST = app.APPTYPE AND tp1743.tpartabl=1743 AND tp1743.LANGUE='RU' AND tp1743.tparcmag=0 and tp1743.is_actual = '1'
    WHERE coalesce(app.APPAUTO,0) = 1
        and cast(app.appdmaj as date) between period_start and period_end
        and app.is_actual = '1';


    drop table if exists tmp_gold_reserve_auto_days_forecast;

    create temp table tmp_gold_reserve_auto_days_forecast (
            appcinl int8,
            appsite int4,
            appddeb timestamp(0),
            appdfin timestamp(0),
            days_forecast numeric(22,5)
        );

    INSERT INTO tmp_gold_reserve_auto_days_forecast(appcinl, appsite, appddeb, appdfin, days_forecast)
    SELECT app.appcinl, app.appsite, app.appddeb, app.appdfin,
    cast(coalesce(ROUND(SUM(CASE extract(day from cas.CASDATE - sem.SEMDDEB)
                            WHEN 0 THEN REPCJ01
                            WHEN 1 THEN REPCJ02
    						WHEN 2 THEN REPCJ03
    						WHEN 3 THEN REPCJ04
    						WHEN 4 THEN REPCJ05
                            WHEN 5 THEN REPCJ06
                            ELSE REPCJ07 END  * COALESCE(PVDCALC, 0)),
                           3),
                     0) as numeric(22,5)) as days_forecast
    FROM gold_refgwr_ods.v_artparpre app
    LEFT JOIN (SELECT LFOROBID, LFOCINL, max(LFOCINLR) AS inlr
        		FROM gold_refgwr_ods.v_lienfor
        		WHERE LFOTLIEN = 1 and is_actual = '1'
        		GROUP BY LFOROBID, LFOCINL) lf ON lf.LFOROBID = app.APPSITE AND lf.LFOCINL = app.APPCINL
    LEFT JOIN gold_refcesh_ods.v_artul atl ON atl.ARUCINL = coalesce(lf.inlr, app.APPCINL) and atl.is_actual = '1'
    LEFT JOIN (SELECT REPFDSID, REPSITE, REPCJ01, REPCJ02, REPCJ03, REPCJ04, REPCJ05, REPCJ06, REPCJ07, row_number() over(PARTITION BY REPFDSID, REPSITE ORDER BY repdmaj desc) rn
               FROM gold_refgwr_ods.v_fctrep
               WHERE is_actual = '1') fr ON fr.REPFDSID = atl.ARUFDSID AND fr.repsite = app.APPSITE AND fr.rn = 1
    LEFT JOIN gold_refcesh_ods.v_calsite cas ON app.APPSITE  = cas.CASSITE AND cas.CASDATE BETWEEN app.appddeb + 1*7 * interval '1 days' AND app.appddeb + 2*7*interval '1 days' and cas.is_actual = '1'
    LEFT JOIN gold_refgwr_ods.v_fctsem sem ON cas.CASDATE BETWEEN sem.SEMDDEB AND sem.SEMDFIN and sem.is_actual = '1'
    LEFT JOIN v_fctentpvh_historical fep ON fep.PVESITE = cas.CASSITE AND fep.PVECINL = coalesce(lf.inlr, app.APPCINL)
    LEFT JOIN v_fctdetpvh_historical fdp ON fep.pveid = fdp.PVDEID AND sem.SEMNSEM = fdp.PVDNSEM
    WHERE app.appauto = 1 and app.is_actual = '1' and cast(app.appdmaj as date) between period_start and period_end
    GROUP BY app.appcinl, app.appsite, app.appddeb, app.appdfin;


  INSERT INTO tmp_gold_reserve
        (code_lm, code_logistics, reserve_type, reserve_code, qty, start_date, end_date, store_num, qty_per_day, updated_dttm)
    SELECT tgr.code_lm,
	   tgr.code_logistics,
	   tgr.reserve_type,
	   tgr.reserve_code,
	   aut.days_forecast,
	   tgr.start_date,
	   tgr.end_date,
	   tgr.store_num,
	   case when count(pvdnsem) = 0 then null
       	    else  round(((sum(coalesce(fdp.pvdcorr, coalesce(pvdcalc, 0))) / (count(fdp.pvdnsem) * 7) ) * aut.days_forecast),2) end as qty_per_day,
	   current_timestamp
    FROM tmp_gold_reserve_raw tgr
    LEFT JOIN tmp_gold_reserve_auto_days_forecast aut on tgr.code_logistics = aut.appcinl
                                                      and tgr.store_num = aut.appsite
                                                      and tgr.start_date = aut.appddeb
                                                      and tgr.end_date = aut.appdfin
    LEFT JOIN gold_refgwr_ods.v_fctsem fcs1 on tgr.beginning_dt  BETWEEN fcs1.SEMDDEB AND fcs1.SEMDFIN  and fcs1.is_actual = '1'
    LEFT JOIN gold_refgwr_ods.v_fctsem fcs2 on tgr.beginning_dt + aut.days_forecast * interval '1 days' BETWEEN fcs2.SEMDDEB AND fcs2.SEMDFIN and fcs2.is_actual = '1'
    LEFT JOIN v_fctentpvh_historical fep ON pvesite = tgr.store_num AND pvecinl = tgr.code_logistics
    LEFT JOIN v_fctdetpvh_historical fdp on fep.pveid = fdp.pvdeid AND fdp.PVDNSEM BETWEEN fcs1.SEMNSEM AND fcs2.SEMNSEM
    GROUP BY tgr.code_lm,
	   tgr.code_logistics,
	   tgr.reserve_type,
	   tgr.reserve_code,
	   aut.days_forecast,
	   tgr.start_date,
	   tgr.end_date,
	   tgr.store_num;

	delete
        from gold_reserve gr
        where exists (select 1
    		    from tmp_gold_reserve t
    		    where gr.code_lm = t.code_lm and gr.code_logistics = t.code_logistics
    		    and gr.reserve_type = t.reserve_type and gr.start_date = t.start_date
    		    and gr.store_num = t.store_num);

	insert into gold_reserve (code_lm, code_logistics, reserve_type, reserve_code, qty, start_date, end_date, store_num, qty_per_day, updated_dttm)
	select code_lm, code_logistics, reserve_type, reserve_code, qty, start_date, end_date, store_num, qty_per_day, updated_dttm
	from tmp_gold_reserve;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into replenishment_marts.gold_reserve' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','gold_reserve');
    raise notice '[%] Function finished.' , date_trunc('second' , clock_timestamp())::text;
    raise notice '==================================== FINISH =====================================';
    return 0;
end;
$function$
;
