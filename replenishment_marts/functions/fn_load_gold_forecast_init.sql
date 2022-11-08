--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_forecast_init
CREATE OR REPLACE FUNCTION fn_load_gold_forecast_init(cal_date date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted bigint;
begin
    raise notice '==================================== START =====================================';
    raise notice '[%] Inserting into replenishment_marts.gold_forecast' , date_trunc('second' , clock_timestamp())::text;

    drop table if exists tmp_gold_forecast;

    create temp table tmp_gold_forecast (
        code_lm varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        code_logistics bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        estm_forecast decimal(10,3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        adj_week_forecast decimal(10,3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        err_calc decimal(10,3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        site_code bigint not null encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        first_date_sale timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        avail_date_hist timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        week_num bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        "year" bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        pvdnsem bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        sale_chisen numeric NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        calculation_year bigint NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        calculation_week bigint NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        calculation_date date NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
    )
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (code_lm, pvdnsem);

    INSERT INTO tmp_gold_forecast (code_lm, code_logistics, estm_forecast, adj_week_forecast, err_calc, site_code,
                               first_date_sale, avail_date_hist, pvdnsem, week_num, "year", sale_chisen, calculation_year,
                               calculation_week, calculation_date)
    select artrac.ARTCEXR as code_lm,
           artul.ARUCINL as code_logistics,
           fdp.DPPVDCALC as estm_forecast,
           fdp.DPPVDCORR as adj_week_forecast,
           fdp.DPPVDERRA as err_calc,
           fep.DPPVESITE as site_code,
           fep.DPPVEDPV as first_date_sale,
           fep.DPPVEDHV as avail_date_hist,
           fdp.DPPVDNSEM as pvdnsem,
           fdp.DPPVDNSEM % 100 as week_num,
           fdp.DPPVDNSEM/100 as "year",
           fdp.DPPVDVRET as sale_chisen,
           sem.semnsem/100 as calculation_year,
           sem.semnsem % 100 as calculation_week,
           sem.semddeb as calculation_date
    from gold_refgwr_ods.v_fctdetpvh_dp fdp
    join gold_refgwr_ods.v_fctentpvh_dp fep on fdp.dppvdeid = fep.dppveid
    join gold_refcesh_ods.v_artul artul on fep.dppvecinl = artul.arucinl
    join gold_refcesh_ods.v_artrac artrac on artul.arucinr = artrac.artcinr
    join gold_refgwr_ods.v_fctsem sem on fdp.DPPVDINS between sem.semddeb and sem.semdfin
    where artul.is_actual = '1' and artrac.is_actual = '1' and fdp.is_actual = '1' and fep.is_actual = '1'
        and sem.is_actual = '1' and cal_date = cast(fdp.DPPVDINS as date);

    INSERT INTO tmp_gold_forecast (code_lm, code_logistics, estm_forecast, adj_week_forecast, err_calc, site_code,
                               first_date_sale, avail_date_hist, pvdnsem, week_num, "year", sale_chisen, calculation_year,
                               calculation_week, calculation_date)
    select artrac.ARTCEXR as code_lm,
           artul.ARUCINL as code_logistics,
           fdp.DPPVDCALC as estm_forecast,
           fdp.DPPVDCORR as adj_week_forecast,
           fdp.DPPVDERRA as err_calc,
           fep.DPPVESITE as site_code,
           fep.DPPVEDPV as first_date_sale,
           fep.DPPVEDHV as avail_date_hist,
           fdp.DPPVDNSEM as pvdnsem,
           fdp.DPPVDNSEM % 100 as week_num,
           fdp.DPPVDNSEM/100 as "year",
           fdp.DPPVDVRET as sale_chisen,
           sem.semnsem/100 as calculation_year,
           sem.semnsem % 100 as calculation_week,
           sem.semddeb as calculation_date
    from gold_refgwr_ods.v_fctdetpvh_dp_hist fdp
    join gold_refgwr_ods.v_fctentpvh_dp_hist fep on fdp.dppvdeid = fep.dppveid
    join gold_refcesh_ods.v_artul artul on fep.dppvecinl = artul.arucinl
    join gold_refcesh_ods.v_artrac artrac on artul.arucinr = artrac.artcinr
    join gold_refgwr_ods.v_fctsem sem on fdp.DPPVDINS between sem.semddeb and sem.semdfin
    where artul.is_actual = '1' and artrac.is_actual = '1'
        and sem.is_actual = '1' and cal_date = cast(fdp.DPPVDINS as date)
        and not exists (select 1
                        from tmp_gold_forecast t
                        where artrac.ARTCEXR = t.code_lm
                        and fdp.DPPVDNSEM = t.pvdnsem
                        and fep.DPPVESITE = t.site_code
                        and sem.semnsem = t.calculation_week);

    DELETE
    FROM gold_forecast g
    WHERE EXISTS (select 1
                  from tmp_gold_forecast t
                  where g.code_lm = t.code_lm
                  and g.pvdnsem = t.pvdnsem
                  and g.site_code = t.site_code
                  and g.calculation_week = t.calculation_week);

    INSERT INTO gold_forecast(code_lm, code_logistics, estm_forecast, adj_week_forecast, err_calc, site_code,
           first_date_sale, avail_date_hist, week_num, "year", sale_chisen, calculation_year, calculation_week,
           calculation_date)
    SELECT code_lm, code_logistics, estm_forecast, adj_week_forecast, err_calc, site_code,
                      first_date_sale, avail_date_hist, week_num, "year", sale_chisen, calculation_year, calculation_week,
                      calculation_date
    FROM tmp_gold_forecast;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into replenishment_marts.gold_forecast' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','gold_forecast');
    raise notice '[%] Function finished.' , date_trunc('second' , clock_timestamp())::text;
    raise notice '==================================== FINISH =====================================';
    return 0;
end;
$function$
;
