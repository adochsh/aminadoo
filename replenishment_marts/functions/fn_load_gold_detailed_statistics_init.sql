--liquibase formatted sql
--changeset 60098727:create:fn_load_gold_detailed_statistics_init
CREATE OR REPLACE FUNCTION fn_load_gold_detailed_statistics_init(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin

raise notice '==================================== START =====================================';
    raise notice '[%] Creation of temp precalculated tables start' , date_trunc('second' , clock_timestamp())::text;

    drop table if exists tmp_orders_link;
    drop table if exists tmp_statistics_users;
    drop table if exists tmp_gold_statistics_cover;
    drop table if exists tmp_bbxd;

    create temp table tmp_orders_link (
        key_number bigint,
        h public.hstore,
        tech_case_no int
    	)
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed randomly;

    insert into tmp_orders_link (key_number, h, tech_case_no)
    select cast(ecdcomm1 as bigint) as key_number, public.hstore(array_agg(flow order by flow), array_agg(orders order by flow)) as h, 1 as tech_case_no
    from (
    select ecdcomm1, substring(fc.fccnum, 4, 2) as flow, string_agg(ecdcexcde, ', ') as orders
    from v_cdeentcde_historical ecd
        join gold_refcesh_ods.v_fouccom fc on ecd.ecdccin = fc.fccccin and fc.is_actual = '1'
        where substring(fc.fccnum, 4, 2) in ('LS', 'EM', 'RD', 'RM')
        and  (ecdcomm1 ~ E'^\\d+$')
    group by ecdcomm1, substring(fc.fccnum, 4, 2)) tmp
    group by ecdcomm1;

    raise notice '[%] Inserted % rows into tmp_orders_link (proposals)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_orders_link (key_number, h, tech_case_no)
    select ecd.ecdcincde, public.hstore(substring(fc.fccnum, 4, 2), string_agg(ecdcexcde, ', ')) as h, 2 as tech_case_no
    from v_cdeentcde_historical ecd
        join gold_refcesh_ods.v_fouccom fc on ecd.ecdccin = fc.fccccin and fc.is_actual = '1'
        join gold_refgwr_ods.v_lmv_siteinteg lsi on ecd.ecdsite = lsi.site and lsi.is_actual = '1'
        where substring(fc.fccnum, 4, 2) in ('LS', 'EM', 'RD', 'RM')
        and  (not (ecdcomm1 ~ E'^\\d+$') or ecdcomm1 is null)
    group by ecd.ecdcincde, substring(fc.fccnum, 4, 2);

    raise notice '[%] Inserted % rows into tmp_orders_link (orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    create temp table tmp_statistics_users (
        putnprop numeric(22, 0),
        ausextuser varchar(1280),
        putdmaj timestamp
        )
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed randomly;

    insert into tmp_statistics_users (putnprop, ausextuser, putdmaj)
    select putnprop, ausextuser, putdmaj
    from (select putnprop, ausextuser, putdmaj, row_number() over(partition by putnprop order by putdmaj desc) rn
          from gold_refgwr_ods.v_SPE_KLT_PRPTRACEUSERS skp, gold_refgwr_ods.v_adm_users adu
          where pututil = aususer and skp.is_actual = '1' and adu.is_actual = '1') a
    where rn = 1;

    raise notice '[%] Inserted % rows into tmp_statistics_users' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    create temp table tmp_gold_statistics_cover(nprop bigint, pdcvapar numeric(22,5), cover numeric(22,5),
       												delivery_date timestamp(0), cover_end_date timestamp(0));

    insert into tmp_gold_statistics_cover(nprop, pdcvapar, cover, delivery_date, cover_end_date)
    select lpc.lpcnprop,
      	cast(round((sum(lpc.lpcqtec * lpc.LPCAPNH)),1) as numeric(22,5)) as pdcvapar_new,
       	cast(avg(pdc.pdcdfin::date - pdc.pdcdliv::date) as numeric(22,5)) as cover,
       	max(pdc.pdcdliv) as delivery_date,
       	max(pdc.pdcdfin) as cover_end_date
    from v_prpentprop_historical pdc
    join v_prpdetprop_historical lpc on lpc.lpcnprop = pdc.pdcnprop
    group by lpc.lpcnprop;

    raise notice '[%] Inserted % rows into tmp_gold_statistics_cover' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    raise notice '[%] Creation of temp precalculated tables end' , date_trunc('second' , clock_timestamp())::text;

    raise notice '[%] Creation of aggregated mart start' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_corr_detailed;

    insert into tmp_corr_detailed (site, code_internal, prop_date, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, prop_item_qty,
    	prop_coeff_euro, prop_line_code, contract_numbers, ord_price, order_item_qty, ord_item_price, item, tech_case_no)
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
    	   art.artcexr as item,
    	   1 as tech_case_no
    from v_prpentprop_historical pdc
    join v_prpdetprop_historical lpc on lpc.lpcnprop = pdc.pdcnprop
    join v_cdeentcde_historical ecd on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    left join v_cdedetcde_historical dcd on ecd.ecdcexcde = dcd.dcdcexcde and lpc.lpccinr = dcd.dcdcinr
    left join tmp_orders_link tol on pdc.pdcnprop = tol.key_number and tol.tech_case_no = 1
    LEFT JOIN gold_refcesh_ods.v_artrac art ON lpc.lpccinr = art.artcinr and art.is_actual = '1'
    where cast(pdc.pdcdcde as date) >= period_start and cast(pdc.pdcdcde as date) <= period_end
    group by pdc.pdcsite, pdc.pdcccin, pdc.pdcdcde, pdc.pdcnprop, pdc.pdctpro, pdc.pdccfin, pdc.pdcvalide,
    	lpc.lpccinr, tol.h, art.artcexr;

    raise notice '[%] Inserted % rows into tmp_corr_detailed (proposals with orders 1)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_corr_detailed (site, code_internal, prop_date, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, prop_item_qty,
    	prop_coeff_euro, prop_line_code, contract_numbers, ord_price, order_item_qty, ord_item_price, item, tech_case_no)
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
    	   art.artcexr as item,
    	   1 as tech_case_no
    from v_cdedetcde_historical dcd
    	join v_cdeentcde_historical ecd on ecd.ecdcexcde = dcd.dcdcexcde
    	join v_prpentprop_historical pdc on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    	left join v_prpdetprop_historical lpc on lpc.lpcnprop = pdc.pdcnprop and lpc.lpccinr = dcd.dcdcinr
    	left join tmp_orders_link tol on pdc.pdcnprop = tol.key_number and tol.tech_case_no = 1
    	LEFT JOIN gold_refcesh_ods.v_artrac art ON dcd.DCDCINR = art.artcinr and art.is_actual = '1'
    where lpc.lpccinr is null
    	and cast(pdc.pdcdcde as date) >= period_start and cast(pdc.pdcdcde as date) <= period_end
    group by pdc.pdcsite, pdc.pdcccin, pdc.pdcdcde, pdc.pdcnprop, pdc.pdctpro, pdc.pdccfin, pdc.pdcvalide,
    	dcd.dcdcinr, tol.h, art.artcexr;

    raise notice '[%] Inserted % rows into tmp_corr_detailed (proposals with orders 2)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_corr_detailed (site, code_internal, prop_date, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, prop_item_qty,
    	prop_coeff_euro, prop_line_code, contract_numbers, ord_price, order_item_qty, ord_item_price, item, tech_case_no)
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
    	   art.artcexr as item,
    	   2 as tech_case_no
    from v_cdedetcde_historical dcd
    join v_cdeentcde_historical ecd on ecd.ecdcexcde = dcd.dcdcexcde
    left join v_prpentprop_historical pdc on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    join tmp_orders_link tol on ecd.ecdcincde = tol.key_number and tol.tech_case_no = 2
    LEFT JOIN gold_refcesh_ods.v_artrac art ON dcd.DCDCINR = art.artcinr and art.is_actual = '1'
    where pdc.pdcnprop is null and ecd.ecdsite <> ecd.ecdcincde and ecd.ecdnvalo is null
    and cast(ecd.ecddcom as date) >= period_start and cast(ecd.ecddcom as date) <= period_end;

    raise notice '[%] Inserted % rows into tmp_corr_detailed (orders without proposals)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_corr_detailed (
    	site, code_internal, prop_date, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, prop_item_qty,
    	prop_coeff_euro, prop_line_code, contract_numbers, ord_price, order_item_qty, ord_item_price, item, tech_case_no, prp_amt)
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
    	   art.artcexr as item,
    	   3 as tech_case_no,
    	   lpc.lpccoefeuro * lpc.lpcqtec as prp_amt
    from v_prpentprop_historical pdc
    left join v_prpdetprop_historical lpc on lpc.lpcnprop = pdc.pdcnprop
    left join v_cdeentcde_historical ecd on ecd.ecdcomm1 = cast(pdc.pdcnprop as varchar)
    left join tmp_corr_detailed t on pdc.pdcnprop = t.prop_number
    LEFT JOIN gold_refcesh_ods.v_artrac art ON lpc.LPCCINR = art.artcinr and art.is_actual = '1'
    where ecd.ecdcomm1 is null and t.prop_number is null
    and pdc.pdcvalide = 1
    and cast(pdc.pdcdcde as date) >= period_start and cast(pdc.pdcdcde as date) <= period_end;

    raise notice '[%] Inserted % rows into tmp_corr_detailed (proposals without orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    delete
    from gold_corrections_detailed s
    where exists(
    	select 1
    	from tmp_corr_detailed t
    	where s.prop = t.prop_number
    		and t.tech_case_no = 1);

    insert into gold_corrections_detailed (
    	reg, store, dep, order_date, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, supp, suppname,
        prop_price, delivery_date, cover_end_date, cover, franco, ord_price, item, order_item_qty, prop_item_qty,
    	corrected, full_prop_price, full_order_price, bbxd_type)
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
    	fra.supp as supp,
        fra.suppname as suppname,
        cov.pdcvapar as prop_price,
        cov.delivery_date as delivery_date,
        cov.cover_end_date as cover_end_date,
        cov.cover as cover,
        fra.franco as franco,
        tcd.ord_price as ord_price,
        tcd.item as item,
        tcd.order_item_qty as order_item_qty,
    	tcd.prop_item_qty as prop_item_qty,
    	coalesce(tcd.order_item_qty, 0) - coalesce(tcd.prop_item_qty, 0) as corrected,
    	sum(tcd.prop_item_qty * prop_coeff_euro) over(partition by tcd.prop_number) as full_prop_price,
    	sum(tcd.ord_price) over(partition by tcd.prop_number) as full_order_price,
    	0 as bbxd_type
    from tmp_corr_detailed tcd
    left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
    left join gold_refcesh_ods.v_foudgene fou on tcd.code_internal = fou.foucfin  and fou.is_actual = '1'
    left join tmp_gold_statistics_cover cov on cov.nprop = tcd.prop_number
    left join replenishment_marts.v_gold_statistics fra on cast(fra.prop as varchar(13)) = cast(tcd.prop_number as varchar(13))
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
            AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and tcd.site = sat.satsite and sat.is_actual = '1'
    where tcd.tech_case_no = 1;

    create temp table tmp_bbxd (
        	order_number varchar(13),
        	bbxd_lines_qty double precision,
        	bbxd_qty double precision,
            bbxd_amount double precision);

    insert into tmp_bbxd (order_number, bbxd_lines_qty,	bbxd_qty, bbxd_amount)
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


        delete
        from gold_corrections_detailed s
        where exists(
        	select 1
        	from tmp_corr_detailed t
        	where coalesce(t.contract_numbers -> 'LS', t.contract_numbers -> 'EM',
        		  		   t.contract_numbers -> 'RD', t.contract_numbers -> 'RM') =
        		  coalesce(s.ord_ls, s.ord_em, s.ord_rd, s.ord_rm)
        		and t.tech_case_no = 2);

        insert into gold_corrections_detailed (
        	reg, store, dep, order_date, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, supp, suppname,
            prop_price, delivery_date, cover_end_date, cover, franco, ord_price, item, order_item_qty, prop_item_qty,
        	corrected, full_prop_price, full_order_price, bbxd_type)
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
        	fra.supp as supp,
            fra.suppname as suppname,
            cov.pdcvapar as prop_price,
            cov.delivery_date,
            cov.cover_end_date,
            cov.cover as cover,
            fra.franco as franco,
            tcd.ord_price as ord_price,
            tcd.item as item,
            tcd.order_item_qty as order_item_qty,
        	0 as prop_item_qty,
        	coalesce(tcd.order_item_qty, 0) - coalesce(tcd.prop_item_qty, 0) as corrected,
        	0 as full_prop_price,
        	sum(tcd.ord_price) over(partition by tcd.contract_numbers) as full_order_price,
        	case when bbxd.order_number is not null then 1 else 0 end as bbxd_type
        from tmp_corr_detailed tcd
        left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
        left join gold_refcesh_ods.v_foudgene fou on tcd.code_internal = fou.foucfin  and fou.is_actual = '1'
        left join tmp_gold_statistics_cover cov on cov.nprop = tcd.prop_number
        left join replenishment_marts.v_gold_statistics fra
        	on coalesce(fra.ord_ls, fra.ord_em, fra.ord_rd, fra.ord_rm) = coalesce(tcd.contract_numbers -> 'LS', tcd.contract_numbers -> 'EM',
        												  tcd.contract_numbers -> 'RD', tcd.contract_numbers -> 'RM')
        left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
                AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and tcd.site = sat.satsite and sat.is_actual = '1'
        left join tmp_bbxd bbxd on bbxd.order_number = coalesce(tcd.contract_numbers -> 'LS', tcd.contract_numbers -> 'EM',
                               												  tcd.contract_numbers -> 'RD', tcd.contract_numbers -> 'RM')
        where tcd.tech_case_no = 2;

        raise notice '[%] Inserted % rows into gold_corrections_detailed (orders without proposals)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

        delete
        from gold_corrections_detailed s
        where exists(
        	select 1
        	from tmp_corr_detailed t
        	where s.prop = t.prop_number
        		and t.tech_case_no = 3);

        insert into gold_corrections_detailed (
        	reg, store, dep, order_date, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, supp, suppname,
            prop_price, delivery_date, cover_end_date, cover, franco, ord_price, item, order_item_qty, prop_item_qty,
        	corrected, full_prop_price, full_order_price, bbxd_type)
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
        	fra.supp as supp,
            fra.suppname as suppname,
            cov.pdcvapar as prop_price,
            cov.delivery_date,
            cov.cover_end_date,
            cov.cover as cover,
            fra.franco as franco,
            tcd.ord_price as ord_price,
            tcd.item as item,
            tcd.order_item_qty as order_item_qty,
        	tcd.prop_item_qty as prop_item_qty,
        	coalesce(tcd.order_item_qty, 0) - coalesce(tcd.prop_item_qty, 0) as corrected,
        	sum(tcd.prop_item_qty * prop_coeff_euro) over(partition by tcd.prop_number) as full_prop_price,
        	0 as full_order_price,
        	0 as bbxd_type
        from tmp_corr_detailed tcd
        left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
        left join gold_refcesh_ods.v_foudgene fou on tcd.code_internal = fou.foucfin  and fou.is_actual = '1'
        left join tmp_gold_statistics_cover cov on cov.nprop = tcd.prop_number
        left join replenishment_marts.v_gold_statistics fra  on cast(fra.prop as varchar(13)) = cast(tcd.prop_number as varchar(13))
        left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
                AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and tcd.site = sat.satsite and sat.is_actual = '1'
        where tcd.tech_case_no = 3;

    return 0;
end;
$function$
;
