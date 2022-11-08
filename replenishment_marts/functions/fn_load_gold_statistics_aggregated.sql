--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_statistics_aggregated_incremental
CREATE OR REPLACE FUNCTION fn_load_gold_statistics_aggregated(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin

    raise notice '[%] Creation of aggregated mart start' , date_trunc('second' , clock_timestamp())::text;

    drop table if exists tmp_gold_statistics_aggregated_step1;

    create temp table tmp_gold_statistics_aggregated_step1 (
        store int4,
        dep varchar(8),
        prop_date timestamp(0),
        prop double precision,
        ord_ls varchar(4000),
        ord_em varchar(4000),
        ord_rd varchar(4000),
        ord_rm varchar(4000),
        type varchar(4000),
        supp varchar(4000),
        suppname varchar(4000),
        prop_price double precision,
        cover double precision,
        ord_price double precision,
        qty double precision,
        changed_qty double precision,
        negative_correction double precision,
        positive_correction double precision,
        foucfin int8,
        rms_reject int2
        )
    with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
    distributed by (prop);

    drop table if exists tmp_gold_statistics_aggregated_step2;

   CREATE temp TABLE tmp_gold_statistics_aggregated_step2 (
	store int4 NULL,
	dep varchar(8) NULL,
	prop_date timestamp(0) NULL,
	prop float8 NULL,
	ord_ls varchar(4000) NULL,
	ord_em varchar(4000) NULL,
	ord_rd varchar(4000) NULL,
	ord_rm varchar(4000) NULL,
	"type" varchar(4000) NULL,
	supp varchar(4000) NULL,
	suppname varchar(4000) NULL,
	prop_price float8 NULL,
	cover float8 NULL,
	franco float8 NULL,
	ord_price float8 NULL,
	qty float8 NULL,
	changed_qty float8 NULL,
	util varchar(1280) NULL,
	dmaj varchar(20) NULL,
	reg int2 NULL,
	negative_correction float8 NULL,
	positive_correction float8 NULL,
	rms_reject_items int4 NULL,
	rms_reject_qty float8 NULL,
	bbxd_lines_qty float8 NULL,
	bbxd_qty float8 NULL,
	bbxd_amount float8 NULL,
	updated_dttm timestamp NOT NULL DEFAULT clock_timestamp(),
	target_dep numeric(12,3) NULL,
	target_reg numeric(12,3) NULL,
	target_store numeric(12,3) NULL
    )
    WITH (appendonly=true, compresslevel=1,	orientation=column,	compresstype=zstd)
    distributed by (prop);

    raise notice '[%] Start inserting into gold_statistics (proposals with orders)' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_gold_statistics_aggregated_step1;
    truncate table tmp_gold_statistics_aggregated_step2;

    insert into tmp_gold_statistics_aggregated_step1 (store, dep, prop_date, prop, ord_ls, ord_em,
            ord_rd, ord_rm, type, supp, suppname, prop_price, cover, ord_price, qty, changed_qty,
            negative_correction, positive_correction, rms_reject)
    select
        tcd.site as store,
        substring(fcc.fccnum, 7, 2) as dep,
        tcd.prop_date as prop_date,
        tcd.prop_number as prop,
        tcd.contract_numbers -> 'LS' as ord_ls,
        tcd.contract_numbers -> 'EM' as ord_em,
        tcd.contract_numbers -> 'RD' as ord_rd,
        tcd.contract_numbers -> 'RM' as ord_rm,
        tp.tparlibl as type,
        fou.foucnuf as supp,
        fou.foulibl as suppname,
        tcd.pdcvapar as prop_price,
        tcd.cover_delivery as cover,
        tcd.ord_price as ord_price,
        case when tcd.prop_item_qty > 0 or tcd.order_item_qty  > 0 then 1 else 0 end as qty,
        case when coalesce(tcd.order_item_qty, -1) <> coalesce(tcd.prop_item_qty, -1) and (tcd.prop_item_qty > 0 or tcd.order_item_qty  > 0) then 1 else 0 end as changed_qty,
        case when coalesce(prop_item_qty, -1) > coalesce(order_item_qty, -1) then (coalesce(prop_item_qty,0) - coalesce(order_item_qty,0)) * case when ord_item_price is null or ord_item_price = 0 then prop_coeff_euro else ord_item_price end else 0 end as negative_correction,
        case when coalesce(prop_item_qty, -1) < coalesce(order_item_qty, -1) then (coalesce(order_item_qty,0) - coalesce(prop_item_qty,0)) * case when ord_item_price is null or ord_item_price = 0 then prop_coeff_euro else ord_item_price end else 0 end as positive_correction,
        tcd.rms_reject
    from tmp_corr_detailed tcd
    left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
    left join gold_refcesh_ods.v_tra_parpostes tp on tp.tparcmag = 10 and tp.tpartabl = 1703 and tp.tparpost = tcd.prop_type and tp.langue = 'RU' and tp.is_actual = '1'
    left join gold_refcesh_ods.v_foudgene fou on tcd.supp_int_code = fou.foucfin  and fou.is_actual = '1'
    where tcd.tech_case_no = 1
    and coalesce(tcd.prop_date, tcd.order_date) between period_start and period_end;

    insert into tmp_gold_statistics_aggregated_step2 (store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, type, supp, suppname, prop_price,
                cover, franco, ord_price, qty, changed_qty, util, dmaj, reg, negative_correction, positive_correction, rms_reject_items, rms_reject_qty,
                bbxd_lines_qty, bbxd_qty, bbxd_amount, target_dep, target_reg, target_store)
    select
        s1.store,
        s1.dep,
        s1.prop_date,
        s1.prop,
        s1.ord_ls,
        s1.ord_em,
        s1.ord_rd,
        s1.ord_rm,
        s1.type,
        s1.supp,
        s1.suppname,
        sum(s1.prop_price) as prop_price,
        avg(s1.cover) as cover,
        avg(fra.franco) as franco,
        sum(s1.ord_price) as ord_price,
        sum(s1.qty) as qty,
        sum(s1.changed_qty) as changed_qty,
        usr.ausextuser as util,
        usr.putdmaj as dmaj,
        cast(sat.satvaln as smallint) as reg,
        sum(s1.negative_correction) as negative_correction,
        sum(s1.positive_correction) as positive_correction,
        coalesce(sum(s1.rms_reject), 0) as rms_reject_items,
        coalesce(sum(s1.rms_reject*s1.prop_price), 0) as rms_reject_qty,
        null as bbxd_lines_qty,
        null as bbxd_qty,
        null as bbxd_amount,
        avg(par_dep.parvan1/100) as target_dep,
        avg(par_reg.parvan1/100) as target_reg,
        avg(par_store.parvan1/100) as target_store
    from tmp_gold_statistics_aggregated_step1 s1
    left join tmp_statistics_franco fra on fra.key_number = cast(s1.prop as varchar(13)) and fra.tech_case_no = 1
    left join tmp_statistics_users usr on usr.putnprop = s1.prop
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
            AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and s1.store = sat.satsite and sat.is_actual = '1'
    left join gold_refcesh_ods.v_parpostes par_dep on par_dep.parcmag=10 and par_dep.partabl=9005 and par_dep.parpost = cast(s1.dep as int) and par_dep.is_actual='1'
    left join gold_refcesh_ods.v_parpostes par_store on par_store.parcmag=10 and par_store.partabl=9005 and par_store.parpost = 16 and par_store.is_actual='1'
    left join gold_refcesh_ods.v_parpostes par_reg on par_reg.parcmag=10 and par_reg.partabl=9005 and par_reg.parpost = 17 and par_dep.is_actual='1'
    group by s1.store, s1.dep, s1.prop_date, s1.prop, s1.ord_ls, s1.ord_em, s1.ord_rd, s1.ord_rm, s1.type, usr.ausextuser,
        usr.putdmaj, s1.supp, s1.suppname, sat.satvaln;

    delete
    from gold_statistics s
    where exists(
    	select 1
    	from tmp_corr_detailed t
    	where s.prop = t.prop_number
    		and t.tech_case_no = 1
    		and coalesce(t.prop_date, t.order_date) between period_start and period_end);

    insert into gold_statistics (store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, type, supp, suppname, prop_price,
            cover, franco, ord_price, qty, changed_qty, util, dmaj, reg, negative_correction, positive_correction,
            bbxd_lines_qty, bbxd_qty, bbxd_amount, target_dep, target_reg, target_store, supplier_type, rms_reject_items,
            rms_reject_qty)
    select
        s2.store,
        s2.dep,
        s2.prop_date,
        s2.prop,
        s2.ord_ls,
        s2.ord_em,
        s2.ord_rd,
        s2.ord_rm,
        s2.type,
        s2.supp,
        s2.suppname,
        s2.prop_price as prop_price,
        s2.cover as cover,
        s2.franco as franco,
        s2.ord_price as ord_price,
        s2.qty as qty,
        s2.changed_qty as changed_qty,
        s2.util,
        s2.dmaj,
        s2.reg,
        s2.negative_correction as negative_correction,
        s2.positive_correction as positive_correction,
        s2.bbxd_lines_qty as bbxd_lines_qty,
        s2.bbxd_qty as bbxd_qty,
        s2.bbxd_amount as bbxd_amount,
        s2.target_dep as target_dep,
        s2.target_reg as target_reg,
        s2.target_store as target_store,
        case when coalesce (lre.LRESPCDEV,3) = 3 then 'Рабочий лист' when lre.LRESPCDEV = 5 then 'Автозаказ' end as supplier_type,
        s2.rms_reject_items,
        s2.rms_reject_qty
    from tmp_gold_statistics_aggregated_step2 s2
    join gold_refcesh_ods.v_foudgene fou on foucnuf=supp and fou.is_actual ='1'
    left join gold_refgwr_ods.v_lienreap lre on store=lre.lresite and fou.foucfin=lre.lrecfin and lre.is_actual ='1'
    left join gold_refcesh_ods.v_FOUCCOM com on com.fccccin=lre.lreccin  and com.is_actual ='1'
    where s2.prop_date between coalesce(com.fccddeb, s2.prop_date) and coalesce(com.fccdfin, s2.prop_date) and coalesce(substring(com.FCCNUM,1,5), 'FL-LS')='FL-LS' and coalesce(RIGHT(com.FCCNUM, 2), s2.dep) = s2.dep;


    raise notice '[%] Inserted % rows into gold_statistics (proposals with orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;


    raise notice '[%] Start inserting into gold_statistics (orders without proposals)' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_gold_statistics_aggregated_step1;
    truncate table tmp_gold_statistics_aggregated_step2;

    insert into tmp_gold_statistics_aggregated_step1 (store, dep, prop_date, prop, ord_ls, ord_em,
                ord_rd, ord_rm, type, supp, suppname, prop_price, cover, ord_price, qty, changed_qty,
                negative_correction, positive_correction, rms_reject)
    select
        tcd.site as store,
        substring(fcc.fccnum, 7, 2) as dep,
        tcd.order_date as prop_date,
        tcd.prop_number as prop,
        tcd.contract_numbers -> 'LS' as ord_ls,
        tcd.contract_numbers -> 'EM' as ord_em,
        tcd.contract_numbers -> 'RD' as ord_rd,
        tcd.contract_numbers -> 'RM' as ord_rm,
        NULL as type,
        fou.foucnuf as supp,
        fou.foulibl as suppname,
        NULL as prop_price,
        NULL as cover,
        tcd.ord_price as ord_price,
        case when tcd.prop_item_qty > 0 or tcd.order_item_qty  > 0 then 1 else 0 end as qty,
        0  as changed_qty,
        NULL as negative_correction,
        case when coalesce(prop_item_qty, -1) < coalesce(order_item_qty, -1) then (coalesce(order_item_qty,0) - coalesce(prop_item_qty,0)) * case when ord_item_price is null or ord_item_price = 0 then prop_coeff_euro else ord_item_price end else 0 end as positive_correction,
        tcd.rms_reject
    from tmp_corr_detailed tcd
    left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
    left join gold_refcesh_ods.v_foudgene fou on tcd.supp_int_code = fou.foucfin  and fou.is_actual = '1'
    where tcd.tech_case_no = 2
    and coalesce(tcd.prop_date, tcd.order_date) between period_start and period_end;


    insert into tmp_gold_statistics_aggregated_step2 (store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, type, supp, suppname, prop_price,
                         cover, franco, ord_price, qty, changed_qty, util, dmaj, reg, negative_correction, positive_correction, rms_reject_items, rms_reject_qty,
                         bbxd_lines_qty, bbxd_qty, bbxd_amount, target_dep, target_reg, target_store)
    select
        s1.store,
        s1.dep,
        s1.prop_date,
        s1.prop,
        s1.ord_ls,
        s1.ord_em,
        s1.ord_rd,
        s1.ord_rm,
        s1.type,
        s1.supp,
        s1.suppname,
        sum(s1.prop_price) as prop_price,
        avg(s1.cover) as cover,
        avg(fra.franco) as franco,
        sum(s1.ord_price) as ord_price,
        sum(s1.qty) as qty,
        sum(s1.changed_qty) as changed_qty,
        NULL as util,
        NULL as dmaj,
        cast(sat.satvaln as smallint) as reg,
        sum(s1.negative_correction) as negative_correction,
        sum(s1.positive_correction) as positive_correction,
        coalesce(sum(s1.rms_reject), 0) as rms_reject_items,
        coalesce(sum(s1.rms_reject*s1.prop_price), 0) as rms_reject_qty,
    	avg(bbxd.bbxd_lines_qty) as bbxd_lines_qty,
    	avg(bbxd.bbxd_qty) as bbxd_qty,
    	avg(bbxd.bbxd_amount) as bbxd_amount,
        avg(par_dep.parvan1/100) as target_dep,
        avg(par_reg.parvan1/100) as target_reg,
        avg(par_store.parvan1/100) as target_store
    from tmp_gold_statistics_aggregated_step1 s1
    left join tmp_statistics_franco fra on fra.key_number = coalesce(s1.ord_ls, s1.ord_em,
    												  s1.ord_rd, s1.ord_rm) and fra.tech_case_no = 2
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
            AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and s1.store = sat.satsite and sat.is_actual = '1'
    left join tmp_statistics_bbxd bbxd on bbxd.order_number = coalesce(s1.ord_ls, s1.ord_em, s1.ord_rd, s1.ord_rm)
    left join gold_refcesh_ods.v_parpostes par_dep on par_dep.parcmag=10 and par_dep.partabl=9005 and par_dep.parpost = cast(s1.dep as int) and par_dep.is_actual='1'
    left join gold_refcesh_ods.v_parpostes par_store on par_store.parcmag=10 and par_store.partabl=9005 and par_store.parpost = 16 and par_store.is_actual='1'
    left join gold_refcesh_ods.v_parpostes par_reg on par_reg.parcmag=10 and par_reg.partabl=9005 and par_reg.parpost = 17 and par_dep.is_actual='1'
    group by s1.store, s1.dep, s1.prop_date, s1.prop, s1.ord_ls, s1.ord_em, s1.ord_rd, s1.ord_rm, s1.type, s1.supp, s1.suppname, sat.satvaln;

    delete
    from gold_statistics s
    where exists(
    	select 1
    	from tmp_corr_detailed t
    	where coalesce(t.contract_numbers -> 'LS', t.contract_numbers -> 'EM',
    		  		   t.contract_numbers -> 'RD', t.contract_numbers -> 'RM') =
    		  coalesce(s.ord_ls, s.ord_em, s.ord_rd, s.ord_rm)
    		and t.tech_case_no = 2
    		and coalesce(t.prop_date, t.order_date) between period_start and period_end);


    insert into gold_statistics (store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, type, supp, suppname, prop_price,
            cover, franco, ord_price, qty, changed_qty, util, dmaj, reg, negative_correction, positive_correction,
            bbxd_lines_qty, bbxd_qty, bbxd_amount, target_dep, target_reg, target_store, supplier_type, rms_reject_items,
            rms_reject_qty)
    select
        s2.store,
        s2.dep,
        s2.prop_date,
        s2.prop,
        s2.ord_ls,
        s2.ord_em,
        s2.ord_rd,
        s2.ord_rm,
        s2.type,
        s2.supp,
        s2.suppname,
        s2.prop_price as prop_price,
        s2.cover as cover,
        s2.franco as franco,
        s2.ord_price as ord_price,
        s2.qty as qty,
        s2.changed_qty as changed_qty,
        s2.util,
        s2.dmaj,
        s2.reg,
        s2.negative_correction as negative_correction,
        s2.positive_correction as positive_correction,
        s2.bbxd_lines_qty as bbxd_lines_qty,
        s2.bbxd_qty as bbxd_qty,
        s2.bbxd_amount as bbxd_amount,
        s2.target_dep as target_dep,
        s2.target_reg as target_reg,
        s2.target_store as target_store,
        case when coalesce (lre.LRESPCDEV,3) = 3 then 'Рабочий лист' when lre.LRESPCDEV = 5 then 'Автозаказ' end as supplier_type,
        s2.rms_reject_items,
        s2.rms_reject_qty
    from tmp_gold_statistics_aggregated_step2 s2
    join gold_refcesh_ods.v_foudgene fou on foucnuf=supp and fou.is_actual ='1'
    left join gold_refgwr_ods.v_lienreap lre on store=lre.lresite and fou.foucfin=lre.lrecfin and lre.is_actual ='1'
    left join gold_refcesh_ods.v_FOUCCOM com on com.fccccin=lre.lreccin  and com.is_actual ='1'
    where s2.prop_date between coalesce(com.fccddeb, s2.prop_date) and coalesce(com.fccdfin, s2.prop_date) and coalesce(substring(com.FCCNUM,1,5), 'FL-LS')='FL-LS' and coalesce(RIGHT(com.FCCNUM, 2), s2.dep) = s2.dep;

    raise notice '[%] Inserted % rows into gold_statistics (orders without proposals)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    raise notice '[%] Start inserting into gold_statistics (proposals without orders)' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_gold_statistics_aggregated_step1;
    truncate table tmp_gold_statistics_aggregated_step2;

    insert into tmp_gold_statistics_aggregated_step1 (store, dep, prop_date, prop, ord_ls, ord_em,
                ord_rd, ord_rm, type, supp, suppname, prop_price, cover, ord_price, qty, changed_qty,
                negative_correction, positive_correction, rms_reject)
    select
        tcd.site as store,
    	substring(fcc.fccnum, 7, 2) dep,
    	tcd.prop_date as prop_date,
    	tcd.prop_number as prop,
    	NULL as ORD_ls,
    	NULL as ORD_EM,
    	NULL as ORD_RD,
    	NULL as ORD_RM,
    	tp.tparlibl as type,
    	fou.foucnuf as supp,
        fou.foulibl as suppname,
        tcd.pdcvapar as prop_price,
        tcd.cover_delivery as cover,
        NULL as ord_price,
        tcd.prop_item_qty,
        0 as changed_qty,
        tcd.prp_amt as negative_correction,
        case when coalesce(prop_item_qty, -1) < coalesce(order_item_qty, -1) then (coalesce(order_item_qty,0) - coalesce(prop_item_qty,0)) * case when ord_item_price is null or ord_item_price = 0 then prop_coeff_euro else ord_item_price end else 0 end as positive_correction,
        tcd.rms_reject
    from tmp_corr_detailed tcd
    left join gold_refcesh_ods.v_fouccom fcc on tcd.code_internal = fcc.fccccin and fcc.is_actual = '1'
    left join gold_refcesh_ods.v_tra_parpostes tp on tp.tparcmag = 10 and tp.tpartabl = 1703 and tp.tparpost = tcd.prop_type and tp.langue = 'RU' and tp.is_actual = '1'
    left join gold_refcesh_ods.v_foudgene fou on tcd.supp_int_code = fou.foucfin  and fou.is_actual = '1'
    where tcd.tech_case_no = 3
    and coalesce(tcd.prop_date, tcd.order_date) between period_start and period_end;

    insert into tmp_gold_statistics_aggregated_step2 (store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, type, supp, suppname, prop_price,
                      cover, franco, ord_price, qty, changed_qty, util, dmaj, reg, negative_correction, positive_correction, rms_reject_items, rms_reject_qty,
                      bbxd_lines_qty, bbxd_qty, bbxd_amount, target_dep, target_reg, target_store)
    select
        s1.store,
        s1.dep,
        s1.prop_date,
        s1.prop,
        s1.ord_ls,
        s1.ord_em,
        s1.ord_rd,
        s1.ord_rm,
        s1.type,
        s1.supp,
        s1.suppname,
        sum(s1.prop_price) as prop_price,
        avg(s1.cover) as cover,
        avg(fra.franco) as franco,
        sum(s1.ord_price) as ord_price,
        sum(s1.qty) as qty,
        sum(s1.changed_qty) as changed_qty,
    	'PropWOOrder' as util,
        usr.putdmaj as dmaj,
        cast(sat.satvaln as smallint) as reg,
        sum(s1.negative_correction) as negative_correction,
        sum(s1.positive_correction) as positive_correction,
        coalesce(sum(s1.rms_reject), 0) as rms_reject_items,
        coalesce(sum(s1.rms_reject*s1.prop_price), 0) as rms_reject_qty,
        null as bbxd_lines_qty,
        null as bbxd_qty,
        null as bbxd_amount,
        avg(par_dep.parvan1/100) as target_dep,
        avg(par_reg.parvan1/100) as target_reg,
        avg(par_store.parvan1/100) as target_store
    from tmp_gold_statistics_aggregated_step1 s1
    left join tmp_statistics_franco fra on fra.key_number = cast(s1.prop as varchar(13)) and fra.tech_case_no = 1
    left join tmp_statistics_users usr on usr.putnprop = s1.prop
    left join gold_refcesh_ods.v_sitattri sat on sat.satcla='SETTINGS' AND sat.satatt='REG'
            AND cast(current_timestamp as date) between cast(sat.satddeb as date) and cast(sat.satdfin as date) and s1.store = sat.satsite and sat.is_actual = '1'
    left join gold_refcesh_ods.v_parpostes par_dep on par_dep.parcmag=10 and par_dep.partabl=9005 and par_dep.parpost = cast(s1.dep as int) and par_dep.is_actual='1'
    left join gold_refcesh_ods.v_parpostes par_store on par_store.parcmag=10 and par_store.partabl=9005 and par_store.parpost = 16 and par_store.is_actual='1'
    left join gold_refcesh_ods.v_parpostes par_reg on par_reg.parcmag=10 and par_reg.partabl=9005 and par_reg.parpost = 17 and par_dep.is_actual='1'
    group by s1.store, s1.dep, s1.prop_date, s1.prop, s1.ord_ls, s1.ord_em, s1.ord_rd, s1.ord_rm, s1.type,
        usr.putdmaj, s1.supp, s1.suppname, sat.satvaln;

    delete
    from gold_statistics s
    where exists(
    	select 1
    	from tmp_corr_detailed t
    	where s.prop = t.prop_number
    		and t.tech_case_no = 3
    		and coalesce(t.prop_date, t.order_date) between period_start and period_end);

    insert into gold_statistics (store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, type, supp, suppname, prop_price,
            cover, franco, ord_price, qty, changed_qty, util, dmaj, reg, negative_correction, positive_correction,
            bbxd_lines_qty, bbxd_qty, bbxd_amount, target_dep, target_reg, target_store, supplier_type, rms_reject_items,
            rms_reject_qty)
    select
        s2.store,
        s2.dep,
        s2.prop_date,
        s2.prop,
        s2.ord_ls,
        s2.ord_em,
        s2.ord_rd,
        s2.ord_rm,
        s2.type,
        s2.supp,
        s2.suppname,
        s2.prop_price as prop_price,
        s2.cover as cover,
        s2.franco as franco,
        s2.ord_price as ord_price,
        s2.qty as qty,
        s2.changed_qty as changed_qty,
        s2.util,
        s2.dmaj,
        s2.reg,
        s2.negative_correction as negative_correction,
        s2.positive_correction as positive_correction,
        s2.bbxd_lines_qty as bbxd_lines_qty,
        s2.bbxd_qty as bbxd_qty,
        s2.bbxd_amount as bbxd_amount,
        s2.target_dep as target_dep,
        s2.target_reg as target_reg,
        s2.target_store as target_store,
        case when coalesce (lre.LRESPCDEV,3) = 3 then 'Рабочий лист' when lre.LRESPCDEV = 5 then 'Автозаказ' end as supplier_type,
        s2.rms_reject_items,
        s2.rms_reject_qty
    from tmp_gold_statistics_aggregated_step2 s2
    join gold_refcesh_ods.v_foudgene fou on foucnuf=supp and fou.is_actual ='1'
    left join gold_refgwr_ods.v_lienreap lre on store=lre.lresite and fou.foucfin=lre.lrecfin and lre.is_actual ='1'
    left join gold_refcesh_ods.v_FOUCCOM com on com.fccccin=lre.lreccin  and com.is_actual ='1'
    where s2.prop_date between coalesce(com.fccddeb, s2.prop_date) and coalesce(com.fccdfin, s2.prop_date) and coalesce(substring(com.FCCNUM,1,5), 'FL-LS')='FL-LS' and coalesce(RIGHT(com.FCCNUM, 2), s2.dep) = s2.dep;

    raise notice '[%] Inserted % rows into gold_statistics (proposals without orders)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    delete from gold_statistics
    where (length(ord_ls)=10 or length(ord_em)=10 or length(ord_rd)=10 or length(ord_rm)=10)
    and substr(supp,1,2)!='DC';

    delete from gold_statistics
    where reg=99;

    delete from gold_statistics
    where store in (
    	select pasrescint
    	from gold_refcesh_ods.v_parscopes
    	where pastabl = 9003 and paspost = 4 and cast(pasdate as date)>current_date);

    raise notice '[%] Creation of aggregated mart end' , date_trunc('second' , clock_timestamp())::text;

    return 0;
end;
$function$
;
