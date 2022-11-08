--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_target_service_level
CREATE OR REPLACE FUNCTION fn_load_gold_target_service_level(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    week_start int;
    week_end int;
begin

	select semnsem
	into week_start
	from gold_refgwr_ods.v_fctsem sem
	where period_start between sem.semddeb and sem.semdfin and sem.is_actual = '1';

	select semnsem
	into week_end
	from gold_refgwr_ods.v_fctsem sem
	where period_end between sem.semddeb and sem.semdfin and sem.is_actual = '1';

	raise notice 'week_start = %' , week_start::text;
	raise notice 'week_end = %' , week_end::text;

	raise notice '==================================== START =====================================';
    raise notice '[%] Deleting weeks to be processed start' , date_trunc('second' , clock_timestamp())::text;

	delete from gold_target_service_level
	where week_num between week_start and week_end;

	raise notice '[%] Deleting weeks to be processed finish' , date_trunc('second' , clock_timestamp())::text;

	raise notice '[%] Inserting weeks to be processed start' , date_trunc('second' , clock_timestamp())::text;

    drop table if exists tmp_kltservicelevel;

	create temp table tmp_kltservicelevel
		(sltcinl int8,
		 sltsite int4,
		 nsem int4,
		 sltsltar numeric(5, 2));

	insert into tmp_kltservicelevel(sltcinl, sltsite, nsem, sltsltar)
	select slt.sltcinl, slt.sltsite, sem.semnsem, slt.sltsltar
	from (
		select sltcinl, sltsite, sltcalcdate as ddeb, coalesce(lead(sltcalcdate) over (partition by sltcinl, sltsite order by sltcalcdate), current_date + 1) as dfin, sltsltar
		from gold_refgwr_ods.v_spe_kltservicelevel
		where is_actual = '1') slt
	join gold_refgwr_ods.v_fctsem sem on sem.semddeb between slt.ddeb and slt.dfin and sem.is_actual = '1' and sem.semnsem between week_start and week_end;

    drop table if exists tmp_artparexc_slice;

	create temp table tmp_artparexc_slice
		(nsem int4,
		 apxcinl int8,
		 apxsite int4,
		 apxclages int2,
		 apxtsatt numeric(5, 2));

	insert into tmp_artparexc_slice	(nsem, apxcinl, apxsite, apxclages, apxtsatt)
	select semnsem, apxcinl, apxsite, apxclages, apxtsatt
	from (select sem.semnsem, apx.apxcinl, apx.apxsite, apx.apxclages, apx.apxtsatt,
			row_number() over(partition by sem.semnsem, apx.apxcinl, apx.apxsite order by apx.apxddeb desc) rn
		from v_artparexc_historical apx
		join gold_refgwr_ods.v_fctsem sem on sem.semddeb between apx.apxddeb and apx.apxdfin and sem.is_actual = '1'
		where sem.semnsem between week_start and week_end) t
	where rn = 1;

	insert into gold_target_service_level(code_logistics,site_code,family_code,class,week_num,week_year,week_month,target_serv_lvl,accepted_serv_lvl)
	select apx.apxcinl as code_logistics,
	   apx.apxsite as site_code,
	   aru.arufdsid as family_code,
	   apx.apxclages as class,
	   sem.semnsem as week_num,
	   sem.semnsem/100 as week_year,
	   extract(month from sem.semddeb) as week_month,
	   case
	       when apx.apxtsatt is not null then apx.apxtsatt
	       when slt.sltsltar is not null then slt.sltsltar
	       WHEN apx.apxclages = 1 AND fsi.FSITSCIBLA IS NOT NULL THEN fsi.FSITSCIBLA
           WHEN apx.apxclages = 2 AND fsi.FSITSCIBLB IS NOT NULL THEN fsi.FSITSCIBLB
           WHEN apx.apxclages = 3 AND fsi.FSITSCIBLC IS NOT NULL THEN fsi.FSITSCIBLC
       ELSE par.PARVAN1 end as target_serv_lvl,
	   par.parvan2 as accepted_service_lvl
	from tmp_artparexc_slice apx
	left join gold_refcesh_ods.v_artul aru on apx.apxcinl = aru.arucinl and aru.is_actual = '1'
	left join gold_refcesh_ods.v_parpostes par on par.parpost = apx.apxclages AND partabl=1701 AND parcmag=10 and par.is_actual = '1'
	left join gold_refgwr_ods.v_parfamsit fsi on fsi.fsifdsid = aru.arufdsid
										  and fsi.fsisite = apx.apxsite and fsi.is_actual = '1'
	left join tmp_kltservicelevel slt on slt.sltcinl = apx.apxcinl and slt.sltsite = apx.apxsite and apx.nsem = slt.nsem
	left join gold_refgwr_ods.v_fctsem sem on coalesce(apx.nsem, slt.nsem) = sem.semnsem and sem.is_actual = '1';

	raise notice '[%] Inserting weeks to be processed start' , date_trunc('second' , clock_timestamp())::text;

    return 0;
end;
$function$
;
