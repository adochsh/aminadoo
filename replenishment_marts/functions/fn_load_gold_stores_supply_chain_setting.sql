--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_stores_supply_chain_setting

CREATE OR REPLACE FUNCTION fn_load_gold_stores_supply_chain_setting()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin

	drop table if exists temp_active_suppliers_base;
	create temp table temp_active_suppliers_base as (
		select spssite
			, substring(fouccom.fccnum, 7, 2) as dep
			, foudgene.foucnuf as supp
			, foudgene.foulibl as supp_name
			, spscfin
			, araccin
			, lredret
			, case when lienreap.lrespcdev =3 then 'Ручн'
			       when lienreap.lrespcdev =5 then 'Авто'
			  end as order_type
			, case when substring(fouccom.fcclib,1,3) = 'ДКК' then substring(fouccom.fccnum,4,2) || '-' || substring(fouccom.fcclib,5,2)
			      else substring(fouccom.fccnum,4,2)
			  end as dkk
			, case when lienreap.lretreap = 2 then 'Разбивать'
			       when lienreap.lretreap = 3 then 'Добивать'
			  end as typp -- Тип управления (Разбивать)
			, case when t.spsxddc is not null and substring(t.spsxddc,4,3)='999' then 'через РЦ'
				   when t.spsxddc is not null and substring(t.spsxddc,4,3)='060' then 'BBXD'
				   when t.spsxddc is not null and substring(t.spsxddc,4,3)='999' then 'через РЦ'
					   --WHEN substring(FOUDGENE.foucnuf,1,2)='DC' THEN 'Stock'
				   else 'Прямой'
			  end as scheme_type
			, spsfranco
			, spsdp, spsdr
			, spscdlu, spscdma, spscdme, spscdje, spscdve, spscdsa, spscddi
			, spsxddr, spsxddp
		 from gold_refgwr_ods.v_lmv_fouscheme t
		 inner join (select arasite, aracfin, araccin, row_number() over(partition by arasite, aracfin, araccin order by araccin) as rn
					 from gold_refcesh_ods.v_artuc
					 where now()::date between araddeb and aradfin and is_actual='1'
		  ) a on spssite =a.arasite and spscfin = a.aracfin and a.rn=1
		 left join gold_refcesh_ods.v_foudgene foudgene on foudgene.foucfin =t.spscfin and foudgene.is_actual='1'
		 left join gold_refgwr_ods.v_lienreap lienreap on lienreap.lreccin =a.araccin
														and lienreap.lresite=t.spssite
														and lienreap.lrecfin=t.spscfin and lienreap.is_actual='1'
		 left join gold_refcesh_ods.v_fouccom fouccom on fouccom.fccccin =lienreap.lreccin and fouccom.fccnum like '%F%'
																						   and substring(fouccom.fccnum,4,2)<>'DC'
																						   and fouccom.is_actual='1'
		WHERE now()::date BETWEEN spsddeb AND spsdfin AND t.is_actual='1'  and  spssite<>1
	);

	drop table if exists props;
	create temp table props as (
		select pdcsite, pdccfin, pdcnprop, pdcdcde, r
		from (  select pdcsite, pdccfin, pdcnprop, pdcdcde
					, dense_rank() over (partition by pdcsite, pdccfin order by pdcdcde desc) as r
				from replenishment_marts.v_prpentprop_historical
				where pdcvalide = 1
					and pdccfin in (select spscfin from temp_active_suppliers_base)
					and pdcdcde::date between now()::date-84 and now()::date
		) f where r<=7
	);

	drop table if exists fran_share_of_props;
	create temp table fran_share_of_props as (
	select p.pdcsite, p.pdccfin   --, p.pdcnprop , p.pdcdcde
		, decode(sum( case when p.r<=2 then p7.lpcqtec * p7.lpcapnh else 0 end ), 0, 0
				, (sum(case when p.r<=2 then p7.lpcarrcde * p7.lpcapnh else 0 end))/ (sum(case when p.r<=2 then p7.lpcqtec * p7.lpcapnh else 0 end))  ) as p2_fran_share
		, decode(sum(p7.lpcqtec * p7.lpcapnh), 0, 0
				, (sum(p7.lpcarrcde * p7.lpcapnh))/ (sum(p7.lpcqtec * p7.lpcapnh))  ) as p7_fran_share
	from props p
	inner join replenishment_marts.v_prpdetprop_historical p7 on p.pdcsite =p7.lpcsite
											  and p.pdccfin =p7.lpccfin
											  and p.pdcnprop =p7.lpcnprop
	group by p.pdcsite, p.pdccfin   --, p.pdcnprop, p.pdcdcde
	);



	drop table if exists tmp_sup_freq;
	create temp table tmp_sup_freq as
	select rep_site, rep_sup, rep_freq
	from (select rep_site, rep_sup, rep_freq
					, row_number() over(partition by  rep_site, rep_sup order by rep_dcretrue desc) rn
				from gold_refgwr_ods.v_lmv_intstockrep_hist_v2 vlihv
				where is_actual='1' and rep_sup in (select supp from temp_active_suppliers_base)

		) rep where rep.rn=1;

	truncate table gold_stores_supply_chain_setting;

	insert into gold_stores_supply_chain_setting ( store_num,
	dep_code , supp_code , araccin , spscfin , supp_name ,
	order_type , dkk , order_days , srok , srok_dostavki_v_magazin ,
	delay_value , r_srok_check_d , theor_optim , min_max_diff ,
	scheme_type , franco , afranko , typp , add_conditions_to_franco , fran_qtyshare)
	select t.spssite as store_num
		, t.dep as dep_code
		, t.supp as supp_code
		, t.araccin
		, t.spscfin
		, t.supp_name as supp_name
		, t.order_type
		, t.dkk
		, replace (rtrim(concat_ws('',decode(t.spscdlu,1,'ПН ', '')
						, decode(t.SPSCDMA,1,'ВТ ', '')
						, decode(t.SPSCDME,1,'СР ', '')
						, decode(t.SPSCDJE,1,'ЧТ ', '')
					    , decode(t.SPSCDVE,1,'ПТ ', '')
						, decode(t.SPSCDSA,1,'СБ ', '')
						, decode(t.SPSCDDI,1,'ВС ', '') ), ' ') , ' ',', ') as order_days  --Дни выхода заказа
		, case when lmv3.r_srok>35 then rep.rep_freq::text  || '(' || '35' || ')'
			   when lmv3.r_srok<=35 then rep.rep_freq::text  || '(' || lmv3.r_srok || ')'
	      end as srok --Срок между заказами (рассчётный)  -- 7 (3)
		, case when (t.lredret - lmv2.delivery_exc) > 0  --Срок подготовки + Срок доставки + (Отрицательную задержку)
				 then t.spsdp + t.spsdr + (t.lredret-lmv2.delivery_exc )
			   else (t.spsdp + t.spsdr) end as srok_dostavki_v_magazin  --Срок доставки в магазин
		, coalesce(t.LREDRET,0) - coalesce(lmv2.delivery_exc,0) as delay_value   --Задержка поставки текущая
		, lmv3.r_srok_check_d as r_srok_check_d  --Схема поставки оптимальна? НЕТ +6
		, case when ((coalesce(spsdr, 0) +least(decode(liscdlu, 1, lisdplu, 999), decode(liscdma, 1, lisdpma, 999),
									    decode(liscdme, 1, lisdpme, 999), decode(liscdje, 1, lisdpje, 999),
									    decode(liscdve, 1, lisdpve, 999), decode(liscdsa, 1, lisdpsa, 999),
									    decode(liscddi, 1, lisdpdi, 999) ))
				      -(greatest(decode(liscdlu, 1, lislilu, 0),
						     decode(liscdma, 1, lislima, 0),
						     decode(liscdme, 1, lislime, 0),
						     decode(liscdje, 1, lislije, 0),
						     decode(liscdve, 1, lislive, 0),
						     decode(liscdsa, 1, lislisa, 0),
						     decode(liscddi, 1, lislidi, 0)) -coalesce(spsxddp, 0) -coalesce(spsxddr, 0) +coalesce(delivery_exc, 0))) <0
			  then 'НЕТ (+' || (abs(((coalesce(spsdr, 0) + least(decode(liscdlu, 1, lisdplu, 999), decode(liscdma, 1, lisdpma, 999),
													     decode(liscdme, 1, lisdpme, 999), decode(liscdje, 1, lisdpje, 999),
													     decode(liscdve, 1, lisdpve, 999), decode(liscdsa, 1, lisdpsa, 999),
													     decode(liscddi, 1, lisdpdi, 999) ))
			      - (greatest(decode(liscdlu, 1, lislilu, 0),
					      decode(liscdma, 1, lislima, 0),
					      decode(liscdme, 1, lislime, 0),
					      decode(liscdje, 1, lislije, 0),
					      decode(liscdve, 1, lislive, 0),
					      decode(liscdsa, 1, lislisa, 0),
					      decode(liscddi, 1, lislidi, 0) ) -coalesce(spsxddp, 0) -coalesce(spsxddr, 0) +coalesce(delivery_exc, 0)))))::text || ')'
			  else 'ДА' end theor_optim
		, (coalesce(spsdr, 0) + least(decode(liscdlu, 1, lisdplu, 999), decode(liscdma, 1, lisdpma, 999),
						   decode(liscdme, 1, lisdpme, 999), decode(liscdje, 1, lisdpje, 999),
						   decode(liscdve, 1, lisdpve, 999), decode(liscdsa, 1, lisdpsa, 999),
						   decode(liscddi, 1, lisdpdi, 999) ))
	      - (greatest(decode(liscdlu, 1, lislilu, 0),
			     decode(liscdma, 1, lislima, 0),
			     decode(liscdme, 1, lislime, 0),
			     decode(liscdje, 1, lislije, 0),
			     decode(liscdve, 1, lislive, 0),
			     decode(liscdsa, 1, lislisa, 0),
			     decode(liscddi, 1, lislidi, 0) ) -coalesce(spsxddp, 0) -coalesce(spsxddr, 0) +coalesce(delivery_exc, 0)) as min_max_diff
		, t.scheme_type  --Схема поставки
		, t.spsfranco as franco --Контрактное франко р.
		, lmv2.franco as afranko --Альтернативное франко р.
		, t.typp -- Тип управления
	    , case when lmv3.r_vm1 is null then ''
			   else lmv3.r_vm1 || lmv3.r_vm2 || lmv3.r_vm3 end as add_conditions_to_franco
		, case when (t.lredret - lmv2.delivery_exc) >7 then p.p2_fran_share
			   when (t.lredret - lmv2.delivery_exc) <=7 then p.p7_fran_share end as fran_qtyshare	--доля добора до франко в %
	from temp_active_suppliers_base t
	left join gold_refgwr_ods.v_lmv_intfouscheme_exception lmv2 on lmv2.site =t.spssite
													    and lmv2.supplier::text =t.supp::text and lmv2.is_actual='1'
	left join (select r_supp, r_store, r_srok, r_srok_check_d, r_vm1, r_vm2, r_vm3, r_dcre
				, row_number() over(partition by r_supp, r_store order by r_dcre desc) rn
			    from gold_refgwr_ods.v_lmv_supfrancorep lmv3 where lmv3.is_actual='1'
		) lmv3 on lmv3.r_store = t.spssite and lmv3.r_supp::text =t.supp::text and lmv3.rn=1
	left join gold_refcesh_ods.v_lienserv lienserv on lienserv.liscfin =t.spscfin
												   and lienserv.lisccin =t.araccin
												   and lienserv.lissite =t.spssite
												   and lienserv.lisdfin::date ='2049-12-31'::date and lienserv.is_actual='1'
	left join fran_share_of_props p on p.pdcsite =t.spssite and p.pdccfin =t.spscfin
	left join tmp_sup_freq rep on rep.rep_sup =t.supp and rep.rep_site =t.spssite;


    return 0;
end;
$function$
;
