--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_order_corrections_analysis
CREATE OR REPLACE FUNCTION fn_load_gold_order_corrections_analysis()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    v_min_receipt_date date;
begin

    raise notice '==================================== START =====================================';
    raise notice '[%] Function started.' , date_trunc('second' , clock_timestamp())::text;

    drop table if exists reserve_types;

    create temporary table reserve_types (reserve_code INTEGER,
    									  l1_flag INTEGER,
    									  l2_flag INTEGER);
    do $$ begin
        perform public.rdm('dict_reserve_types_for_limits', 'reserve_types');
    end $$;

    drop table if exists temp_base;

    create temp table temp_base as
    select site as store, code_internal, order_date, prop_number, prop_type, supp_int_code, prop_is_valid, coalesce(prop_item_qty, 0) as prop_item_qty,
        prop_coeff_euro, prop_line_code, contract_numbers as orders, ord_price, coalesce(order_item_qty, 0) as order_item_qty, ord_item_price, item_price, item,
        tech_case_no, prop_date, prp_amt, item_internal, cov_period, stock_qty, intransit_qty, lpcarrcde,
        lpccumbes, lpccumprev, sdpp, lpcarrref, pdcvapar, cover_delivery, delivery_date, cover_end_date
    from tmp_corr_detailed tcd
	where tcd.tech_case_no = 1
	    and coalesce(tcd.order_item_qty,0) + coalesce(tcd.prop_item_qty, 0) > 0;


    drop table if exists tmp_receipts;

    create temp table tmp_receipts (lm_code text,
    								store_num int4,
    								opened_date date,
    								qty_sold numeric);

    drop table if exists temp_sales;

    create temp table temp_sales as (
	select b.item, b.store as store, b.order_date,b.cover_end_date
		, sum(coalesce(rl.line_quantity,0)) as sum_sold
		, sum(coalesce(rl.line_quantity,0)) / nullif(max(b.cov_period), 0) as avg_sold
	from temp_base b
	join dds.v_receipt_lines_public rl on line_item_id::text =b.item and rl.store_id = b.store
		 and rl.opened_date::date between b.order_date and b.cover_end_date- interval '1 days'
		 and line_type IN ('Sales', 'pickedUp orders')
		 and coalesce(tpnet_line_operation_cancel_flag , 0) <> -1
		 and line_item_type = 'Normal'
		 and (tpnet_receipt_type in ('RT', 'RR') or tpnet_receipt_type in ('SA', 'FI')
		      or (tpnet_receipt_type= 'NM' and tpnet_receipt_operation_type <> 'EFT_MAINT'))
	join rms_p009qtzb_rms_ods.v_uda_item_lov uil on uil.item = rl.line_item_id  and uil.is_actual = '1'
	join rms_p009qtzb_rms_ods.v_uda_values uv on  uil.uda_id = uv.uda_id
		and uil.uda_value = uv.uda_value and uil.uda_id = 5 and uv.uda_value_desc <> 'S' and uv.is_actual = '1'
	group by b.item, b.store, b.order_date,b.cover_end_date
    );


    drop table if exists tmp_pcb;
    create temp table tmp_pcb (item text, pcb numeric);

    insert into tmp_pcb (item, pcb)
    select isc.item,
    	   case when isc.round_lvl = 'EA' then 1
    		    when isc.round_lvl = 'I' then isc.INNER_PACK_SIZE
    		    when isc.round_lvl = 'C' then isc.SUPP_PACK_SIZE
    		    when isc.round_lvl = 'L' then isc.ti*isc.SUPP_PACK_SIZE
    		    when isc.round_lvl = 'P' then isc.ti*isc.hi*isc.SUPP_PACK_SIZE
    		    else 1 end as pcb
           from rms_p009qtzb_rms_ods.v_item_supp_country isc
           where isc.is_actual='1' AND isc.primary_supp_ind = 'Y' AND isc.primary_country_ind = 'Y';

    drop table if exists tmp_stockrep;
    create temp table tmp_stockrep (rep_nprop int8, rep_art text, abc text, srok numeric);

    insert into tmp_stockrep (rep_nprop, rep_art, abc, srok)
    select rep_nprop, rep_art, abc, srok
    from (
    	 select  rep_nprop, rep_art, rep_abc as abc, rep_freq as srok
    	      ,row_number() over(partition by  rep_nprop, rep_art order by rep_dcretrue desc) rn
    	 from gold_refgwr_ods.v_lmv_intstockrep_hist_v2 vlihv
    	 where is_actual='1'
         ) rep
    where rn = 1;


    drop table if exists temp_reserv_days;
    create temp table temp_reserv_days as
	SELECT app.APPSITE AS store
    		, b.item_internal
    	    , b.order_date
    	    , b.delivery_date
    		, sum(case when appupre =2 and dres.l1_flag = 1 then app.APPQTE else 0 end) * max(b.SDPP)
    		    + sum(case when appupre =1 and dres.l1_flag = 1 then app.APPQTE else 0 end) AS reservs
    	   	, sum(case when appupre =2 and dres.l2_flag = 1 then app.APPQTE else 0 end) * max(b.SDPP)
    	   	    + sum(case when appupre =1 and dres.l2_flag = 1 then app.APPQTE else 0 end) AS l2
    	FROM gold_refgwr_ods.v_artparpre app
    	join temp_base b on item_internal=appcinl
    					and app.APPSITE =b.store
    					and b.delivery_date between app.APPDDEB and app.APPDFIN- interval '1 days'
    	left JOIN gold_refcesh_ods.v_tra_parpostes tp1737 ON tp1737.TPARPOST = app.APPUPRE AND tp1737.tpartabl=1737 AND tp1737.LANGUE='RU' AND tp1737.tparcmag=0
    	left  JOIN gold_refcesh_ods.v_tra_parpostes tp1743 ON tp1743.TPARPOST = app.APPTYPE AND tp1743.tpartabl=1743 AND tp1743.LANGUE='RU' AND tp1743.tparcmag=0
    	join reserve_types dres on app.apptype = dres.reserve_code
    	WHERE coalesce(app.APPAUTO,0) <> 1
    	group by app.APPSITE, b.item_internal, b.order_date, b.delivery_date;


    drop table if exists temp_all;
    create temp table temp_all as (
	select b.item
		, b.store
		-- L1 = [Все резервы]+ [x]*[СДПП]*cover_period*[y] ; L2 = [Все ПМ]
				-- Если L1-L2 < 1 упаковки, то L1 = L2 + 1 упаковки
		, case when round((reservs + 0.4* SDPP * srok)/p.pcb)*p.pcb -round((l2)/p.pcb)*p.pcb < pcb*2
			   then 'round(l2/pcb)*pcb + pcb*2'
			   else 'round((reservs + 0.4 *SDPP *srok)/pcb)*pcb'
		  end as l1_case
		, case when round((reservs + 0.4* SDPP * srok)/p.pcb)*p.pcb -round(l2/p.pcb)*p.pcb < pcb*2
			   													then round(l2/p.pcb)*p.pcb + p.pcb*2
			   else round((reservs + 0.4* SDPP * srok)/p.pcb)*p.pcb
		  end as l1
		----Берем сток+в пути на момент предложения и минус продажи
		, (b.stock_qty +b.intransit_qty) -coalesce(s.sum_sold,0) +b.order_item_qty as target_stock
		, (b.stock_qty +b.intransit_qty) -coalesce(s.sum_sold,0) - b.LPCARRCDE + b.prop_item_qty as prop_target
		, (b.stock_qty +b.intransit_qty) -coalesce(s.sum_sold,0) - b.LPCARRCDE + b.order_item_qty as ord_target
		, l2
		, p.pcb
		, reservs
		, SDPP
		, b.cov_period
		, rep.srok
		, b.prop_item_qty as prop_qty
		, b.order_item_qty  as ord_qty
		, b.order_item_qty - b.prop_item_qty as correction
 		, LPCCUMBES as need_qty  ---->чистая потребность
--		, (order_item_qty -prop_item_qty) as adj_user_qty -----корректировка User-а
--	    , lpcarrcde as adj_franco_qty 					  -----корректировка Franco
-- 	   	, order_item_qty - (LPCCUMBES+lpcarrref) -LPCARRCDE as error_gold_qty  -----корректировка Gold
 		, LPCARRCDE as all_rounds_qty--> Количество SKU округленных до заказа
	    , LPCARRREF as pcb_round_qty-- Округленное количество SKU    select 336.0 +336.0*2= 1008

 		, s.sum_sold as qty_sold  --- продажи за период покрытия
		, b.stock_qty as stock_qty  --gold
		, b.intransit_qty as intransit_qty  --gold
		, case when avg_sold<>0 then round((b.stock_qty +b.intransit_qty) /s.avg_sold)
			   else 0 end as stock_days
		, LPCCUMPREV -- Совокупный прогноз SKU
		, rep.abc
		, b.prop_number
		, b.orders
		, b.prop_date::date
		, b.order_date::date
		, b.delivery_date::date
		, b.cover_end_date::date
		, item_price
	from temp_base b
	left join temp_reserv_days r ON r.item_internal =b.item_internal and r.store =b.store and r.order_date =b.order_date and r.delivery_date =b.delivery_date
	left join temp_sales s ON s.item =b.item and s.store =b.store and s.order_date =b.order_date
	left join tmp_pcb p on p.item = b.item
    left join tmp_stockrep rep on rep.rep_nprop =b.prop_number and rep.rep_art =b.item
    );

    truncate table gold_order_corrections_analysis;

    insert into gold_order_corrections_analysis(money_user, money_rounds, money_gold, ostatok2, e_rounds, e_gold,
                      ostatoki_desc, ostatok1, Е_user, item, store, l1_case, l1, target_stock,
                      l2, pcb, reservs, cov_period, srok, prop_qty, ord_qty,
                      need_qty, all_rounds_qty, pcb_round_qty, qty_sold, stock_qty, intransit_qty, stock_days,
                      frsct_cov, abc, prop_id, orders, prop_date, order_date, delivery_date, cover_end_date,
                      prop_targetstock, ord_targetstock)
    select Е_user*item_price as money_user
    	, (case when target_stock>l1 then coalesce(least((ostatoK1-Е_user), all_rounds_qty), 0) else 0 end) * item_price as money_rounds
    	, (case when target_stock>l1 then coalesce(ostatoK1 -Е_user, 0) - coalesce(least((ostatoK1 -Е_user), all_rounds_qty), 0)
    		   when target_stock<l2 then ostatoK1-Е_user else 0 end) *item_price as money_gold
    	---K2= K - Е(user)   --остаток 2
    	, case when target_stock>l1 then coalesce(ostatoK1 -Е_user, 0) else 0 end as ostatoK2
    	---Eround = IF prop_gold >0 then (K2, ROUND_QTY) else K2
    	, case when target_stock>l1 then coalesce(least((ostatoK1 -Е_user), all_rounds_qty), 0) else 0 end  as E_rounds
    	---E_gold= K – Eround
    	, case when target_stock>l1 then coalesce(ostatoK1 -Е_user, 0) - coalesce(least((ostatoK1 -Е_user), all_rounds_qty), 0)
    		   when target_stock<l2 then ostatoK1-Е_user else 0 end as E_gold
    	, ostatoki_desc, ostatok1, Е_user, item, store, l1_case, l1, target_stock
    	, l2, pcb, reservs, cov_period, srok, prop_qty
    	, ord_qty, need_qty, all_rounds_qty, pcb_round_qty, qty_sold
    	, stock_qty, intransit_qty, stock_days, lpccumprev as frsct_cov, abc, prop_id
    	, orders, prop_date, order_date, delivery_date, cover_end_date, prop_target, ord_target
    from (
    	select
    		  ----Остаток 1
    		  case when target_stock>l1 then 'пересток'
    			   when target_stock<l2 then 'недосток' else 'норма'
    		  end as ostatoki_desc
    		, case when target_stock>l1 then (target_stock -L1)
    			   when target_stock<l2 then (L2 -target_stock) else 0
    		  end as ostatoK1  --остаток 1  =K1

    		  ----ERROR USER
    		, case when correction>0 and target_stock>l1 then least((target_stock-l1), correction)
    			   when correction<0 and target_stock<l2 then least((l2-target_stock), abs(correction))
    			else 0
    		  end as Е_user     ---Е(user)= MIN (K1, корректировка пользователя)
    		, item_price, item, store, l1_case, l1, target_stock
    	, prop_target, ord_target, l2, pcb, reservs, cov_period, srok, prop_qty
    	, ord_qty, correction, need_qty, all_rounds_qty, pcb_round_qty, qty_sold
    	, stock_qty, intransit_qty, stock_days, lpccumprev, abc, prop_number as prop_id
    	, orders, prop_date, order_date, delivery_date, cover_end_date
    	from temp_all t1
    ) t2;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into replenishment_marts.gold_order_corrections_analysis' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','gold_order_corrections_analysis');

    raise notice '[%] Function finished.' , date_trunc('second' , clock_timestamp())::text;
    raise notice '==================================== FINISH =====================================';
    return 0;
end;
$function$
;
