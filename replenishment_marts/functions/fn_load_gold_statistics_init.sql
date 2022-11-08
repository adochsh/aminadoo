--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_statistics_init
CREATE OR REPLACE FUNCTION fn_load_gold_statistics_init()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    min_mart_dttm timestamp(0);
begin

	truncate table gold_statistics;

    insert into gold_statistics (store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, type, supp, suppname, prop_price,
                cover, franco, ord_price, qty, changed_qty, util, dmaj, reg, negative_correction, positive_correction,
                bbxd_lines_qty, bbxd_qty, bbxd_amount, target_dep, target_reg, target_store, supplier_type)
    select a.store, a.dep, a.prop_date, a.prop, a.ord_ls, a.ord_em, a.ord_rd, a.ord_rm, a.type, a.supp, a.suppname, a.prop_price,
                a.cover, a.franco, a.ord_price, a.qty, a.changed_qty, a.util, a.dmaj, a.reg, a.negative_correction, a.positive_correction,
                a.bbxd_lines_qty, a.bbxd_qty, a.bbxd_amount,
                      par_dep.parvan1/100 as target_dep,
                      par_reg.parvan1/100 as target_reg,
                      par_store.parvan1/100 as target_store,
                case when coalesce (lre.LRESPCDEV,3) = 3 then 'Рабочий лист' when lre.LRESPCDEV = 5 then 'Автозаказ' end as ORDER_TYPE
    from (select store, dep, prop_date, prop, ord_ls, ord_em, ord_rd, ord_rm, type, supp, suppname, prop_price,
                      cover, franco, ord_price, qty, changed_qty, util, dmaj, reg, negative_correction, positive_correction,
                      bbxd_lines_qty, bbxd_qty, bbxd_amount,
                      row_number() over(partition by prop, ord_ls, ord_em, ord_rd, ord_rm order by prop_date desc) as rn
          from gold_refgwr_ods.v_rep_statistics_v2
          where is_actual = '1') a
    left join gold_refcesh_ods.v_parpostes par_dep on par_dep.parcmag=10 and par_dep.partabl=9005 and par_dep.parpost = cast(a.dep as int) and par_dep.is_actual='1'
    left join gold_refcesh_ods.v_parpostes par_store on par_store.parcmag=10 and par_store.partabl=9005 and par_store.parpost = 16 and par_store.is_actual='1'
    left join gold_refcesh_ods.v_parpostes par_reg on par_reg.parcmag=10 and par_reg.partabl=9005 and par_reg.parpost = 17 and par_dep.is_actual='1'
    join gold_refcesh_ods.v_foudgene fou on foucnuf=a.supp and fou.is_actual ='1'
    left join gold_refgwr_ods.v_lienreap lre on store=lre.lresite and fou.foucfin=lre.lrecfin and lre.is_actual ='1'
    left join gold_refcesh_ods.v_FOUCCOM com on com.fccccin=lre.lreccin  and com.is_actual ='1'
    where a.rn = 1 and a.prop_date between coalesce(com.fccddeb, a.prop_date) and coalesce(com.fccdfin, a.prop_date)
        and coalesce(substring(com.FCCNUM,1,5), 'FL-LS')='FL-LS' and coalesce(RIGHT(com.FCCNUM, 2), a.dep) = a.dep;

    return 0;
end;
$function$
;
