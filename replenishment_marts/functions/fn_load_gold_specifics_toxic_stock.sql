--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_specifics_toxic_stock

CREATE OR REPLACE FUNCTION fn_load_gold_specifics_toxic_stock()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    v_sql text;
    v_cnt bigint;
begin

	create temp table tmp_curr_toxic_stock (num_ett bigint, num_art bigint);

	insert into tmp_curr_toxic_stock (num_ett, num_art)
    select store, cast(item as bigint)
    from findir_marts.v_findir_sales_gam_stk_agg_by_item_day
    where toxic_stock_qty <> 0
    and cal_date = current_date - 2;


    truncate table tmp_gold_specifics_toxic_stock;

    select count(*) as cnt
    into v_cnt
    from tmp_curr_toxic_stock;

    if v_cnt > 0 then
        begin

        insert into tmp_gold_specifics_toxic_stock (num_ett, num_art, param, start_date, end_date)
        select s.num_ett, s.num_art, s.param, s.start_date, current_date as end_date
        from v_gold_specifics_art_param s
        left join tmp_curr_toxic_stock t
        on s.num_ett = t.num_ett and s.num_art = t.num_art
        where t.num_art is null
        and s.param = 6
        and current_date between s.start_date and coalesce(s.end_date, current_date + interval '1 day');

        insert into tmp_gold_specifics_toxic_stock (num_ett, num_art, param, start_date, end_date)
        select t.num_ett, t.num_art, 6 as param, current_date as start_date, null as end_date
        from tmp_curr_toxic_stock t
        left join v_gold_specifics_art_param s
        on s.num_ett = t.num_ett and s.num_art = t.num_art
            and current_date between s.start_date and coalesce(s.end_date, current_date + interval '1 day') and s.param = 6
            where s.num_art is null;

        end;
    else
        raise notice 'The source mart is empty';
    end if;

    return 0;
end;
$function$
;
