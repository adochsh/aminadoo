--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_specifics_abc

CREATE OR REPLACE FUNCTION fn_load_gold_specifics_abc()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    v_sql text;
    v_cnt bigint;
begin

	create temp table tmp_curr_abc (num_ett bigint, num_art bigint, param int2);

    insert into tmp_curr_abc (num_ett, num_art, param)
    select store,
           cast(item as bigint),
           case abc_value
               when 1 then 2    -- категория А
               when 2 then 4    -- категория B
               else 5           -- категория C
               end as param
    from findir_marts.v_findir_sales_gam_stk_agg_by_item_day
    where cal_date = current_date - 2
      and abc_value is not null;

    truncate table tmp_gold_specifics_abc;

    select count(*) as cnt
    into v_cnt
    from tmp_curr_abc;

    if v_cnt > 0 then
        begin

        insert into tmp_gold_specifics_abc (num_ett, num_art, param, start_date, end_date)
        select s.num_ett, s.num_art, s.param, s.start_date, current_date as end_date
        from v_gold_specifics_art_param s
        left join tmp_curr_abc t
        on s.num_ett = t.num_ett and s.num_art = t.num_art and s.param = t.param
        where t.num_art is null
        and s.param in (2, 4, 5)
        and current_date between s.start_date and coalesce(s.end_date, current_date + interval '1 day');

        insert into tmp_gold_specifics_abc (num_ett, num_art, param, start_date, end_date)
        select t.num_ett, t.num_art, t.param as param, current_date as start_date, null as end_date
        from tmp_curr_abc t
        left join v_gold_specifics_art_param s
        on s.num_ett = t.num_ett and s.num_art = t.num_art and s.param = t.param
            and current_date between s.start_date and coalesce(s.end_date, current_date + interval '1 day')
            where s.num_art is null;

        end;
    else
        raise notice 'The source mart is empty';
    end if;

    return 0;
end;
$function$
;
