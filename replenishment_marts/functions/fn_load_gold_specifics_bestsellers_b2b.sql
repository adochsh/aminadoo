--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_specifics_bestsellers_b2b

CREATE OR REPLACE FUNCTION fn_load_gold_specifics_bestsellers_b2b()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    v_sql text;
    v_cnt bigint;
begin

    drop table if exists tmp_curr_bestsellers_b2b;

	create temp table tmp_curr_bestsellers_b2b (num_ett bigint, num_art bigint);

	insert into tmp_curr_bestsellers_b2b (num_ett, num_art)
    select num_ett, num_art
    from best_sellers_marts.v_bestsellers
    where rating_ca_b + rating_qte_b + rating_client_b <> 0;


    truncate table tmp_gold_specifics_bestsellers_b2b;

    select count(*) as cnt
    into v_cnt
    from tmp_curr_bestsellers_b2b;

    if v_cnt > 0 then
        begin

        insert into tmp_gold_specifics_bestsellers_b2b (num_ett, num_art, param, start_date, end_date)
        select s.num_ett, s.num_art, s.param, s.start_date, current_date as end_date
        from v_gold_specifics_art_param s
        left join tmp_curr_bestsellers_b2b t
        on s.num_ett = t.num_ett and s.num_art = t.num_art
        where t.num_art is null
        and s.param = 7
        and current_date between s.start_date and coalesce(s.end_date, current_date + interval '1 day');

        insert into tmp_gold_specifics_bestsellers_b2b (num_ett, num_art, param, start_date, end_date)
        select t.num_ett, t.num_art, 7 as param, current_date as start_date, null as end_date
        from tmp_curr_bestsellers_b2b t
        left join v_gold_specifics_art_param s
        on s.num_ett = t.num_ett and s.num_art = t.num_art
            and current_date between s.start_date and coalesce(s.end_date, current_date + interval '1 day') and s.param = 7
            where s.num_art is null;

        end;
    else
        raise notice 'The source mart is empty';
    end if;

    drop table if exists tmp_curr_bestsellers_b2b;

    return 0;
end;
$function$
;
