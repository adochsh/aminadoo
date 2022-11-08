--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_specifics_art_param

CREATE OR REPLACE FUNCTION fn_load_gold_specifics_art_param(external_server text)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    v_sql text;
    v_cnt bigint;
begin

    truncate table tmp_ett_att_for_upd;

    insert into tmp_ett_att_for_upd (num_ett, num_art, param, start_date, end_date)
    select num_ett, num_art, param, start_date, end_date
    from tmp_gold_specifics_best_sellers;

    raise notice '[%] Inserted % rows (updated best sellers)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_ett_att_for_upd (num_ett, num_art, param, start_date, end_date)
    select num_ett, num_art, param, start_date, end_date
    from tmp_gold_specifics_abc;

    raise notice '[%] Inserted % rows (updated abc)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_ett_att_for_upd (num_ett, num_art, param, start_date, end_date)
    select num_ett, num_art, param, start_date, end_date
    from tmp_gold_specifics_dead_stock;

    raise notice '[%] Inserted % rows (updated dead_stock)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_ett_att_for_upd (num_ett, num_art, param, start_date, end_date)
    select num_ett, num_art, param, start_date, end_date
    from tmp_gold_specifics_toxic_stock;

    raise notice '[%] Inserted % rows (updated toxic_stock)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    insert into tmp_ett_att_for_upd (num_ett, num_art, param, start_date, end_date)
    select num_ett, num_art, param, start_date, end_date
    from tmp_gold_specifics_bestsellers_b2b;

    raise notice '[%] Inserted % rows (updated toxic_stock)' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;

    delete from gold_specifics_art_param s
    where current_date between s.start_date and coalesce(s.end_date, current_date + interval '1 day')
    and exists (select 1
                from tmp_ett_att_for_upd t
                where s.num_ett = t.num_ett and s.num_art = t.num_art and s.param = t.param);

    insert into gold_specifics_art_param (num_ett, num_art, param, start_date, end_date)
    select num_ett, num_art, param, start_date, end_date
    from tmp_ett_att_for_upd;

	perform fn_dump_gold_specifics('replenishment_marts', 'tmp_ett_att_for_upd',
    				   'num_ett,num_art,param,start_date,end_date', 'refgwr',
    				   't_dp_gold_art_param', 'num_ett bigint,num_art bigint ,param smallint,start_date date,end_date date',
    				   'num_ett,num_art,param,start_date,end_date',
    				   '1=1', external_server);

    return 0;
end;
$function$
;
