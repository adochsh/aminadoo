--liquibase formatted sql
--changeset 60098727:create:function:fn_dump_gold_specifics

CREATE OR REPLACE FUNCTION fn_dump_gold_specifics(s_schema_name text, s_table_name text, s_cols text, t_schema_name text,
                           t_table_name text, t_ddl_clause text, t_cols text, where_condition text, external_server text)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
    v_sql text;
    v_cnt bigint;
begin
    v_sql := 'DROP EXTERNAL TABLE IF EXISTS pxf_' || s_table_name || ';
        CREATE WRITABLE EXTERNAL TEMPORARY TABLE pxf_' || s_table_name || '(
        ' || t_ddl_clause || ')
        LOCATION (''pxf://' || t_schema_name || '.' || t_table_name || '?PROFILE=Jdbc&SERVER=' || external_server || ''')
    	FORMAT ''CUSTOM'' (FORMATTER=''pxfwritable_export'');';

    raise notice '%', v_sql;
    raise notice '[%] Start inserting into %.%', date_trunc('second' , clock_timestamp())::text, t_schema_name, t_table_name;
    execute v_sql;

    v_sql := 'INSERT INTO pxf_' || s_table_name || '(' || t_cols || ')
			  SELECT ' || s_cols || '
			  FROM ' || s_schema_name || '.v_' || s_table_name || '
			  WHERE ' || where_condition || ';';

	raise notice '%', v_sql;
	execute v_sql;

	get diagnostics v_cnt = row_count;
    raise notice '[%] % rows inserted into %.%', date_trunc('second' , clock_timestamp())::text, v_cnt, t_schema_name, t_table_name;

    return 0;
end;
$function$
;
