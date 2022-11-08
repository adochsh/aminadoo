--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.rms_item_info_fn

CREATE OR REPLACE FUNCTION rms_item_info_fn()
RETURNS boolean
LANGUAGE plpgsql
AS $function$
begin

    drop table if exists ries_arts;
    drop table if exists tmp_item_info_uda_pivot;

    create temp table ries_arts  AS (
	select lm_code
	from (
		select lm_code, row_number() over(partition by pk_id  order by version desc) as rn
		from ries_portal_ods.v_order_articles
	) f where f.rn=1
);

create temp table tmp_item_info_uda_pivot  AS (
    SELECT a.item,
         max(CASE WHEN a.uda_id = 5 THEN a.uda_value_desc ELSE NULL END) AS gamma,
         max(CASE WHEN a.uda_id = 7 THEN a.uda_value_desc ELSE NULL END) AS mdd,
         max(CASE WHEN a.uda_id = 377 THEN a.uda_value_desc ELSE NULL END) AS brand,
         max(CASE WHEN a.uda_id = 8 THEN a.uda_value_desc ELSE NULL END) AS best_price,
         max(CASE WHEN a.uda_id = 10 THEN a.uda_value_desc ELSE NULL END) AS import_attr,
         max(CASE WHEN a.uda_id = 12 THEN a.uda_value_desc ELSE NULL END) AS flow_type,
         max(CASE WHEN a.uda_id = 21 THEN a.uda_value_desc ELSE NULL END) AS top1000
    FROM   (
       select uil.item, uil.uda_id, uv.uda_value_desc
        , row_number() OVER(PARTITION BY uil.item, uil.uda_id ORDER BY uil.last_update_datetime DESC) AS rn
       FROM rms_p009qtzb_rms_ods.v_uda_item_lov uil -- гигантский справочник по артикулам
         JOIN rms_p009qtzb_rms_ods.v_uda_values uv ON uil.uda_id = uv.uda_id AND uil.uda_value = uv.uda_value AND uil.is_actual='1' AND uv.is_actual='1'
       WHERE uil.uda_id IN (5, 7, 8, 10, 12, 21, 377) and uil.item in (select lm_code from ries_arts)
       ) a
  WHERE a.rn = 1
  GROUP BY a.item
 );

truncate table rms_item_info;
insert into rms_item_info (lm_code, adeo_code, lm_name, dep_code, dep_name, sub_dep_code, sub_dep_name, lm_type, lm_type_desc
				, lm_subtype, lm_subtype_desc, import_attr, flow_type, top1000, brand, mdd, best_price, gamma, dbf_rms_item_info)
SELECT im.item as lm_code
     , import_attr.commodity as adeo_code
     , im.short_desc as lm_name
     , SUBSTR(LPAD(im.dept::VARCHAR ,4,'0'),1,2)::INTEGER as dep_code
     , g.group_name as dep_name
     , im.dept as sub_dep_code
     , deps.dept_name as sub_dep_name
   	 , im.class as lm_type
     , cl.class_name as lm_type_desc
     , im.subclass as lm_subtype
     , scl.sub_name as lm_subtype_desc
     , pvt.import_attr
     , pvt.flow_type
     , pvt.top1000
     , pvt.brand
     , pvt.mdd
     , pvt.best_price
     , pvt.gamma
     , true as dbf_rms_item_info
FROM rms_p009qtzb_rms_ods.v_item_master im
LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_item_supp_country isc ON isc.item = im.item AND isc.is_actual='1'
LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_item_supplier item_supplier ON item_supplier.item = im.item
                                  								  AND isc.supplier = item_supplier.supplier
                                  								  AND item_supplier.is_actual='1'
LEFT OUTER JOIN tmp_item_info_uda_pivot pvt on im.item = pvt.item
LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_deps deps on deps.dept = im.dept and deps.is_actual='1'
LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_groups g on g.group_no = SUBSTR(LPAD(im.dept::VARCHAR ,4,'0'),1,2)::int and g.is_actual='1'
LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_item_import_attr import_attr on import_attr.item::bigint=im.item::BIGINT and import_attr.is_actual='1'
LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_class cl on im.class = cl.class and im.dept = cl.dept and cl.is_actual = '1'
LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_subclass scl on im.class = scl.class and scl.subclass=im.subclass and im.dept = scl.dept and scl.is_actual = '1'
WHERE im.item_number_type = 'ITEM' AND isc.primary_supp_ind = 'Y' AND isc.primary_country_ind = 'Y' AND item_supplier.primary_supp_ind = 'Y' AND im.is_actual='1'
  AND im.item in (select lm_code from ries_arts);

return 0;
end;
$function$;
