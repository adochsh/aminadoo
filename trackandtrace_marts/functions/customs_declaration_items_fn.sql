--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.customs_declaration_items_fn

CREATE OR REPLACE FUNCTION customs_declaration_items_fn()
RETURNS boolean
LANGUAGE plpgsql
AS $function$
begin

truncate table customs_declaration_items;
insert into customs_declaration_items (cd_number, tnved_code, good_number, sub_goods_number, ean_code, adeo_code, quantity
                                        , unit, technical_description, manufacturer, dbf_customs_declaration_items)
select gd."number" as cd_number
     , gdg.tnved_code
     , gdg.good_number
     , gdg_articles.sub_goods_number
     , gdg_articles.ean_code
     , gdg_articles.adeo_code
     , gdg_articles.quantity
     , gdg_articles.unit
     , gdg_articles.technical_description
     , gdg_articles.manufacturer
     , true as dbf_customs_declaration_items
FROM goodsdeclarations_ods.v_goods_declarations gd  --Таможенная декларация (Уровень 1)
--Товары таможенной декларации (Уровень 2)
         left join goodsdeclarations_ods.v_goods_declaration_goods gdg
                   on gdg.goods_declaration_number =gd."number" and gdg.is_actual ='1'
    --Артикулы товара таможенной декларации (Уровень 3)
         left join goodsdeclarations_ods.v_goods_declaration_good_descriptions gdg_articles
                   on gdg_articles.goods_declaration_number =gd."number"
                       and gdg_articles.good_number =gdg.good_number and gdg_articles.is_actual ='1'
where gd.is_actual ='1';

return 0;
end;
$function$;
