create or replace view v_customs_declaration_items as
select cd_number
     , tnved_code
     , good_number
     , sub_goods_number
     , ean_code
     , adeo_code
     , quantity
     , unit
     , technical_description
     , manufacturer
     , dbf_customs_declaration_items
from customs_declaration_items;
