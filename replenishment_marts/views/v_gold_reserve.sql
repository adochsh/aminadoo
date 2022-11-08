--liquibase formatted sql

--changeset 60098727:create:view:v_gold_reserve

CREATE OR REPLACE VIEW v_gold_reserve AS
select code_lm,
       code_logistics,
       reserve_type,
       reserve_code,
       qty,
       qty_per_day,
       start_date,
       end_date,
       store_num,
       updated_dttm
from gold_reserve
