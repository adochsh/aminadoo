--liquibase formatted sql

--changeset 60098727:create:view:v_gold_target_service_level

CREATE OR REPLACE VIEW v_gold_target_service_level AS
select code_logistics,
       site_code,
       family_code,
       class,
       week_num,
       week_year,
       week_month,
       target_serv_lvl,
       accepted_serv_lvl,
       created_dttm,
       updated_dttm
from gold_target_service_level
