--liquibase formatted sql

--changeset 60098727:create:view:v_rms_trns_sales_daily

CREATE OR REPLACE VIEW v_rms_trns_sales AS
select lm_code,
       opened_date,
       store_num,
       line_type,
       qty_sold,
       price,
       unit_cost,
       ca_ttc,
       created_dttm,
       updated_dttm
from rms_trns_sales
