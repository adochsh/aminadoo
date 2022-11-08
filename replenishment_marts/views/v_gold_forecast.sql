--liquibase formatted sql

--changeset 60098727:create:view:v_gold_forecast

CREATE OR REPLACE VIEW v_gold_forecast AS
select code_lm,
       code_logistics,
       estm_forecast,
       adj_week_forecast,
       err_calc,
       site_code,
       first_date_sale,
       avail_date_hist,
       week_num,
       "year",
       sale_chisen,
       calculation_year,
       calculation_week,
       calculation_date,
       updated_dttm
from gold_forecast
