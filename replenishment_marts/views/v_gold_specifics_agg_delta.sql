--liquibase formatted sql

--changeset 60098727:create:view:v_gold_specifics_agg_delta

CREATE OR REPLACE VIEW v_gold_specifics_agg_delta AS
select
    item_res,
    site_res,
    week_num_res,
    sale,
    correction_res
from gold_specifics_agg_delta
