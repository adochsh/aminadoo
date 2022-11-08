--liquibase formatted sql

--changeset 60098727:create:view:v_gold_specifics_agg_detailed

CREATE OR REPLACE VIEW v_gold_specifics_agg_detailed AS
select
    nk,
    item,
    site,
    week_num,
    correction_sum,
    sale_date,
    id_spec,
    cre_date
from gold_specifics_agg_detailed
