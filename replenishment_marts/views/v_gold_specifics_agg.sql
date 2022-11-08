--liquibase formatted sql

--changeset 60098727:create:view:v_gold_specifics_agg_detailed

CREATE OR REPLACE VIEW v_gold_specifics_agg AS
select
    item_res,
    site_res,
    week_num_res,
    sale,
    correction_res,
    id_spec,
    cre_date
from gold_specifics_agg
