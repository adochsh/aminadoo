--liquibase formatted sql

--changeset 60098727:create:view:v_tmp_gold_specifics_bestsellers_b2b

CREATE OR REPLACE VIEW v_tmp_gold_specifics_bestsellers_b2b AS
select     num_ett,
           num_art,
           param,
           start_date,
           end_date
from tmp_gold_specifics_bestsellers_b2b
