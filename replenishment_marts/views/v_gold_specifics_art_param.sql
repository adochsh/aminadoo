--liquibase formatted sql

--changeset 60098727:create:view:v_gold_specifics_art_param

CREATE OR REPLACE VIEW v_gold_specifics_art_param AS
select     num_ett,
           num_art,
           param,
           start_date,
           end_date,
           updated_dttm
from gold_specifics_art_param
