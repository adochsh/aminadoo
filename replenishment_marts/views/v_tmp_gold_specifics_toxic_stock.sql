--liquibase formatted sql

--changeset 60098727:create:view:v_tmp_gold_specifics_toxic_stock

CREATE OR REPLACE VIEW v_tmp_gold_specifics_toxic_stock AS
select     num_ett,
           num_art,
           param,
           start_date,
           end_date
from tmp_gold_specifics_toxic_stock
