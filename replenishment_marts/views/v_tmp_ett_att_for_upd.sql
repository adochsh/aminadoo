--liquibase formatted sql

--changeset 60098727:create:view:v_tmp_ett_att_for_upd

CREATE OR REPLACE VIEW v_tmp_ett_att_for_upd AS
select     num_ett,
           num_art,
           param,
           start_date,
           end_date
from tmp_ett_att_for_upd
