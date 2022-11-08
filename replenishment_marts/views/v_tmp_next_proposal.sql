--liquibase formatted sql

--changeset 60098727:create:view:v_tmp_next_proposal

CREATE OR REPLACE VIEW v_tmp_next_proposal AS
select prop_id,
       next_delivery_date,
       srok
from tmp_next_proposal
