--liquibase formatted sql

--changeset 60098727:create:view:v_replenishment_loads_log

CREATE OR REPLACE VIEW v_replenishment_loads_log AS
select process_name,
       step,
       action_name,
       action_time,
       row_count
from replenishment_loads_log



