--liquibase formatted sql

--changeset 60098727:grant:replenishment_marts:defaults runAlways:true runOnChange:true
SELECT public.fn_grant_on_all_objects('replenishment_marts', 'r', 'replenishment_marts_r');

--changeset 60098727:grant:replenishment_marts_rwx runOnChange:true

GRANT replenishment_marts_rwx TO replenishment_marts_etlbot;

--changeset 60098727:grant:replenishment_marts_owner runOnChange:true

GRANT replenishment_marts_owner TO dba;

--changeset 60098727:grant:replenishment_marts_r runOnChange:true

GRANT replenishment_marts_r TO "lm-dataread-public";

--changeset 60098727:grant:role:replenishment_marts_r_cfo_marts_etlbot runOnChange:true

GRANT replenishment_marts_r TO "cfo_marts_etlbot";

--changeset 60081477:grant:role:replenishment_marts_r_findir_etlbot runOnChange:true

GRANT replenishment_marts_r TO findir_etlbot;
--changeset 60084096:grant:changelog_reader

GRANT SELECT ON schemachangelog, schemachangeloglock TO changelog_reader;
