--liquibase formatted sql

--changeset 60098727:set:search_path:replenishment_marts runAlways:true runOnChange:true

set search_path to replenishment_marts, public;
alter table schemachangelog owner to replenishment_marts_owner;
alter table schemachangeloglock owner to replenishment_marts_owner;
set role replenishment_marts_owner;

--changeset 60098727:create:default:roles

CREATE ROLE replenishment_marts_w NOLOGIN ADMIN replenishment_marts_owner;
CREATE ROLE replenishment_marts_r NOLOGIN ADMIN replenishment_marts_owner;
CREATE ROLE replenishment_marts_x NOLOGIN ADMIN replenishment_marts_owner;
CREATE ROLE replenishment_marts_rw NOLOGIN in role replenishment_marts_r, replenishment_marts_w ADMIN replenishment_marts_owner;
CREATE ROLE replenishment_marts_rwx NOLOGIN in role replenishment_marts_rw, replenishment_marts_x ADMIN replenishment_marts_owner;

--changeset 60098727:grant:replenishment_marts:defaults runOnChange:true
GRANT USAGE ON SCHEMA replenishment_marts TO replenishment_marts_x;
GRANT USAGE ON SCHEMA replenishment_marts TO replenishment_marts_w;
ALTER DEFAULT PRIVILEGES IN SCHEMA replenishment_marts GRANT EXECUTE ON FUNCTIONS TO replenishment_marts_x;
ALTER DEFAULT PRIVILEGES IN SCHEMA replenishment_marts GRANT ALL ON SEQUENCES TO replenishment_marts_w;
ALTER DEFAULT PRIVILEGES IN SCHEMA replenishment_marts GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO replenishment_marts_w;
