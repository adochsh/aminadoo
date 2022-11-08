--liquibase formatted sql

--changeset 60098727:set:search_path:trackandtrace_marts runAlways:true runOnChange:true

set search_path to trackandtrace_marts, public;
alter table schemachangelog owner to trackandtrace_marts_owner;
alter table schemachangeloglock owner to trackandtrace_marts_owner;
set role trackandtrace_marts_owner;

--changeset 60098727:create:default:roles

CREATE ROLE trackandtrace_marts_w NOLOGIN ADMIN trackandtrace_marts_owner;
CREATE ROLE trackandtrace_marts_r NOLOGIN ADMIN trackandtrace_marts_owner;
CREATE ROLE trackandtrace_marts_x NOLOGIN ADMIN trackandtrace_marts_owner;
CREATE ROLE trackandtrace_marts_rw NOLOGIN in role trackandtrace_marts_r, trackandtrace_marts_w ADMIN trackandtrace_marts_owner;
CREATE ROLE trackandtrace_marts_rwx NOLOGIN in role trackandtrace_marts_rw, trackandtrace_marts_x ADMIN trackandtrace_marts_owner;

--changeset 60098727:grant:trackandtrace_marts:defaults runOnChange:true
GRANT USAGE ON SCHEMA trackandtrace_marts TO trackandtrace_marts_x;
GRANT USAGE ON SCHEMA trackandtrace_marts TO trackandtrace_marts_w;
ALTER DEFAULT PRIVILEGES IN SCHEMA trackandtrace_marts GRANT EXECUTE ON FUNCTIONS TO trackandtrace_marts_x;
ALTER DEFAULT PRIVILEGES IN SCHEMA trackandtrace_marts GRANT ALL ON SEQUENCES TO trackandtrace_marts_w;
ALTER DEFAULT PRIVILEGES IN SCHEMA trackandtrace_marts GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO trackandtrace_marts_w;
