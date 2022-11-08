--liquibase formatted sql

--changeset 60098727:drop:view:trackandtrace_marts.v_import_datamart

DROP VIEW IF EXISTS v_import_datamart;

--changeset 60079434:drop:view:trackandtrace_marts.v_import_datamart:update_column_type

DROP VIEW IF EXISTS v_import_datamart;

--changeset 60067166:drop:view:trackandtrace_marts.v_import_datamart:rename_column:ean_code

DROP VIEW IF EXISTS v_import_datamart;

--changeset 60067166:drop:views:trackandtrace_marts.v_import_datamart:alter_columns_type_to_date
DROP VIEW IF EXISTS v_import_datamart;

--changeset 60067166:drop:views:trackandtrace_marts.v_import_datamart:full_import_mart
DROP VIEW IF EXISTS v_import_datamart;

--changeset 60115905:drop:views:trackandtrace_marts.v_import_datamart:fix_types
DROP VIEW IF EXISTS
    v_direct_mode_datamart,
    v_full_import_datamart;

--changeset 60115905:drop:views:trackandtrace_marts.v_import_datamart:full_import_mart
DROP VIEW IF EXISTS
    v_direct_mode_datamart,
    v_full_import_datamart;

--changeset 60115905:drop:views:trackandtrace_marts.v_import_datamart_v2
DROP VIEW IF EXISTS
    v_import_datamart
    , v_direct_mode_datamart
    , v_full_import_datamart;

--changeset 60115905:drop:views:v1.2.2
DROP VIEW IF EXISTS
    v_full_import_datamart
    , v_import_datamart_v2
    , v_rms_orders
    , v_ries_orders;
