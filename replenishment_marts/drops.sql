--liquibase formatted sql

--changeset 60098727:drop:view:v_gold_reserve
DROP VIEW IF EXISTS v_gold_reserve;

--changeset 60098727:drop:view:v_gold_reserve_2
DROP VIEW IF EXISTS v_gold_reserve;

--changeset 60098727:drop:view:v_gold_reserve_3
DROP VIEW IF EXISTS v_gold_reserve;

--changeset 60098727:drop:view:v_gold_reserve_4
DROP VIEW IF EXISTS v_gold_reserve;

--changeset 60098727:drop:view:v_tmp_corr_detailed
DROP VIEW IF EXISTS v_tmp_corr_detailed;

--changeset 60098727:drop:view:v_gold_corrections_detailed
DROP VIEW IF EXISTS v_gold_corrections_detailed;

--changeset 60098727:drop:view:v_gold_statistics
DROP VIEW IF EXISTS v_gold_statistics;

--changeset 60098727:drop:view:v_gold_target_service_level
DROP VIEW IF EXISTS v_gold_target_service_level;

--changeset 60098727:drop:view:v_gold_reserve_5
DROP VIEW IF EXISTS v_gold_reserve;

--changeset 60098727:drop:function:fn_load_gold_reserve
DROP FUNCTION IF EXISTS fn_load_gold_reserve();

--changeset 60098727:drop:view:v_gold_statistics:add_partitioning
DROP VIEW IF EXISTS v_gold_statistics;

--changeset 60098727:drop:view:v_gold_corrections_detailed:add_partitioning
DROP VIEW IF EXISTS v_gold_corrections_detailed;

--changeset 60098727:drop:table:gold_statistics:add_partitioning
DROP TABLE IF EXISTS gold_statistics;

--changeset 60098727:drop:table:gold_corrections_detailed:add_partitioning
DROP TABLE IF EXISTS gold_corrections_detailed;

--changeset 60098727:drop:view:v_tmp_corr_detailed_2
DROP VIEW IF EXISTS v_tmp_corr_detailed;

--changeset 60098727:drop:view:v_gold_statistics:add_supplier_type
DROP VIEW IF EXISTS v_gold_statistics;

--changeset 60098727:drop:function:fn_load_gold_specifics_2080
DROP FUNCTION IF EXISTS fn_load_gold_specifics_2080();

--changeset 60098727:drop:view:v_tmp_gold_specifics_2080
DROP VIEW IF EXISTS v_tmp_gold_specifics_2080;

--changeset 60098727:drop:table:tmp_gold_specifics_2080
DROP TABLE IF EXISTS v_tmp_gold_specifics_2080;

--changeset 60098727:drop:function:fn_load_gold_detailed_statistics_init
DROP FUNCTION IF EXISTS fn_load_gold_detailed_statistics_init(date,date);

--changeset 60098727:drop:function:fn_load_gold_statistics
DROP FUNCTION IF EXISTS fn_load_gold_reserve(date, date);

--changeset 60098727:drop:view:v_gold_reserve_7
DROP VIEW IF EXISTS v_gold_reserve;

--changeset 60098727:drop:function:fn_load_gold_forecast_backup
DROP FUNCTION IF EXISTS fn_load_gold_forecast_backup();

--changeset 60098727:drop:function:fn_load_gold_forecast
DROP FUNCTION IF EXISTS fn_load_gold_forecast();

--changeset 60098727:drop:view:v_gold_forecast
DROP VIEW IF EXISTS v_gold_forecast;

--changeset 60098727:drop:function:fn_load_gold_historical_tables
DROP FUNCTION IF EXISTS fn_load_gold_historical_tables();

--changeset 60098727:drop:view:artparexc_historical:change_partitioning
DROP VIEW IF EXISTS v_artparexc_historical;

--changeset 60098727:drop:table:artparexc_historical:change_partitioning
DROP TABLE IF EXISTS artparexc_historical;

--changeset 60098727:drop:view:fctdetpvh_historical:change_partitioning
DROP VIEW IF EXISTS v_fctdetpvh_historical;

--changeset 60098727:drop:table:fctdetpvh_historical:change_partitioning
DROP TABLE IF EXISTS fctdetpvh_historical;

--changeset 60098727:drop:view:fctentpvh_historical:change_partitioning
DROP VIEW IF EXISTS v_fctentpvh_historical;

--changeset 60098727:drop:table:fctentpvh_historical:change_partitioning
DROP TABLE IF EXISTS fctentpvh_historical;

--changeset 60098727:drop:view:gold_forecast:change_partitioning
DROP VIEW IF EXISTS v_gold_forecast;

--changeset 60098727:drop:table:gold_forecast:change_partitioning
DROP TABLE IF EXISTS gold_forecast;

--changeset 60098727:drop:view:v_tmp_corr_detailed_3
DROP VIEW IF EXISTS v_tmp_corr_detailed;

--changeset 60098727:drop:table:tmp_corr_detailed
DROP TABLE IF EXISTS tmp_corr_detailed;

--changeset 60098727:drop:table:tmp_statistics_gold_statistics_cover
DROP TABLE IF EXISTS tmp_statistics_gold_statistics_cover;

--changeset 60098727:drop:table:tmp_statistics_gold_statistics_cover_detailed
DROP TABLE IF EXISTS tmp_statistics_gold_statistics_cover_detailed;

--changeset 60098727:drop:function:fn_load_gold_statistics_aggregated
DROP FUNCTION IF EXISTS fn_load_gold_statistics_aggregated();

--changeset 60098727:drop:function:fn_load_gold_statistics_detailed
DROP FUNCTION IF EXISTS fn_load_gold_statistics_detailed();

--changeset 60098727:drop:function:fn_load_gold_reserve_init:add_parameters
DROP FUNCTION IF EXISTS fn_load_gold_reserve_init_backup();
DROP FUNCTION IF EXISTS fn_load_gold_reserve_init();

--changeset 60098727:drop:view:supplier:add_updated_dttm
DROP VIEW IF EXISTS v_gold_supplier_delays;
DROP VIEW IF EXISTS v_gold_stores_supply_chain_setting;

--changeset 60098727:drop:view:v_tmp_corr_detailed_4
DROP VIEW IF EXISTS v_tmp_corr_detailed;

--changeset 60098727:drop:view:v_gold_corrections_detailed:add_rms_reject
DROP VIEW IF EXISTS v_gold_corrections_detailed;

--changeset 60098727:drop:view:v_gold_statistics:add_rms_reject
DROP VIEW IF EXISTS v_gold_statistics;

--changeset 60098727:drop:backup_functions
DROP FUNCTION IF EXISTS fn_load_gold_reserve_backup(date, date);
DROP FUNCTION IF EXISTS fn_load_gold_statistics_common_backup(date, date);
DROP FUNCTION IF EXISTS fn_load_gold_target_service_level_backup(date, date);
DROP FUNCTION IF EXISTS fn_load_gold_stores_supply_chain_setting_backup();
DROP FUNCTION IF EXISTS fn_load_gold_new_items_backup();

--changeset 60098727:drop:v_gold_order_corrections_analysis:v20
DROP VIEW IF EXISTS v_gold_order_corrections_analysis;
DROP VIEW IF EXISTS v_tmp_corr_detailed;

--changeset 60098727:drop:view:v_gold_corrections_detailed:add_cc_orders
DROP VIEW IF EXISTS v_gold_corrections_detailed;

--changeset 60098727:drop:view:v_gold_statistics:add_cc_orders
DROP VIEW IF EXISTS v_gold_statistics;
