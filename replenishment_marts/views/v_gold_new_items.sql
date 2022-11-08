--liquibase formatted sql

--changeset 60098727:create:view:v_gold_new_items

CREATE OR REPLACE VIEW v_gold_new_items AS
select
    store,
    dep,
    item,
    item_desc,
    type,
    gamma,
    supplier,
    suplier_name,
    top,
    status,
    zapas,
    v_puti,
    pm,
    reserv_novinki,
    average_sales,
    median_sales,
    cover_period,
    updated_dttm
FROM gold_new_items
