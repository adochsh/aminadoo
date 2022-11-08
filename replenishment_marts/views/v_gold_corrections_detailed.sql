--liquibase formatted sql

--changeset 60098727:create:view:v_gold_corrections_detailed

CREATE OR REPLACE VIEW v_gold_corrections_detailed AS
select reg,
        store,
        dep,
        order_date,
        prop_date,
        prop,
        ord_ls,
        ord_em,
        ord_rd,
        ord_rm,
        ord_cc,
        supp,
        suppname,
        prop_price,
        delivery_date,
        cover_end_date,
        cover,
        franco,
        ord_price,
        item,
        order_item_qty,
        prop_item_qty,
       	corrected,
       	rms_reject,
       	reject_reason,
       	full_prop_price,
       	full_order_price,
       	bbxd_type,
       	updated_dttm
from gold_corrections_detailed
