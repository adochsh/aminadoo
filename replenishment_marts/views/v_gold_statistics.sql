--liquibase formatted sql

--changeset 60098727:create:view:v_gold_statistics

CREATE OR REPLACE VIEW v_gold_statistics AS
select store,
        dep,
        prop_date,
        prop,
        ord_ls,
        ord_em,
        ord_rd,
        ord_rm,
        ord_cc,
        type,
        supp,
        suppname,
        prop_price,
        cover,
        franco,
        ord_price,
        qty,
        changed_qty,
        util,
        dmaj,
        reg,
        negative_correction,
        positive_correction,
        rms_reject_items,
        rms_reject_qty,
        bbxd_lines_qty,
        bbxd_qty,
        bbxd_amount,
        target_dep,
        target_reg,
        target_store,
        updated_dttm,
        supplier_type
from gold_statistics
