--liquibase formatted sql

--changeset 60098727:create:table:gold_corrections_detailed:add_partitioning

create table gold_corrections_detailed (
	reg smallint,
	store int,
    dep varchar(8),
    prop_date timestamp(0),
    prop double precision,
    ord_ls varchar(4000),
    ord_em varchar(4000),
    ord_rd varchar(4000),
    ord_rm varchar(4000),
    supp varchar(4000),
    suppname varchar(4000),
    prop_price double precision,
    cover double precision,
    franco double precision,
    ord_price double precision,
    item varchar(13),
    order_item_qty double precision,
    prop_item_qty double precision,
	corrected double precision,
	full_prop_price double precision,
	full_order_price double precision,
	updated_dttm TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP(),
	order_date timestamp(0),
    delivery_date timestamp(0),
    cover_end_date timestamp(0),
    bbxd_type int)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (store, prop_date)
PARTITION BY RANGE (prop_date) (DEFAULT PARTITION other);


--changeset 60098727:alter:table:gold_corrections_detailed:add_rms_rejects

ALTER TABLE gold_corrections_detailed ADD rms_reject int2 encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_corrections_detailed ADD reject_reason int2 encoding (compresslevel=1,compresstype=zstd,blocksize=32768);

--changeset 60098727:alter:table:gold_corrections_detailed:add_cc_orders

ALTER TABLE gold_corrections_detailed ADD ord_cc varchar(4000) encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
