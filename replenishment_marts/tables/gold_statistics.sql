--liquibase formatted sql

--changeset 60098727:create:table:gold_statistics:add_partitioning

create table gold_statistics (
	store int,
    dep varchar(8),
    prop_date timestamp(0),
    prop double precision,
    ord_ls varchar(4000),
    ord_em varchar(4000),
    ord_rd varchar(4000),
    ord_rm varchar(4000),
    type varchar(4000),
    supp varchar(4000),
    suppname varchar(4000),
    prop_price double precision,
    cover double precision,
    franco double precision,
    ord_price double precision,
    qty double precision,
    changed_qty double precision,
    util varchar(1280),
    dmaj varchar(20),
    reg smallint,
    negative_correction double precision,
    positive_correction double precision,
    bbxd_lines_qty double precision,
	bbxd_qty double precision,
    bbxd_amount double precision,
    updated_dttm TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP(),
    target_dep numeric(12, 3),
    target_reg numeric(12, 3),
    target_store numeric(12, 3))
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (store, prop_date)
PARTITION BY RANGE (prop_date) (DEFAULT PARTITION other);

--changeset 60098727:alter:table:gold_statistics:add_supplier_type
ALTER TABLE gold_statistics ADD supplier_type text encoding (compresslevel=1,compresstype=zstd,blocksize=32768);

--changeset 60098727:alter:table:gold_statistics:add_rms_reject
ALTER TABLE gold_statistics ADD rms_reject_items double precision encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_statistics ADD rms_reject_qty double precision encoding (compresslevel=1,compresstype=zstd,blocksize=32768);

--changeset 60098727:alter:table:gold_statistics:add_cc_orders
ALTER TABLE gold_statistics ADD ord_cc varchar(4000) encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
