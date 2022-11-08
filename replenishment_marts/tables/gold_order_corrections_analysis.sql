--liquibase formatted sql

--changeset 60098727:create:table:gold_order_corrections_analysis

create table gold_order_corrections_analysis
    (prop_id int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     orders text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     prop_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     order_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     delivery_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     cover_end_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     item varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     store int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     prop_qty numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     ord_qty numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     lpcarrcde numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     stockqty_at_propdate numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     intransit_at_propdate numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     stock_days int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     qty_sold numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     reservs numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     l1 numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     prop_targetstock  numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     ord_targetstock numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     l2 numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     pcb int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     need_qty numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     lpccumprev numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     lpcarrref numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     cov_period int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     srok int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     abc text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     error_qty numeric(12,2) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     error_name varchar(30) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
     updated_dttm timestamp without time zone default now() not null encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (store, prop_date)
PARTITION BY RANGE (prop_id) (DEFAULT PARTITION other);

--changeset 60098727:alter:table:gold_order_corrections_analysis:v20
ALTER TABLE gold_order_corrections_analysis ADD l1_case text encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD target_stock numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD money_user numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD money_gold numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD money_rounds numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD ostatoK2 numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD E_rounds numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD ostatoK1 numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD Е_user numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD E_gold numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE gold_order_corrections_analysis ADD ostatoki_desc text encoding (compresslevel=1,compresstype=zstd,blocksize=32768);

ALTER TABLE gold_order_corrections_analysis RENAME COLUMN lpcarrcde TO all_rounds_qty;
ALTER TABLE gold_order_corrections_analysis RENAME COLUMN LPCARRREF TO pcb_round_qty;
ALTER TABLE gold_order_corrections_analysis RENAME COLUMN LPCCUMPREV TO frsct_cov;
ALTER TABLE gold_order_corrections_analysis RENAME COLUMN stockqty_at_propdate TO stock_qty;
ALTER TABLE gold_order_corrections_analysis RENAME COLUMN intransit_at_propdate TO intransit_qty;

ALTER TABLE gold_order_corrections_analysis DROP COLUMN error_qty;
ALTER TABLE gold_order_corrections_analysis DROP COLUMN error_name;
