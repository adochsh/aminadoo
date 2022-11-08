--liquibase formatted sql

--changeset 60098727:create:table:gold_new_items

create table gold_new_items (
    store bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    dep text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    item bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    item_desc text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    type text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    gamma text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    supplier bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    suplier_name text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    top text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    status bpchar(1) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    zapas numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    v_puti bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    pm int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    reserv_novinki int  encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    average_sales numeric(14, 3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    median_sales numeric(14, 3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    cover_period bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    updated_dttm timestamp without time zone default now() not null encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (item);
