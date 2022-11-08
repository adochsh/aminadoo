--liquibase formatted sql

--changeset 60098727:create:table:gold_specifics_agg_detailed

create table gold_specifics_agg_detailed (
    nk text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    item varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    site int4 encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    week_num int4  encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    correction_sum numeric(14, 3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    sale_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    id_spec int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    cre_date timestamp(0) default current_timestamp encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (item);
