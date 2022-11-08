--liquibase formatted sql

--changeset 60098727:create:table:gold_specifics_agg

create table gold_specifics_agg (
    item_res varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    site_res int4 encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    week_num_res int4 encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    sale numeric(14, 3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    id_spec int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    correction_res numeric(14, 3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    cre_date timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (item_res);
