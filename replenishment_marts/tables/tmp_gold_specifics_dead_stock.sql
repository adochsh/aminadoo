--liquibase formatted sql

--changeset 60098727:create:table:tmp_gold_specifics_dead_stock

create unlogged table tmp_gold_specifics_dead_stock (
    num_ett	bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    num_art	bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    param smallint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    start_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    end_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (num_art);
