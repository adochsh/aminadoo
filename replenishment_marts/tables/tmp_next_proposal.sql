--liquibase formatted sql

--changeset 60098727:create:table:tmp_next_proposal

create unlogged table tmp_next_proposal(
        prop_id bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        next_delivery_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        srok int4 encoding (compresslevel=1,compresstype=zstd,blocksize=32768))
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (prop_id);
