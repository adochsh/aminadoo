--liquibase formatted sql

--changeset 60098727:create:table:tmp_statistics_orders_link

create unlogged table tmp_statistics_orders_link (
        key_number bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        h public.hstore encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        tech_case_no int encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
    	)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (key_number, tech_case_no);
