--liquibase formatted sql

--changeset 60098727:create:table:tmp_statistics_bbxd

create unlogged table tmp_statistics_bbxd (
    	order_number varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	bbxd_lines_qty double precision encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	bbxd_qty double precision encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        bbxd_amount double precision encoding (compresslevel=1,compresstype=zstd,blocksize=32768))
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (order_number);
