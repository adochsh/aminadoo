--liquibase formatted sql

--changeset 60098727:create:table:tmp_statistics_users

create unlogged table tmp_statistics_users (
        putnprop numeric(22, 0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        ausextuser varchar(1280) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        putdmaj timestamp encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
        )
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (putnprop);
