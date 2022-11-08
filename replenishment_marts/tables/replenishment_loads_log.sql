--liquibase formatted sql

--changeset 60098727:create:table:replenishment_loads_log

create table replenishment_loads_log (
    process_name varchar(200) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    step varchar(200) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    action_name varchar(200) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    action_time timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    row_count bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed randomly;
