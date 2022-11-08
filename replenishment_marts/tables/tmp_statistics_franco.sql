--liquibase formatted sql

--changeset 60098727:create:table:tmp_statistics_franco

create unlogged table tmp_statistics_franco
    (tech_case_no int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    key_number varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    franco double precision encoding (compresslevel=1,compresstype=zstd,blocksize=32768))
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (key_number, tech_case_no);
