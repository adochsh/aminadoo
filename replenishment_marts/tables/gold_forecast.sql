--liquibase formatted sql

--changeset 60098727:create:table:gold_forecast:change_partitioning

create table gold_forecast (
    code_lm varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    code_logistics bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    estm_forecast decimal(10,3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    adj_week_forecast decimal(10,3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    err_calc decimal(10,3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    site_code bigint not null encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    first_date_sale timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    avail_date_hist timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    week_num bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    "year" bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    pvdnsem bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    sale_chisen numeric NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    calculation_year bigint NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    calculation_week bigint NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    calculation_date date NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    updated_dttm timestamp without time zone default now() not null encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (code_lm, pvdnsem)
PARTITION BY RANGE (calculation_date) (DEFAULT PARTITION other);

