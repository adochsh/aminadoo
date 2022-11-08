--liquibase formatted sql

--changeset 60098727:create:table:gold_reserve

create table gold_reserve (
    code_lm varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    code_logistics bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    reserve_type varchar(20) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    qty smallint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    start_date timestamp without time zone encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    end_date timestamp without time zone encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    qty_type smallint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    store_num integer encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (code_lm);


--changeset 60098727:alter:table:gold_reserve

ALTER TABLE gold_reserve
ALTER COLUMN qty_type TYPE VARCHAR(255);

--changeset 60098727:alter:table:gold_reserve_2

ALTER TABLE gold_reserve
ALTER COLUMN qty TYPE int;

--changeset 60098727:alter:table:gold_reserve_3
ALTER TABLE gold_reserve ADD qty_per_day numeric(22, 5) NULL;

--changeset 60098727:alter:table:gold_reserve_4
ALTER TABLE gold_reserve drop column qty_type;

--changeset 60098727:alter:table:gold_reserve_5

ALTER TABLE gold_reserve
ALTER COLUMN qty TYPE numeric(12, 2);

--changeset 60098727:alter:table:gold_reserve_6
ALTER TABLE gold_reserve ADD updated_dttm timestamp(0) NULL;

--changeset 60098727:alter:table:gold_reserve_7
ALTER TABLE gold_reserve ADD reserve_code int2 NULL;
