--liquibase formatted sql

--changeset 60098727:create:table:tt_temp_table

CREATE TABLE tt_temp_table (
    is_init boolean
)
    WITH (appendonly = 'true', compresslevel = '1', orientation = 'row', compresstype = zstd);
