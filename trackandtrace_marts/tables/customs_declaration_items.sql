--liquibase formatted sql

--changeset 60115905:create:table:trackandtrace_marts.customs_declaration_items


CREATE TABLE customs_declaration_items (
      cd_number text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , tnved_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , good_number int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , sub_goods_number int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , ean_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , adeo_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , quantity numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , unit text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , technical_description text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , manufacturer text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , dbf_customs_declaration_items boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
)
WITH (appendonly='true', compresslevel='1', orientation='column', compresstype=zstd)
distributed by (cd_number);
