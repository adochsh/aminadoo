--liquibase formatted sql

--changeset 60115905:create:table:trackandtrace_marts.rms_item_info

CREATE TABLE rms_item_info (
  lm_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , adeo_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , lm_name text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , dep_code int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , dep_name text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , sub_dep_code int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , sub_dep_name text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , lm_type int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , lm_type_desc text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , lm_subtype int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , lm_subtype_desc text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , import_attr text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , flow_type text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , top1000 text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , brand text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , mdd text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , best_price text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , gamma text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , dbf_rms_item_info boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
)
WITH (appendonly='true', compresslevel='1', orientation='column', compresstype=zstd)
distributed by (lm_code);
