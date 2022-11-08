--liquibase formatted sql

--changeset 60115905:create:table:trackandtrace_marts.rms_orders

CREATE TABLE rms_orders (
  order_id text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , lm_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , supplier_code int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , supplier_name text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , v_loc int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , loc int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , loc_name text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , order_date date ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , status text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , unit_price numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , dpac_item numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , order_qty numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , received_qty numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , order_dpac_amount numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , received_dpac_amount numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , order_amount numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , received_amount numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
  , dbf_rms_orders boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
)
WITH (appendonly='true', compresslevel='1', orientation='column', compresstype=zstd)
distributed by (order_id)
PARTITION BY RANGE (order_date) (DEFAULT PARTITION other);

--changeset 60115905:alter:table:trackandtrace_marts.rms_orders.status_oc
ALTER TABLE rms_orders
    ADD COLUMN status_oc text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768);
