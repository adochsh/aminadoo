--liquibase formatted sql

--changeset 60115905:create:table:trackandtrace_marts.rms_orders_shipments

CREATE TABLE rms_orders_shipments (
   order_id text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , invoice_id text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , lm_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , adeo_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , invoice_upload date ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , receiving_date1 date ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , receiving_date2 date ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , to_loc int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , ship_expected numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , ship_received numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
   , dbf_rms_orders_shipments boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
)
WITH (appendonly='true', compresslevel='1', orientation='column', compresstype=zstd)
distributed by (order_id);
