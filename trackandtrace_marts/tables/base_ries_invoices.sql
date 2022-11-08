--liquibase formatted sql

--changeset 60115905:create:table:trackandtrace_marts.base_ries_invoices

CREATE TABLE base_ries_invoices (
    invoice_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    oc_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    order_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    shipment_id int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    rms_id int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    adeo_order_number text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    delivery_terms text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    payment_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    seal text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_type text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_total_volume numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_qty numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_qty_pkgs numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_total_amount numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_net_weight numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_gross_weight numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_pallets numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    adeo_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    lm_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ean_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_price numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_curr text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_hs_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_base_ries_invoices boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
)
WITH (appendonly='true', compresslevel='1', orientation='column', compresstype=zstd)
distributed by (invoice_id)
PARTITION BY RANGE (invoice_date) (DEFAULT PARTITION other);
