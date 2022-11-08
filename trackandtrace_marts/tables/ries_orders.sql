--liquibase formatted sql

--changeset 60098727:create:table:trackandtrace_marts.ries_orders

CREATE TABLE ries_orders (
    order_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , order_date timestamp  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , oc_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , priority text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , oc_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , adeo_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , lm_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , ean_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , etd_oc date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , eta_oc date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , port_of_loading text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , incoterms text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , season_validity_date_from date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , season_validity_date_to date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , oc_total_amount numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , manufacturer text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , desc_eng text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , ries_order_qty numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , article_total_amount numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , under_certification text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , samples_qty text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , samples_required text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , doc_serial_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , doc_serial_valid text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , doc_type text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , doc_batch_issue text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , doc_serial_expiry text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , doc_batch_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , tech_regulation text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , awb_or_container_samples_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , samples_pi_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , initial_pi_sending_date text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , deadline_pi_sending_date text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , sample_sending_way text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , sample_sending_date text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , deadline_sample_sending_date text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , sample_sending_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , reason_sample_sending_late text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , sample_sending_delay text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
    , dbf_ries_orders boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
)
WITH (appendonly='true', compresslevel='1', orientation='column', compresstype=zstd)
distributed by (order_id)
PARTITION BY RANGE (order_date) (DEFAULT PARTITION other);

--changeset 60067166:alter:table:ries_orders:add:columns:transport_instruction
ALTER TABLE ries_orders
    ADD COLUMN route_id integer ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN samples_sent_code integer ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN samples_required_code integer ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN calculated_etd text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN comments_for_etd text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN transport_instruction text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768);

--changeset 60115905:alter:table:ries_orders:add:columns:oreder_status
ALTER TABLE ries_orders
    ADD COLUMN order_status text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768);
