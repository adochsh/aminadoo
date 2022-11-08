--liquibase formatted sql

--changeset 60115905:create:table:trackandtrace_marts.customs_declaration

CREATE TABLE customs_declaration (
    cd_number text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_numbers text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_ids_capacities text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    modified_date date ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    modified_time time ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_procedure text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_status_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_status text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_status_date timestamp ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    registration_date date ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    out_date timestamp ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_number int ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    tnved_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_status_code text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_status text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_status_date date ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_status_time time ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_net_weight numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_gross_weight numeric ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_desc text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    accrual_basis_payment text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    customs_rate text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    customs_tax text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_consignment_info text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_airbill_info text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_railbill_info text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_trnsprtbill_info text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_another_trnsprtbill_info text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_doc_info text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_customs_declaration boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
)
WITH (appendonly='true', compresslevel='1', orientation='column', compresstype=zstd)
distributed by (cd_number);
