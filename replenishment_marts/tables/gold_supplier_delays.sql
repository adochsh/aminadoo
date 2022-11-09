--liquibase formatted sql

--changeset 60098727:create:table:gold_supplier_delays

CREATE TABLE gold_supplier_delays (
    ord_tsf_no text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	shipment_id int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	supplier_id int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	from_loc_type text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	from_loc int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	to_loc int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	to_loc_type text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	item text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	ord_tsf_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	plan_receive_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	receive_date1_to_wh date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	receive_date date encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	delay int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	flow_type text encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	order_qty numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	received_qty numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	not_received_qty numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
)
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (ord_tsf_no);

--changeset 60098727:alter:table:gold_supplier_delays
ALTER TABLE gold_supplier_delays ADD updated_dttm timestamptz NOT NULL DEFAULT CLOCK_TIMESTAMP() encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
