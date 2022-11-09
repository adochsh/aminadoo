--liquibase formatted sql

--changeset 60098727:create:table:tmp_corr_detailed_full

create table tmp_corr_detailed(
        site int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        code_internal bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        order_date timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        prop_number bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        prop_type smallint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        supp_int_code bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        prop_is_valid int encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        prop_item_qty numeric(9, 3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        prop_coeff_euro numeric(12, 5) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        prop_line_code bigint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        contract_numbers public.hstore encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        ord_price numeric(15, 3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        order_item_qty numeric(9, 3) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        ord_item_price numeric(15, 5) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        item varchar(13) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        tech_case_no smallint encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        prop_date timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        prp_amt numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        item_internal int8 encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        cov_period int4 ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
        stock_qty int8 ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
        intransit_qty int8 ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
        lpcarrcde int8 ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
        lpccumbes int8 ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
        lpccumprev numeric(20,10) ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
        lpcarrref int8 ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
        pdcvapar numeric(22,5) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        cover_delivery numeric(22,5) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        delivery_date timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
        cover_end_date timestamp(0) encoding (compresslevel=1,compresstype=zstd,blocksize=32768)
        )
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (order_date);


--changeset 60098727:alter:table:tmp_corr_detailed:add_rms_rejects

ALTER TABLE tmp_corr_detailed ADD rms_reject int2 encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
ALTER TABLE tmp_corr_detailed ADD reject_reason int2 encoding (compresslevel=1,compresstype=zstd,blocksize=32768);

--changeset 60098727:alter:table:tmp_corr_detailed:add_sdpp

ALTER TABLE tmp_corr_detailed ADD sdpp numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);

--changeset 60098727:alter:table:tmp_corr_detailed:add_item_price
ALTER TABLE tmp_corr_detailed ADD item_price numeric encoding (compresslevel=1,compresstype=zstd,blocksize=32768);
