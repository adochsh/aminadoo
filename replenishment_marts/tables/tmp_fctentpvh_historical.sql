--liquibase formatted sql

--changeset 60098727:create:table:tmp_fctentpvh_historical

create unlogged table tmp_fctentpvh_historical (
    	pveid int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvesite int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvecinl int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedpvh timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedobs int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvefobs int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedprv int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvefprv int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pveerra numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvecdif int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvecvar numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedcre timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedmaj timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pveutil varchar(48) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvealert int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvecalbes int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedeca int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvecanal int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvetypvl int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pveflux int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvesysext int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedpv timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedhv timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedem timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pvedur int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    	pveign int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768))
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (pveid);
