--liquibase formatted sql

--changeset 60098727:create:table:fctdetpvh_historical:change_partitioning

create table fctdetpvh_historical (
	pvdeid int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdnsem int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdreel numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdcalc numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdorig varchar(24) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdcorr numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvddcor timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdcdel numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvddcre timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvddmaj timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdutil varchar(48) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdmlis numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdtend numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvderra numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdvret numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdbase numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdccli numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvdlev numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	pvderrac numeric(10,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	updated_dttm timestamp without time zone default now() not null encoding (compresslevel=1,compresstype=zstd,blocksize=32768))
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (pvdeid)
PARTITION BY RANGE (pvddmaj) (DEFAULT PARTITION other);
