--liquibase formatted sql

--changeset 60098727:create:table:tmp_artparexc_historical

create unlogged table tmp_artparexc_historical (
    apxcinl int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxsite int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxddeb timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxdfin timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxtsatt numeric(5,2) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxtsmini numeric(5,2) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxclages int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxmodges int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxcomm varchar(1020) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxdcre timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxdmaj timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxutil varchar(48) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxstatut int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxtypvl int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
    apxfdvtol int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768))
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (apxcinl);
