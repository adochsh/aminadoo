--liquibase formatted sql

--changeset 60098727:create:table:cdedetcde_historical

create table cdedetcde_historical (
	dcdqpro numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddcre timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddmaj timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdutil varchar(48) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddprg varchar(48) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdseqvl int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddcpt timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdccpt varchar(64) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcduapp int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcduaut numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdligprx int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdnops int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcoli numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdnligp int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcoefq numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcoefp numeric(15,10) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcexta int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdaetat int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdmotif int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdenvetat int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdaqtec numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdvaloetat int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdppublic numeric(15,5) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdmua int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdminua int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdmaxua int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdtpsr int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcinlm int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdseqvlm int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdflgmut int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdnolv int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdflir int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcodea varchar(80) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcexr varchar(56) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcoca varchar(56) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcoul varchar(56) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdrefc varchar(80) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcean varchar(56) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdedou int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdrdou varchar(120) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcfina int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdccina int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdtrans int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdctrl int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdartvalo int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcinro int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcinlo int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdseqvlo int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdqteco numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdlckdis int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdaltf int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdnfilfa int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdsitlia int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcinm int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddiststat int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcincde int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcexcde varchar(52) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdnolign varchar(52) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdprop int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdsite int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcfin int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdccin int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdnfilf int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddcom timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddliv timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdctva int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdetat int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcina int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcinl int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcinr int8 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcode varchar(56) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdtcod int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdpcu numeric(8,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdupcu int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdqtei numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdqtec numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdqtes numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdral numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdgra numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdprix numeric(15,5) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdpbac numeric(15,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdpvsa numeric(20,10) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdpvsr numeric(20,10) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdtypa int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcduauvc numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdmulc int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcoefa numeric(4,2) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdarav int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdqmul numeric(8,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdqdiv int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddlc timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddel int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcddprec timestamp(0) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdgrre numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdprfa numeric(15,5) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdmtdr numeric(15,5) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdmtvi numeric(15,5) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdtran numeric(15,5) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdnego numeric(15,5) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdordr int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdpvttc numeric(15,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdgpds int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdcomm1 varchar(320) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdspcb int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdpcb int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcduvcc int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcduvcp int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcduvcs int4 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdclcus varchar(80) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdlinp int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdtypul int2 NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	dcdstpr numeric(9,3) NULL encoding (compresslevel=1,compresstype=zstd,blocksize=32768),
	updated_dttm timestamp without time zone default now() not null encoding (compresslevel=1,compresstype=zstd,blocksize=32768))
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (DCDCINCDE)
PARTITION BY RANGE (DCDDCOM) (DEFAULT PARTITION other);
