--liquibase formatted sql

--changeset 60098727:create:table:gold_target_service_level

create table gold_target_service_level (
		code_logistics bigint,
		site_code int,
		family_code bigint,
		class int,
		week_num int,
		week_year int,
		week_month int,
		target_serv_lvl numeric(5, 2),
		accepted_serv_lvl numeric(12, 3),
		created_dttm TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP(),
		updated_dttm TIMESTAMP NOT NULL DEFAULT CLOCK_TIMESTAMP())
with (appendonly = 'true', compresslevel = '1', orientation = 'column', compresstype = zstd)
distributed by (week_num);

--changeset 60098727:alter:table:gold_target_service_level

ALTER TABLE gold_target_service_level
ALTER COLUMN target_serv_lvl TYPE numeric(12, 3);

