--liquibase formatted sql

--changeset 60098727:create:view:v_artparexc_historical

CREATE OR REPLACE VIEW v_artparexc_historical AS
select 	apxcinl,
       	apxsite,
       	apxddeb,
       	apxdfin,
       	apxtsatt,
       	apxtsmini,
       	apxclages,
       	apxmodges,
       	apxcomm,
       	apxdcre,
       	apxdmaj,
       	apxutil,
       	apxstatut,
       	apxtypvl,
       	apxfdvtol,
       	updated_dttm
from artparexc_historical
