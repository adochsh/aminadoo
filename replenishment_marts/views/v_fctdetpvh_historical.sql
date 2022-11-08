--liquibase formatted sql

--changeset 60098727:create:view:v_fctdetpvh_historical

CREATE OR REPLACE VIEW v_fctdetpvh_historical AS
SELECT
    pvdeid,
    pvdnsem,
    pvdreel,
    pvdcalc,
    pvdorig,
    pvdcorr,
    pvddcor,
    pvdcdel,
    pvddcre,
    pvddmaj,
    pvdutil,
    pvdmlis,
    pvdtend,
    pvderra,
    pvdvret,
    pvdbase,
    pvdccli,
    pvdlev,
    pvderrac,
    updated_dttm
from fctdetpvh_historical
