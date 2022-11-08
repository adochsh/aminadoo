--liquibase formatted sql

--changeset 60098727:create:view:v_fctentpvh_historical

CREATE OR REPLACE VIEW v_fctentpvh_historical AS
SELECT
    pveid,
    pvesite,
    pvecinl,
    pvedpvh,
    pvedobs,
    pvefobs,
    pvedprv,
    pvefprv,
    pveerra,
    pvecdif,
    pvecvar,
    pvedcre,
    pvedmaj,
    pveutil,
    pvealert,
    pvecalbes,
    pvedeca,
    pvecanal,
    pvetypvl,
    pveflux,
    pvesysext,
    pvedpv,
    pvedhv,
    pvedem,
    pvedur,
    pveign,
    updated_dttm
from fctentpvh_historical
