--liquibase formatted sql

--changeset 60098727:create:view:v_prpentprop_historical

CREATE OR REPLACE VIEW v_prpentprop_historical AS
SELECT
    pdcnprop,
    pdcsite,
    pdccfin,
    pdcnfilf,
    pdcccin,
    pdcvavar,
    pdcuavar,
    pdcvapar,
    pdcuapar,
    pdcnbrapp,
    pdcnpal,
    pdcdcde,
    pdcdliv,
    pdcsitli,
    pdcnbuc,
    pdcnbcol,
    pdcnbpal,
    pdcnbeuro,
    pdcnbpoint,
    pdcnbm3,
    pdcnbton,
    pdcvalide,
    pdcnbbar,
    pdcdenl,
    pdctpro,
    pdcstat,
    pdcbnmi,
    pdcbnma,
    pdcstmi,
    pdcstma,
    pdcstrsmi,
    pdcstrsma,
    pdcstrami,
    pdcstrama,
    pdcstrcsmi,
    pdcstrcsma,
    pdcstrcami,
    pdcstrcama,
    pdcsemmi,
    pdcsemma,
    pdcstkma,
    pdcsrlma,
    pdcsecma,
    pdcprvma,
    pdcarrma,
    pdccomm,
    pdcecart,
    pdcdcre,
    pdcdmaj,
    pdcutil,
    pdcqpro,
    pdcqspe,
    pdcdfin,
    pdcctrsp,
    pdcnbalert,
    pdcgparef,
    pdcqtsit,
    pdchram,
    pdcenlev,
    pdcaltern,
    pdcqftdu,
    pdchval,
    pdcnbuoa1,
    pdctuoa1,
    pdcnbuoa2,
    pdctuoa2,
    pdcnbuoa3,
    pdctuoa3,
    pdcnbcral,
    pdcnbnw,
    pdcuanw,
    pdcnbuo1,
    pdcnbuo2,
    pdcnbuo3,
    pdcmini,
    pdctypuo,
    pdcnbmin,
    pdcuomin,
    pdcorig,
    pdcdlivi,
    updated_dttm
from prpentprop_historical
