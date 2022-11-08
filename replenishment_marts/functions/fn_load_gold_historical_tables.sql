--liquibase formatted sql
--changeset 60098727:create:function:fn_load_gold_historical_tables
CREATE OR REPLACE FUNCTION fn_load_gold_historical_tables(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin
--------------------------------------------   ARTPAREXC   -------------------------------------------------------------
    raise notice '==================================== START =====================================';
    raise notice '[%] Inserting into replenishment_marts.artparexc_historical' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_artparexc_historical;

    INSERT INTO tmp_artparexc_historical (apxcinl, apxsite, apxddeb, apxdfin, apxtsatt, apxtsmini, apxclages,
                               apxmodges, apxcomm, apxdcre, apxdmaj, apxutil, apxstatut, apxtypvl,
                               apxfdvtol)
    SELECT apxcinl, apxsite, apxddeb, apxdfin, apxtsatt, apxtsmini, apxclages, apxmodges, apxcomm, apxdcre,
            apxdmaj, apxutil, apxstatut, apxtypvl, apxfdvtol
    FROM gold_refgwr_ods.v_artparexc
    WHERE is_actual = '1'
        and cast(apxdmaj as date) between period_start and period_end;

    DELETE FROM artparexc_historical h
    WHERE EXISTS (SELECT 1
                  FROM tmp_artparexc_historical a
                  WHERE h.apxcinl = a.apxcinl and h.apxtypvl = a.apxtypvl
                       and h.apxsite = a.apxsite and h.apxddeb = a.apxddeb);

    INSERT INTO artparexc_historical (apxcinl, apxsite, apxddeb, apxdfin, apxtsatt, apxtsmini, apxclages,
                               apxmodges, apxcomm, apxdcre, apxdmaj, apxutil, apxstatut, apxtypvl,
                               apxfdvtol)
    SELECT apxcinl, apxsite, apxddeb, apxdfin, apxtsatt, apxtsmini, apxclages, apxmodges, apxcomm, apxdcre,
            apxdmaj, apxutil, apxstatut, apxtypvl, apxfdvtol
    FROM tmp_artparexc_historical h;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % actual rows into replenishment_marts.artparexc_historical' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','artparexc_historical');

    raise notice '[%] Inserting into replenishment_marts.cdedetcde_historical' , date_trunc('second' , clock_timestamp())::text;

--------------------------------------------   CDEDETCDE   -------------------------------------------------------------
    truncate table tmp_cdedetcde_historical;

    INSERT INTO tmp_cdedetcde_historical (dcdcincde, dcdcexcde, dcdnolign, dcdprop, dcdsite, dcdcfin, dcdccin,
                               dcdnfilf, dcddcom, dcddliv, dcdctva, dcdetat, dcdcina, dcdcinl, dcdcinr,
                               dcdcode, dcdtcod, dcdpcu, dcdupcu, dcdqtei, dcdqtec, dcdqtes, dcdral,
                               dcdgra, dcdprix, dcdpbac, dcdpvsa, dcdpvsr, dcdtypa, dcduauvc, dcdmulc,
                               dcdcoefa, dcdarav, dcdqmul, dcdqdiv, dcddlc, dcddel, dcddprec, dcdgrre,
                               dcdprfa, dcdmtdr, dcdmtvi, dcdtran, dcdnego, dcdordr, dcdpvttc, dcdgpds,
                               dcdcomm1, dcdspcb, dcdpcb, dcduvcc, dcduvcp, dcduvcs, dcdclcus, dcdlinp,
                               dcdtypul, dcdstpr, dcdqpro, dcddcre, dcddmaj, dcdutil, dcddprg, dcdseqvl,
                               dcddcpt, dcdccpt, dcduapp, dcduaut, dcdligprx, dcdnops, dcdcoli, dcdnligp,
                               dcdcoefq, dcdcoefp, dcdcexta, dcdaetat, dcdmotif, dcdenvetat, dcdaqtec, dcdvaloetat,
                               dcdppublic, dcdmua, dcdminua, dcdmaxua, dcdtpsr, dcdcinlm, dcdseqvlm, dcdflgmut,
                               dcdnolv, dcdflir, dcdcodea, dcdcexr, dcdcoca, dcdcoul, dcdrefc, dcdcean, dcdedou,
                               dcdrdou, dcdcfina, dcdccina, dcdtrans, dcdctrl, dcdartvalo, dcdcinro, dcdcinlo,
                               dcdseqvlo, dcdqteco, dcdlckdis, dcdaltf, dcdnfilfa, dcdsitlia, dcdcinm, dcddiststat)
    SELECT dcdcincde, dcdcexcde, dcdnolign, dcdprop, dcdsite, dcdcfin, dcdccin,
                               dcdnfilf, dcddcom, dcddliv, dcdctva, dcdetat, dcdcina, dcdcinl, dcdcinr,
                               dcdcode, dcdtcod, dcdpcu, dcdupcu, dcdqtei, dcdqtec, dcdqtes, dcdral,
                               dcdgra, dcdprix, dcdpbac, dcdpvsa, dcdpvsr, dcdtypa, dcduauvc, dcdmulc,
                               dcdcoefa, dcdarav, dcdqmul, dcdqdiv, dcddlc, dcddel, dcddprec, dcdgrre,
                               dcdprfa, dcdmtdr, dcdmtvi, dcdtran, dcdnego, dcdordr, dcdpvttc, dcdgpds,
                               dcdcomm1, dcdspcb, dcdpcb, dcduvcc, dcduvcp, dcduvcs, dcdclcus, dcdlinp,
                               dcdtypul, dcdstpr, dcdqpro, dcddcre, dcddmaj, dcdutil, dcddprg, dcdseqvl,
                               dcddcpt, dcdccpt, dcduapp, dcduaut, dcdligprx, dcdnops, dcdcoli, dcdnligp,
                               dcdcoefq, dcdcoefp, dcdcexta, dcdaetat, dcdmotif, dcdenvetat, dcdaqtec, dcdvaloetat,
                               dcdppublic, dcdmua, dcdminua, dcdmaxua, dcdtpsr, dcdcinlm, dcdseqvlm, dcdflgmut,
                               dcdnolv, dcdflir, dcdcodea, dcdcexr, dcdcoca, dcdcoul, dcdrefc, dcdcean, dcdedou,
                               dcdrdou, dcdcfina, dcdccina, dcdtrans, dcdctrl, dcdartvalo, dcdcinro, dcdcinlo,
                               dcdseqvlo, dcdqteco, dcdlckdis, dcdaltf, dcdnfilfa, dcdsitlia, dcdcinm, dcddiststat
    FROM gold_refcesh_ods.v_cdedetcde
    WHERE is_actual = '1'
        and cast(dcddmaj as date) between period_start and period_end;

    DELETE FROM cdedetcde_historical h
    WHERE EXISTS (SELECT 1
                  FROM tmp_cdedetcde_historical a
                  WHERE h.dcdcincde = a.dcdcincde and h.dcdnolign = a.dcdnolign
                       and h.dcdnligp = a.dcdnligp);

    INSERT INTO cdedetcde_historical (dcdcincde, dcdcexcde, dcdnolign, dcdprop, dcdsite, dcdcfin, dcdccin,
                               dcdnfilf, dcddcom, dcddliv, dcdctva, dcdetat, dcdcina, dcdcinl, dcdcinr,
                               dcdcode, dcdtcod, dcdpcu, dcdupcu, dcdqtei, dcdqtec, dcdqtes, dcdral,
                               dcdgra, dcdprix, dcdpbac, dcdpvsa, dcdpvsr, dcdtypa, dcduauvc, dcdmulc,
                               dcdcoefa, dcdarav, dcdqmul, dcdqdiv, dcddlc, dcddel, dcddprec, dcdgrre,
                               dcdprfa, dcdmtdr, dcdmtvi, dcdtran, dcdnego, dcdordr, dcdpvttc, dcdgpds,
                               dcdcomm1, dcdspcb, dcdpcb, dcduvcc, dcduvcp, dcduvcs, dcdclcus, dcdlinp,
                               dcdtypul, dcdstpr, dcdqpro, dcddcre, dcddmaj, dcdutil, dcddprg, dcdseqvl,
                               dcddcpt, dcdccpt, dcduapp, dcduaut, dcdligprx, dcdnops, dcdcoli, dcdnligp,
                               dcdcoefq, dcdcoefp, dcdcexta, dcdaetat, dcdmotif, dcdenvetat, dcdaqtec, dcdvaloetat,
                               dcdppublic, dcdmua, dcdminua, dcdmaxua, dcdtpsr, dcdcinlm, dcdseqvlm, dcdflgmut,
                               dcdnolv, dcdflir, dcdcodea, dcdcexr, dcdcoca, dcdcoul, dcdrefc, dcdcean, dcdedou,
                               dcdrdou, dcdcfina, dcdccina, dcdtrans, dcdctrl, dcdartvalo, dcdcinro, dcdcinlo,
                               dcdseqvlo, dcdqteco, dcdlckdis, dcdaltf, dcdnfilfa, dcdsitlia, dcdcinm, dcddiststat)
    SELECT dcdcincde, dcdcexcde, dcdnolign, dcdprop, dcdsite, dcdcfin, dcdccin,
                               dcdnfilf, dcddcom, dcddliv, dcdctva, dcdetat, dcdcina, dcdcinl, dcdcinr,
                               dcdcode, dcdtcod, dcdpcu, dcdupcu, dcdqtei, dcdqtec, dcdqtes, dcdral,
                               dcdgra, dcdprix, dcdpbac, dcdpvsa, dcdpvsr, dcdtypa, dcduauvc, dcdmulc,
                               dcdcoefa, dcdarav, dcdqmul, dcdqdiv, dcddlc, dcddel, dcddprec, dcdgrre,
                               dcdprfa, dcdmtdr, dcdmtvi, dcdtran, dcdnego, dcdordr, dcdpvttc, dcdgpds,
                               dcdcomm1, dcdspcb, dcdpcb, dcduvcc, dcduvcp, dcduvcs, dcdclcus, dcdlinp,
                               dcdtypul, dcdstpr, dcdqpro, dcddcre, dcddmaj, dcdutil, dcddprg, dcdseqvl,
                               dcddcpt, dcdccpt, dcduapp, dcduaut, dcdligprx, dcdnops, dcdcoli, dcdnligp,
                               dcdcoefq, dcdcoefp, dcdcexta, dcdaetat, dcdmotif, dcdenvetat, dcdaqtec, dcdvaloetat,
                               dcdppublic, dcdmua, dcdminua, dcdmaxua, dcdtpsr, dcdcinlm, dcdseqvlm, dcdflgmut,
                               dcdnolv, dcdflir, dcdcodea, dcdcexr, dcdcoca, dcdcoul, dcdrefc, dcdcean, dcdedou,
                               dcdrdou, dcdcfina, dcdccina, dcdtrans, dcdctrl, dcdartvalo, dcdcinro, dcdcinlo,
                               dcdseqvlo, dcdqteco, dcdlckdis, dcdaltf, dcdnfilfa, dcdsitlia, dcdcinm, dcddiststat
    FROM tmp_cdedetcde_historical h;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % actual rows into replenishment_marts.cdedetcde_historical' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','cdedetcde_historical');

--------------------------------------------   CDEENTCDE   -------------------------------------------------------------
    raise notice '[%] Inserting into replenishment_marts.cdeentcde_historical' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_cdeentcde_historical;

    INSERT INTO tmp_cdeentcde_historical (ecdcincde, ecdcexcde, ecdprop, ecdsite, ecdcfin, ecdccin, ecdnfilf, ecdcinglo,
                               ecdcexglo, ecddcom, ecddliv, ecddlim, ecdadliv, ecdnfilc, ecdetat, ecdtype, ecdurg,
                               ecdconf, ecdgrel, ecdfran, ecddevi, ecdtxch, ecdnego, ecdcomm1, ecdcomm2, ecdenlev,
                               ecddenlev, ecdtrans, ecdcouc, ecdordr, ecddbas, ecdcred, ecddarr, ecddds, ecdddep,
                               ecdjour, ecdmreg, ecddpai, ecdpori, ecdinco, ecdlieu2, ecdtrsp, ecdvoli, ecdvolr,
                               ecdpdsi, ecdpdsr, ecdtyim, ecdnbjm, ecddval, ecdeimp, ecdcomi1, ecdcomi2, ecdcomi3,
                               ecdcomi4, ecdcomi5, ecdflux, ecdsitli, ecddcla, ecdftype, ecdnlis, ecdesco, ecdnjesc,
                               ecdcons, ecdrele, ecdintf, ecdorig, ecdrepl, ecdmess, ecddcre, ecddmaj, ecdutil, ecddprg,
                               ecdirem, ecdnliv, ecddcpt, ecdccpt, ecdcdetrsp, ecdetrsp, ecdnatcde, ecdnseq, ecddrdv,
                               ecdnroute, ecdlieu, ecdvalof, ecddenvr, ecdcinbdv, ecdcsin, ecdcfina, ecdccina, ecdnfilfa,
                               ecdsitlia, ecdconsid, ecdutilcons, ecdirecyc, ecdcrgp, ecdflir, ecdddrec, ecdtcart,
                               ecdrefgpa, ecdrefext, ecdfrap, ecdedou, ecdcincdeor, ecdsiteor, ecdrenb, ecdavpal,
                               ecddprote, ecdncami, ecdaltern, ecdproper, ecdvcom, ecdaltf, ecdtpro, ecdnvalo,
                               ecdcinap, ecdcexogl, ecddenvoi, ecdaetat, ecdmotif, ecdenvetat, ecdvaloetat, ecdmotifsol)
    SELECT ecdcincde, ecdcexcde, ecdprop, ecdsite, ecdcfin, ecdccin, ecdnfilf, ecdcinglo,
                               ecdcexglo, ecddcom, ecddliv, ecddlim, ecdadliv, ecdnfilc, ecdetat, ecdtype, ecdurg,
                               ecdconf, ecdgrel, ecdfran, ecddevi, ecdtxch, ecdnego, ecdcomm1, ecdcomm2, ecdenlev,
                               ecddenlev, ecdtrans, ecdcouc, ecdordr, ecddbas, ecdcred, ecddarr, ecddds, ecdddep,
                               ecdjour, ecdmreg, ecddpai, ecdpori, ecdinco, ecdlieu2, ecdtrsp, ecdvoli, ecdvolr,
                               ecdpdsi, ecdpdsr, ecdtyim, ecdnbjm, ecddval, ecdeimp, ecdcomi1, ecdcomi2, ecdcomi3,
                               ecdcomi4, ecdcomi5, ecdflux, ecdsitli, ecddcla, ecdftype, ecdnlis, ecdesco, ecdnjesc,
                               ecdcons, ecdrele, ecdintf, ecdorig, ecdrepl, ecdmess, ecddcre, ecddmaj, ecdutil, ecddprg,
                               ecdirem, ecdnliv, ecddcpt, ecdccpt, ecdcdetrsp, ecdetrsp, ecdnatcde, ecdnseq, ecddrdv,
                               ecdnroute, ecdlieu, ecdvalof, ecddenvr, ecdcinbdv, ecdcsin, ecdcfina, ecdccina, ecdnfilfa,
                               ecdsitlia, ecdconsid, ecdutilcons, ecdirecyc, ecdcrgp, ecdflir, ecdddrec, ecdtcart,
                               ecdrefgpa, ecdrefext, ecdfrap, ecdedou, ecdcincdeor, ecdsiteor, ecdrenb, ecdavpal,
                               ecddprote, ecdncami, ecdaltern, ecdproper, ecdvcom, ecdaltf, ecdtpro, ecdnvalo,
                               ecdcinap, ecdcexogl, ecddenvoi, ecdaetat, ecdmotif, ecdenvetat, ecdvaloetat, ecdmotifsol
    FROM gold_refcesh_ods.v_cdeentcde
    WHERE is_actual = '1'
       AND cast(ecddmaj as date) between period_start and period_end;

    DELETE FROM cdeentcde_historical h
    WHERE EXISTS (SELECT 1
                  FROM tmp_cdeentcde_historical a
                  WHERE h.ecdcincde = a.ecdcincde);

    INSERT INTO cdeentcde_historical (ecdcincde, ecdcexcde, ecdprop, ecdsite, ecdcfin, ecdccin, ecdnfilf, ecdcinglo,
                               ecdcexglo, ecddcom, ecddliv, ecddlim, ecdadliv, ecdnfilc, ecdetat, ecdtype, ecdurg,
                               ecdconf, ecdgrel, ecdfran, ecddevi, ecdtxch, ecdnego, ecdcomm1, ecdcomm2, ecdenlev,
                               ecddenlev, ecdtrans, ecdcouc, ecdordr, ecddbas, ecdcred, ecddarr, ecddds, ecdddep,
                               ecdjour, ecdmreg, ecddpai, ecdpori, ecdinco, ecdlieu2, ecdtrsp, ecdvoli, ecdvolr,
                               ecdpdsi, ecdpdsr, ecdtyim, ecdnbjm, ecddval, ecdeimp, ecdcomi1, ecdcomi2, ecdcomi3,
                               ecdcomi4, ecdcomi5, ecdflux, ecdsitli, ecddcla, ecdftype, ecdnlis, ecdesco, ecdnjesc,
                               ecdcons, ecdrele, ecdintf, ecdorig, ecdrepl, ecdmess, ecddcre, ecddmaj, ecdutil, ecddprg,
                               ecdirem, ecdnliv, ecddcpt, ecdccpt, ecdcdetrsp, ecdetrsp, ecdnatcde, ecdnseq, ecddrdv,
                               ecdnroute, ecdlieu, ecdvalof, ecddenvr, ecdcinbdv, ecdcsin, ecdcfina, ecdccina, ecdnfilfa,
                               ecdsitlia, ecdconsid, ecdutilcons, ecdirecyc, ecdcrgp, ecdflir, ecdddrec, ecdtcart,
                               ecdrefgpa, ecdrefext, ecdfrap, ecdedou, ecdcincdeor, ecdsiteor, ecdrenb, ecdavpal,
                               ecddprote, ecdncami, ecdaltern, ecdproper, ecdvcom, ecdaltf, ecdtpro, ecdnvalo,
                               ecdcinap, ecdcexogl, ecddenvoi, ecdaetat, ecdmotif, ecdenvetat, ecdvaloetat, ecdmotifsol)
    SELECT ecdcincde, ecdcexcde, ecdprop, ecdsite, ecdcfin, ecdccin, ecdnfilf, ecdcinglo,
                               ecdcexglo, ecddcom, ecddliv, ecddlim, ecdadliv, ecdnfilc, ecdetat, ecdtype, ecdurg,
                               ecdconf, ecdgrel, ecdfran, ecddevi, ecdtxch, ecdnego, ecdcomm1, ecdcomm2, ecdenlev,
                               ecddenlev, ecdtrans, ecdcouc, ecdordr, ecddbas, ecdcred, ecddarr, ecddds, ecdddep,
                               ecdjour, ecdmreg, ecddpai, ecdpori, ecdinco, ecdlieu2, ecdtrsp, ecdvoli, ecdvolr,
                               ecdpdsi, ecdpdsr, ecdtyim, ecdnbjm, ecddval, ecdeimp, ecdcomi1, ecdcomi2, ecdcomi3,
                               ecdcomi4, ecdcomi5, ecdflux, ecdsitli, ecddcla, ecdftype, ecdnlis, ecdesco, ecdnjesc,
                               ecdcons, ecdrele, ecdintf, ecdorig, ecdrepl, ecdmess, ecddcre, ecddmaj, ecdutil, ecddprg,
                               ecdirem, ecdnliv, ecddcpt, ecdccpt, ecdcdetrsp, ecdetrsp, ecdnatcde, ecdnseq, ecddrdv,
                               ecdnroute, ecdlieu, ecdvalof, ecddenvr, ecdcinbdv, ecdcsin, ecdcfina, ecdccina, ecdnfilfa,
                               ecdsitlia, ecdconsid, ecdutilcons, ecdirecyc, ecdcrgp, ecdflir, ecdddrec, ecdtcart,
                               ecdrefgpa, ecdrefext, ecdfrap, ecdedou, ecdcincdeor, ecdsiteor, ecdrenb, ecdavpal,
                               ecddprote, ecdncami, ecdaltern, ecdproper, ecdvcom, ecdaltf, ecdtpro, ecdnvalo,
                               ecdcinap, ecdcexogl, ecddenvoi, ecdaetat, ecdmotif, ecdenvetat, ecdvaloetat, ecdmotifsol
    FROM tmp_cdeentcde_historical;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % actual rows into replenishment_marts.cdeentcde_historical' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','cdeentcde_historical');

--------------------------------------------   FCTDETPVH   -------------------------------------------------------------
    raise notice '[%] Inserting into replenishment_marts.fctdetpvh_historical' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_fctdetpvh_historical;

    INSERT INTO tmp_fctdetpvh_historical (pvdeid, pvdnsem, pvdreel, pvdcalc, pvdorig, pvdcorr, pvddcor, pvdcdel, pvddcre,
                               pvddmaj, pvdutil, pvdmlis, pvdtend, pvderra, pvdvret, pvdbase, pvdccli, pvdlev, pvderrac)
    SELECT dppvdeid, dppvdnsem, dppvdreel, dppvdcalc, dppvdorig, dppvdcorr, dppvddcor, dppvdcdel, dppvddcre,
                               dppvddmaj, dppvdutil, dppvdmlis, dppvdtend, dppvderra, dppvdvret, dppvdbase, dppvdccli,
                               dppvdlev, dppvderrac
    FROM gold_refgwr_ods.v_fctdetpvh_dp
    WHERE is_actual = '1'
        AND cast(dppvddmaj as date) between period_start and period_end;

    DELETE FROM fctdetpvh_historical h
    WHERE EXISTS (SELECT 1
                  FROM tmp_fctdetpvh_historical a
                  WHERE a.pvdeid = h.pvdeid and a.pvdnsem = h.pvdnsem);

    INSERT INTO fctdetpvh_historical (pvdeid, pvdnsem, pvdreel, pvdcalc, pvdorig, pvdcorr, pvddcor, pvdcdel, pvddcre,
                               pvddmaj, pvdutil, pvdmlis, pvdtend, pvderra, pvdvret, pvdbase, pvdccli, pvdlev, pvderrac)
    SELECT pvdeid, pvdnsem, pvdreel, pvdcalc, pvdorig, pvdcorr, pvddcor, pvdcdel, pvddcre,
                               pvddmaj, pvdutil, pvdmlis, pvdtend, pvderra, pvdvret, pvdbase, pvdccli, pvdlev, pvderrac
    FROM tmp_fctdetpvh_historical h;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % actual rows into replenishment_marts.fctdetpvh_historical' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','fctdetpvh_historical');


--------------------------------------------   FCTENTPVH   -------------------------------------------------------------
    raise notice '[%] Inserting into replenishment_marts.fctentpvh_historical' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_fctentpvh_historical;

    INSERT INTO tmp_fctentpvh_historical (pveid, pvesite, pvecinl, pvedpvh, pvedobs, pvefobs, pvedprv, pvefprv, pveerra,
                               pvecdif, pvecvar, pvedcre, pvedmaj, pveutil, pvealert, pvecalbes, pvedeca, pvecanal,
                               pvetypvl, pveflux, pvesysext, pvedpv, pvedhv, pvedem, pvedur, pveign)
    SELECT dppveid, dppvesite, dppvecinl, dppvedpvh, dppvedobs, dppvefobs, dppvedprv, dppvefprv, dppveerra,
                               dppvecdif, dppvecvar, dppvedcre, dppvedmaj, dppveutil, dppvealert, dppvecalbes, dppvedeca,
                               dppvecanal, dppvetypvl, dppveflux, dppvesysext, dppvedpv, dppvedhv, dppvedem, dppvedur,
                               dppveign
    FROM gold_refgwr_ods.v_fctentpvh_dp
    WHERE is_actual = '1'
        AND cast(dppvedmaj as date) between period_start and period_end;

    DELETE FROM fctentpvh_historical h
    WHERE EXISTS (SELECT 1
                  FROM tmp_fctentpvh_historical a
                  WHERE a.pveid = h.pveid);

    INSERT INTO fctentpvh_historical (pveid, pvesite, pvecinl, pvedpvh, pvedobs, pvefobs, pvedprv, pvefprv, pveerra,
                               pvecdif, pvecvar, pvedcre, pvedmaj, pveutil, pvealert, pvecalbes, pvedeca, pvecanal,
                               pvetypvl, pveflux, pvesysext, pvedpv, pvedhv, pvedem, pvedur, pveign)
    SELECT pveid, pvesite, pvecinl, pvedpvh, pvedobs, pvefobs, pvedprv, pvefprv, pveerra,
                               pvecdif, pvecvar, pvedcre, pvedmaj, pveutil, pvealert, pvecalbes, pvedeca, pvecanal,
                               pvetypvl, pveflux, pvesysext, pvedpv, pvedhv, pvedem, pvedur, pveign
    FROM tmp_fctentpvh_historical h;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % actual rows into replenishment_marts.fctentpvh_historical' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','fctentpvh_historical');

--------------------------------------------   PRPDETPROP   ------------------------------------------------------------

    raise notice '[%] Inserting into replenishment_marts.prpdetprop_historical' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_prpdetprop_historical;

    INSERT INTO tmp_prpdetprop_historical (lpcnprop, lpccinl, lpcseqvl, lpcsite, lpcsecu, lpcapnh, lpcrpnh, lpccval, lpcqtec,
                               lpcqtei, lpcdlv, lpcnbspec, lpcqpro, lpcqspec, lpccumprev, lpcarrref, lpcarrcde, lpcqstr,
                               lpcqral, lpcapab, lpcnjprev, lpcnjstk, lpcnjsrl, lpcnjbes, lpcnjcde, lpcqteo, lpcprecaut,
                               lpcdcre, lpcdmaj, lpcutil, lpccdeprev, lpcqgra, lpcqanti, lpccdecli, lpccumret, lpccalbes,
                               lpcnolign, lpcnligp, lpcqtsit, lpctval, lpcdlivgpa, lpcapgpa, lpcqftdu, lpcrall, lpcnjsrlh,
                               lpcnjpliv, lpcnjcdeh, lpccumbes, lpcbesbrut, lpcprecret, lpcsecuret, lpcprecblk, lpcprecnblk,
                               lpcnjcouv, lpcrsem, lpcqsti, lpcqst, lpcqprob, lpcvmflm, erpcrit1, erpcrit2, erpcrit3,
                               lpcqtgpa, lpcpdsemb, lpcupdsemb, lpccfin, lpcccin, lpcnfilf, lpccinr, lpccinluvc,
                               lpcmotif, lpccomm, lpcverif, lpcflux, lpcminua, lpcmultua, lpcnjstkh, lpccinli, lpcseqvli,
                               lpccinlo, lpcseqvlo, lpccoefcol, lpccoefcou, lpccoefpal, lpccoefeuro, lpccoefpoint,
                               lpccoefvol, lpccoefpds, lpcclages, lpccinlmaitre, lpccoefflien, lpcpdsmoy, lpcresid,
                               lpctypvl, lpcdfdv, lpcaradeb, lpcarafin)
    SELECT lpcnprop, lpccinl, lpcseqvl, lpcsite, lpcsecu, lpcapnh, lpcrpnh, lpccval, lpcqtec,
                               lpcqtei, lpcdlv, lpcnbspec, lpcqpro, lpcqspec, lpccumprev, lpcarrref, lpcarrcde, lpcqstr,
                               lpcqral, lpcapab, lpcnjprev, lpcnjstk, lpcnjsrl, lpcnjbes, lpcnjcde, lpcqteo, lpcprecaut,
                               lpcdcre, lpcdmaj, lpcutil, lpccdeprev, lpcqgra, lpcqanti, lpccdecli, lpccumret, lpccalbes,
                               lpcnolign, lpcnligp, lpcqtsit, lpctval, lpcdlivgpa, lpcapgpa, lpcqftdu, lpcrall, lpcnjsrlh,
                               lpcnjpliv, lpcnjcdeh, lpccumbes, lpcbesbrut, lpcprecret, lpcsecuret, lpcprecblk, lpcprecnblk,
                               lpcnjcouv, lpcrsem, lpcqsti, lpcqst, lpcqprob, lpcvmflm, erpcrit1, erpcrit2, erpcrit3,
                               lpcqtgpa, lpcpdsemb, lpcupdsemb, lpccfin, lpcccin, lpcnfilf, lpccinr, lpccinluvc,
                               lpcmotif, lpccomm, lpcverif, lpcflux, lpcminua, lpcmultua, lpcnjstkh, lpccinli, lpcseqvli,
                               lpccinlo, lpcseqvlo, lpccoefcol, lpccoefcou, lpccoefpal, lpccoefeuro, lpccoefpoint,
                               lpccoefvol, lpccoefpds, lpcclages, lpccinlmaitre, lpccoefflien, lpcpdsmoy, lpcresid,
                               lpctypvl, lpcdfdv, lpcaradeb, lpcarafin
    FROM gold_refgwr_ods.v_prpdetprop
    WHERE is_actual = '1'
        AND cast(lpcdmaj as date) between period_start and period_end;

    DELETE FROM prpdetprop_historical h
    WHERE EXISTS (SELECT 1
                  FROM tmp_prpdetprop_historical a
                  WHERE a.lpcnprop = h.lpcnprop and a.lpcnolign = h.lpcnolign and a.lpcnligp = h.lpcnligp);

    INSERT INTO prpdetprop_historical (lpcnprop, lpccinl, lpcseqvl, lpcsite, lpcsecu, lpcapnh, lpcrpnh, lpccval, lpcqtec,
                               lpcqtei, lpcdlv, lpcnbspec, lpcqpro, lpcqspec, lpccumprev, lpcarrref, lpcarrcde, lpcqstr,
                               lpcqral, lpcapab, lpcnjprev, lpcnjstk, lpcnjsrl, lpcnjbes, lpcnjcde, lpcqteo, lpcprecaut,
                               lpcdcre, lpcdmaj, lpcutil, lpccdeprev, lpcqgra, lpcqanti, lpccdecli, lpccumret, lpccalbes,
                               lpcnolign, lpcnligp, lpcqtsit, lpctval, lpcdlivgpa, lpcapgpa, lpcqftdu, lpcrall, lpcnjsrlh,
                               lpcnjpliv, lpcnjcdeh, lpccumbes, lpcbesbrut, lpcprecret, lpcsecuret, lpcprecblk, lpcprecnblk,
                               lpcnjcouv, lpcrsem, lpcqsti, lpcqst, lpcqprob, lpcvmflm, erpcrit1, erpcrit2, erpcrit3,
                               lpcqtgpa, lpcpdsemb, lpcupdsemb, lpccfin, lpcccin, lpcnfilf, lpccinr, lpccinluvc,
                               lpcmotif, lpccomm, lpcverif, lpcflux, lpcminua, lpcmultua, lpcnjstkh, lpccinli, lpcseqvli,
                               lpccinlo, lpcseqvlo, lpccoefcol, lpccoefcou, lpccoefpal, lpccoefeuro, lpccoefpoint,
                               lpccoefvol, lpccoefpds, lpcclages, lpccinlmaitre, lpccoefflien, lpcpdsmoy, lpcresid,
                               lpctypvl, lpcdfdv, lpcaradeb, lpcarafin)
    SELECT lpcnprop, lpccinl, lpcseqvl, lpcsite, lpcsecu, lpcapnh, lpcrpnh, lpccval, lpcqtec,
                               lpcqtei, lpcdlv, lpcnbspec, lpcqpro, lpcqspec, lpccumprev, lpcarrref, lpcarrcde, lpcqstr,
                               lpcqral, lpcapab, lpcnjprev, lpcnjstk, lpcnjsrl, lpcnjbes, lpcnjcde, lpcqteo, lpcprecaut,
                               lpcdcre, lpcdmaj, lpcutil, lpccdeprev, lpcqgra, lpcqanti, lpccdecli, lpccumret, lpccalbes,
                               lpcnolign, lpcnligp, lpcqtsit, lpctval, lpcdlivgpa, lpcapgpa, lpcqftdu, lpcrall, lpcnjsrlh,
                               lpcnjpliv, lpcnjcdeh, lpccumbes, lpcbesbrut, lpcprecret, lpcsecuret, lpcprecblk, lpcprecnblk,
                               lpcnjcouv, lpcrsem, lpcqsti, lpcqst, lpcqprob, lpcvmflm, erpcrit1, erpcrit2, erpcrit3,
                               lpcqtgpa, lpcpdsemb, lpcupdsemb, lpccfin, lpcccin, lpcnfilf, lpccinr, lpccinluvc,
                               lpcmotif, lpccomm, lpcverif, lpcflux, lpcminua, lpcmultua, lpcnjstkh, lpccinli, lpcseqvli,
                               lpccinlo, lpcseqvlo, lpccoefcol, lpccoefcou, lpccoefpal, lpccoefeuro, lpccoefpoint,
                               lpccoefvol, lpccoefpds, lpcclages, lpccinlmaitre, lpccoefflien, lpcpdsmoy, lpcresid,
                               lpctypvl, lpcdfdv, lpcaradeb, lpcarafin
    FROM tmp_prpdetprop_historical h;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % actual rows into replenishment_marts.prpdetprop_historical' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','prpdetprop_historical');

--------------------------------------------   PRPENTPROP   ------------------------------------------------------------

    raise notice '[%] Inserting into replenishment_marts.prpdetprop_historical' , date_trunc('second' , clock_timestamp())::text;

    truncate table tmp_prpentprop_historical;

    INSERT INTO tmp_prpentprop_historical (pdcnprop, pdcsite, pdccfin, pdcnfilf, pdcccin, pdcvavar, pdcuavar, pdcvapar,
                               pdcuapar, pdcnbrapp, pdcnpal, pdcdcde, pdcdliv, pdcsitli, pdcnbuc, pdcnbcol, pdcnbpal,
                               pdcnbeuro, pdcnbpoint, pdcnbm3, pdcnbton, pdcvalide, pdcnbbar, pdcdenl, pdctpro, pdcstat,
                               pdcbnmi, pdcbnma, pdcstmi, pdcstma, pdcstrsmi, pdcstrsma, pdcstrami, pdcstrama, pdcstrcsmi,
                               pdcstrcsma, pdcstrcami, pdcstrcama, pdcsemmi, pdcsemma, pdcstkma, pdcsrlma, pdcsecma,
                               pdcprvma, pdcarrma, pdccomm, pdcecart, pdcdcre, pdcdmaj, pdcutil, pdcqpro, pdcqspe,
                               pdcdfin, pdcctrsp, pdcnbalert, pdcgparef, pdcqtsit, pdchram, pdcenlev, pdcaltern,
                               pdcqftdu, pdchval, pdcnbuoa1, pdctuoa1, pdcnbuoa2, pdctuoa2, pdcnbuoa3, pdctuoa3,
                               pdcnbcral, pdcnbnw, pdcuanw, pdcnbuo1, pdcnbuo2, pdcnbuo3, pdcmini, pdctypuo, pdcnbmin,
                               pdcuomin, pdcorig, pdcdlivi)
    SELECT pdcnprop, pdcsite, pdccfin, pdcnfilf, pdcccin, pdcvavar, pdcuavar, pdcvapar,
                               pdcuapar, pdcnbrapp, pdcnpal, pdcdcde, pdcdliv, pdcsitli, pdcnbuc, pdcnbcol, pdcnbpal,
                               pdcnbeuro, pdcnbpoint, pdcnbm3, pdcnbton, pdcvalide, pdcnbbar, pdcdenl, pdctpro, pdcstat,
                               pdcbnmi, pdcbnma, pdcstmi, pdcstma, pdcstrsmi, pdcstrsma, pdcstrami, pdcstrama, pdcstrcsmi,
                               pdcstrcsma, pdcstrcami, pdcstrcama, pdcsemmi, pdcsemma, pdcstkma, pdcsrlma, pdcsecma,
                               pdcprvma, pdcarrma, pdccomm, pdcecart, pdcdcre, pdcdmaj, pdcutil, pdcqpro, pdcqspe,
                               pdcdfin, pdcctrsp, pdcnbalert, pdcgparef, pdcqtsit, pdchram, pdcenlev, pdcaltern,
                               pdcqftdu, pdchval, pdcnbuoa1, pdctuoa1, pdcnbuoa2, pdctuoa2, pdcnbuoa3, pdctuoa3,
                               pdcnbcral, pdcnbnw, pdcuanw, pdcnbuo1, pdcnbuo2, pdcnbuo3, pdcmini, pdctypuo, pdcnbmin,
                               pdcuomin, pdcorig, pdcdlivi
    FROM gold_refgwr_ods.v_prpentprop
    WHERE is_actual = '1'
        AND cast(pdcdmaj as date) between period_start and period_end;

    DELETE FROM prpentprop_historical h
    WHERE EXISTS (SELECT 1
                  FROM tmp_prpentprop_historical a
                  WHERE a.pdcnprop = h.pdcnprop);

    INSERT INTO prpentprop_historical (pdcnprop, pdcsite, pdccfin, pdcnfilf, pdcccin, pdcvavar, pdcuavar, pdcvapar,
                               pdcuapar, pdcnbrapp, pdcnpal, pdcdcde, pdcdliv, pdcsitli, pdcnbuc, pdcnbcol, pdcnbpal,
                               pdcnbeuro, pdcnbpoint, pdcnbm3, pdcnbton, pdcvalide, pdcnbbar, pdcdenl, pdctpro, pdcstat,
                               pdcbnmi, pdcbnma, pdcstmi, pdcstma, pdcstrsmi, pdcstrsma, pdcstrami, pdcstrama, pdcstrcsmi,
                               pdcstrcsma, pdcstrcami, pdcstrcama, pdcsemmi, pdcsemma, pdcstkma, pdcsrlma, pdcsecma,
                               pdcprvma, pdcarrma, pdccomm, pdcecart, pdcdcre, pdcdmaj, pdcutil, pdcqpro, pdcqspe,
                               pdcdfin, pdcctrsp, pdcnbalert, pdcgparef, pdcqtsit, pdchram, pdcenlev, pdcaltern,
                               pdcqftdu, pdchval, pdcnbuoa1, pdctuoa1, pdcnbuoa2, pdctuoa2, pdcnbuoa3, pdctuoa3,
                               pdcnbcral, pdcnbnw, pdcuanw, pdcnbuo1, pdcnbuo2, pdcnbuo3, pdcmini, pdctypuo, pdcnbmin,
                               pdcuomin, pdcorig, pdcdlivi)
    SELECT pdcnprop, pdcsite, pdccfin, pdcnfilf, pdcccin, pdcvavar, pdcuavar, pdcvapar,
                               pdcuapar, pdcnbrapp, pdcnpal, pdcdcde, pdcdliv, pdcsitli, pdcnbuc, pdcnbcol, pdcnbpal,
                               pdcnbeuro, pdcnbpoint, pdcnbm3, pdcnbton, pdcvalide, pdcnbbar, pdcdenl, pdctpro, pdcstat,
                               pdcbnmi, pdcbnma, pdcstmi, pdcstma, pdcstrsmi, pdcstrsma, pdcstrami, pdcstrama, pdcstrcsmi,
                               pdcstrcsma, pdcstrcami, pdcstrcama, pdcsemmi, pdcsemma, pdcstkma, pdcsrlma, pdcsecma,
                               pdcprvma, pdcarrma, pdccomm, pdcecart, pdcdcre, pdcdmaj, pdcutil, pdcqpro, pdcqspe,
                               pdcdfin, pdcctrsp, pdcnbalert, pdcgparef, pdcqtsit, pdchram, pdcenlev, pdcaltern,
                               pdcqftdu, pdchval, pdcnbuoa1, pdctuoa1, pdcnbuoa2, pdctuoa2, pdcnbuoa3, pdctuoa3,
                               pdcnbcral, pdcnbnw, pdcuanw, pdcnbuo1, pdcnbuo2, pdcnbuo3, pdcmini, pdctypuo, pdcnbmin,
                                pdcuomin, pdcorig, pdcdlivi
    FROM tmp_prpentprop_historical h;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % actual rows into replenishment_marts.prpentprop_historical' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','prpentprop_historical');

    raise notice '[%] Function finished.' , date_trunc('second' , clock_timestamp())::text;
    raise notice '==================================== FINISH =====================================';
    return 0;
end;
$function$
;
