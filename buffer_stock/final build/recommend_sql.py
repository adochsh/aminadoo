script = """
    WITH temp_base AS (
    SELECT tuc.ARACEXR AS item, tuc.arasite AS store
        , tuc.ARACINR, tuc.ARACINL, tuc.ARACFIN
        , COALESCE(cou.STOVALI,0) AS stock_store  --- сток на пополнение
    FROM artuc tuc
    LEFT JOIN REFCESH.stocouch cou ON cou.STOTPOS=0 AND cou.STOCINL =tuc.ARACINL AND cou.STOSITE =tuc.arasite
    WHERE tuc.ARATCDE = 1 AND tuc.ARATFOU = 1 AND tuc.ARACDBLE = 1 AND TRUNC(SYSDATE) BETWEEN tuc.ARADDEB AND tuc.ARADFIN
        AND tuc.ARACEXR IN ({})
        AND tuc.arasite	IN ({})
    )
    , tmp_orders AS (
    SELECT oh.LOCATION AS store
        , ol.ITEM
        , oh.SUPPLIER --, oh.ORDER_NO
        , min(oh.WRITTEN_DATE) AS min_order_date
        , min(oh.not_after_date) AS min_delivery_date
        , max(oh.not_after_date) AS max_delivery_date
        , sum(COALESCE(decode(ol.QTY_RECEIVED, NULL, ol.QTY_ORDERED, 0),0)) AS qty_in_transit
        , count(oh.ORDER_NO) AS cnt_orders
    FROM ordhead@RMS_APPRO oh
    JOIN ordloc@RMS_APPRO ol ON ol.ORDER_NO= oh.ORDER_NO
    WHERE status ='A' AND extract(YEAR FROM WRITTEN_DATE) =extract(YEAR FROM sysdate)
        AND oh.LOC_TYPE='S' AND coalesce(oh.po_type, '-') != 'CC' AND oh.INCLUDE_ON_ORDER_IND = 'Y'
        AND ol.item IN ({})
        AND oh.LOCATION IN ({})
        AND oh.WRITTEN_DATE <=trunc(SYSDATE)
    GROUP BY oh.LOCATION, ol.ITEM, oh.SUPPLIER
    )

    , tmp_calendars AS (
    SELECT foup.FPLCFIN, foup.FPLSITE AS store
        , foup.FPLDRAM  , foup.FPLDLIV
        , fouc.foucnuf  , fouc.foulibl
        , DENSE_RANK() over(PARTITION BY foup.FPLCFIN, foup.FPLSITE ORDER BY foup.FPLDRAM) AS rn
    FROM FOUPLAN foup
    LEFT JOIN foudgene fouc ON fouc.foucfin =foup.FPLCFIN
    WHERE FPLCFIN IN (SELECT DISTINCT ARACFIN FROM temp_base) AND FPLSITE IN ({})
      AND foup.FPLDRAM >=trunc(SYSDATE) AND foup.FPLDLIV <=trunc(SYSDATE+INTERVAL '90' day)    )

    , tmp_ranges AS (
    SELECT t.ITEM, t.STORE
         , oh.MIN_ORDER_DATE, oh.MIN_DELIVERY_DATE, oh.MAX_DELIVERY_DATE, oh.cnt_orders, qty_in_transit
         , ca.FPLDRAM AS plan_ord_date, ca.FPLDLIV AS plan_deliv_date
         , COALESCE(to_char(oh.SUPPLIER), ca.FOUCNUF) AS SUPPLIER, ca.FOULIBL
         , t.STOCK_STORE, ARACINR, ARACINL, ARACFIN
    FROM temp_base t
    LEFT JOIN tmp_orders oh ON oh.ITEM =t.item AND oh.store =t.store
    LEFT JOIN (SELECT DISTINCT * FROM tmp_calendars WHERE rn=1) ca ON ca.FPLCFIN =t.ARACFIN AND ca.store =t.store
    )

    -------прогнозы
    , tmp_gold_forecast AS (
    SELECT ARACINL, ITEM, STORE, PLAN_ORD_DATE, PLAN_DELIV_DATE--,cas.CASDATE--, atl.ARUFDSID
        , TO_NUMBER(coalesce(ROUND(AVG(
                                        CASE cas.CASDATE -sem.SEMDDEB
                                            WHEN 0 THEN COALESCE(REPCJ01,0.142857142857143)
                                            WHEN 1 THEN COALESCE(REPCJ02,0.142857142857143)
                                            WHEN 2 THEN COALESCE(REPCJ03,0.142857142857143)
                                            WHEN 3 THEN COALESCE(REPCJ04,0.142857142857143)
                                            WHEN 4 THEN COALESCE(REPCJ05,0.142857142857143)
                                            WHEN 5 THEN COALESCE(REPCJ06,0.142857142857143)
                                            ELSE REPCJ07 END  *coalesce(pvdcorr, coalesce(pvdcalc, 0))    )
                                   , 3 ), 0) ) as avg_daily_frcst
        , TO_NUMBER(coalesce(ROUND(sum(
                                        CASE cas.CASDATE -sem.SEMDDEB
                                            WHEN 0 THEN COALESCE(REPCJ01,0.142857142857143)
                                            WHEN 1 THEN COALESCE(REPCJ02,0.142857142857143)
                                            WHEN 2 THEN COALESCE(REPCJ03,0.142857142857143)
                                            WHEN 3 THEN COALESCE(REPCJ04,0.142857142857143)
                                            WHEN 4 THEN COALESCE(REPCJ05,0.142857142857143)
                                            WHEN 5 THEN COALESCE(REPCJ06,0.142857142857143)
                                            ELSE REPCJ07 END  *coalesce(pvdcorr, coalesce(pvdcalc, 0))    )
                                   , 3 ), 0) ) as sum_frsct
    FROM tmp_ranges tgr
    LEFT JOIN ( SELECT LFOROBID, LFOCINL, max(LFOCINLR) AS inlr
                FROM refgwr.lienfor WHERE LFOTLIEN in (1,0)  ---Замена/Вариант
                GROUP BY LFOROBID, LFOCINL
    ) lf ON lf.LFOROBID =tgr.STORE AND lf.LFOCINL =tgr.ARACINL
    LEFT JOIN refcesh.artul atl ON atl.ARUCINL =coalesce(lf.inlr, tgr.ARACINL)
    LEFT JOIN (SELECT REPFDSID, REPSITE, REPCJ01, REPCJ02, REPCJ03, REPCJ04, REPCJ05, REPCJ06, REPCJ07
                    , row_number() over(PARTITION BY REPFDSID, REPSITE ORDER BY repdmaj desc) rn
               FROM refgwr.fctrep
        ) fr ON fr.REPFDSID =atl.ARUFDSID AND fr.repsite =tgr.STORE AND fr.rn =1
    LEFT JOIN refcesh.calsite cas ON cas.CASSITE =tgr.STORE
                                 AND cas.CASDATE BETWEEN PLAN_ORD_DATE AND PLAN_DELIV_DATE
    LEFT JOIN refgwr.fctsem sem ON cas.CASDATE BETWEEN sem.SEMDDEB AND sem.SEMDFIN
    JOIN refgwr.FCTENTPVH fep ON fep.PVESITE =cas.CASSITE AND fep.PVECINL =coalesce(lf.inlr, atl.ARUCINL)
    JOIN refgwr.FCTDETPVH fdp ON fep.pveid =fdp.PVDEID AND sem.SEMNSEM =fdp.PVDNSEM
    GROUP BY tgr.ARACINL, ITEM, STORE, PLAN_ORD_DATE, PLAN_DELIV_DATE
    )

    -------резервы
    , tmp_gold_reserve AS (
    SELECT tgr.item, tgr.store
            , max(CASE WHEN tgr.QTY_TYPE ='Units' THEN tgr.qty ELSE 0 end) AS reserv_qty
            , max(CASE WHEN tgr.QTY_TYPE ='Days' THEN tgr.qty ELSE 0 end) AS reserv_days
    FROM (
        SELECT t.item, app.APPSITE AS store,
               app.APPTYPE AS r_code, tp1743.TPARLIBL AS reserve_type,
               app.APPDDEB, app.APPDFIN,
               app.APPQTE AS qty,
               tp1737.TPARLIBL AS qty_type
        FROM tmp_ranges t
        JOIN refgwr.artparpre app ON t.ARACINL =app.APPCINL AND t.STORE =app.APPSITE AND SYSDATE>= app.APPDDEB AND SYSDATE<= app.APPDFIN
        JOIN refcesh.tra_parpostes tp1737 ON tp1737.TPARPOST = app.APPUPRE AND tp1737.tpartabl=1737 AND tp1737.LANGUE='RU' AND tp1737.tparcmag=0
        JOIN refcesh.tra_parpostes tp1743 ON tp1743.TPARPOST = app.APPTYPE AND tp1743.tpartabl=1743 AND tp1743.LANGUE='RU' AND tp1743.tparcmag=0
    ) tgr
    GROUP BY tgr.item, tgr.store
    )

    -------текущий сток на складе
    , rms_current_wh_stock AS (
        SELECT ils.item
        	, sum(ils.stock_on_hand) AS fact_stock_wh922   ---фактический/физический сток на складе
        	, sum(ils.stock_on_hand)-sum(TSF_RESERVED_QTY)-sum(non_sellable_qty)  AS stock_wh922   ---свободный сток на складе
          	, sum(TSF_RESERVED_QTY) as reserv_wh
          	, sum(non_sellable_qty) as non_sellable_qty
        FROM item_loc_soh@RMS_APPRO ils
        JOIN wh@RMS_APPRO wh on ils.loc = wh.wh and wh.wh <> wh.physical_wh
        LEFT OUTER JOIN inv_status_qty@RMS_APPRO isq ON isq.item = ils.item AND isq.loc_type = ils.loc_type AND isq.location = ils.loc AND isq.inv_status = 5
        WHERE ils.stock_on_hand <> 0 and ils.item IN (SELECT item FROM tmp_ranges)
        AND  TO_NUMBER(substr(to_char(ils.loc), 1, 3)) = 922
        GROUP BY ils.item
    )

    -------трансфера
    , tmp_transfers AS (
    SELECT td.item, th.TO_LOC AS store
        , sum(decode(td.received_qty, NULL, td.tsf_qty, 0)) AS qty_transfers
     from tsfhead@RMS_APPRO th
     JOIN tsfdetail@RMS_APPRO td ON td.tsf_no = th.tsf_no
     WHERE status ='A' AND TO_LOC_TYPE='S'
        AND td.item IN (SELECT item FROM tmp_ranges)
        AND th.TO_LOC IN (SELECT store FROM tmp_ranges)
     GROUP BY td.item, th.TO_LOC
    )


, temp AS (
    select t.STORE, t.ITEM, t.SUPPLIER
            , (CASE WHEN t.MIN_DELIVERY_DATE IS not NULL THEN t.PLAN_DELIV_DATE - t.MIN_DELIVERY_DATE  -- если ближайшая доставка по заказам больше плановой доставки от сег дня?
                   ELSE t.PLAN_DELIV_DATE - trunc(SYSDATE)
              END) AS D2D1_DIFF_DAYS  ---> разница [датой доставки от текущего дня в будущем]- [между ближайшей датой доставки по существующим заказам]
                                        --	, но если заказов не было , то [дата доставки следующего заказа от текущего дня в будущем]
                                        --- [дата доставки планового заказа от текущего дня]
            , CASE WHEN t.MIN_DELIVERY_DATE IS NULL THEN t.PLAN_DELIV_DATE -trunc(SYSDATE)
                   ELSE t.MIN_DELIVERY_DATE -trunc(SYSDATE)
              END AS D1_DAYS_FROM_NOW
            , round((COALESCE(t.QTY_IN_TRANSIT,0) +COALESCE(qty_transfers,0))/avg_daily_frcst) AS transit_stock_days
            , t.STOCK_STORE
            , whs.stock_wh922
            , whs.fact_stock_wh922
            , whs.reserv_wh
            , whs.non_sellable_qty
            , avg_daily_frcst
            , round((sum(AVG_DAILY_FRCST) over(PARTITION BY t.ITEM)) / (count(t.STORE) over(PARTITION BY t.ITEM) ), 3) AS avg_for_all_stores


            , (CASE WHEN t.MIN_DELIVERY_DATE IS NULL THEN t.PLAN_DELIV_DATE -trunc(SYSDATE)
                   ELSE t.MIN_DELIVERY_DATE -trunc(SYSDATE)
               END) *avg_daily_frcst AS FRCST_FROM_NOW_TO_D1
            , COALESCE(tsf.qty_transfers,0) AS qty_transfers
            , t.CNT_ORDERS
            , t.QTY_IN_TRANSIT
            , t.MIN_ORDER_DATE
            , t.MIN_DELIVERY_DATE
            , t.MAX_DELIVERY_DATE
            , t.PLAN_ORD_DATE
            , t.PLAN_DELIV_DATE
            , rs.reserv_days *avg_daily_frcst +rs.reserv_qty AS RESERVS
            , rs.reserv_qty, rs.reserv_days
            , t.FOULIBL
            , case when isc.round_lvl = 'EA' then 1
                   when isc.round_lvl = 'I' then isc.INNER_PACK_SIZE
                   when isc.round_lvl = 'C' then isc.SUPP_PACK_SIZE
                   when isc.round_lvl = 'L' then isc.ti*isc.SUPP_PACK_SIZE
                   when isc.round_lvl = 'P' then isc.ti*isc.hi*isc.SUPP_PACK_SIZE
                   else 1 end as pcb
            , isc.round_lvl, isc.SUPP_PACK_SIZE
            , isc.supp_pack_size*isc.hi*isc.ti as pal_val
        from tmp_ranges t
        LEFT JOIN rms_current_wh_stock whs ON whs.item =t.item
        LEFT JOIN tmp_gold_reserve rs ON rs.item =t.item AND rs.store =t.store
        LEFT JOIN tmp_gold_forecast frcst ON frcst.item =t.item AND frcst.store =t.store
        LEFT JOIN tmp_transfers tsf ON tsf.item =t.item AND tsf.store =t.store
        LEFT JOIN item_supp_country@RMS_APPRO isc ON isc.item = t.item AND isc.primary_supp_ind = 'Y' AND isc.primary_country_ind = 'Y'
)

SELECT
	 (CASE WHEN t.stock_days <3 AND t.D1_DAYS_FROM_NOW <=3 THEN GREATEST(t.D2D1_DIFF_DAYS -t.transit_stock_days +safety_days,0)
           WHEN t.stock_days <3 AND t.D1_DAYS_FROM_NOW > 3 THEN GREATEST(t.D2D1_DIFF_DAYS -t.transit_stock_days -stock_days_curr,0) +safety_days +(-1)*stock_days
           ELSE 0
      END ) * avg_daily_frcst AS push_qty_raw
    , ceil((CASE WHEN t.stock_days <3 AND t.D1_DAYS_FROM_NOW <=3 THEN GREATEST(t.D2D1_DIFF_DAYS -t.transit_stock_days +safety_days,0)
	        	 WHEN t.stock_days <3 AND t.D1_DAYS_FROM_NOW > 3 THEN GREATEST(t.D2D1_DIFF_DAYS -t.transit_stock_days -stock_days_curr,0) +safety_days +(-1)*stock_days
	        	 ELSE 0
       		END ) * avg_daily_frcst/pcb )*pcb AS push_qty_round
    , t.*
FROM (
	SELECT 2 AS safety_days
 		, round((t.STOCK_STORE -t.RESERVS -t.FRCST_FROM_NOW_TO_D1) /t.avg_daily_frcst) AS stock_days
        , round((t.STOCK_STORE -t.RESERVS) /t.avg_daily_frcst) AS stock_days_curr
        , round(stock_wh922/avg_for_all_stores) AS stockdays_wh922
        , CASE WHEN round(stock_wh922/avg_for_all_stores)  <14 THEN 'Заканчивается сток на РЦ !!!'
        	   WHEN round(stock_wh922/avg_for_all_stores)  <1 THEN 'Закончился сток на РЦ !!!' ELSE '' END alert
		,  t.*
	FROM temp t
) t

"""
