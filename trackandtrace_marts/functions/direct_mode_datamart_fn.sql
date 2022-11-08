--liquibase formatted sql
--changeset 60094988:create:trackandtrace_marts.direct_mode_datamart_fn

CREATE OR REPLACE FUNCTION direct_mode_datamart_fn()
    RETURNS boolean
    LANGUAGE plpgsql
AS $function$
begin

    DROP TABLE IF EXISTS rms_tmp_item_info_uda_pivot_temp;
    DROP TABLE IF EXISTS rms_item_info_temp;
    DROP TABLE IF EXISTS rms_dpac_temp;
    DROP TABLE IF EXISTS rms_shipments_temp;
    DROP TABLE IF EXISTS rms_orders_temp;
    DROP TABLE IF EXISTS rms_invoices_temp;
    DROP TABLE IF exists warehouse_names_temp;

    DROP TABLE IF EXISTS st_temp;
    DROP TABLE IF EXISTS order_ship_temp;

    DROP TABLE IF EXISTS goods_declaration_invoices;
    DROP TABLE IF EXISTS goods_declarations_temp;
    DROP TABLE IF EXISTS goods_declarations_statuses;
    DROP TABLE IF EXISTS forwarders_distributions_temp;
    DROP TABLE IF exists invoices_temp;
    DROP TABLE IF exists invoices_split_status;
    DROP TABLE IF exists invoices_split_precalc_status;
    DROP TABLE IF EXISTS precalculated_temp;
    DROP TABLE IF EXISTS temp_all;

    DROP TABLE IF EXISTS direct_pi_temp;
    DROP TABLE IF EXISTS direct_invoice_temp;
    DROP TABLE IF EXISTS direct_arf_temp;
    ---- <RMS ITEM PART> -----

    create temp table rms_tmp_item_info_uda_pivot_temp  AS (
        SELECT a.item,
               max(CASE WHEN a.uda_id = 5 THEN a.uda_value_desc ELSE NULL END) AS gamma,
               max(CASE WHEN a.uda_id = 7 THEN a.uda_value_desc ELSE NULL END) AS mdd,
               max(CASE WHEN a.uda_id = 377 THEN a.uda_value_desc ELSE NULL END) AS brand,
               max(CASE WHEN a.uda_id = 8 THEN a.uda_value_desc ELSE NULL END) AS best_price,
               max(CASE WHEN a.uda_id = 10 THEN a.uda_value_desc ELSE NULL END) AS import_attr,
               max(CASE WHEN a.uda_id = 12 THEN a.uda_value_desc ELSE NULL END) AS flow_type,
               max(CASE WHEN a.uda_id = 21 THEN a.uda_value_desc ELSE NULL END) AS top_1000
        FROM   (
                   select uil.item, uil.uda_id, uv.uda_value_desc
                        , row_number() OVER(PARTITION BY uil.item, uil.uda_id ORDER BY uil.last_update_datetime DESC) AS rn
                   FROM rms_p009qtzb_rms_ods.v_uda_item_lov uil
                            JOIN rms_p009qtzb_rms_ods.v_uda_values uv ON uil.uda_id = uv.uda_id AND uil.uda_value = uv.uda_value AND uil.is_actual='1' AND uv.is_actual='1'
                   WHERE uil.uda_id IN (5, 7, 8, 10, 12, 21, 377)
               ) a
        WHERE a.rn = 1
        GROUP BY a.item
    );

    create temp table rms_item_info_temp as (
        SELECT im.item::text as lm_code
             , im.item_parent as ean_code
             , im.short_desc as lm_name
             , SUBSTR(LPAD(im.dept::VARCHAR ,4,'0'),1,2)::INTEGER as dep_code
             , g.group_name as dep_name
             , im.dept as sub_dep_code
             , deps.dept_name as sub_dep_name
             , pvt.import_attr
             , pvt.flow_type
             , pvt.top_1000
             , pvt.brand
             , pvt.mdd
             , pvt.best_price
             , pvt.gamma
        FROM rms_p009qtzb_rms_ods.v_item_master im
                 INNER JOIN rms_p009qtzb_rms_ods.v_item_supp_country isc ON isc.item = im.item and isc.is_actual='1'
                 INNER JOIN rms_p009qtzb_rms_ods.v_item_supplier item_supplier ON item_supplier.item = im.item
            AND isc.supplier = item_supplier.supplier  and item_supplier.is_actual='1'
                 LEFT JOIN rms_tmp_item_info_uda_pivot_temp pvt on im.item = pvt.item
                 LEFT JOIN rms_p009qtzb_rms_ods.v_deps deps on deps.dept = im.dept and deps.is_actual='1'
                 LEFT JOIN rms_p009qtzb_rms_ods.v_groups g on g.group_no = SUBSTR(LPAD(im.dept::VARCHAR ,4,'0'),1,2)::int and g.is_actual='1'
                 LEFT JOIN rms_p009qtzb_rms_ods.v_item_import_attr import_attr on import_attr.item::bigint=im.item::BIGINT and import_attr.is_actual='1'
        WHERE im.item_number_type = 'ITEM' AND isc.primary_supp_ind = 'Y' AND isc.primary_country_ind = 'Y' AND item_supplier.primary_supp_ind = 'Y' AND im.is_actual='1'
    );

    create temp table rms_dpac_temp as (
        select item as rms_lm_code
             , loc
             , sum(dp.value) as dpac_item
        from rms_p009qtzb_rms_ods.v_xxlm_rms_item_loc_comp_dpac dp
        where dp.loc_type = 'W' AND dp.is_actual = '1'
        group by item, loc);


    ---- <RMS ORDERS & SHIPMENTS PART> -----
    CREATE TEMPORARY TABLE warehouse_names_temp (code INTEGER, name_rus TEXT, name_eng TEXT);
    DO $$ BEGIN
        PERFORM public.rdm('ries_warehouse_names', 'warehouse_names_temp');
    END $$;

    create temp table rms_orders_temp as (
        select oh.order_no::text as order_id
             , oh.vendor_order_no as pi_id
             , oh.APP_DATETIME as pi_date
             , ol.item::text as lm_code
             , oh.supplier as supplier_code
             , sups.sup_name as supplier_name
             , ol.location as v_loc
             , substr(ol.location::text,1,3)::int as loc
             , wn.code || ' ' || wn.name_rus as loc_name
             , oh.written_date::date as order_date
             , CASE
                   WHEN oh.status = 'W' THEN 'Worksheet'
                   WHEN oh.status = 'A' THEN 'Approved'
                   WHEN oh.status = 'S' THEN 'Sent'
                   WHEN oh.status = 'C' THEN 'Closed'
            END as order_status
             , ol.unit_cost as unit_price
             , ol.QTY_ORDERED as order_qty          --  , case when oh.status = 'A' then ol.QTY_ORDERED - ol.QTY_RECEIVED else null end in_transit_qty --, oh.import_country_id
             , ol.QTY_RECEIVED as received_qty
             , ol.QTY_ORDERED * ol.unit_cost as item_amount
             , ol.QTY_RECEIVED * ol.unit_cost as received_amount
             , dpac.dpac_item::numeric
             , ol.QTY_ORDERED * dpac.dpac_item as order_dpac_amount
             , ol.QTY_RECEIVED * dpac.dpac_item as received_dpac_amount
             , it.ean_code
             , it.lm_name
             , it.dep_code
             , it.dep_name
             , it.sub_dep_code
             , it.sub_dep_name
             , it.import_attr
             , it.flow_type
             , it.top_1000
             , it.brand
             , it.mdd
             , it.best_price
             , it.gamma
             , case
                   when coalesce(sup_info.cont20, sup_info.cont40, sup_info.cont40hc, sup_info.contlcl) is not null then 'SEA'
                   when sup_info.conttr is not null then 'TRUCK'
            end as transport_mode
             , cfa.varchar2_1 as supplier_contract_number
             , cfa.date_21 as supplier_contract_date
             , sup_info.attribute1 as status_oc
        from rms_p009qtzb_rms_ods.v_ORDHEAD oh
                 inner join rms_p009qtzb_rms_ods.v_ORDLOC ol on ol.ORDER_NO = oh.ORDER_NO and ol.is_actual='1'
                 left join rms_p009qtzb_rms_ods.v_sups sups ON sups.supplier = oh.supplier and sups.is_actual='1'
                 left join warehouse_names_temp wn on wn.code = substr(ol.location::text,1,3)::int
                 left join rms_dpac_temp dpac on dpac.rms_lm_code = ol.item and ol.location::int= dpac.loc::int
                 left join rms_item_info_temp it on it.lm_code = ol.item
                 left join rms_p009qtzb_rms_ods.v_XXLM_RMS_SUP_INFORM sup_info
                           on sup_info.order_no = oh.order_no
                               and sup_info.pi_no = oh.vendor_order_no
                               and sup_info.is_actual = '1'
                 left join rms_p009qtzb_rms_ods.v_SUPS_CFA_EXT cfa
                           on cfa.supplier = oh.supplier
                               and cfa.group_id = 23
                               and cfa.is_actual = '1'
        where oh.is_actual='1'
          -- Заказы на РЦ потока сток
          AND ol.location < 1000000
          AND (
                ol.location BETWEEN 906000 AND 906198 OR
                ol.location BETWEEN 908000 AND 908198 OR
                ol.location BETWEEN 912000 AND 912198 OR
                ol.location BETWEEN 922000 AND 922198 OR
                ol.location BETWEEN 921000 AND 921198
            )
          -- Не хаб, не bbxd
          AND ol.location NOT IN (922060, 922063, 906113, 912113, 922113, 921113)
          -- Только строки, где есть непринятые количества
          -- Заказы, у которых активно поле Включить заказ. товары
          AND oh.INCLUDE_ON_ORDER_IND = 'Y'
          -- Не заказы HUB, BBXD
          --AND excl.order_no IS NULL
          -- AND ol.QTY_ORDERED > 0
          AND oh.currency_code <> 'RUR'
          AND sups.supplier_parent <> 199
          AND oh.IMPORT_ORDER_IND = 'Y'
    ) ;

    create temp table rms_shipments_temp as (
        select s.order_no::text as order_id --- ID1 rk.order_id
             , s.ASN as rms_invoice_id --номер инвойса   --- ID4
             , sku.item::text as lm_code  --- ID3  rk.lm_code
             , xx.first_receiving_date::date as invoice_upload
             , xx.lm_wh_arrival::date  as receiving_date1 -- Первая приемка
             , s.receive_date::date as receiving_date2  --Вторая приемка
             , s.to_loc::text as to_loc
             , sum(sku.qty_expected) as ship_expected
             , sum(sku.qty_received) as ship_received
             , s.courier as container
        from rms_p009qtzb_rms_ods.v_shipment s
                 inner join rms_p009qtzb_rms_ods.v_shipsku sku ON s.shipment = sku.shipment and to_loc_type ='W' and sku.is_actual ='1'
                 left join rms_p009qtzb_rms_ods.v_xxlm_rms_first_receiving xx ON s.shipment=xx.shipment and xx.is_actual ='1'
        where s.is_actual ='1' and s.to_loc_type ='W' and s.from_loc_type is null
        group by s.order_no,
                 s.ASN,
                 sku.item::text,
                 xx.first_receiving_date::date,
                 xx.lm_wh_arrival::date,
                 s.receive_date::date,
                 s.to_loc,
                 s.courier
    );

    create temp table rms_invoices_temp as (
        select h.order_no::text as order_id
             , h.ext_doc_id as invoice_number
             , h.status
             , h.type
             , h.location
             , h.doc_date::date as invoice_date
             , d.item::text as lm_code
             , d.invoice_qty as invoice_qty
             , d.unit_cost as invoice_price
             , h.currency_code as invoice_curr
             , h.total_cost as invoice_total_amount
        from rms_p009qtzb_rms_ods.v_im_doc_head h
                 inner join rms_p009qtzb_rms_ods.v_xxlm_if_invoice_head ih on ih.doc_number = h.ext_doc_id and ih.is_actual = '1'
                 left join rms_p009qtzb_rms_ods.v_im_invoice_detail d ON d.doc_id = h.doc_id and d.is_actual = '1'
        where h.is_actual = '1' and d.item is not null and ih.loc_type = 'W'
    );

---<DIRECT SUPPLIER PART>---
    create temp table direct_pi_temp as (
        select vdpa.description_english as lm_name_eng,
               vdpa.packaging_type as delivery_unit_ordered_unit_code,
               vdpa.manufacturer_address,
               vdpa.tn_ved_code as hs_code,
               vdpa.lm_code,
               vdpa.ean_code,
               vdp.order_number,
               vdp.number,
               vdp.incoterms,
               vdp.qty_of_container_type_20,
               vdp.qty_of_container_type_40,
               vdp.qty_of_container_type_40hc,
               vdp.volume_lcl,
               CASE
                   WHEN (vdp.qty_of_container_type_20 IS NOT NULL or vdp.qty_of_container_type_40 IS NOT NULL or vdp.qty_of_container_type_40hc IS NOT NULL or vdp.volume_lcl IS NOT NULL ) THEN 'SEA'
                   ELSE 'TRUCK'
                   END as transport,
               vdp.qty_of_standard_truck,
               vdp.port_of_discharge,
               vdp.etd as etd_oc,
               vdp.eta as eta_oc
        from ries_entities_ods.v_direct_pi_articles vdpa
                 left join ries_entities_ods.v_direct_pi vdp on vdpa.pi_number = vdp.number and vdp.is_actual = '1'
        where vdpa.is_actual = '1'
    );

    create temp table direct_invoice_temp as (
        select vdia.purchase_price as price,
               vdia.currency,
               vdia.lm_code,
               vdia.tn_ved_code as hs_code,
               vdia.qty_pcs,
               vdia.qty_pkgs,
               vdi.pi_number,
               vdi.total_qty_peaces,
               vdi.total_qty_places,
               vdi.number as invoice_number,
               vdi.delivery_terms as incoterms,
               vdi.date,
               vdi.container_or_truck,
               CASE
                   WHEN vdi.container_number IS NOT NULL THEN vdi.container_number
                   ELSE vdi.truck_number
                   END            as actual_container,
               vdi.container_type,
               vdi.total_volume as total_volume,
               vdi.port_of_discharge,
               vdi.place_of_loading,
               vdi.total_amount,
               vdi.total_weight_brutto,
               vdi.total_weight_netto,
               vdi.total_pallets,
               vdi.direction
        from ries_entities_ods.v_direct_invoices_articles vdia
                 left join ries_entities_ods.v_direct_invoices vdi on vdia.invoice_id = vdi.id and vdi.is_actual = '1'
        where vdia.is_actual = '1'
    );

    create temp table direct_arf_temp as (
        select vaa.hs_rus_code,
               vaa.lm_code,
               vaa.adeo_code,
               vaa.cert_obligation,
               vaa.document_of_conformity_type,
               vaa.technical_information,
               vaa.serial_doc_number,
               vaa.pi_number,
               CASE
                   WHEN vaa.samples_amount > 0 THEN 'YES'
                   ELSE 'NO'
                   END     as samples_required,
               vaa.samples_amount,
               vaa.country_of_origin_rus
        from ries_entities_ods.v_arf_articles as vaa
        where vaa.is_actual = '1'
    );





--- <SHIPMENT TRACKING PART> -------

    create temp table st_temp AS (
        select ship."orderConfirmationOrProformaInvoiceNumber"    as st_oc_id
             , ship."invoiceNumber"    as  st_invoice_id
             , ship."actualContainerNumber"    as  actual_container
             , ship."containerNumber" as   st_container_id
             , ship.number as  swb
             , ship."modifiedContainerNumber"  as  modified_container
             , ship."loadingType"  as  loading_type
             , containers.type as container_type
             , ship."forwarderPlanningEstimationTimeOfArrivalCustoms"  as  custom_in_plan
             , ship."destinationRailwayStation"    as  destination_rail_station
             , ship."truckArrivalAtPortOfLoadingInFollowUp"    as  truck_arr_pol_fact
             , ship."realDateOfDeliveryAtPortOfLoadingInFollowUp"  as  rdd_pol
             , ship."planningEstimationTimeOfArrivalOnPortOfDischargeInFollowUp"   as  etd_pod_plan_fwr
             , ship."forwarderFactRealTimeOfArrivalOnPortOfDischarge"  as  rta_pod
             , ship."forwarderPlanningEstimationTimeOfDeliveryOnPortOfDischarge"   as  etd_pod_plan
             , ship."forwarderFactRealDateOfDeliveryOnPortOfDischarge" as  rdd_pod
             , ship."realDateOfDeliveryOnRailStation"  as  rdd_rail_station
             , ship."factDateOfDropOff"    as  drop_off
             , ship."realTimeOfArrivalOnRailwayStation"    as  rta_rail_station
             , ship."planningEstimationTimeOfArrivalOfWarehouse"   as  arrival_wh_plan
             , ship."factActualTimeOfArrivalOfWarehouse"   as  arrival_wh_fact
             , ship."forwardersComment"    as  forwarder_comments
             , ship."transportUnit"    as  tu
             , ship."forwarder"    as  release_to
             , ship."modifiedForwarder"    as  release_to_modified
             , ship."transportTypeFromPortOfDischarge" as  transport_from_pod
             , ship."percentageOfLoading"  as  loading_percent
             , ship."forwarderApplicationCreationDate" as  fwr_appl_create_date
             , ship."forwarderApplicationConfirmationDate" as  fwr_appl_confirm_date
             , ship."customsTerminal"  as  customs_terminal
             , ship."brokers"  as  customs_broker
             , ship."brokersComment"   as  comments_broker
             , ship."customsIn"    as  custom_in_fact
             , ship."importCoordinator"    as  import_specialist
             , ship."customsSpecialist"    as  cd_specialist
             , ship."certificationSpecialist"  as  certification_specialist
             , ship."portOfDischarge" as port_of_discharge
             , ship."planningEstimationTimeOfDischargeFromPortOfLoading" as etd_pol_plan
             , ship."seaLineForwarder" as sea_line_forwarder
             , ship."portOfLoading" as port_of_loading
             --, ship."transportMode" as transport_mode
             , null as transport_mode
        from ries_report_ods.v_shipments ship
                 left join ries_report_ods.v_containers containers on ship."actualContainerNumber"=containers.number and containers.is_actual ='1'
        where ship.is_actual ='1'
    );

----< goods declarations >-----

    CREATE TEMPORARY TABLE goods_declarations_statuses (status_code INTEGER, description_rus TEXT, description_eng TEXT);
    DO $$ BEGIN
        PERFORM public.rdm('ries_goods_declaration_statuses_descriptions', 'goods_declarations_statuses');
    END $$;

    create temp table goods_declaration_invoices as (
        select
            good_number,
            goods_declaration_number,
            array_agg(docs.number) as invoice_numbers
        from goodsdeclarations_ods.v_goods_declaration_good_documents docs
        where docs.type = '04021'
          and docs.is_actual = '1'
        group by good_number, goods_declaration_number
    );

    create temp table goods_declarations_temp as (
        select distinct declarations.number as declaration_number,
                        declarations.procedure as declaration_procedure,
                        declarations.status_code as declaration_status_code,
                        declarations.status_code || ' ' || declaration_statuses.description_rus as declaration_status,
                        declarations.status_timestamp as declaration_status_date,
                        CASE WHEN declarations.status_code IN ('10','12') THEN declarations.status_timestamp ELSE null
                            END as declaration_cus_out,
                        declarations.modify_timestamp as declaration_modifiy,
                        declarations.registration_date as declaration_registration_date,
                        goods.good_number as good_number,
                        goods.tnved_code as good_tnved_code,
                        goods.status_code as good_status_code,
                        goods.status_code || ' ' || good_statuses.description_rus as good_status,
                        goods.status_timestamp as good_status_timestamp,
                        goods.description as good_description,
                        descriptions.adeo_code as cd_adeo_code,
                        descriptions.ean_code as cd_ean_code,
                        descriptions.quantity as article_quantity,
                        descriptions.unit as article_unit,
                        descriptions.technical_description as article_description,
                        descriptions.manufacturer as article_manufacturer,
                        docs.invoice_numbers
        from goodsdeclarations_ods.v_goods_declarations declarations
                 left join goodsdeclarations_ods.v_goods_declaration_goods goods on  declarations.number = goods.goods_declaration_number
            and goods.is_actual = '1'
                 left join goodsdeclarations_ods.v_goods_declaration_good_descriptions descriptions on  descriptions.good_number = goods.good_number
            and descriptions.goods_declaration_number = goods.goods_declaration_number and descriptions.is_actual = '1'
                 left join goods_declaration_invoices docs on  docs.good_number = goods.good_number
            and docs.goods_declaration_number = goods.goods_declaration_number
                 left join goods_declarations_statuses declaration_statuses on declarations.status_code = declaration_statuses.status_code::text
                 left join goods_declarations_statuses good_statuses on goods.status_code = good_statuses.status_code::text
        where declarations.is_actual='1'
    );

-----< forwarders_distributions >-------

    create temp table forwarders_distributions_temp as (
        select
            order_confirmation_number,
            forwarder,
            etd_pol_plan,
            port_of_discharge
        from ries_report_ods.v_forwarders_distributions fd
        where fd.is_actual = '1'
    );


-- precalculated fieldt hat are used in kpi formulas

    create temp table precalculated_temp as (
        select rms_ord.order_id  -- номер заказа
             , rms_ord.pi_id as pi_number  -- oc заказа
             , rms_ord.lm_code
             , rms_ships.container
             , rms_ships.rms_invoice_id as invoice_number
             , rms_ships.to_loc::int as loc
             , coalesce(st.transport_mode, dpt.transport, rms_ord.transport_mode) as transport
             , coalesce(st.port_of_discharge, fd.port_of_discharge, dpt.port_of_discharge , dit.port_of_discharge) as pod
        from rms_orders_temp rms_ord
                 left join rms_shipments_temp rms_ships
                           on rms_ships.order_id = rms_ord.order_id
                               and rms_ships.lm_code = rms_ord.lm_code
                               and rms_ships.to_loc::int = rms_ord.loc::int
                 left join st_temp st
                           on st.st_oc_id = rms_ord.pi_id
                               and st.actual_container = rms_ships.container
                               and coalesce(st.st_invoice_id,'') = coalesce(rms_ships.rms_invoice_id,'')
                 left join forwarders_distributions_temp fd on fd.order_confirmation_number = rms_ord.pi_id
                 left join direct_pi_temp dpt on rms_ord.lm_code = dpt.lm_code and rms_ord.pi_id = dpt.number
                 left join direct_invoice_temp dit on rms_ord.lm_code = dit.lm_code and st.st_invoice_id = dit.invoice_number
    );

-----< invoice split >-------

    create temp table invoices_temp as (
        select  oc_id
             , container_number
             , invoice_number
             , max(invoice_status) as invoice_status
        from
            (
                select   dir_invoices.pi_number as oc_id
                     , dir_invoices.invoice_number as invoice_number
                     , dir_invoices.actual_container as container_number
                     , 1 as invoice_status
                from direct_invoice_temp dir_invoices
                union
                select  invoice_entity.oc_number as oc_id
                     , invoice_entity.container_number
                     , invoice_entity.invoice_number
                     , case when invoice_entity.invoice_status = 'DELETED' then 2 else 1 end as invoice_status
                from  ries_entities_ods.v_invoices invoice_entity
            ) as invoices
        where invoices.invoice_number is not null
        group by  invoices.oc_id
               , invoices.container_number
               , invoices.invoice_number
    );


    create temp table invoices_split_precalc_status as (
        select  oc_id
             , container_number
             , case when invoice_count > 1 then 3 else 1 end as invoice_status
        from
            (
                select  invoices_temp.oc_id
                     , invoices_temp.container_number
                     , count(*) as invoice_count
                from  invoices_temp
                group by  invoices_temp.oc_id
                       , invoices_temp.container_number
            ) as invoices_count
    );

    create temp table invoices_split_status as (
        select  inv.oc_id
             , inv.container_number
             , inv.invoice_number
             , case when inv_stat.invoice_status = 3 then 'Splitted\Replaced\Additonal' else case when inv.invoice_status = 2  then 'Deleted' else 'Regular' end  end as invoice_status
        from invoices_temp as inv
                 left join invoices_split_precalc_status as inv_stat
                           on inv.oc_id = inv_stat.oc_id and inv.container_number = inv_stat.container_number

    );

    create temp table temp_all as (
        select
            'Direct' as supplier_mode
             , dit.direction as direction
             , rms_ord.order_id  -- номер заказа
             , rms_ord.order_date
             , rms_ord.pi_id as pi_number  -- oc заказа
             , null as priority -- rk.priority
             , rms_ord.pi_date
             , 'No possibility to track' as status_global
             , null as adeo_code
             , rms_ord.lm_code
             , rms_ord.ean_code
             , rms_ord.order_status as rms_order_status
             , rms_ord.supplier_contract_number
             , rms_ord.supplier_contract_date
             , dpt.etd_oc --rk.etd_oc
             , dpt.eta_oc -- rk.eta_oc
             , coalesce(st.port_of_loading, dit.place_of_loading) as pol--rk.port_of_loading
             , coalesce(dit.incoterms, dpt.incoterms) as incoterms -- rk.incoterms
             , null as season_validity_date_from-- rk.season_validity_date_from
             , null as season_validity_date_to-- rk.season_validity_date_to
             , sum(rms_ord.item_amount::decimal) over(partition by rms_ord.order_id, rms_ord.lm_code) as order_amount-- rk.oc_total_amount
             , rms_ord.item_amount as item_amount-- rk.article_total_amount
             , dpt.manufacturer_address-- rk.manufacturer
             , dpt.lm_name_eng -- rk.desc_eng
             , dat.country_of_origin_rus as country
             , null as ries_order_qty-- rk.ries_order_qty
             , dat.cert_obligation as under_certification-- rk.under_certification
             , dat.samples_amount as samples_qty-- rk.samples_qty
             , dat.samples_required -- rk.samples_required
             , dat.serial_doc_number as doc_serial_id-- rk.doc_serial_id
             , null as doc_serial_valid-- rk.doc_serial_valid
             , dat.document_of_conformity_type as doc_type-- rk.doc_type
             , null as doc_batch_issue-- rk.doc_batch_issue
             , null as doc_serial_expiry-- rk.doc_serial_expiry
             , null as doc_batch_id-- rk.doc_batch_id
             , dat.technical_information as tech_regulation-- rk.tech_regulation regexp_replace(tech_regulation, '[\[\]]','', 'g')
             , null as awb_or_container_samples_id-- rk.awb_or_container_samples_id
             , null as samples_pi_id-- rk.samples_pi_id
             , null as initial_pi_sending_date-- rk.initial_pi_sending_date
             , null as deadline_pi_sending_date-- rk.deadline_pi_sending_date
             , null as sample_sending_way-- rk.sample_sending_way
             , null as sample_sending_date-- rk.sample_sending_date
             , null as deadline_sample_sending_date-- rk.deadline_sample_sending_date
             , null as sample_sending_kpi-- rk.sample_sending_kpi
             , null as reason_sample_sending_late-- rk.reason_sample_sending_late
             , null as sample_sending_delay-- rk.sample_sending_delay
             , null as kalypso_oc_id-- rk.kalypso_oc_id
             , null as kalypso_order_id-- rk.kalypso_order_id
             , null as K_rms_order_id-- rk.K_rms_order_id
             , null as adeo_order_id-- rk.kalypso_adeo_order_id

             , null as container_number1-- rk.container_id1 -- shipped_container_number
             , null as container_number2-- rk.container_id2 -- delivered_container_number
             , null as kalypso_adeo_code-- rk.kalypso_adeo_code
             , null as adeo_dep_name-- rk.adeo_dep_name
             , dpt.delivery_unit_ordered_unit_code-- rk.delivery_unit_ordered_unit_code
             , null as delivery_type-- rk.delivery_type
             , null as production_diff-- rk.production_diff
             , null as purchasing_incoterm_code-- rk.purchasing_incoterm_code
             , null as purchasing_incoterm_city_code-- rk.purchasing_incoterm_city_code
             , null as country_of_loading_code-- rk.country_of_loading_code
             , null as country_of_loading_name-- rk.country_of_loading_name
             , null as city_of_discharge_code-- rk.city_of_discharge_code
             , null as city_of_discharge_name-- rk.city_of_discharge_name
             , null as status_kalypso-- rk.status_kalypso
             , null as shipped_container_number-- rk.shipped_container_number
             , null as delivered_container_number-- rk.delivered_container_number
             , null as shipped_qty-- rk.shipped_information_number_of_units
             , null as to_be_shipped_information_number_of_units-- rk.to_be_shipped_information_number_of_units
             , null as to_be_shipped_information_volume-- rk.to_be_shipped_information_volume
             , null as shipped_information_volume-- rk.shipped_information_volume
             , null as etd_pod_updated-- rk.updated_delivery_date
             , coalesce(st.etd_pol_plan, fd.etd_pol_plan) as etd_pol_plan
             , null as comments_kalypso--rk.comments
             , null as eta_confirmed--rk.confirmed_delivery_date
             , null as etd_confirmed--rk.confirmed_shipping_date
             , null as ship_release_date--rk.shipment_release_date

             , null as container_type_1st_leg--rk.container_type_1st_leg
             , null as loading_type_1st_leg--rk.loading_type_1st_leg
             , null as container_type_2nd_leg--rk.container_type_2nd_leg
             , null as loading_type_2nd_leg--rk.loading_type_2nd_leg
             , null as forwarder_id--rk.forwarder_id
             , st.sea_line_forwarder as main_transport_company_id--rk.main_transport_company_id
             , null as bu_name--rk.bu_name

             , st.st_oc_id
             , st.st_invoice_id
             , coalesce(st.actual_container, rms_ships.container, dit.actual_container) as actual_container
             , st.st_container_id
             , st.swb
             , st.modified_container
             , st.loading_type
             , coalesce(st.container_type, dit.container_type) as container_type
             , st.custom_in_plan
             , st.destination_rail_station
             , st.truck_arr_pol_fact
             , st.rdd_pol
             , st.etd_pod_plan_fwr
             , st.rta_pod
             , st.etd_pod_plan
             , st.rdd_pod
             , st.rdd_rail_station
             , st.drop_off
             , st.rta_rail_station
             , st.arrival_wh_plan
             , st.arrival_wh_fact
             , st.forwarder_comments
             , st.tu
             , coalesce(st.release_to, fd.forwarder) as release_to
             , st.release_to_modified
             , st.transport_from_pod
             , cast(st.loading_percent as numeric) as loading_percent
             , st.fwr_appl_create_date
             , st.fwr_appl_confirm_date
             , st.customs_terminal
             , st.customs_broker
             , st.comments_broker
             , st.custom_in_fact
             , st.import_specialist
             , st.cd_specialist
             , st.certification_specialist
             , st.port_of_discharge
             , coalesce(precalc.transport, dit.container_or_truck) as transport

             , rms_ships.rms_invoice_id
             , rms_ord.supplier_code
             , rms_ord.supplier_name
             , rms_ord.dpac_item
             , rms_ships.invoice_upload as status_invoice
             , rms_ships.receiving_date1
             , rms_ships.receiving_date2
             , rms_ships.ship_expected
             , rms_ships.ship_received as received_qty
             , rms_ord.v_loc
             , rms_ord.loc
             , rms_ord.loc_name as wh
             , rms_ord.unit_price
             , rms_ord.order_qty
             --  , rms_ord.received_qty
             , rms_ord.order_dpac_amount
             , rms_ord.received_dpac_amount
             --, rms_ord.order_amount
             , rms_ord.received_amount
             , coalesce(rms_ships.rms_invoice_id, dit.invoice_number) as invoice_number
             , coalesce(rms_inv.invoice_date, dit.date) as invoice_date

             , cd.declaration_number as cd_number
             , cd.declaration_procedure as cd_proc
             , cd.declaration_status_code
             , cd.declaration_status as cd_status
             , cd.declaration_status_date as cd_status_date
             , cd.declaration_cus_out as cd_out
             , cd.declaration_modifiy
             , cd.declaration_registration_date as cd_date
             , cd.good_number as cd_good_id
             , cd.good_tnved_code as cn_code
             , cd.good_status_code
             , cd.good_status as cd_good_status
             , cd.good_status_timestamp as cd_good_status_date
             , cd.good_description as cd_good_desc
             , cd.cd_adeo_code
             , cd.article_quantity as cd_lm_qty
             , cd.article_unit as cd_good_unit_type
             , cd.article_description as cd_lm_desc
             , cd.article_manufacturer as cd_lm_manufacturer
             , cd.invoice_numbers

             , rms_ord.lm_name as lm_name_ru
             , rms_ord.dep_code
             , rms_ord.dep_name
             , rms_ord.sub_dep_code
             , rms_ord.sub_dep_name
             , rms_ord.import_attr
             , rms_ord.flow_type
             , rms_ord.top_1000
             , rms_ord.brand
             , rms_ord.mdd
             , rms_ord.best_price
             , rms_ord.gamma
             , rms_ord.status_oc

             , null as docs_invoice_id--ship_docs.docs_invoice_id
             , dit.total_volume as invoice_total_volume
             , coalesce(rms_inv.invoice_qty, dit.qty_pcs) as invoice_qty
             , dit.qty_pkgs as invoice_qty_pkgs
             , coalesce(rms_inv.invoice_price, dit.price) as invoice_price
             , coalesce(rms_inv.invoice_curr, dit.currency) as invoice_curr
             , coalesce(rms_inv.invoice_total_amount, dit.total_amount) as invoice_total_amount
             , dit.total_weight_netto as invoice_net_weight
             , dit.total_weight_brutto as invoice_gross_weight
             , dit.total_pallets as invoice_pallets
             , null as letter_of_credit
             , dit.hs_code as invoice_hs_code
             , precalc.pod
             , case
                   when precalc.transport  ='SEA' and precalc.pod = 'VOSTOCHNIY, PORT' and cd.declaration_status_date is not null
                       then date_part('day', cd.declaration_status_date - cd.declaration_registration_date - interval '4 days')
                   when precalc.transport ='SEA' and precalc.pod = 'VOSTOCHNIY, PORT' and cd.declaration_status_date is null
                       then date_part('day', now() - cd.declaration_registration_date - interval '4 days')
                   when precalc.transport <>'SEA' and precalc.pod <> 'VOSTOCHNIY, PORT' and cd.declaration_status_date is not null
                       then date_part('day', cd.declaration_status_date - cd.declaration_registration_date - interval '2 days')
                   when precalc.transport <>'SEA' and precalc.pod <> 'VOSTOCHNIY, PORT' and cd.declaration_status_date is null
                       then date_part('day', now() - cd.declaration_registration_date - interval '2 days')
            end as date_diff
             , null as prod_deviation
             , null as ship_deviation

             ----POD arr deviation---  ST & RiesKalypso
             , case when st.rta_pod IS  null and dpt.eta_oc is not null and  now()::date - dpt.eta_oc <= 0 THEN NULL
                    when st.rta_pod IS  null and dpt.eta_oc is not null and  now()::date - dpt.eta_oc > 0 THEN  now()::date - dpt.eta_oc
                    when st.rta_pod IS not null and dpt.eta_oc is not null THEN  st.rta_pod - dpt.eta_oc
            end as pod_arr_deviation
             --
             ---Customs In Delay days----------  ST & CD
             , case
                   when st.custom_in_fact::date IS NULL and cd.declaration_registration_date IS NOT NULl
                       then null ----'Cus In missing in ST'
                   when st.custom_in_fact::date IS NULL and cd.declaration_registration_date IS null
                       then null
                   when st.custom_in_fact::date IS not NULL and cd.declaration_registration_date IS NOT NULl
                       then (cd.declaration_registration_date  - st.custom_in_fact::date)
                   when st.custom_in_fact::date IS not NULL and cd.declaration_registration_date IS NULl
                       then (now()::date - st.custom_in_fact::date )
            end as custom_in_delay_days
             -----arrival wh deviation------  ST
             , case when  st.arrival_wh_plan IS NOT NULL and st.arrival_wh_fact IS NOT NULL and st.arrival_wh_plan is not null then  st.arrival_wh_fact::date - st.arrival_wh_plan::date
                    when  st.arrival_wh_plan IS NOT NULL and st.arrival_wh_fact IS NULL and st.arrival_wh_plan is not null and  now()::date- st.arrival_wh_plan::date<= 0 then NULL
                    when  st.arrival_wh_plan IS NOT NULL and st.arrival_wh_fact IS NULL and st.arrival_wh_plan is not null and  now()::date- st.arrival_wh_plan::date> 0 then  now()::date-st.arrival_wh_plan
                    when  st.arrival_wh_plan IS null then null
            end as arrival_wh_deviation

             --
             --      -----2nd accep. Delay------   ST & RMS
             , case when  st.arrival_wh_fact IS NOT NULL and rms_ships.receiving_date2 IS NOT NULL and date_part('day',rms_ships.receiving_date2 - st.arrival_wh_fact- interval '2 days') <= 0 then 0
                    when  st.arrival_wh_fact IS NOT NULL and rms_ships.receiving_date2 IS NOT NULL and date_part('day',rms_ships.receiving_date2 - st.arrival_wh_fact- interval '2 days') > 0
                        then date_part('day',rms_ships.receiving_date2 - st.arrival_wh_fact- interval '2 days')
                    when  st.arrival_wh_fact IS NOT NULL and rms_ships.receiving_date2 IS NULL and date_part('day',now()::date - st.arrival_wh_fact- interval '2 days') <= 0 then 0
                    when  st.arrival_wh_fact IS NOT NULL and rms_ships.receiving_date2 IS NULL and date_part('day',now()::date - st.arrival_wh_fact- interval '2 days') > 0
                        then date_part('day',now()::date - st.arrival_wh_fact- interval '2 days')
                    when st.arrival_wh_fact IS NULL then null
            end as second_acceptance_delay
             , null as eta_wh_deviation
             --      ----Invoice split-----
             , inv_split.invoice_status as invoice_status
        from rms_orders_temp rms_ord
                 left join rms_shipments_temp rms_ships
                           on rms_ships.order_id = rms_ord.order_id
                               and rms_ships.lm_code = rms_ord.lm_code
                               and rms_ships.to_loc::int = rms_ord.loc::int
                 left join rms_invoices_temp rms_inv
                           on rms_ships.rms_invoice_id = rms_inv.invoice_number
                               and rms_ships.lm_code = rms_inv.lm_code
                 left join st_temp st
                           on st.st_oc_id = rms_ord.pi_id
                               and st.actual_container = rms_ships.container
                               and coalesce(st.st_invoice_id,'') = coalesce(rms_ships.rms_invoice_id,'')
                 left join goods_declarations_temp cd
                           on  (cd.cd_adeo_code = rms_ord.lm_code or cd.cd_ean_code = rms_ord.ean_code)
                               and rms_ships.rms_invoice_id = ANY(cd.invoice_numbers)
                 left join forwarders_distributions_temp fd on fd.order_confirmation_number = rms_ord.pi_id
                 left join precalculated_temp precalc
                           on precalc.order_id = rms_ord.order_id
                               and precalc.lm_code = rms_ord.lm_code
                               and precalc.loc = rms_ord.loc
                               and coalesce(precalc.container,'') = coalesce(rms_ships.container,'')
                               and coalesce(precalc.invoice_number,'') = coalesce(rms_ships.rms_invoice_id,'')
                 left join direct_pi_temp dpt
                           on dpt.lm_code = rms_ord.lm_code
                               and dpt.number = rms_ord.pi_id
                 left join direct_arf_temp dat
                           on dat.lm_code = rms_ord.lm_code
                               and dat.pi_number = rms_ord.pi_id
                 left join direct_invoice_temp dit
                           on rms_ord.lm_code = dit.lm_code
                               and rms_ships.rms_invoice_id = dit.invoice_number
                 left join invoices_split_status inv_split
                           on inv_split.invoice_number = rms_ships.rms_invoice_id
    );
----< MART >-----
    truncate table direct_mode_datamart;
    insert into direct_mode_datamart (supplier_mode, direction, order_id, order_date, adeo_order_id, pi_number, pi_date, adeo_code, lm_code, ean_code,
                                      lm_name_ru, lm_name_eng, status_global, rms_order_status, eta_wh_calc, second_acceptance_wh_plan, eta_wh_calc_ym, eta_wh_calc_yw,
                                      dep_code, dep_name, dep, sub_dep_code, sub_dep_name, sub_dep, supplier_code, supplier_name, supplier, country,
                                      flow_type, top_1000, mdd, brand, best_price, gamma, priority, loc, order_qty, shipped_qty, received_qty,
                                      remain_to_ship_qty, dpac_item, received_dpac_amount, remain_dpac_amount, item_amount, order_amount, incoterms,
                                      season_validity_date_from, season_validity_date_to, supplier_contract_number, supplier_contract_date, payment_date,
                                      status_kalypso, adeo_dep_name, bu_name, swb, transport,
                                      invoice_status, invoice_number, invoice_date, actual_container, container_number1, container_number2,
                                      modified_container, container_type, container_type_1st_leg, container_type_2nd_leg, loading_type,
                                      loading_type_1st_leg, loading_type_2nd_leg, delivery_unit_volume, delivery_unit_ordered_unit_code,
                                      forwarder_id, main_transport_company_id, pol, pod, destination_rail_station, wh, etd_oc, eta_oc, ship_release_date,
                                      ship_deadline, truck_arr_pol_fact, etd_pol_plan, etd_confirmed, rdd_pol, eta_confirmed, etd_pod_updated,
                                      etd_pod_plan_fwr, eta_pod_plan, rta_pod, transport_from_pod, custom_in_plan, custom_in_fact, etd_pod_plan,
                                      rdd_pod, rta_rail_station, rdd_rail_station, arrival_wh_plan, arrival_wh_fact, drop_off, receiving_date1,
                                      receiving_date2, forwarder_comments, tu, release_to, release_to_modified, loading_percent, customs_terminal,
                                      customs_broker, comments_broker, fwr_appl_create_date, fwr_appl_confirm_date, invoice_total_volume,
                                      invoice_qty, invoice_qty_pkgs, invoice_price, invoice_curr, invoice_total_amount, invoice_net_weight, invoice_gross_weight, invoice_pallets,
                                      letter_of_credit, comments_kalypso, cd_number, cd_proc, cd_date, cd_status, cd_status_date,
                                      cd_out, cd_decision, cd_decision_date, cd_decision_time, cd_modified_date, cd_modified_time, cd_good_id,
                                      cn_code, invoice_hs_code, cd_good_status, cd_good_status_date, cd_good_status_time, cd_good_desc, container_capacity,
                                      good_net_weight, good_gross_weight, cd_customs_type, accrual_basis_payment, customs_rate, customs_tax,
                                      cd_consignment_id, cd_consignment_date, cd_airbill_id, cd_airbill_date, cd_railbill_id, cd_railbill_date,
                                      cd_trnsprtbill_id, cd_trnsprtbill_date, cd_another_trnsprtbill_id, cd_another_trnsprtbill_date, cd_doc_id,
                                      cd_doc_issuedate, cd_lm_qty, cd_good_unit_type, cd_lm_desc, cd_lm_manufacturer, under_certification, doc_type,
                                      tech_regulation, doc_serial_id, doc_serial_issue, doc_serial_expiry, doc_serial_valid, doc_batch_id,
                                      doc_batch_issue, gln, manufacturer_address, production_site_address, samples_required, samples_qty,
                                      samples_pi_id, initial_pi_sending_date, deadline_pi_sending_date, sample_sending_way, sample_sending_date,
                                      awb_or_container_samples_id, deadline_sample_sending_date, reason_sample_sending_late, oc_status, arf_status,
                                      pi_sample_status, current_order_task, doc_conformity_status, doc_ship_status, doc_set_status, status_oc,
                                      status_invoice, import_specialist, appro_specialist, cd_specialist, certification_specialist,
                                      custom_in_calc, prod_kpi, prod_deviation, ship_kpi, ship_deviation, pod_arr_kpi, pod_arr_deviation,
                                      custom_in_kpi, custom_in_delay_days, cd_request_kpi, cd_request_delay, custom_out_kpi, custom_out_delay_days,
                                      sample_sending_kpi, sample_sending_delay, arrival_wh_kpi, arrival_wh_deviation, second_acceptance_kpi,
                                      second_acceptance_delay, eta_wh_kpi, eta_wh_deviation )
    select
        supplier_mode
         , direction
         , order_id
         , order_date
         , adeo_order_id
         , pi_number
         , pi_date
         , adeo_code::text
         , lm_code::text
         , ean_code::text
         , lm_name_ru
         , lm_name_eng
         , status_global  --Статус импорта !
         , rms_order_status
         , null as eta_wh_calc --!
         , null as second_acceptance_wh_plan
         , null as eta_wh_calc_ym
         , null as eta_wh_calc_yw
         , dep_code::int
         , dep_name
         , dep_code || ' ' || dep_name as dep
         , sub_dep_code
         , sub_dep_name
         , sub_dep_code || ' ' || sub_dep_name as sub_dep
         , supplier_code::int
         , supplier_name
         , supplier_code || ' ' || supplier_name as supplier
         , country --- -> ARF
         , flow_type
         , top_1000
         , mdd
         , brand
         , best_price
         , gamma
         , priority
         , v_loc as loc
         , ries_order_qty
         , shipped_qty
         , received_qty
         , sum(shipped_qty::decimal) over(partition by order_id, lm_code) as remain_to_ship_qty  -- в разрезе заказа --, as remain_to_ship_qty    -- в разрезе инвойса
         , dpac_item
         , received_dpac_amount
         , dpac_item * (ries_order_qty::decimal-shipped_qty::decimal) as remain_dpac_amount  -- dpac_item* QTY_REMAIN_SHIP
         , item_amount
         , order_amount      ---> total_amount from order_confirmations  2 релиз
         , incoterms
         , season_validity_date_from
         , season_validity_date_to
         , supplier_contract_number
         , supplier_contract_date
         , null as payment_date
         , status_kalypso
         , adeo_dep_name
         , bu_name
         , swb
         , transport --->change  --!
         , invoice_status ---> Invoice split
         , invoice_number
         , invoice_date
         , CASE WHEN actual_container is not null THEN actual_container
                WHEN actual_container is null AND shipped_container_number is not null THEN shipped_container_number
                WHEN actual_container is null AND shipped_container_number is null AND delivered_container_number is not null
                    THEN delivered_container_number END AS actual_container
         , container_number1  --shipped_container_number
         , container_number2 --delivered_container_number
         , modified_container
         , container_type
         , container_type_1st_leg
         , container_type_2nd_leg
         , loading_type
         , loading_type_1st_leg
         , loading_type_2nd_leg
         , decode(to_be_shipped_information_volume::decimal, 0, shipped_information_volume::decimal, to_be_shipped_information_volume::decimal) as delivery_unit_volume
         , delivery_unit_ordered_unit_code
         , forwarder_id
         , main_transport_company_id
         , pol
         , pod
         , destination_rail_station
         , wh  -- RMS
         , etd_oc::date
         , eta_oc::date
         , ship_release_date::date
         , null as ship_deadline
         , truck_arr_pol_fact
         , etd_pol_plan::date
         , etd_confirmed::date
         , rdd_pol::date
         , eta_confirmed::date
         , etd_pod_updated::date
         , etd_pod_plan_fwr::date
         , coalesce(etd_pod_plan_fwr, etd_pod_updated) AS eta_pod_plan
         , rta_pod::date  --forwarderFactRealTimeOfArrivalOnPortOfDischarge
         , transport_from_pod::text
         , custom_in_plan::date  --forwarderPlanningEstimationTimeOfArrivalCustoms
         , custom_in_fact::date   --customsIn
         , etd_pod_plan::date  --forwarderPlanningEstimationTimeOfDeliveryOnPortOfDischarge
         , rdd_pod::date   --forwarderFactRealDateOfDeliveryOnPortOfDischarge
         , rta_rail_station::date
         , rdd_rail_station::date
         , arrival_wh_plan::date
         , arrival_wh_fact::date
         , drop_off::date
         , receiving_date1::date
         , receiving_date2::date
         , forwarder_comments
         , tu
         , release_to
         , release_to_modified
         , loading_percent
         , customs_terminal
         , customs_broker
         , comments_broker
         , fwr_appl_create_date
         , fwr_appl_confirm_date
         , invoice_total_volume
         , invoice_qty
         , invoice_qty_pkgs
         , invoice_price
         , invoice_curr
         , invoice_total_amount
         , invoice_net_weight
         , invoice_gross_weight
         , invoice_pallets
         , letter_of_credit
         , comments_kalypso
         , cd_number
         , cd_proc
         , cd_date
         , cd_status
         , cd_status_date
         , cd_out
         , null as cd_decision--alta.cd_decision
         , null as cd_decision_date--alta.cd_decision_date
         , null as cd_decision_time--alta.cd_decision_time
         , null as cd_modified_date--alta.cd_modified_date
         , null as cd_modified_time--alta.cd_modified_time
         , cd_good_id
         , cn_code
         , invoice_hs_code
         , cd_good_status
         , cd_good_status_date
         , null as cd_good_status_time--alta.cd_good_status_time
         , cd_good_desc
         , null as container_capacity--alta.container_capacity
         , null as good_net_weight--alta.good_net_weight
         , null as good_gross_weight--alta.good_gross_weight
         , null as cd_customs_type--alta.cd_customs_type
         , null as accrual_basis_payment--alta.accrual_basis_payment
         , null as customs_rate--alta.customs_rate
         , null as customs_tax--alta.customs_tax
         , null as cd_consignment_id--alta.cd_consignment_id
         , null as cd_consignment_date--alta.cd_consignment_date
         , null as cd_airbill_id--alta.cd_airbill_id
         , null as cd_airbill_date--alta.cd_airbill_date
         , null as cd_railbill_id--alta.cd_railbill_id
         , null as cd_railbill_date--alta.cd_railbill_date
         , null as cd_trnsprtbill_id--alta.cd_trnsprtbill_id
         , null as cd_trnsprtbill_date--alta.cd_trnsprtbill_date
         , null as cd_another_trnsprtbill_id--alta.cd_another_trnsprtbill_id
         , null as cd_another_trnsprtbill_date--alta.cd_another_trnsprtbill_date
         , null as cd_doc_id--alta.cd_doc_id
         , null as cd_doc_issuedate--alta.cd_doc_issuedate
         , cd_lm_qty
         , cd_good_unit_type
         , cd_lm_desc
         , cd_lm_manufacturer
         , under_certification
         , doc_type
         , tech_regulation
         , doc_serial_id
         , null as doc_serial_issue
         , doc_serial_expiry
         , doc_serial_valid
         , doc_batch_id
         , doc_batch_issue
         , null as gln
         , manufacturer_address
         , null as production_site_address
         , samples_required
         , samples_qty
         , samples_pi_id
         , initial_pi_sending_date
         , deadline_pi_sending_date
         , sample_sending_way
         , sample_sending_date
         , awb_or_container_samples_id
         , deadline_sample_sending_date
         , reason_sample_sending_late
         , null as oc_status
         , null as arf_status
         , null as pi_sample_status
         , null as current_order_task
         , null as doc_conformity_status
         , null as doc_ship_status
         , null as doc_set_status
         , status_oc
         , status_invoice ---> uploading_invoice
         , import_specialist
         , null as appro_specialist
         , cd_specialist
         , certification_specialist
         , null as custom_in_calc
         ----Production KPI-----
         , null as prod_kpi
         , prod_deviation
         ----Shipping KPI-----
         , null as ship_kpi
         , ship_deviation
         ----POD arr KPI-----
         , case when pod_arr_deviation > 0 THEN 'Delay'
                when pod_arr_deviation <= 0 THEN 'On Time'
        end as  pod_arr_kpi
         , pod_arr_deviation
         ----Custom In KPI-------
         , case when custom_in_delay_days = 0 then 'On time'
                when custom_in_delay_days > 0 then 'Delay'
                when custom_in_delay_days < 0 then 'Discrepancies'
                when custom_in_fact::date IS NULL and cd_date IS NOT NULl then 'Cus In missing in ST'
        end as custom_in_kpi
         , custom_in_delay_days
         -----cd_request_kpi----
         , case when cd_date - rta_pod =0 then 'On Time'
                when cd_date - rta_pod <0 then 'Discrepancies'
                when cd_date - rta_pod >0 then 'Delay'
        end as cd_request_kpi
         , cd_date - rta_pod as cd_request_delay
         --------Customs OUT KPI ----------
         , case when arrival_wh_plan is not null and date_diff <= 0 then 'On Time'
                when arrival_wh_plan is not null and date_diff > 0 then 'Delay'
        end as custom_out_kpi
         --------Customs OUT delay days----------
         , case when arrival_wh_plan is not null and date_diff <= 0 then 0
                when arrival_wh_plan is not null and date_diff > 0 then date_diff
        end as custom_out_delay_days
         , sample_sending_kpi
         , sample_sending_delay::int
         -----arrival wh kpi------
         , case when arrival_wh_deviation <=0 then 'On Time'
                when arrival_wh_deviation > 0 then 'Delay'
        end as arrival_wh_kpi
         , arrival_wh_deviation
         -----2nd accep. KPI------
         , case  when second_acceptance_delay <= 0 then 'On Time'
                 when second_acceptance_delay > 0 then 'Delay'
        end as second_acceptance_kpi
         , second_acceptance_delay
         -----ETA WH KPI----------
         , null as eta_wh_kpi
         , eta_wh_deviation
    FROM temp_all;




    return 0;
end;

$function$;
