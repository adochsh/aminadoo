--liquibase formatted sql
--changeset 60098727:create:trackandtrace_marts.import_datamart_fn

CREATE OR REPLACE FUNCTION import_datamart_fn()
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
begin

DROP TABLE IF EXISTS ries_temp;
DROP TABLE IF EXISTS kalypso_temp;
DROP TABLE IF EXISTS ries_kalypso_temp;
DROP TABLE IF EXISTS tmp_item_info_uda_pivot;
DROP TABLE IF EXISTS item_info;
DROP TABLE IF EXISTS dpac;
DROP TABLE IF EXISTS rms_shipments_temp;
DROP TABLE IF EXISTS rms_orders_temp;
DROP TABLE IF EXISTS rms_only_orders_temp;
DROP TABLE IF EXISTS order_ship_temp;
DROP TABLE IF EXISTS rms_all_temp;
DROP TABLE IF EXISTS st_temp;
DROP TABLE IF EXISTS article_shipment_statuses_temp;
DROP TABLE IF EXISTS goods_declaration_invoices;
DROP TABLE IF EXISTS goods_declarations_temp;
DROP TABLE IF EXISTS goods_declarations_statuses;
DROP TABLE IF EXISTS shipping_docs;
DROP TABLE IF EXISTS ord_tasks;
DROP TABLE IF EXISTS forwarders_distributions;
DROP TABLE IF EXISTS temp_all;
DROP TABLE IF exists invoices_temp;
DROP TABLE IF exists invoices_split_status;
DROP TABLE IF exists warehouse_names;
DROP TABLE IF exists invoices_split_precalc_status_temp ;

 --- <RIES PART> -------
create temp table ries_temp as (
	SELECT ord."number"::text as order_id  -- номер заказа
      , ord.creation_date as order_date
      , oc."number" as oc_id  -- oc заказа
      , ord.priority
      , oc.order_confirmation_date::date as oc_date
      , ord_articles.adeo_code::text
      , ord_articles.lm_code::text
      , ord_articles.ean_code ::text
      , (oc.etd::date) as etd_oc
      , (oc.eta::date) as eta_oc
      , (oc.port_of_loading) as port_of_loading
      , (oc.delivery_terms) as incoterms
      , (oc.season_validity_date_from)::date
      , (oc.season_validity_date_to)::date
      , oc.total_amount as oc_total_amount
      , (ord_articles.manufacturer)
      , (ord_articles.description_eng) as desc_eng
      , (ord_articles.quantity_ordered)::numeric as ries_order_qty
      , (ord_articles.total_amount) as article_total_amount
      , (ord_articles.aggregate_state_cert_obligation) as under_certification
      , (ord_articles.aggregate_state_samples_amount) as samples_qty
      , (ord_articles.aggregate_state_samples_required) as samples_required
      , (ord_articles.aggregate_state_serial_doc_number) as doc_serial_id
      , (ord_articles.aggregate_state_availability_of_a_valid_serial_doc) as doc_serial_valid
      , (ord_articles.aggregate_state_document_of_conformity_type) as doc_type
      , (ord_articles.aggregate_state_date_of_expiry_of_batch_doc) as doc_batch_issue
      , (ord_articles.aggregate_state_date_of_expiry_of_serial_doc) as doc_serial_expiry
      , (ord_articles.aggregate_state_batch_doc_number) as doc_batch_id
      , (ord_articles.aggregate_state_regulation_numbers) as tech_regulation
      , (ord_articles.aggregate_state_sample_state_awb_or_container_no_for_samples_sending) as awb_or_container_samples_id
      , (ord_articles.aggregate_state_sample_state_pi_number) as samples_pi_id
      , (ord_articles.aggregate_state_sample_state_initial_pi_sending_date) as initial_pi_sending_date
      , (ord_articles.aggregate_state_sample_state_deadline_of_pi_sending_date) as deadline_pi_sending_date
      , (ord_articles.aggregate_state_sample_state_sample_sending_way) as sample_sending_way
      , (ord_articles.aggregate_state_sample_state_sample_sending_date) as sample_sending_date
      , (ord_articles.aggregate_state_sample_state_deadline_for_sample_sending) as deadline_sample_sending_date
      , (ord_articles.aggregate_state_sample_state_kpi_of_sample_sending) as sample_sending_kpi
      , (ord_articles.aggregate_state_sample_state_reason_of_sample_sending_late) as reason_sample_sending_late
      , (ord_articles.aggregate_state_sample_state_late_days_of_pi) as sample_sending_delay
  FROM (select "number", creation_date, priority, row_number() over(partition by pk_id  order by version desc) as rn FROM ries_portal_ods.v_order ) ord
     INNER JOIN (select "number"
                    , order_number
                    , order_confirmation_date
                    , etd
                    , eta
                    , port_of_loading
                    , delivery_terms
                    , season_validity_date_from
                    , season_validity_date_to
                    , total_amount
                    , is_active
                    , row_number() over(partition by pk_id  order by version desc) as rn
                 FROM ries_portal_ods.v_order_confirmation
     ) oc on oc.order_number = ord."number"  -- Номер заказа
             and oc.is_active=True and oc.rn=1  -- Активный ОС в текущем заказе
     LEFT JOIN (select order_number
                    , order_confirmation_number
                    , status
                    , adeo_code
                    , lm_code
                    , ean_code
                    , manufacturer
                    , description_eng
                    , quantity_ordered
                    , total_amount
                    , aggregate_state_cert_obligation
                    , aggregate_state_samples_amount
                    , aggregate_state_samples_required
                    , aggregate_state_serial_doc_number
                    , aggregate_state_availability_of_a_valid_serial_doc
                    , aggregate_state_document_of_conformity_type
                    , aggregate_state_date_of_expiry_of_batch_doc
                    , aggregate_state_date_of_expiry_of_serial_doc
                    , aggregate_state_batch_doc_number
                    , aggregate_state_regulation_numbers
                    , aggregate_state_sample_state_awb_or_container_no_for_samples_sending
                    , aggregate_state_sample_state_pi_number
                    , aggregate_state_sample_state_initial_pi_sending_date
                    , aggregate_state_sample_state_deadline_of_pi_sending_date
                    , aggregate_state_sample_state_sample_sending_way
                    , aggregate_state_sample_state_sample_sending_date
                    , aggregate_state_sample_state_deadline_for_sample_sending
                    , aggregate_state_sample_state_kpi_of_sample_sending
                    , aggregate_state_sample_state_reason_of_sample_sending_late
                    , aggregate_state_sample_state_late_days_of_pi
                    , row_number() over(partition by pk_id  order by version desc) as rn
            from ries_portal_ods.v_order_articles ) ord_articles on ord_articles.order_number = ord."number" --Номер заказа
                                              and ord_articles.order_confirmation_number = oc."number"  --Номер ОС
                                              and ord_articles.rn=1 and ord_articles.status<>'ocCancelled'
    WHERE ord.rn=1
  ) ;



 --- <KALYPSO PART> -------

create temp table kalypso_temp AS (
	 select order_articles.bu_contract as kalypso_oc_id
	      , order_articles.contractual_order_number as kalypso_order_id -- номер заказа в калипсо
	      , order_articles.bu_order as K_rms_order_id  -- rms order
	      , orders."number" as kalypso_adeo_order_id    --order Adeo
	      , ship_articles.invoice_number as kalypso_invoice_id  -- invoiceNumber
	      , ship_articles.shipped_container_number as container_id1
	      , ship_articles.delivered_container_number as container_id2
	      , order_articles.international_product_item_id as kalypso_adeo_code
	     ---< contractual_order_articles >----
	      , order_articles.international_department_name as adeo_dep_name
	      , order_articles.delivery_unit_ordered_unit_code  ---
	     ---< contractual_order_transport_legs >----
	      , legs.transport_mode_code as delivery_type
	      , case when legs.transport_mode_code = 'SEA' then 10
	             when legs.transport_mode_code = 'TRUCK' then 7
	             when legs.transport_mode_code = 'TRAIN' then 10
	        end as production_diff
	      , legs.purchasing_incoterm_code
	      , legs.purchasing_incoterm_city_code
	      , legs.country_of_loading_code
	      , legs.country_of_loading_name
	      , legs.city_of_discharge_code
	      , legs.city_of_discharge_name
	     ---< contractual_order_shipment_articles >----
	      , ship_articles.order_line_status_name as status_kalypso
	      , ship_articles.shipped_container_number
	      , ship_articles.delivered_container_number
	      , ship_articles.shipped_information_number_of_units::numeric
	      , ship_articles.to_be_shipped_information_number_of_units::numeric
	      , ship_articles.to_be_shipped_information_volume
	      , ship_articles.shipped_information_volume
	      , ship_articles.updated_delivery_date
	      , ship_articles.updated_shipping_date
	      , ship_articles."comments" as comments
	      , ship_articles.confirmed_delivery_date
	      , ship_articles.confirmed_shipping_date
	      , ship_articles.shipment_release_date
	      , ship_articles.selling_invoice_date
	     ---< contractual_order_containers 1 leg >----
	      , containers1.category_type as container_type_1st_leg         --Тип контейнера на 1 плече
	      , containers1.loading_category as loading_type_1st_leg        --Тип загрузки (CFS/LCL/LM3/FCL)
	     ---< contractual_order_containers 2 leg >----
	      , containers2.category_type as container_type_2nd_leg         --Тип контейнера на 2 плече
	      , containers2.loading_category as loading_type_2nd_leg        --Тип загрузки (CFS/LCL/LM3/FCL)
	      , containers2.forwarder_id            --Экспедитор АДЕО
	      , containers2.main_transport_company_id           --Компания, которая осуществляет перевозку
	     ---< contractual_orders >----
	      , orders.bu_name
	 from ries_report_ods.v_contractual_orders orders
	   left join ries_report_ods.v_contractual_order_shipment_articles ship_articles
	       on ship_articles.contractual_order_number = orders."number" and ship_articles.is_actual='1' and ship_articles.order_line_status_name not in ('CANCELLED', 'NOT CONFIRMED')
	   left join ( select bu_contract, contractual_order_number, bu_order , international_product_item_id ,international_department_name
		  				, max(delivery_unit_ordered_unit_code) as delivery_unit_ordered_unit_code
			       from ries_report_ods.v_contractual_order_articles where is_actual='1'
			       group by bu_contract, contractual_order_number, bu_order, international_product_item_id , international_department_name
			) order_articles on order_articles.contractual_order_number = orders."number"
	                        and order_articles.international_product_item_id = ship_articles.international_product_item_id
	   left join ries_report_ods.v_contractual_order_transport_legs legs on legs.contractual_order_number = orders."number" and legs.is_actual='1'
	   left join ries_report_ods.v_contractual_order_containers containers1
	   		on containers1."number" = ship_articles.shipped_container_number and containers1.is_actual='1'
	            and containers1.contractual_order_number = orders."number"
	   left join ries_report_ods.v_contractual_order_containers containers2 on containers2."number" = ship_articles.delivered_container_number and containers2.is_actual='1'
	                                       and containers2.contractual_order_number = orders."number"
	   where orders.is_actual='1'
  );

 -----<RIES and KALYPSO JOIN>--------

create temp table ries_kalypso_temp as (
	select ries.order_id  -- номер заказа
          , ries.order_date
          , ries.oc_id  -- oc заказа
          , ries.priority
          , ries.oc_date
          , ries.adeo_code
          , ries.lm_code
          , ries.ean_code
          , ries.etd_oc
          , ries.eta_oc
          , ries.port_of_loading
          , ries.incoterms
          , ries.season_validity_date_from
          , ries.season_validity_date_to
          , ries.oc_total_amount
          , ries.manufacturer
          , ries.desc_eng
          , ries.ries_order_qty
          , ries.article_total_amount
          , ries.under_certification
          , ries.samples_qty
          , ries.samples_required
          , ries.doc_serial_id
          , ries.doc_serial_valid
          , ries.doc_type
          , ries.doc_batch_issue
          , ries.doc_serial_expiry
          , ries.doc_batch_id
          , ries.tech_regulation
          , ries.awb_or_container_samples_id
          , ries.samples_pi_id
          , ries.initial_pi_sending_date
          , ries.deadline_pi_sending_date
          , ries.sample_sending_way
          , ries.sample_sending_date
          , ries.deadline_sample_sending_date
          , ries.sample_sending_kpi
          , ries.reason_sample_sending_late
          , ries.sample_sending_delay
          , k.kalypso_oc_id
	      , k.kalypso_order_id
	      , k.K_rms_order_id
	      , k.kalypso_adeo_order_id
	      , k.kalypso_invoice_id
	      , k.container_id1
	      , k.container_id2
	      , k.kalypso_adeo_code
	      , k.adeo_dep_name
	      , k.delivery_unit_ordered_unit_code
	      , k.delivery_type
	      , k.production_diff
	      , k.purchasing_incoterm_code
	      , k.purchasing_incoterm_city_code
	      , k.country_of_loading_code
	      , k.country_of_loading_name
	      , k.city_of_discharge_code
	      , k.city_of_discharge_name
	      , k.status_kalypso
	      , k.shipped_container_number
	      , k.delivered_container_number
	      , k.shipped_information_number_of_units
	      , k.to_be_shipped_information_number_of_units
	      , k.to_be_shipped_information_volume
	      , k.shipped_information_volume
	      , k.updated_delivery_date
	      , k.updated_shipping_date
	      , k.comments
	      , k.confirmed_delivery_date
	      , k.confirmed_shipping_date
	      , k.shipment_release_date
	      , k.selling_invoice_date
	      , k.container_type_1st_leg
	      , k.loading_type_1st_leg
	      , k.container_type_2nd_leg
	      , k.loading_type_2nd_leg
	      , k.forwarder_id
	      , k.main_transport_company_id
	      , k.bu_name
	from ries_temp ries
	  left join kalypso_temp k on ries.oc_id = decode(substring(ries.oc_id,1,5), 'S2009', k.kalypso_order_id, k.kalypso_oc_id)
	  		and k.kalypso_adeo_code =ries.adeo_code
);


 ---- <RMS ITEM PART> -----

create temp table tmp_item_info_uda_pivot  AS (
    SELECT a.item,
         max(CASE WHEN a.uda_id = 5 THEN a.uda_value_desc ELSE NULL END) AS gamma,
         max(CASE WHEN a.uda_id = 7 THEN a.uda_value_desc ELSE NULL END) AS mdd,
         max(CASE WHEN a.uda_id = 377 THEN a.uda_value_desc ELSE NULL END) AS brand,
         max(CASE WHEN a.uda_id = 8 THEN a.uda_value_desc ELSE NULL END) AS best_price,
         max(CASE WHEN a.uda_id = 10 THEN a.uda_value_desc ELSE NULL END) AS import_attr,
         max(CASE WHEN a.uda_id = 12 THEN a.uda_value_desc ELSE NULL END) AS flow_type,
         max(CASE WHEN a.uda_id = 21 THEN a.uda_value_desc ELSE NULL END) AS top1000
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


create temp table item_info as (
    SELECT im.item::text  as rms_im_lm_code
         , import_attr.commodity as rms_adeo_code
         , im.short_desc as lm_name
         , SUBSTR(LPAD(im.dept::VARCHAR ,4,'0'),1,2)::INTEGER as dep_code
         , g.group_name as dep_name
         , im.dept as sub_dep_code
         , deps.dept_name as sub_dep_name
         , pvt.import_attr
         , pvt.flow_type
         , pvt.top1000
         , pvt.brand
         , pvt.mdd
         , pvt.best_price
         , pvt.gamma
    FROM rms_p009qtzb_rms_ods.v_item_master im
        LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_item_supp_country isc ON isc.item = im.item and isc.is_actual='1'
        LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_item_supplier item_supplier ON item_supplier.item = im.item
                                          AND isc.supplier = item_supplier.supplier  and item_supplier.is_actual='1'
        LEFT OUTER JOIN tmp_item_info_uda_pivot pvt on im.item = pvt.item
        LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_deps deps on deps.dept = im.dept and deps.is_actual='1'
        LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_groups g on g.group_no = SUBSTR(LPAD(im.dept::VARCHAR ,4,'0'),1,2)::int and g.is_actual='1'
        LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_item_import_attr import_attr on import_attr.item::bigint=im.item::BIGINT and import_attr.is_actual='1'
    WHERE im.item_number_type = 'ITEM' AND isc.primary_supp_ind = 'Y' AND isc.primary_country_ind = 'Y' AND item_supplier.primary_supp_ind = 'Y' AND im.is_actual='1'
    );


create temp table dpac as (
     select item as rms_lm_code
          , loc
          , sum(dp.value) as dpac_item
     from rms_p009qtzb_rms_ods.v_xxlm_rms_item_loc_comp_dpac dp
     where dp.loc_type = 'W' AND dp.is_actual = '1'
     group by item, loc);


 ---- <RMS ORDERS & SHIPMENTS PART> -----

create temp table rms_shipments_temp as (
    SELECT s.order_no::text as order_id --- ID1 rk.order_id
        , s.ASN as rms_invoice_id --номер инвойса   --- ID4
        , sku.item::text as lm_code  --- ID3  rk.lm_code
        , xx.first_receiving_date::date as invoice_upload
        , xx.lm_wh_arrival::date  as receiving_date1 -- Первая приемка
        , s.receive_date::date as receiving_date2  --Вторая приемка
        , s.to_loc::text as to_loc
        , sum(sku.qty_expected) as ship_expected
        , sum(sku.qty_received) as ship_received
    FROM rms_p009qtzb_rms_ods.v_shipment s
     left join rms_p009qtzb_rms_ods.v_shipsku sku ON s.shipment = sku.shipment and to_loc_type ='W' and sku.is_actual ='1'
     left join rms_p009qtzb_rms_ods.v_xxlm_rms_first_receiving xx ON s.shipment=xx.shipment and xx.is_actual ='1'
    where s.is_actual ='1' and s.to_loc_type ='W' and s.from_loc_type is null
         and s.order_no::text in (select order_id from ries_temp)
    group by s.order_no, s.ASN, sku.item::text, xx.first_receiving_date::date, xx.lm_wh_arrival::date, s.receive_date::date, s.to_loc
);

create temp table rms_only_orders_temp as (
  select oh.order_no::text as order_id
       , oh.supplier as supplier_code
       , sups.sup_name as supplier_name
       , oh.written_date::date as order_date
       , CASE
            WHEN oh.status = 'W' THEN 'Worksheet'
            WHEN oh.status = 'A' THEN 'Approved'
            WHEN oh.status = 'S' THEN 'Sent'
            WHEN oh.status = 'C' THEN 'Closed'
         END as status
  from rms_p009qtzb_rms_ods.v_ORDHEAD oh
   LEFT JOIN rms_p009qtzb_rms_ods.v_sups sups ON sups.supplier = oh.supplier and sups.is_actual='1'
  where oh.is_actual='1'
    and oh.order_no::text in (select order_id from ries_temp)
 ) ;

create temp table rms_orders_temp as (
  select oh.order_no::text as order_id    --- добавить distinct проблема с дубляжами
       , ol.item::text as lm_code
       , oh.supplier as supplier_code
       , sups.sup_name as supplier_name
       , ol.location as v_loc
       , substr(ol.location::text,1,3)::int as loc
       , oh.written_date::date as order_date
       , CASE
            WHEN oh.status = 'W' THEN 'Worksheet'
            WHEN oh.status = 'A' THEN 'Approved'
            WHEN oh.status = 'S' THEN 'Sent'
            WHEN oh.status = 'C' THEN 'Closed'
         END as status
      , ol.unit_cost as unit_price
      , ol.QTY_ORDERED as order_qty          --  , case when oh.status = 'A' then ol.QTY_ORDERED - ol.QTY_RECEIVED else null end in_transit_qty --, oh.import_country_id
      , ol.QTY_RECEIVED as received_qty
  from rms_p009qtzb_rms_ods.v_ORDHEAD oh
   inner join rms_p009qtzb_rms_ods.v_ORDLOC ol on ol.ORDER_NO =oh.ORDER_NO and ol.is_actual='1'
   LEFT OUTER JOIN rms_p009qtzb_rms_ods.v_sups sups ON sups.supplier = oh.supplier and sups.is_actual='1'
  where oh.is_actual='1'
    and oh.order_no::text in (select order_id from ries_temp)
 ) ;

CREATE TEMPORARY TABLE warehouse_names (code INTEGER, name_rus TEXT, name_eng TEXT);
DO $$ BEGIN
    PERFORM public.rdm('ries_warehouse_names', 'warehouse_names');
END $$;

create temp table rms_all_temp as (
 select oh.order_id as rms_order_id--- ID1 rk.order_id
        , oh.lm_code as rms_lm_code
        , oh.supplier_code
        , oh.supplier_name
        , oh.status as order_status
        , dpac.dpac_item::numeric
        , oh.v_loc::int
        , oh.loc::int
        , wn.code || ' ' || wn.name_rus as loc_name
        , oh.order_date as rms_order_date
        , oh.unit_price
        , oh.order_qty
    	, oh.received_qty
    	, oh.order_qty * dpac.dpac_item as order_dpac_amount
    	, oh.received_qty * dpac.dpac_item as received_dpac_amount
	    , oh.order_qty * oh.unit_price as order_amount
	    , oh.received_qty * oh.unit_price as received_amount
 from rms_orders_temp oh
  left join dpac on dpac.rms_lm_code= oh.lm_code and oh.v_loc::int= dpac.loc::int
  left join warehouse_names wn on wn.code = oh.loc
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
      , containers.type as  type
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
   from ries_report_ods.v_shipments ship
   left join ries_report_ods.v_containers containers on ship."actualContainerNumber"=containers.number and containers.is_actual ='1'
   where ship.is_actual ='1'
);

----< article shipment statuses > ----

create temp table article_shipment_statuses_temp as (
  select    order_confirmation_number as asst_oc_id  --номер ОС
        , invoice_number as asst_invoice_id
      , container_number as asst_container_id
      , order_number as asst_order_id  --номер заказа RMS
      , adeo_code as asst_adeo_code
      , eta     --Расчетная дата прибытия на РЦ
      , customs_in  --Расчетная дата прибытия на таможню
      , status  -----> status_global
      , transport_mode  --SEA / TRUCK
      , port_of_discharge
  from ries_report_ods.v_article_shipment_statuses
  where is_actual='1'
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

-----< shipping docs >-------

create temp table shipping_docs as (
	SELECT ship_invoice.invoice_number as docs_invoice_id
		, s.pk_id as shipment_docs_id
		, s."version"
	    , s.state
	    , row_number() over(partition by invoice_number, s.pk_id  order by s.version desc) as rn
	FROM ( select pk_id, "version", state, row_number() over(partition by pk_id  order by version desc) as rn
       from ries_portal_ods.v_shipment) s
		   left join ( select invoice_number, shipment_id, shipment_version,
		               row_number() over(partition by pk_id  order by shipment_version desc) as rn
					   from ries_portal_ods.v_shipment_invoice) ship_invoice on ship_invoice.shipment_id = s.pk_id
															 			   and ship_invoice.shipment_version =s.version
															 			   and ship_invoice.rn=1
	where s.rn=1
);

-----< ord_tasks >-------

create temp table ord_tasks as (
	SELECT  order_number as ord_id
		    , array_agg(name) as task
	FROM (
		  SELECT order_number, name, state, type, row_number() over (partition by pk_id order by updated_at desc, version desc) as rn
		  FROM ries_portal_ods.v_task as tasks
	) tasks
	WHERE tasks.rn = 1 AND tasks.state = 'new' AND tasks.type = 'task'
	GROUP BY order_number
);

-----< forwarders_distributions >-------

create temp table forwarders_distributions as (
	select
		order_confirmation_number,
		forwarder,
		etd_pol_plan
    from ries_report_ods.v_forwarders_distributions fd
	where fd.is_actual = '1'
);

-----< invoice split >-------

create temp table invoices_temp as (
    select  oc_id
            , container_number
            , invoice_number
            , max(invoice_status) as invoice_status
    from
    (
        select  ries.oc_id
                , ship_articles.delivered_container_number as container_number
                , ship_articles.invoice_number
                , 1 as invoice_status
        from  ries_temp ries
        left join ries_report_ods.v_contractual_order_articles order_articles
        on order_articles.is_actual = '1'
        and ries.oc_id = decode(
            substring(ries.oc_id, 1, 5),
            'S2009',
            order_articles.contractual_order_number,
            order_articles.bu_contract
        )
        left join ries_report_ods.v_contractual_order_shipment_articles ship_articles
        on order_articles.international_product_item_id = ship_articles.international_product_item_id
        and ship_articles.is_actual = '1'
        and order_articles.contractual_order_number = ship_articles.contractual_order_number
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

-----< result mart >-------

create temp table temp_all as (
	select
	        'Trade' as supplier_mode
	        , rk.order_id  -- номер заказа
            , rk.order_date
            , rk.oc_id  -- oc заказа
            , rk.priority
            , rk.oc_date
            , rk.adeo_code
            , rk.lm_code
            , rk.ean_code
            , rk.etd_oc
            , rk.eta_oc
            , rk.port_of_loading
            , rk.incoterms
            , rk.season_validity_date_from
            , rk.season_validity_date_to
            , rk.oc_total_amount
            , rk.article_total_amount
            , rk.manufacturer
            , rk.desc_eng
            , rk.ries_order_qty
            , rk.under_certification
            , rk.samples_qty
            , rk.samples_required
            , rk.doc_serial_id
            , rk.doc_serial_valid
            , rk.doc_type
            , rk.doc_batch_issue
            , rk.doc_serial_expiry
            , rk.doc_batch_id
            , rk.tech_regulation
            , rk.awb_or_container_samples_id
            , rk.samples_pi_id
            , rk.initial_pi_sending_date
            , rk.deadline_pi_sending_date
            , rk.sample_sending_way
            , rk.sample_sending_date
            , rk.deadline_sample_sending_date
            , rk.sample_sending_kpi
            , rk.reason_sample_sending_late
            , rk.sample_sending_delay
            , rk.kalypso_oc_id
            , rk.kalypso_order_id
            , rk.K_rms_order_id
            , rk.kalypso_adeo_order_id
            , rk.kalypso_invoice_id
            , rk.container_id1
            , rk.container_id2
            , rk.kalypso_adeo_code
            , rk.adeo_dep_name
            , rk.delivery_unit_ordered_unit_code
            , rk.delivery_type
            , rk.production_diff
            , rk.purchasing_incoterm_code
            , rk.purchasing_incoterm_city_code
            , rk.country_of_loading_code
            , rk.country_of_loading_name
            , rk.city_of_discharge_code
            , rk.city_of_discharge_name
            , rk.status_kalypso
            , rk.shipped_container_number
            , rk.delivered_container_number
            , rk.shipped_information_number_of_units
            , rk.to_be_shipped_information_number_of_units
            , rk.to_be_shipped_information_volume
            , rk.shipped_information_volume
            , rk.updated_delivery_date
            , coalesce(st.etd_pol_plan, fd.etd_pol_plan, rk.updated_shipping_date) as etd_pol_plan
            , rk.comments
            , rk.confirmed_delivery_date
            , rk.confirmed_shipping_date
            , rk.shipment_release_date
            , rk.selling_invoice_date
            , rk.container_type_1st_leg
            , rk.loading_type_1st_leg
            , rk.container_type_2nd_leg
            , rk.loading_type_2nd_leg
            , rk.forwarder_id
            , rk.main_transport_company_id
            , rk.bu_name

			, st.st_oc_id
            , st.st_invoice_id
            , st.actual_container
            , st.st_container_id
            , st.swb
            , st.modified_container
            , st.loading_type
            , st.type
            , st.custom_in_plan
            , st.destination_rail_station
            , st.truck_arr_pol_fact
            , coalesce(st.rdd_pol, rk.confirmed_shipping_date) as rdd_pol
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
            , st.loading_percent
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

			, a_ship_status.asst_oc_id
            , a_ship_status.asst_invoice_id
            , a_ship_status.asst_container_id
            , a_ship_status.asst_order_id
            , a_ship_status.asst_adeo_code
            , a_ship_status.eta
            , a_ship_status.customs_in
            , a_ship_status.status
            , a_ship_status.transport_mode

			, rms_order.order_id as rms_order_id
            , rms_ships.rms_invoice_id
            , rms.rms_lm_code
            , rms_order.supplier_code
            , rms_order.supplier_name
            , rms_order.status as rms_order_status
            , rms.dpac_item
            , rms_ships.invoice_upload
            , rms_ships.receiving_date1
            , rms_ships.receiving_date2
            , rms_ships.ship_expected
            , rms_ships.ship_received
            , rms.v_loc
            , rms.loc
            , rms.loc_name
            , rms_order.order_date as rms_order_date
            , rms.unit_price
            , rms.order_qty
            , rms.received_qty
            , rms.order_dpac_amount
            , rms.received_dpac_amount
            , rms.order_amount
            , rms.received_amount

			, cd.declaration_number
            , cd.declaration_procedure
            , cd.declaration_status_code
            , cd.declaration_status
            , cd.declaration_status_date
            , cd.declaration_cus_out
            , cd.declaration_modifiy
            , cd.declaration_registration_date
            , cd.good_number
            , cd.good_tnved_code
            , cd.good_status_code
            , cd.good_status
            , cd.good_status_timestamp
            , cd.good_description
            , cd.cd_adeo_code
            , cd.article_quantity
            , cd.article_unit
            , cd.article_description
            , cd.article_manufacturer
            , cd.invoice_numbers

			, rms_im.rms_im_lm_code
            , rms_im.rms_adeo_code
            , rms_im.lm_name
            , rms_im.dep_code
            , rms_im.dep_name
            , rms_im.sub_dep_code
            , rms_im.sub_dep_name
            , rms_im.import_attr
            , rms_im.flow_type
            , rms_im.top1000
            , rms_im.brand
            , rms_im.mdd
            , rms_im.best_price
            , rms_im.gamma

			, ship_docs.docs_invoice_id
            , ship_docs.shipment_docs_id
            , ship_docs."version"
            , ship_docs.state

			, ord_tasks.ord_id
            , ord_tasks.task

			, (case when a_ship_status.transport_mode ='TRUCK' then rk.etd_oc -interval '7 days'
	                when a_ship_status.transport_mode ='SEA' then rk.etd_oc -interval '10 days'
	                when a_ship_status.transport_mode ='TRAIN' then rk.etd_oc -interval '10 days'
	          end)::date as ship_deadline
			, case
			       when rms_order.status is null THEN 'No possibility to track'
                   when rms_ships.receiving_date2 is null and rms_order.status = 'Closed' THEN 'Closed'
			       when rms_ships.receiving_date2 is not null THEN 'Closed'
	               when rms_ships.receiving_date1 is not null and rms_ships.receiving_date2 is null
	                    AND now()::date <= (rms_ships.receiving_date1 + interval '2 days' ) THEN '1st Acceptance at WH'
	               when rms_ships.receiving_date1 is null AND rms_ships.receiving_date2 is null AND a_ship_status.status is not null
			       THEN a_ship_status.status  ELSE 'CREATED'
			  end as status_global  --Статус импорта
		    , decode(rms_ships.receiving_date1, null, a_ship_status.eta, rms_ships.receiving_date2) as eta_wh_calc
		    , decode(rms_ships.receiving_date1, null, a_ship_status.eta, rms_ships.receiving_date2) ::date + interval '2 days'  as second_acceptance_wh_plan
			, a_ship_status.transport_mode as transport
			, a_ship_status.port_of_discharge as pod
			, case when a_ship_status.transport_mode  ='SEA' and a_ship_status.port_of_discharge = 'VOSTOCHNIY, PORT' and cd.declaration_status_date is not null
				 		then date_part('day', cd.declaration_status_date - cd.declaration_registration_date -interval '4 days')
				  when a_ship_status.transport_mode ='SEA' and a_ship_status.port_of_discharge = 'VOSTOCHNIY, PORT' and cd.declaration_status_date is null
					 		then date_part('day', now() - cd.declaration_registration_date -interval '4 days')
				  when a_ship_status.transport_mode <>'SEA' and a_ship_status.port_of_discharge <> 'VOSTOCHNIY, PORT' and cd.declaration_status_date is not null
					 		then date_part('day', cd.declaration_status_date - cd.declaration_registration_date -interval '2 days')
				  when a_ship_status.transport_mode <>'SEA' and a_ship_status.port_of_discharge <> 'VOSTOCHNIY, PORT' and cd.declaration_status_date is null
					 		then date_part('day', now() - cd.declaration_registration_date -interval '2 days')
	  		  end as date_diff

			----Production deviation---   RiesKalypso
	         , CASE WHEN rk.shipment_release_date is NOT NULL and rk.etd_oc is NOT NULL THEN rk.shipment_release_date -(rk.etd_oc -production_diff)
	                WHEN rk.shipment_release_date is null and rk.etd_oc is NOT NULL and  now()::date -(rk.etd_oc -production_diff) <= 0 THEN null -- без этого условия юзер будет видеть большие отрицательные цифры
	                WHEN rk.shipment_release_date is null and rk.etd_oc is NOT NULL and  now()::date -(rk.etd_oc -production_diff) > 0 THEN now()::date -(rk.etd_oc -production_diff)
	           end AS prod_deviation

			----Shipping deviation-----  ST & RiesKalypso
	         , CASE WHEN rk.delivery_type in ('SEA','TRAIN') AND rk.confirmed_shipping_date is not null AND rk.etd_oc is NOT NULL  THEN  rk.confirmed_shipping_date- rk.etd_oc
	                WHEN rk.delivery_type ='TRUCK' AND st.rdd_pol is not null AND rk.etd_oc is NOT NULL  THEN  st.rdd_pol -rk.etd_oc
	                WHEN rk.confirmed_shipping_date is null AND st.rdd_pol is null AND  rk.etd_oc is NOT NULL AND now()::date -rk.etd_oc >0 THEN  now()::date -rk.etd_oc
	           end AS ship_deviation

		     ----POD arr deviation---  ST & RiesKalypso
	        , case when st.rta_pod IS  null and rk.eta_oc is not null and  now()::date - rk.eta_oc <= 0 THEN NULL
	               when st.rta_pod IS  null and rk.eta_oc is not null and  now()::date - rk.eta_oc > 0 THEN  now()::date - rk.eta_oc
	               when st.rta_pod IS not null and rk.eta_oc is not null THEN  st.rta_pod - rk.eta_oc
	          end as pod_arr_deviation

			---Customs In Delay days----------  ST & CD
	        , case when st.custom_in_fact::date IS NULL and cd.declaration_registration_date IS NOT NULl then null ----'Cus In missing in ST'
			 	  when st.custom_in_fact::date IS NULL and cd.declaration_registration_date IS null then null
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

			-----2nd accep. Delay------   ST & RMS
		    , case when  st.arrival_wh_fact IS NOT NULL and rms_ships.receiving_date2 IS NOT NULL and date_part('day',rms_ships.receiving_date2 - st.arrival_wh_fact- interval '2 days') <= 0 then 0
		            when  st.arrival_wh_fact IS NOT NULL and rms_ships.receiving_date2 IS NOT NULL and date_part('day',rms_ships.receiving_date2 - st.arrival_wh_fact- interval '2 days') > 0
		                then date_part('day',rms_ships.receiving_date2 - st.arrival_wh_fact- interval '2 days')
		            when  st.arrival_wh_fact IS NOT NULL and rms_ships.receiving_date2 IS NULL and date_part('day',now()::date - st.arrival_wh_fact- interval '2 days') <= 0 then 0
		            when  st.arrival_wh_fact IS NOT NULL and rms_ships.receiving_date2 IS NULL and date_part('day',now()::date - st.arrival_wh_fact- interval '2 days') > 0
		                then date_part('day',now()::date - st.arrival_wh_fact- interval '2 days')
		            when st.arrival_wh_fact IS NULL then null
		      end as second_acceptance_delay
		    , date_part('day', decode(rms_ships.receiving_date1, null, a_ship_status.eta, rms_ships.receiving_date2) - (rk.eta_oc +interval'30 days'))  as eta_wh_deviation

            ----Invoice split-----
            , inv_split.invoice_status as invoice_status
FROM ries_kalypso_temp rk
	left join st_temp st on st.st_oc_id =rk.oc_id and st.st_container_id = regexp_replace(rk.container_id2,'[^A-Za-z0-9]','','g')
	           			and coalesce(st.st_invoice_id,'') = coalesce(rk.kalypso_invoice_id,'')
	left join article_shipment_statuses_temp a_ship_status on a_ship_status.asst_oc_id =rk.oc_id
	                    and a_ship_status.asst_adeo_code =rk.adeo_code
	                    and coalesce(a_ship_status.asst_container_id,'')=coalesce(rk.container_id2 ,'')
	                    and coalesce(a_ship_status.asst_invoice_id,'')=coalesce(rk.kalypso_invoice_id ,'')
	left join rms_all_temp rms on rms.rms_order_id::text =rk.order_id
								   and rms.rms_lm_code = rk.lm_code
	left join rms_only_orders_temp rms_order on rms_order.order_id::text =rk.order_id
    left join rms_shipments_temp rms_ships on rms.rms_order_id =rms_ships.order_id
                              and rms.rms_lm_code =rms_ships.lm_code
                              and rms.loc::int = rms_ships.to_loc::int
                              and coalesce(rk.kalypso_invoice_id,'') = coalesce(rms_ships.rms_invoice_id, '')
	left join item_info rms_im on rms_im.rms_im_lm_code=rk.lm_code
	left join goods_declarations_temp cd on  (cd.cd_adeo_code = rk.adeo_code
	                                    or cd.cd_adeo_code = rk.lm_code or cd.cd_ean_code = rk.ean_code)
									    and rk.kalypso_invoice_id=ANY(cd.invoice_numbers)
	left join shipping_docs ship_docs on ship_docs.docs_invoice_id = rk.kalypso_invoice_id and ship_docs.rn=1
	left join ord_tasks on ord_tasks.ord_id = rk.order_id
	left join forwarders_distributions fd on fd.order_confirmation_number = rk.oc_id
    left join invoices_split_status inv_split
        on inv_split.invoice_number= rk.kalypso_invoice_id
);


truncate table import_datamart;
insert into import_datamart (supplier_mode, direction, order_id, order_date, adeo_order_id, oc_number, oc_date, adeo_code, lm_code, ean_code,
   lm_name_ru, lm_name_eng, status_global, rms_order_status, eta_wh_calc, second_acceptance_wh_plan, eta_wh_calc_ym, eta_wh_calc_yw,
   dep_code, dep_name, dep, sub_dep_code, sub_dep_name, sub_dep, supplier_code, supplier_name, supplier, country,
   flow_type, top_1000, mdd, brand, best_price, gamma, priority, loc, order_qty, shipped_qty, received_qty,
   remain_to_ship_qty, dpac_item, received_dpac_amount, remain_dpac_amount, item_amount, order_amount, incoterms,
   season_validity_date_from, season_validity_date_to, supplier_contract_number, supplier_contract_date,
   payment_date, status_kalypso, adeo_dep_name, bu_name, swb, transport,
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
       , null as direction
       , order_id
	   , order_date as order_date
	   , kalypso_adeo_order_id as adeo_order_id
	   , oc_id as oc_number
	   , oc_date
	   , adeo_code::text
	   , lm_code::text
	   , ean_code::text
	   , lm_name as lm_name_ru
	   , desc_eng as lm_name_eng
	   , status_global  --Статус импорта !
	   , rms_order_status
	   , eta_wh_calc --!
	   , second_acceptance_wh_plan
	   , TO_CHAR(temp_all.eta_wh_calc, 'MM YYYY') as eta_wh_calc_ym
	   , TO_CHAR(temp_all.eta_wh_calc, 'WW YYYY') as eta_wh_calc_yw
	   , dep_code::int
	   , dep_name
	   , dep_code || ' ' || dep_name as dep
	   , sub_dep_code
	   , sub_dep_name
	   , sub_dep_code || ' ' || sub_dep_name as sub_dep
	   , supplier_code::int
	   , supplier_name
	   , supplier_code || ' ' || supplier_name as supplier
	   , null as country --- -> ARF
	   , flow_type
	   , top1000 as top_1000
	   , mdd
	   , brand
	   , best_price
	   , gamma
	   , priority
	   , v_loc as loc
	   , ries_order_qty
	   , shipped_information_number_of_units as shipped_qty   ---> KALYPSO by invoice
	   , ship_received as received_qty --->   RMS by invoice
	   , sum(to_be_shipped_information_number_of_units) over(partition by order_id, adeo_code) as remain_to_ship_qty  -- в разрезе заказа --, as remain_to_ship_qty    -- в разрезе инвойса
	   , dpac_item as dpac_item
	   , received_dpac_amount as received_dpac_amount
	   , dpac_item * (ries_order_qty-shipped_information_number_of_units) as remain_dpac_amount  -- dpac_item* QTY_REMAIN_SHIP
	   , article_total_amount as item_amount
	   , oc_total_amount as order_amount      ---> total_amount from order_confirmations  2 релиз
	   , incoterms
	   , season_validity_date_from as season_validity_date_from
	   , season_validity_date_to as season_validity_date_to
	   , null as supplier_contract_number
	   , null as supplier_contract_date
	   , null as payment_date
	   , status_kalypso
	   , adeo_dep_name
	   , bu_name
	   , swb
	   , transport --->change  --!
	   , invoice_status ---> Invoice split
	   , kalypso_invoice_id as invoice_number
	   , selling_invoice_date as invoice_date
	   , CASE WHEN actual_container is not null THEN actual_container
			      WHEN actual_container is null AND shipped_container_number is not null THEN shipped_container_number
			      WHEN actual_container is null AND shipped_container_number is null AND delivered_container_number is not null
			      THEN delivered_container_number END AS actual_container
	   , container_id1  as container_number1  --shipped_container_number
	   , container_id2  as container_number2 --delivered_container_number
	   , modified_container
	   , CASE WHEN actual_container is not null THEN type
	          WHEN actual_container is null AND container_id1 is not null THEN container_type_1st_leg
	          WHEN actual_container is null AND container_id1 is null AND container_id2 is not null THEN container_type_2nd_leg END AS container_type
	   , container_type_1st_leg
	   , container_type_2nd_leg
	   , CASE WHEN actual_container is not null THEN loading_type
	          WHEN actual_container is null AND container_id1 is not null THEN loading_type_1st_leg
	          WHEN actual_container is null AND container_id1 is null AND container_id2 is not null THEN loading_type_2nd_leg END AS loading_type
	    , loading_type_1st_leg
	    , loading_type_2nd_leg
	    , decode(to_be_shipped_information_volume, 0, shipped_information_volume, to_be_shipped_information_volume) as delivery_unit_volume
	    , delivery_unit_ordered_unit_code
	    , forwarder_id
	    , main_transport_company_id
	    , port_of_loading as pol
	    , pod
	    , destination_rail_station
	    , loc_name as wh  -- RMS
	    , etd_oc::date
	    , eta_oc::date
	    , shipment_release_date::date as ship_release_date
	    , ship_deadline
	    , truck_arr_pol_fact
	    , etd_pol_plan::date
	    , confirmed_shipping_date::date as etd_confirmed
	    , rdd_pol::date
	    , confirmed_delivery_date::date as eta_confirmed
	    , updated_delivery_date::date as etd_pod_updated
	    , etd_pod_plan_fwr::date
	    , coalesce(etd_pod_plan_fwr, updated_delivery_date) AS eta_pod_plan
	    , rta_pod::date  --forwarderFactRealTimeOfArrivalOnPortOfDischarge
 	    , transport_from_pod::text
	    , custom_in_plan::date::date  --forwarderPlanningEstimationTimeOfArrivalCustoms
	    , custom_in_fact::date   --customsIn
 	    , etd_pod_plan::date  --forwarderPlanningEstimationTimeOfDeliveryOnPortOfDischarge
	    , rdd_pod::date   --forwarderFactRealDateOfDeliveryOnPortOfDischarge
	    , rta_rail_station::date
	    , rdd_rail_station::date
	    , arrival_wh_plan::date
	    , arrival_wh_fact::date
	    , drop_off::date
	    , receiving_date1::date as receiving_date1
	    , receiving_date2::date as receiving_date2
	    , forwarder_comments
	    , tu
	    , release_to
	    , release_to_modified
	    , cast(loading_percent as numeric) as loading_percent
	    , customs_terminal
	    , customs_broker
	    , comments_broker
	    , fwr_appl_create_date
	    , fwr_appl_confirm_date
	    , null as invoice_total_volume
	    , null as invoice_qty
	    , null as invoice_qty_pkgs
	    , null as invoice_price
	    , null as invoice_curr
	    , null as invoice_total_amount
	    , null as invoice_net_weight
	    , null as invoice_gross_weight
	    , null as invoice_pallets
	    , null as letter_of_credit
	    , comments as comments_kalypso
	    , declaration_number as cd_number
	    , declaration_procedure as cd_proc
	    , declaration_registration_date as cd_date
	    , declaration_status as cd_status
	    , declaration_status_date as cd_status_date
	    , declaration_cus_out as cd_out
	    , null as cd_decision--alta.cd_decision
	    , null as cd_decision_date--alta.cd_decision_date
	    , null as cd_decision_time--alta.cd_decision_time
	    , null as cd_modified_date--alta.cd_modified_date
	    , null as cd_modified_time--alta.cd_modified_time
	    , good_number as cd_good_id
	    , good_tnved_code as cn_code
	    , null as invoice_hs_code
	    , good_status as cd_good_status
	    , good_status_timestamp as cd_good_status_date
	    , null as cd_good_status_time--alta.cd_good_status_time
	    , good_description as cd_good_desc
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
        , article_quantity as cd_lm_qty
		, article_unit as cd_good_unit_type
	  	, article_description as cd_lm_desc
	  	, article_manufacturer as cd_lm_manufacturer
	    , under_certification
	    , doc_type
	    , regexp_replace(tech_regulation, '[\[\]]','', 'g') as tech_regulation
	    , doc_serial_id
	    , 'Реестр сертификации' as doc_serial_issue
	    , doc_serial_expiry
	    , doc_serial_valid
	    , doc_batch_id
	  	, doc_batch_issue
	    , 'Реестр сертификации' as gln
	    , manufacturer as manufacturer_address
	    , 'Реестр сертификации' as production_site_address
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
	    , array_to_string(task, ',') as current_order_task
	    , 'Реестр сертификации' as doc_conformity_status
	    , state as doc_ship_status
	    , null as doc_set_status
	    , rms_order_status as status_oc
	    , invoice_upload as status_invoice ---> uploading_invoice
	    , import_specialist
	    , null as appro_specialist
	    , cd_specialist
	    , certification_specialist
	    , customs_in::date as custom_in_calc
	     ----Production KPI-----
	    , CASE WHEN prod_deviation > 0 then 'Delay'
	          WHEN prod_deviation <= 0 then 'On Time'
	      end AS prod_kpi
	    , prod_deviation
	     ----Shipping KPI-----
	    , CASE WHEN ship_deviation <=0 THEN 'On Time'
	           WHEN ship_deviation >0 THEN 'Delay'
	      end AS ship_kpi
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
		   	   when custom_in_fact::date IS NULL and declaration_registration_date IS NOT NULl then 'Cus In missing in ST'
	   	  end as custom_in_kpi
	    , custom_in_delay_days
	     -----cd_request_kpi----
	    , case when declaration_registration_date -rta_pod =0 then 'On Time'
	   		   when declaration_registration_date -rta_pod <0 then 'Discrepancies'
	   		   when declaration_registration_date -rta_pod >0 then 'Delay'
	      end as cd_request_kpi
		, declaration_registration_date -rta_pod as cd_request_delay
	     --------Customs OUT KPI ----------
	    , case when arrival_wh_plan is not null and date_diff <= 0 then 'On Time'
		 	   when arrival_wh_plan is not null and date_diff > 0 then 'Delay'
		  end as custom_out_kpi
	     --------Customs OUT delay days----------
		, case when arrival_wh_plan is not null and date_diff <= 0 then 0
		 	   when arrival_wh_plan is not null and date_diff > 0 then date_diff
		  end  as custom_out_delay_days
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
	    , case when eta_wh_deviation > 0 then 'Delay'
	           when eta_wh_deviation <= 0 then 'On Time'
	     end eta_wh_kpi
	    , eta_wh_deviation
FROM temp_all;
return 0;
end;

$function$
;
