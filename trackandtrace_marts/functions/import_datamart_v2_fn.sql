--liquibase formatted sql
--changeset 60098727:create:trackandtrace_marts.customs_declaration_items_fn

CREATE OR REPLACE FUNCTION import_datamart_v2_fn()
    RETURNS boolean
    LANGUAGE plpgsql
AS $function$
begin


    drop table if exists shipping_docs;
    drop table if exists ord_tasks;
    drop table if exists forwarders_distributions;
    drop table if exists article_shipment_statuses_temp;
    drop table if exists customs_declaration_items_temp;
    drop table if exists temp_all;



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
        select order_confirmation_number,
               forwarder,
               etd_pol_plan
        from ries_report_ods.v_forwarders_distributions fd
        where fd.is_actual = '1'
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

    ----< customs declarations > -----
    create temp table customs_declaration_items_temp as (
        select  cd.cd_number
             , cd.invoice_numbers
             , cd.container_ids_capacities
             , cd.modified_date
             , cd.modified_time
             , cd.cd_procedure
             , cd.cd_status_code
             , cd.cd_status
             , cd.cd_status_date
             , cd.registration_date
             , cd.out_date
             , cd.good_number
             , cd.tnved_code
             , cd.good_status_code
             , cd.good_status
             , cd.good_status_date
             , cd.good_status_time
             , cd.good_net_weight
             , cd.good_gross_weight
             , cd.good_desc
             , cd.accrual_basis_payment
             , cd.customs_rate
             , cd.customs_tax
             , cd.cd_consignment_info
             , cd.cd_airbill_info
             , cd.cd_railbill_info
             , cd.cd_trnsprtbill_info
             , cd.cd_another_trnsprtbill_info
             , cd.cd_doc_info
             , cd.dbf_customs_declaration

             , cdi.sub_goods_number
             , cdi.ean_code
             , cdi.adeo_code
             , cdi.quantity
             , cdi.unit
             , cdi.technical_description
             , cdi.manufacturer
             , cdi.dbf_customs_declaration_items
        from customs_declaration cd
                 left join customs_declaration_items cdi on cd.good_number = cdi.good_number and cdi.cd_number = cd.cd_number
    );


    create temp table temp_all as (
        select r.order_id, r.oc_id as oc_id, r.adeo_code, r.lm_code
             -----base_invoice
             , b.invoice_id as  bi_invoice_id, b.oc_id as bi_oc, b.container_id as bi_container
             -----kalypso
             , k.oc_id as k_oc, k.invoice_id as k_invoice_id, k.container_id1, k.container_id2, k.adeo_code as k_adeo_code
             -----st
             , st.oc_id as st_oc_id, st.invoice_id as st_invoice, st.container_id as st_container_id
             -----rms_shipments
             , rms_ship.order_id as rms_ship_order_id, rms_ship.invoice_id   as rms_ship_invoice_id, rms_ship.to_loc  , rms_ship.lm_code   as rms_ship_lm_code
             -----rms_orders
             , rms_ord.order_id as rms_ord_order_id, rms_ord.loc, rms_ord.status as rms_order_status, rms_ord.order_date as rms_ord_order_date, rms_ord.supplier_code as rms_ord_supplier_code, rms_ord.lm_code as rms_ord_lm_code
             -----custom_declaration
             , cd.cd_number, cd.invoice_numbers
             -----custom_declaration_items
             , cd.tnved_code, cd.good_number, cd.sub_goods_number, cd.ean_code as cd_ean_code, cd.adeo_code as cd_adeo_code
             -----article_ship_statuses
             , a_ship_status.asst_invoice_id, a_ship_status.asst_container_id, a_ship_status.asst_adeo_code
             ---select x from shipping_docs
             , ship_docs.docs_invoice_id
             --	select x from ord_tasks
             , ord_tasks.ord_id, ord_tasks.task
             --	select x from forwarders_distributions
             , fd.order_confirmation_number as fd_oc_id
             -------------- PART DEBUG
             , b.dbf_base_ries_invoices, cd.dbf_customs_declaration, cd.dbf_customs_declaration_items, k.dbf_kalypso_containers
             , r.dbf_ries_orders, st.dbf_ries_shipment_tracking, rms_im.dbf_rms_item_info, rms_ord.dbf_rms_orders, rms_ship.dbf_rms_orders_shipments
             ----------------------------------------------------------------------------------------------------------------------------------------------------------------
             ----- RIES part 	--->select x from ries_orders
             , r.priority, r.oc_date, r.ean_code, r.etd_oc, r.eta_oc, r.port_of_loading, r.incoterms
             , r.season_validity_date_from, r.season_validity_date_to, r.oc_total_amount, r.manufacturer, r.desc_eng, r.ries_order_qty, r.article_total_amount
             , r.under_certification, r.samples_qty, r.samples_required, r.doc_serial_id, r.doc_serial_valid, r.doc_type, r.doc_batch_issue, r.doc_serial_expiry
             , r.doc_batch_id, r.tech_regulation, r.awb_or_container_samples_id, r.samples_pi_id, r.initial_pi_sending_date, r.deadline_pi_sending_date, r.sample_sending_way
             , r.sample_sending_date, r.deadline_sample_sending_date, r.sample_sending_kpi, r.reason_sample_sending_late, r.sample_sending_delay
             , r.route_id, r.transport_instruction, r.calculated_etd, r.comments_for_etd, r.samples_sent_code, r.samples_required_code
             ----- KALYPSO part   --->select x from kalypso_containers
             , adeo_order_id as k_adeo_order_id, k.container_creating_date, k.adeo_dep_name
             , k.delivery_unit_ordered_unit_code, k.delivery_type, k.production_diff, k.purchasing_incoterm_code, k.purchasing_incoterm_city_code, k.country_of_loading_code
             , k.country_of_loading_name, k.city_of_discharge_code, k.city_of_discharge_name, k.status_kalypso, k.shipped_container_number, k.delivered_container_number
             , k.shipped_information_number_of_units, k.to_be_shipped_information_number_of_units, k.to_be_shipped_information_volume, k.shipped_information_volume
             , k.updated_delivery_date, k.updated_shipping_date, k.shipment_comments, k.confirmed_delivery_date, k.confirmed_shipping_date, k.shipment_release_date
             , k.selling_invoice_date, k.container_type_1st_leg, k.loading_type_1st_leg, k.container_type_2nd_leg, k.loading_type_2nd_leg, k.forwarder_id, k.main_transport_company_id, k.bu_name
             ----- BASE_INVOICE part  --->select x from base_ries_invoices
             , b.invoice_status, b.shipment_id, b.rms_id
             , b.adeo_order_number as bi_adeo_order_id, b.delivery_terms, b.invoice_date, b.payment_date, b.seal, b.container_type as bi_container_type, b.invoice_total_volume
             , b.invoice_qty, b.invoice_qty_pkgs, b.invoice_total_amount, b.invoice_net_weight, b.invoice_gross_weight, b.invoice_pallets
             , b.invoice_price, b.invoice_curr  , b.invoice_hs_code
             ---- ST PART ---->select x from ries_shipment_tracking
             , st.actual_container, st.swb, st.modified_container, st.loading_type, st.container_type, st.custom_in_plan, st.destination_rail_station, st.truck_arr_pol_fact, st.rdd_pol as st_rdd_pol
             , st.etd_pod_plan_fwr, st.rta_pod
             , st.etd_pod_plan, st.rdd_pod, st.rdd_rail_station, st.drop_off, st.rta_rail_station, st.arrival_wh_plan, st.arrival_wh_fact, st.forwarder_comments, st.tu, st.release_to as st_release_to, st.release_to_modified, st.transport_from_pod
             , st.loading_percent
             , st.fwr_appl_create_date, st.fwr_appl_confirm_date, st.customs_terminal, st.customs_broker, st.comments_broker, st.custom_in_fact, st.import_specialist, st.cd_specialist, st.certification_specialist, st.port_of_discharge as st_port_of_discharge
             , st.etd_pol_plan as st_etd_pol_plan
             ---- rms_orders_shipments PART ---- select x FROM rms_orders_shipments
             , rms_ship.invoice_upload, rms_ship.receiving_date1, rms_ship.receiving_date2, rms_ship.ship_expected, rms_ship.ship_received
             ---- rms_orders PART ---- select x FROM rms_orders
             , rms_ord.supplier_code , rms_ord.loc_name, rms_ord.v_loc, rms_ord.order_date,  rms_ord.unit_price, rms_ord.order_qty, rms_ord.received_qty
             , rms_ord.supplier_name, rms_ord.dpac_item, rms_ord.order_dpac_amount, rms_ord.received_dpac_amount, rms_ord.order_amount, rms_ord.received_amount, rms_ord.status_oc
             ---- RMS ITEMS PART ---- >select x FROM rms_item_info
             , rms_im.lm_name, rms_im.dep_code, rms_im.dep_name, rms_im.sub_dep_code, rms_im.sub_dep_name, rms_im.lm_type, rms_im.lm_type_desc, rms_im.lm_subtype, rms_im.lm_subtype_desc, rms_im.import_attr, rms_im.flow_type
             , rms_im.top1000, rms_im.brand, rms_im.mdd, rms_im.best_price, rms_im.gamma
             ---- Goods Declaration PART ---- >select x FROM customs_declaration
             , cd.container_ids_capacities, cd.modified_date, cd.modified_time, cd.cd_procedure, cd.cd_status_code, cd.cd_status, cd.cd_status_date, cd.registration_date, cd.out_date, cd.good_status_code, cd.good_status, cd.good_status_date
             , cd.good_status_time, cd.good_net_weight, cd.good_gross_weight, cd.good_desc, cd.accrual_basis_payment, cd.customs_rate, cd.customs_tax, cd.cd_consignment_info, cd.cd_airbill_info, cd.cd_railbill_info, cd.cd_trnsprtbill_info
             , cd.cd_another_trnsprtbill_info, cd.cd_doc_info
             ---- Goods Declaration items PART ---- >select x FROM customs_declaration_items
             , cd.quantity, cd.unit, cd.technical_description, cd.manufacturer as cd_manufacturer
             ---- ASS (article shipment status) PART ---- >select x FROM article_shipment_statuses_temp
             , asst_order_id, a_ship_status.eta, a_ship_status.customs_in, a_ship_status.status as asst_status, a_ship_status.transport_mode, a_ship_status.port_of_discharge
             ---- RMS shipnent PART ---- >select x FROM  shipping_docs
             , ship_docs.state
             ---- Forwarders Distribution PART ---- >select x from forwarders_distributions
             , fd.forwarder, fd.etd_pol_plan as fd_etd_pol_plan
             ---- calculations 1rst level
             , coalesce(st.etd_pol_plan, fd.etd_pol_plan, k.updated_shipping_date) as etd_pol_plan
             , coalesce(st.rdd_pol, k.confirmed_shipping_date) as rdd_pol
             , coalesce(st.release_to, fd.forwarder) as release_to
             , (case when a_ship_status.transport_mode ='TRUCK' then r.etd_oc -interval '7 days'
                     when a_ship_status.transport_mode ='SEA' then r.etd_oc -interval '10 days'
                     when a_ship_status.transport_mode ='TRAIN' then r.etd_oc -interval '10 days'
                end)::date as ship_deadline
             ---- it should be removed after ETA WH v2(LMRUBPMS-4025) will be realised
             , case
                 when rms_ship.receiving_date2 is not null
                          and rms_ord.status is not null
                          and rms_ord.status <> 'Closed'
                     THEN 'Received'
                 when rms_ship.receiving_date2 is not null
                          and rms_ord.status = 'Closed'
                     THEN 'Closed'
                 when (rms_ship.receiving_date2 is null and rms_ord.status ='Closed')
                          or (rms_ord.status is null)
                     THEN 'cancelled'
                 when rms_ship.receiving_date1 is not null
                          and rms_ship.receiving_date2 is null
                          and now()::date > (rms_ship.receiving_date1 + interval '2 days')
                     THEN '2nd Acceptance WH Delay'
                 when rms_ship.receiving_date1 is not null
                          and rms_ship.receiving_date2 is null
                          and now()::date <= (rms_ship.receiving_date1 + interval '2 days')
                     THEN '1st Acceptance at WH'
                 when (r.order_status = 'Underway' and a_ship_status.eta < now ()::date)
                          or (b.invoice_status = 'Splitted/Replacing/Additional')
                          or (b.invoice_status = 'Deleted')
                     THEN 'No possibility to track'
                 when rms_ship.receiving_date1 is null
                          and rms_ship.receiving_date2 is null
                          and a_ship_status.status is not null
                     THEN a_ship_status.status
                ELSE 'Created'
            end as status_global  --Статус импорта
             ---- it should be removed after ETA WH v2(LMRUBPMS-4025) will be realised
             , case
                 when rms_ship.receiving_date1 is not null then rms_ship.receiving_date2
                 else (case
                     when (r.order_status = 'Underway' and a_ship_status.eta < now()::date)
                         or (b.invoice_status = 'Splitted/Replacing/Additional')
                         or (b.invoice_status = 'Deleted') then '01.01.1900'::date
                     else a_ship_status.eta
                     end) end as eta_wh_calc
             , decode(rms_ship.receiving_date1, null, a_ship_status.eta, rms_ship.receiving_date2) ::date + interval '2 days'  as second_acceptance_wh_plan
             , a_ship_status.transport_mode as transport
             , a_ship_status.port_of_discharge as pod
             , case when a_ship_status.transport_mode  ='SEA' and a_ship_status.port_of_discharge = 'VOSTOCHNIY, PORT' and cd.cd_status_date is not null
                        then date_part('day', cd.cd_status_date - cd.registration_date -interval '4 days')
                    when a_ship_status.transport_mode ='SEA' and a_ship_status.port_of_discharge = 'VOSTOCHNIY, PORT' and cd.cd_status_date is null
                        then date_part('day', now() - cd.registration_date -interval '4 days')
                    when a_ship_status.transport_mode <>'SEA' and a_ship_status.port_of_discharge <> 'VOSTOCHNIY, PORT' and cd.cd_status_date is not null
                        then date_part('day', cd.cd_status_date - cd.registration_date -interval '2 days')
                    when a_ship_status.transport_mode <>'SEA' and a_ship_status.port_of_discharge <> 'VOSTOCHNIY, PORT' and cd.cd_status_date is null
                        then date_part('day', now() - cd.registration_date -interval '2 days')
                end as date_diff

             ----Production deviation---   RiesKalypso
             , CASE WHEN k.shipment_release_date is NOT NULL and r.etd_oc is NOT NULL THEN k.shipment_release_date -(r.etd_oc -production_diff)
                    WHEN k.shipment_release_date is null and r.etd_oc is NOT NULL and  now()::date -(r.etd_oc -production_diff) <= 0 THEN null -- без этого условия юзер будет видеть большие отрицательные цифры
                    WHEN k.shipment_release_date is null and r.etd_oc is NOT NULL and  now()::date -(r.etd_oc -production_diff) > 0 THEN now()::date -(r.etd_oc -production_diff)
            end AS prod_deviation

             ----Shipping deviation-----  ST & RiesKalypso
             , CASE WHEN k.delivery_type in ('SEA','TRAIN') AND k.confirmed_shipping_date is not null AND r.etd_oc is NOT NULL  THEN  k.confirmed_shipping_date- r.etd_oc
                    WHEN k.delivery_type ='TRUCK' AND st.rdd_pol is not null AND r.etd_oc is NOT NULL  THEN  st.rdd_pol -r.etd_oc
                    WHEN k.confirmed_shipping_date is null AND st.rdd_pol is null AND  r.etd_oc is NOT NULL AND now()::date -r.etd_oc >0 THEN  now()::date -r.etd_oc
            end AS ship_deviation

             ----POD arr deviation---  ST & RiesKalypso
             , case when st.rta_pod IS  null and r.eta_oc is not null and  now()::date - r.eta_oc <= 0 THEN NULL
                    when st.rta_pod IS  null and r.eta_oc is not null and  now()::date - r.eta_oc > 0 THEN  now()::date - r.eta_oc
                    when st.rta_pod IS not null and r.eta_oc is not null THEN  st.rta_pod - r.eta_oc
            end as pod_arr_deviation

             ---Customs In Delay days----------  ST & CD
             , case when st.custom_in_fact::date IS NULL and cd.registration_date IS NOT NULl then null ----'Cus In missing in ST'
                    when st.custom_in_fact::date IS NULL and cd.registration_date IS null then null
                    when st.custom_in_fact::date IS not NULL and cd.registration_date IS NOT NULl
                        then (cd.registration_date  - st.custom_in_fact::date)
                    when st.custom_in_fact::date IS not NULL and cd.registration_date IS NULl
                        then (now()::date - st.custom_in_fact::date )
            end as custom_in_delay_days
             -----arrival wh deviation------  ST
             , case when  st.arrival_wh_plan IS NOT NULL and st.arrival_wh_fact IS NOT NULL and st.arrival_wh_plan is not null then  st.arrival_wh_fact::date - st.arrival_wh_plan::date
                    when  st.arrival_wh_plan IS NOT NULL and st.arrival_wh_fact IS NULL and st.arrival_wh_plan is not null and  now()::date- st.arrival_wh_plan::date<= 0 then NULL
                    when  st.arrival_wh_plan IS NOT NULL and st.arrival_wh_fact IS NULL and st.arrival_wh_plan is not null and  now()::date- st.arrival_wh_plan::date> 0 then  now()::date-st.arrival_wh_plan
                    when  st.arrival_wh_plan IS null then null
            end as arrival_wh_deviation

             -----2nd accep. Delay------   ST & RMS
             , case when  st.arrival_wh_fact IS NOT NULL and rms_ship.receiving_date2 IS NOT NULL and date_part('day',rms_ship.receiving_date2 - st.arrival_wh_fact- interval '2 days') <= 0 then 0
                    when  st.arrival_wh_fact IS NOT NULL and rms_ship.receiving_date2 IS NOT NULL and date_part('day',rms_ship.receiving_date2 - st.arrival_wh_fact- interval '2 days') > 0
                        then date_part('day',rms_ship.receiving_date2 - st.arrival_wh_fact- interval '2 days')
                    when  st.arrival_wh_fact IS NOT NULL and rms_ship.receiving_date2 IS NULL and date_part('day',now()::date - st.arrival_wh_fact- interval '2 days') <= 0 then 0
                    when  st.arrival_wh_fact IS NOT NULL and rms_ship.receiving_date2 IS NULL and date_part('day',now()::date - st.arrival_wh_fact- interval '2 days') > 0
                        then date_part('day',now()::date - st.arrival_wh_fact- interval '2 days')
                    when st.arrival_wh_fact IS NULL then null
            end as second_acceptance_delay
             , date_part('day', decode(rms_ship.receiving_date1, null, a_ship_status.eta, rms_ship.receiving_date2) - (r.eta_oc +interval'30 days'))  as eta_wh_deviation
             ---- (select distinct order_id, oc_id, adeo_code, lm_code, ean_code  from ries_orders)
        from ries_orders r
                 ---- JOIN-1 (select distinct oc_id, invoice_id, container_id, adeo_code from base_ries_invoices)
                 left join base_ries_invoices b ON b.oc_id =r.oc_id  and (b.adeo_code = r.adeo_code or b.lm_code = r.lm_code)
                 ---- JOIN-2 (select distinct oc_id, order_id, invoice_id, container_id2, adeo_code  FROM kalypso_containers)
                 left join kalypso_containers k ON ----1 (OC and CONTAINER) XOR (Invoice) XOR (Oc)
                    ---- 2.1 (INVOICES) Джоин по инвойсам или ОС
                     decode(k.invoice_id, coalesce(b.invoice_id, '1'), k.invoice_id, decode(substring(r.oc_id,1,5), 'S2009', k.order_id, k.oc_id))  -----
                    =decode(k.invoice_id, coalesce(b.invoice_id, '2'), b.invoice_id, r.oc_id)
                    ---- 2.2 (OC and CONTAINER) XOR (Invoice)  Описание джойна и кейсов смотри в миро
                    -- in decode func null==null ->is True!!! поэтому присваеваем 1 и 2, чтобы джойн не происходил, так как значения ключей в обеих системах nulls
                    and
                     ( case when k.invoice_id is not null and b.invoice_id is not null
                            then decode(k.invoice_id, b.invoice_id,
                                        k.invoice_id,
                                        (case  when k.container_id2 is not null and b.container_id is not null
                                               then k.container_id2
                                               else '1' end))
                            else (case when  k.container_id2 is not null and b.container_id is not null
                                       then k.container_id2
                                       else '1' end) end
                     ) =
                     ( case when k.invoice_id is not null and b.invoice_id is not null
                            then decode(k.invoice_id, b.invoice_id,
                                        b.invoice_id,
                                        (case when k.container_id2 is not null and b.container_id is not null
                                              then b.container_id
                                              else '2' end))
                            else (case when k.container_id2 is not null and b.container_id is not null
                                       then b.container_id
                                       else  '1' end) end
                     )
                    ---- 2.3  adeo_code
                    and k.adeo_code=r.adeo_code
                 ---- JOIN-3 (select distinct oc_id, invoice_id, container_id from ries_shipment_tracking)
                 left join ries_shipment_tracking st ON
                    --- 3.1 OC_ID or Invoice_ID
                    decode(st.invoice_id, coalesce(b.invoice_id, '1'), st.invoice_id, decode(st.invoice_id, null, st.oc_id, ''))
                    =decode(b.invoice_id, coalesce(st.invoice_id, '2'), b.invoice_id, r.oc_id)
                    --- 3.2 CONTAINER_ID or Invoice_ID
                    AND decode(st.invoice_id, coalesce(b.invoice_id, '1'), st.invoice_id, st.container_id)
                            =decode(st.invoice_id, coalesce(b.invoice_id, '2'), b.invoice_id, regexp_replace(coalesce(b.container_id, k.container_id2),'[^A-Za-z0-9]','','g'))
                 ---- JOIN-4 (select distinct order_id, invoice_id, receiving_date2,to_loc,adeo_code, lm_code  from rms_orders_shipments)
                 left join rms_orders_shipments rms_ship
                           ON rms_ship.order_id =r.order_id
                               and rms_ship.invoice_id =coalesce(b.invoice_id, k.invoice_id)
                               and decode(rms_ship.lm_code, null, '', rms_ship.lm_code) =decode(rms_ship.lm_code, null, '', r.lm_code)    ---> in decode func null==null is True!!!
                --- JOIN-5 (select distinct order_id, loc, status, order_date, supplier_code, adeo_code,lm_code from rms_orders)
                 left join rms_orders rms_ord  ON rms_ord.order_id =r.order_id
                    and decode(rms_ship.to_loc, null, 1,rms_ord.loc) =decode(rms_ship.to_loc, null, 1, rms_ship.to_loc)
                    and decode(rms_ord.lm_code, null, '', rms_ord.lm_code) =decode(rms_ord.lm_code, null, '', r.lm_code)
                ---- JOIN-6
                left join rms_item_info rms_im on rms_im.lm_code=r.lm_code
                ---- JOIN-7
                 left join customs_declaration_items_temp cd
                           on (cd.adeo_code = r.adeo_code or cd.adeo_code = r.lm_code or cd.ean_code = r.ean_code)
                               and coalesce(b.invoice_id, k.invoice_id)  = ANY(cd.invoice_numbers::text[])
                ---- JOIN-8
                left join article_shipment_statuses_temp a_ship_status on a_ship_status.asst_oc_id =r.oc_id
                    and a_ship_status.asst_adeo_code =r.adeo_code
                    and coalesce(a_ship_status.asst_container_id,'') =coalesce(k.container_id2 ,'')
                    and coalesce(a_ship_status.asst_invoice_id,'') =coalesce(coalesce(b.invoice_id, k.invoice_id) ,'')
                ---- JOIN-9
                left join shipping_docs ship_docs on ship_docs.docs_invoice_id = coalesce(b.invoice_id, k.invoice_id) and ship_docs.rn=1
                ---- JOIN-10
                left join ord_tasks on ord_tasks.ord_id = r.order_id
                ---- JOIN-11
                left join forwarders_distributions fd on fd.order_confirmation_number =r.oc_id
    );



    truncate table  import_datamart_v2;
    insert into import_datamart_v2 (
                                     supplier_mode,direction,order_id,order_date,adeo_order_id,oc_number,oc_date,adeo_code,lm_code,ean_code,lm_name_ru
                                   ,lm_name_eng,status_global,rms_order_status,eta_wh_calc,second_acceptance_wh_plan,eta_wh_calc_ym,eta_wh_calc_yw,dep_code
                                   ,dep_name,dep,sub_dep_code,sub_dep_name,sub_dep,supplier_code,supplier_name,supplier,country,flow_type,top_1000,mdd
                                   ,brand,best_price,gamma,priority,loc,order_qty,shipped_qty,received_qty,remain_to_ship_qty,dpac_item
                                   ,received_dpac_amount,remain_dpac_amount,item_amount,order_amount,incoterms,season_validity_date_from
                                   ,season_validity_date_to,supplier_contract_number,supplier_contract_date,payment_date,status_kalypso,adeo_dep_name
                                   ,bu_name,swb,transport,route_id,samples_sent_code, samples_required_code, invoice_status,invoice_number,invoice_date,actual_container,actual_number_original
                                   ,container_number1,container_number2,modified_container,container_type,container_type_1st_leg,container_type_2nd_leg
                                   ,loading_type,loading_type_1st_leg,loading_type_2nd_leg,delivery_unit_volume,delivery_unit_ordered_unit_code
                                   ,forwarder_id,main_transport_company_id,pol,pod,destination_rail_station,wh,etd_oc,eta_oc,ship_release_date
                                   ,ship_deadline,truck_arr_pol_fact,etd_pol_plan,etd_confirmed,rdd_pol,eta_confirmed,etd_pod_updated,etd_pod_plan_fwr
                                   ,eta_pod_plan,rta_pod,transport_from_pod,custom_in_plan,custom_in_fact,etd_pod_plan,rdd_pod,rta_rail_station
                                   ,rdd_rail_station,arrival_wh_plan,arrival_wh_fact,drop_off,receiving_date1,receiving_date2,forwarder_comments,tu
                                   ,release_to,release_to_modified,loading_percent,customs_terminal,customs_broker,comments_broker,fwr_appl_create_date
                                   ,fwr_appl_confirm_date,invoice_total_volume,invoice_qty,invoice_qty_pkgs,invoice_price,invoice_curr
                                   ,invoice_total_amount,invoice_net_weight,invoice_gross_weight,invoice_pallets,letter_of_credit,comments_kalypso
                                   ,cd_number,cd_proc,cd_date,cd_status,cd_status_date,cd_out,cd_decision,cd_decision_date,cd_decision_time
                                   ,cd_modified_date,cd_modified_time,cd_good_id,cn_code,invoice_hs_code,cd_good_status,cd_good_status_date
                                   ,cd_good_status_time,cd_good_desc,container_capacity,good_net_weight,good_gross_weight,cd_customs_type
                                   ,accrual_basis_payment,customs_rate,customs_tax,cd_consignment_info,cd_consignment_id,cd_consignment_date
                                   ,cd_airbill_info,cd_airbill_id,cd_airbill_date,cd_railbill_info,cd_railbill_id,cd_railbill_date
                                   ,cd_trnsprtbill_info,cd_trnsprtbill_id,cd_trnsprtbill_date,cd_another_trnsprtbill_info,cd_another_trnsprtbill_id
                                   ,cd_another_trnsprtbill_date,cd_doc_info,cd_doc_id,cd_doc_issuedate,cd_lm_qty,cd_good_unit_type,cd_lm_desc
                                   ,cd_lm_manufacturer,under_certification,doc_type,tech_regulation,doc_serial_id,doc_serial_issue,doc_serial_expiry
                                   ,doc_serial_valid,doc_batch_id,doc_batch_issue,gln,manufacturer_address,production_site_address,samples_required
                                   ,samples_qty,samples_pi_id,initial_pi_sending_date,deadline_pi_sending_date,sample_sending_way,sample_sending_date
                                   ,awb_or_container_samples_id,deadline_sample_sending_date,reason_sample_sending_late,calculated_etd,comments_for_etd,oc_status,arf_status
                                   ,pi_sample_status,current_order_task,doc_conformity_status,doc_ship_status,doc_set_status,transport_instruction,status_oc,status_invoice
                                   ,import_specialist,appro_specialist,cd_specialist,certification_specialist
                                   ,custom_in_calc,prod_kpi,prod_deviation,ship_kpi,ship_deviation,pod_arr_kpi,pod_arr_deviation,custom_in_kpi
                                   ,custom_in_delay_days,cd_request_kpi,cd_request_delay,custom_out_kpi,custom_out_delay_days,sample_sending_kpi
                                   ,sample_sending_delay,arrival_wh_kpi,arrival_wh_deviation,second_acceptance_kpi,second_acceptance_delay
                                   ,eta_wh_kpi,eta_wh_deviation, dbf_ries_orders, dbf_base_ries_invoices, dbf_kalypso_containers
                                   , dbf_ries_shipment_tracking, dbf_rms_orders_shipments, dbf_rms_orders, dbf_rms_item_info, dbf_customs_declaration
                                   , dbf_customs_declaration_items
    )
    select 'Trade' as supplier_mode
         , null as direction
         , order_id
         , order_date as order_date
         , coalesce(bi_adeo_order_id, k_adeo_order_id) as adeo_order_id
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
         , payment_date
         , status_kalypso
         , adeo_dep_name
         , bu_name
         , swb
         , transport --->change  --!
         , route_id
         , samples_sent_code
         , samples_required_code
         , invoice_status ---> Invoice split
         , coalesce(bi_invoice_id, k_invoice_id) as invoice_number
         , selling_invoice_date as invoice_date
         , CASE WHEN actual_container is not null THEN actual_container
                WHEN actual_container is null AND shipped_container_number is not null THEN shipped_container_number
                WHEN actual_container is null AND shipped_container_number is null AND delivered_container_number is not null
                    THEN delivered_container_number END AS actual_container  ---final ACTUAL CONTAINER
         , decode(modified_container, null,
        --- if modified_container is null then -> actual_container (final ACTUAL CONTAINER)
                  (CASE WHEN actual_container is not null THEN actual_container
                        WHEN actual_container is null AND shipped_container_number is not null THEN shipped_container_number
                        WHEN actual_container is null AND shipped_container_number is null AND delivered_container_number is not null
                            THEN delivered_container_number END )
        --- else -> modified_container
        ,  modified_container) as actual_number_original ----new column!!!!!
         , container_id1  as container_number1  --shipped_container_number
         , container_id2  as container_number2 --delivered_container_number
         , modified_container
         , CASE WHEN actual_container is not null THEN container_type
                WHEN actual_container is null AND container_id1 is not null THEN container_type_1st_leg
                WHEN actual_container is null AND container_id1 is null AND container_id2 is not null THEN container_type_2nd_leg
        END AS container_type
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
         , invoice_total_volume
         , invoice_qty
         , invoice_qty_pkgs
         , invoice_price
         , invoice_curr
         , invoice_total_amount
         , invoice_net_weight
         , invoice_gross_weight
         , invoice_pallets
         , null as letter_of_credit
         , shipment_comments as comments_kalypso
         , cd_number as cd_number
         , cd_procedure as cd_proc
         , registration_date as cd_date
         , cd_status
         , cd_status_date
         , out_date as cd_out
         , cd_status_code || ' ' || cd_status as cd_decision--alta.cd_decision
         , cast(cd_status_date as date) as cd_decision_date--alta.cd_decision_date
         , cast(cd_status_date as time) cd_decision_time--alta.cd_decision_time
         , modified_date as cd_modified_date--alta.cd_modified_date
         , modified_time as cd_modified_time--alta.cd_modified_time
         , good_number as cd_good_id
         , tnved_code as cn_code
         , invoice_hs_code
         , good_status_code || ' ' || good_status  as cd_good_status
         , good_status_date as cd_good_status_date
         , good_status_time as cd_good_status_time
         , good_desc as cd_good_desc
         , container_ids_capacities as container_capacity
         , good_net_weight as good_net_weight
         , good_gross_weight as good_gross_weight
         , null as cd_customs_type  -- !!!! Заготовка, не удалять
         , accrual_basis_payment as accrual_basis_payment
         , customs_rate as customs_rate
         , customs_tax as customs_tax
         , cd_consignment_info
         , null as cd_consignment_id--  !!!! Удалить после мержа витрин
         , null as cd_consignment_date-- !!!! Удалить после мержа витрин
         , cd_airbill_info
         , null as cd_airbill_id-- !!!! Удалить после мержа витрин
         , null as cd_airbill_date-- !!!! Удалить после мержа витрин
         , cd_railbill_info
         , null as cd_railbill_id-- !!!! Удалить после мержа витрин
         , null as cd_railbill_date-- !!!! Удалить после мержа витрин
         , cd_trnsprtbill_info
         , null as cd_trnsprtbill_id --!!!! Удалить после мержа витрин
         , null as cd_trnsprtbill_date ---!!!! Удалить после мержа витрин
         , cd_another_trnsprtbill_info
         , null as cd_another_trnsprtbill_id--!!!! Удалить после мержа витрин
         , null as cd_another_trnsprtbill_date-- !!!! Удалить после мержа витрин
         , cd_doc_info
         , null as cd_doc_id--!!!! Удалить после мержа витрин
         , null as cd_doc_issuedate--!!!! Удалить после мержа витрин
         , quantity as cd_lm_qty
         , unit as cd_good_unit_type
         , technical_description as cd_lm_desc
         , cd_manufacturer as cd_lm_manufacturer
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
         , calculated_etd
         , comments_for_etd
         , null as oc_status
         , null as arf_status
         , null as pi_sample_status
         , array_to_string(task, ',') as current_order_task
         , 'Реестр сертификации' as doc_conformity_status
         , state as doc_ship_status
         , null as doc_set_status
         , transport_instruction
         , status_oc
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
                when custom_in_fact::date IS NULL and registration_date IS NOT NULl then 'Cus In missing in ST'
        end as custom_in_kpi
         , custom_in_delay_days
         -----cd_request_kpi----
         , case when registration_date -rta_pod =0 then 'On Time'
                when registration_date -rta_pod <0 then 'Discrepancies'
                when registration_date -rta_pod >0 then 'Delay'
        end as cd_request_kpi
         , registration_date -rta_pod as cd_request_delay
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
         , dbf_ries_orders
         , dbf_base_ries_invoices
         , dbf_kalypso_containers
         , dbf_ries_shipment_tracking
         , dbf_rms_orders_shipments
         , dbf_rms_orders
         , dbf_rms_item_info
         , dbf_customs_declaration
         , dbf_customs_declaration_items
    FROM temp_all;
    return 0;
end;
$function$;
