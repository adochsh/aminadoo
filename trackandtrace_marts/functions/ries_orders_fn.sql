--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.ries_orders

CREATE OR REPLACE FUNCTION ries_orders_fn()
    RETURNS boolean
    LANGUAGE plpgsql
AS $function$
begin

    drop table if exists tmp_ord_arts;
    drop table if exists tmp_need_samples_oc;

    create temp table tmp_need_samples_oc as (
        select oc_id
             , order_id
             , case
                   when samples_sent <> 0 then 'Samples dispatched'
                   when tnso.sr_yes <> 0 then 'Need samples'
                   when tnso.sr_no_info <> 0 then 'No info yet'
                   when tnso.sr_no <> 0 then 'No need samples'
                   else 'No info yet' end
            as samples_required
        from (
                 select order_confirmation_number as oc_id
                      , order_number as order_id
                      , count(case oa.aggregate_state_samples_required when 'Yes' then 1 else null end) as sr_yes
                      , count(case oa.aggregate_state_samples_required when 'No info yet' then 1 when null then 1 else null end) as sr_no_info
                      , count(case oa.aggregate_state_samples_required when 'No' then 1 else null end) as sr_no
                      , count(case oa.aggregate_state_sample_state_status when 'samplesSent' then 1 else null end) as samples_sent
                 from ries_portal_ods.v_order_articles oa
                 where oa.status<>'ocCancelled' and oa.is_canceled=False
                 group by oa.order_confirmation_number, order_number) tnso
    );

    create temp table tmp_ord_arts as (
        select adeo_code
             , lm_code
             , ean_code
             , manufacturer
             , description_eng
             , quantity_ordered
             , total_amount
             , aggregate_state_cert_obligation
             , aggregate_state_samples_amount
             , order_number
             , status
             , order_confirmation_number
             , ord_arts.aggregate_state_samples_required
             , ord_arts.aggregate_state_serial_doc_number
             , ord_arts.aggregate_state_availability_of_a_valid_serial_doc
             , ord_arts.aggregate_state_document_of_conformity_type
             , ord_arts.aggregate_state_date_of_expiry_of_batch_doc
             , ord_arts.aggregate_state_date_of_expiry_of_serial_doc
             , ord_arts.aggregate_state_batch_doc_number
             , ord_arts.aggregate_state_regulation_numbers
             , ord_arts.aggregate_state_calculated_etd
             , ord_arts.aggregate_state_comments_for_etd
             , ord_arts.aggregate_state_sample_state_awb_or_container_no_for_samples_sending
             , ord_arts.aggregate_state_sample_state_pi_number
             , ord_arts.aggregate_state_sample_state_initial_pi_sending_date
             , ord_arts.aggregate_state_sample_state_deadline_of_pi_sending_date
             , ord_arts.aggregate_state_sample_state_sample_sending_way
             , ord_arts.aggregate_state_sample_state_sample_sending_date
             , ord_arts.aggregate_state_sample_state_deadline_for_sample_sending
             , ord_arts.aggregate_state_sample_state_kpi_of_sample_sending
             , ord_arts.aggregate_state_sample_state_reason_of_sample_sending_late
             , ord_arts.aggregate_state_sample_state_late_days_of_pi
             , ord_arts.aggregate_state_sample_state_status
             , ord_arts.samples_required
        from (
                 select adeo_code
                      , lm_code
                      , ean_code
                      , manufacturer
                      , description_eng
                      , quantity_ordered
                      , total_amount
                      , aggregate_state_cert_obligation
                      , aggregate_state_samples_amount
                      , order_number
                      , status
                      , order_confirmation_number
                      , ord_arts.aggregate_state_samples_required
                      , ord_arts.aggregate_state_serial_doc_number
                      , ord_arts.aggregate_state_availability_of_a_valid_serial_doc
                      , ord_arts.aggregate_state_document_of_conformity_type
                      , ord_arts.aggregate_state_date_of_expiry_of_batch_doc
                      , ord_arts.aggregate_state_date_of_expiry_of_serial_doc
                      , ord_arts.aggregate_state_batch_doc_number
                      , ord_arts.aggregate_state_regulation_numbers
                      , ord_arts.aggregate_state_calculated_etd
                      , ord_arts.aggregate_state_comments_for_etd
                      , ord_arts.aggregate_state_sample_state_awb_or_container_no_for_samples_sending
                      , ord_arts.aggregate_state_sample_state_pi_number
                      , ord_arts.aggregate_state_sample_state_initial_pi_sending_date
                      , ord_arts.aggregate_state_sample_state_deadline_of_pi_sending_date
                      , ord_arts.aggregate_state_sample_state_sample_sending_way
                      , ord_arts.aggregate_state_sample_state_sample_sending_date
                      , ord_arts.aggregate_state_sample_state_deadline_for_sample_sending
                      , ord_arts.aggregate_state_sample_state_kpi_of_sample_sending
                      , ord_arts.aggregate_state_sample_state_reason_of_sample_sending_late
                      , ord_arts.aggregate_state_sample_state_late_days_of_pi
                      , ord_arts.aggregate_state_sample_state_status
                      , tnso.samples_required
                      , row_number() over(partition by pk_id  order by version desc) as rn
                 from ries_portal_ods.v_order_articles ord_arts
                 left join tmp_need_samples_oc tnso on tnso.oc_id = ord_arts.order_confirmation_number
                 where status<>'ocCancelled' and is_canceled=False
             ) ord_arts where rn =1
    );

truncate table ries_orders;
insert into ries_orders (order_id, order_date, order_status, oc_id, priority, oc_date, adeo_code, lm_code, ean_code, etd_oc, eta_oc
    , port_of_loading, incoterms, season_validity_date_from, season_validity_date_to, oc_total_amount, route_id, manufacturer
    , desc_eng, ries_order_qty, article_total_amount, under_certification, samples_qty, samples_required, doc_serial_id
    , doc_serial_valid, doc_type, doc_batch_issue, doc_serial_expiry, doc_batch_id, tech_regulation, calculated_etd, comments_for_etd
    , awb_or_container_samples_id, samples_pi_id, initial_pi_sending_date, deadline_pi_sending_date, sample_sending_way
    , sample_sending_date, deadline_sample_sending_date, sample_sending_kpi, reason_sample_sending_late, sample_sending_delay
    , transport_instruction, dbf_ries_orders, samples_sent_code, samples_required_code)
select ord."number"::text as order_id  -- номер заказа
	 , ord.creation_date as order_date
     , ord.status as order_status
     , oc."number" as oc_id  -- OC заказа
     , ord.priority
     , oc.order_confirmation_date::date as oc_date
	 , ord_arts.adeo_code::text
	 , ord_arts.lm_code::text
	 , ord_arts.ean_code::text
	 , (oc.etd::date) as etd_oc
     , (oc.eta::date) as eta_oc
     , (oc.port_of_loading) as port_of_loading
     , (oc.delivery_terms) as incoterms
     , (oc.season_validity_date_from)::date
	 , (oc.season_validity_date_to)::date
	 , (oc.total_amount) as oc_total_amount
	 , oc.route_id
     , (ord_arts.manufacturer)
     , (ord_arts.description_eng) as desc_eng
     , (ord_arts.quantity_ordered)::numeric as ries_order_qty
	 , (ord_arts.total_amount) as article_total_amount
     , (ord_arts.aggregate_state_cert_obligation) as under_certification
     , (ord_arts.aggregate_state_samples_amount) as samples_qty
     , (ord_arts.aggregate_state_samples_required) as samples_required
     , (ord_arts.aggregate_state_serial_doc_number) as doc_serial_id
     , (ord_arts.aggregate_state_availability_of_a_valid_serial_doc) as doc_serial_valid
     , (ord_arts.aggregate_state_document_of_conformity_type) as doc_type
     , (ord_arts.aggregate_state_date_of_expiry_of_batch_doc) as doc_batch_issue
     , (ord_arts.aggregate_state_date_of_expiry_of_serial_doc) as doc_serial_expiry
     , (ord_arts.aggregate_state_batch_doc_number) as doc_batch_id
     , (ord_arts.aggregate_state_regulation_numbers) as tech_regulation
     , (ord_arts.aggregate_state_calculated_etd) as calculated_etd
     , (ord_arts.aggregate_state_comments_for_etd) as comments_for_etd
     , (ord_arts.aggregate_state_sample_state_awb_or_container_no_for_samples_sending) as awb_or_container_samples_id
     , (ord_arts.aggregate_state_sample_state_pi_number) as samples_pi_id
     , (ord_arts.aggregate_state_sample_state_initial_pi_sending_date) as initial_pi_sending_date
     , (ord_arts.aggregate_state_sample_state_deadline_of_pi_sending_date) as deadline_pi_sending_date
     , (ord_arts.aggregate_state_sample_state_sample_sending_way) as sample_sending_way
     , (ord_arts.aggregate_state_sample_state_sample_sending_date) as sample_sending_date
     , (ord_arts.aggregate_state_sample_state_deadline_for_sample_sending) as deadline_sample_sending_date
     , (ord_arts.aggregate_state_sample_state_kpi_of_sample_sending) as sample_sending_kpi
     , (ord_arts.aggregate_state_sample_state_reason_of_sample_sending_late) as reason_sample_sending_late
     , (ord_arts.aggregate_state_sample_state_late_days_of_pi) as sample_sending_delay
     , ord_arts.samples_required as transport_instruction
     , true as dbf_ries_orders
     , CASE ord_arts.aggregate_state_sample_state_status
               WHEN 'samplesSent' THEN 3
           END as samples_sent_code
         , CASE ord_arts.aggregate_state_samples_required
               WHEN 'Yes' THEN 2
               WHEN 'No' THEN 1
        END as samples_required_code
FROM (select number, creation_date, priority, status, row_number() over(partition by pk_id  order by version desc) as rn
      FROM ries_portal_ods.v_order ) ord   ----v_order
         INNER JOIN (select number
                          , order_number
                          , is_active
                          , etd
                          , order_confirmation_date
                          , eta
                          , port_of_loading
                          , delivery_terms
                          , season_validity_date_from
                          , season_validity_date_to
                          , total_amount
                          , route_id
                          , row_number() over(partition by pk_id  order by version desc) as rn
                     from ries_portal_ods.v_order_confirmation   ----v_order_confirmation
        ) oc on oc.order_number = ord."number" and oc.is_active=True and oc.rn=1
         LEFT JOIN tmp_ord_arts ord_arts on ord_arts.order_number = ord."number"
                                            and  ord_arts.order_confirmation_number = oc."number"
                                            and  ord_arts.status<>'ocCancelled'
WHERE ord.rn=1;

return 0;
end;
$function$;

