--liquibase formatted sql

--changeset 60098727:create:table:trackandtrace_marts.import_datamart

CREATE TABLE import_datamart_v2 (
    supplier_mode text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    direction text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    order_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    order_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    adeo_order_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    oc_number text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    oc_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    adeo_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    lm_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ean_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    lm_name_ru text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    lm_name_eng text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    status_global text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    rms_order_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    eta_wh_calc date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    second_acceptance_wh_plan date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    eta_wh_calc_ym text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    eta_wh_calc_yw text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dep_code int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dep_name text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dep text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    sub_dep_code int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    sub_dep_name text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    sub_dep text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    supplier_code int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    supplier_name text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    supplier text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    country text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    flow_type text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    top_1000 text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    mdd text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    brand text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    best_price text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    gamma text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    priority text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    loc int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    order_qty numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    shipped_qty numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    received_qty numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    remain_to_ship_qty numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dpac_item numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    received_dpac_amount numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    remain_dpac_amount numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    item_amount numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    order_amount numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    incoterms text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    season_validity_date_from date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    season_validity_date_to date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    supplier_contract_number text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    supplier_contract_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    payment_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    status_kalypso text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    adeo_dep_name text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    bu_name text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    swb text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    transport text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_number text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    actual_container text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    actual_number_original text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_number1 text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_number2 text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    modified_container text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_type text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_type_1st_leg text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_type_2nd_leg text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    loading_type text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    loading_type_1st_leg text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    loading_type_2nd_leg text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    delivery_unit_volume numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    delivery_unit_ordered_unit_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    forwarder_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    main_transport_company_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    pol text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    pod text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    destination_rail_station text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    wh text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    etd_oc date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    eta_oc date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ship_release_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ship_deadline date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    truck_arr_pol_fact date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    etd_pol_plan date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    etd_confirmed date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    rdd_pol date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    eta_confirmed date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    etd_pod_updated date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    etd_pod_plan_fwr date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    eta_pod_plan date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    rta_pod date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    transport_from_pod text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    custom_in_plan date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    custom_in_fact date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    etd_pod_plan date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    rdd_pod date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    rta_rail_station date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    rdd_rail_station date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    arrival_wh_plan date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    arrival_wh_fact date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    drop_off date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    receiving_date1 date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    receiving_date2 date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    forwarder_comments text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    tu text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    release_to text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    release_to_modified text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    loading_percent numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    customs_terminal text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    customs_broker text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    comments_broker text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    fwr_appl_create_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    fwr_appl_confirm_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_total_volume numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_qty numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_qty_pkgs numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_price numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_curr text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_total_amount numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_net_weight numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_gross_weight numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_pallets numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    letter_of_credit text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    comments_kalypso text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_number text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_proc text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_date timestamp  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_status_date timestamp  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_out timestamp  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_decision text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_decision_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_decision_time time  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_modified_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_modified_time time  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_good_id int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cn_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    invoice_hs_code text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_good_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_good_status_date timestamp  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_good_status_time time  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_good_desc text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    container_capacity text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_net_weight numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    good_gross_weight numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_customs_type int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    accrual_basis_payment text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    customs_rate text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    customs_tax text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_consignment_info text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_consignment_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_consignment_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_airbill_info text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_airbill_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_airbill_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_railbill_info text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_railbill_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_railbill_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_trnsprtbill_info text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_trnsprtbill_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_trnsprtbill_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_another_trnsprtbill_info text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_another_trnsprtbill_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_another_trnsprtbill_date date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_doc_info text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_doc_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_doc_issuedate date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_lm_qty numeric  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_good_unit_type text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_lm_desc text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_lm_manufacturer text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    under_certification text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_type text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    tech_regulation text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_serial_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_serial_issue text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_serial_expiry text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_serial_valid text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_batch_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_batch_issue text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    gln text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    manufacturer_address text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    production_site_address text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    samples_required text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    samples_qty text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    samples_pi_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    initial_pi_sending_date text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    deadline_pi_sending_date text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    sample_sending_way text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    sample_sending_date text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    awb_or_container_samples_id text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    deadline_sample_sending_date text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    reason_sample_sending_late text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768), --- ?
    oc_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    arf_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    pi_sample_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    current_order_task text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_conformity_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_ship_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    doc_set_status text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    status_oc text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    status_invoice text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    import_specialist text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    appro_specialist text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_specialist text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    certification_specialist text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    custom_in_calc date  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    prod_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    prod_deviation int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ship_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ship_deviation int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    pod_arr_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    pod_arr_deviation int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    custom_in_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    custom_in_delay_days int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_request_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    cd_request_delay int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    custom_out_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    custom_out_delay_days int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    sample_sending_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    sample_sending_delay int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    arrival_wh_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    arrival_wh_deviation int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    second_acceptance_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    second_acceptance_delay int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    eta_wh_kpi text  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    eta_wh_deviation int  ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_base_ries_invoices boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_customs_declaration boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_customs_declaration_items boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_kalypso_containers boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_ries_orders boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_ries_shipment_tracking boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_rms_item_info boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_rms_orders boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    dbf_rms_orders_shipments boolean ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768)
)
    WITH (appendonly='true', compresslevel='1', orientation='column', compresstype=zstd)
    distributed by (order_id)
    PARTITION BY RANGE (order_date) (DEFAULT PARTITION other);

--changeset 60067166:alter:table:import_datamart_v2:add_transport_instruction
ALTER TABLE import_datamart_v2
    ADD COLUMN route_id integer ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN samples_sent_code integer ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN samples_required_code integer ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN calculated_etd text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN comments_for_etd text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768),
    ADD COLUMN transport_instruction text ENCODING (compresslevel=1,compresstype=zstd,blocksize=32768);
