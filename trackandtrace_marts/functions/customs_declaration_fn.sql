--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.customs_declaration_fn

CREATE OR REPLACE FUNCTION customs_declaration_fn()
    RETURNS boolean
    LANGUAGE plpgsql
AS $function$
begin

    ---------RDM DICTIONARY
    drop table if exists rdm_goods_declarations_statuses;
    CREATE TEMPORARY TABLE rdm_goods_declarations_statuses (status_code INTEGER, description_rus TEXT, description_eng TEXT);
    DO $$ BEGIN
        PERFORM public.rdm('ries_goods_declaration_statuses_descriptions', 'rdm_goods_declarations_statuses');
    END $$;

    ---------Invoices
-- Возможно, стоит сделать как с накладными invoice: date tmp_goods_declaration_good_documents
    drop table if exists tmp_goods_declaration_invoices;
    create temp table tmp_goods_declaration_invoices as (
        select good_number,
               goods_declaration_number,
               array_agg(docs.number) as invoice_numbers
        from goodsdeclarations_ods.v_goods_declaration_good_documents docs
        where docs.type = '04021' --тип документа - Инвойс
          and docs.is_actual = '1'
        group by good_number, goods_declaration_number
    );

---------Types of customs payment
    drop table if exists tmp_goods_declaration_good_charges;
    create temp table tmp_goods_declaration_good_charges as (
        select goods_declaration_number, good_number
             , array_agg( t.charge_type || ': ' || base ORDER BY t.charge_type) as base
             , array_agg( t.charge_type || ': ' || rate ORDER BY t.charge_type) as rate
             , array_agg( t.charge_type || ': ' || value ORDER BY t.charge_type) as tax
        from (
                 select goods_declaration_number, good_number, "type" as charge_type
                      , sum(base) as base
                      , sum(rate) as rate
                      , sum(value) as value
                 from ( select goods_declaration_number, good_number, "type", base, rate, value,
                               row_number() over(partition by goods_declaration_number, good_number, "type", base, rate, value) as rn
                        from goodsdeclarations_ods.v_goods_declaration_good_charges where is_actual ='1'
                      ) t where rn=1
                 group by goods_declaration_number, good_number,"type"
             ) t
        group by goods_declaration_number, good_number
    );


---------Info of CD DOCS in ('02011','02017', '02013', '02015', '02016','02091')
    drop table if exists tmp_goods_declaration_good_documents;
    create temp table tmp_goods_declaration_good_documents as (
        select goods_declaration_number, good_number
             , max(case when "type" ='02011' then doc_nums_dates_array else null end) as doc_02011_nums_dates  ---Коносамент
             , max(case when "type" ='02017' then doc_nums_dates_array else null end) as doc_02017_nums_dates  ---Авианакладная
             , max(case when "type" ='02013' then doc_nums_dates_array else null end) as doc_02013_nums_dates  ---ЖД накладная
             , max(case when "type" ='02015' then doc_nums_dates_array else null end) as doc_02015_nums_dates  ---CMR 1956 (Транспортная накладная)
             , max(case when "type" ='02016' then doc_nums_dates_array else null end) as doc_02016_nums_dates  ---CMR (Транспортная накладная)
             , max(case when "type" ='02091' then doc_nums_dates_array else null end) as doc_02091_nums_dates  ---Разрешительный Документ
        from ( select goods_declaration_number, good_number, "type"
                    , array_agg( "number" || ': ' || TO_CHAR("date", 'MON-DD-YYYY') ORDER BY "date" desc) as doc_nums_dates_array
                    --select distinct type
               from goodsdeclarations_ods.v_goods_declaration_good_documents
               where is_actual ='1' and type in ('02011','02017', '02013', '02015', '02016','02091')
               group by goods_declaration_number, good_number, "type"
             ) w
        group by goods_declaration_number, good_number
    );



    truncate table customs_declaration;
    insert into customs_declaration( cd_number, invoice_numbers, container_ids_capacities, modified_date, modified_time
                                   , cd_procedure, cd_status_code, cd_status, cd_status_date, registration_date, out_date, good_number
                                   , tnved_code, good_status_code, good_status, good_status_date, good_status_time, good_net_weight
                                   , good_gross_weight, good_desc, accrual_basis_payment, customs_rate, customs_tax, cd_consignment_info
                                   , cd_airbill_info, cd_railbill_info, cd_trnsprtbill_info, cd_another_trnsprtbill_info, cd_doc_info
                                   , dbf_customs_declaration)

    select gd."number" as cd_number  ---customs declaration number
         , docs.invoice_numbers
         , cont.containers as container_ids_capacities
         , cast( gd.modify_timestamp as date) as modified_date
         , cast( gd.modify_timestamp as time) as modified_time
         , gd."procedure" as cd_procedure
         , gd.status_code as cd_status_code
         , gd.status_code || ' ' || decl_statuses.description_rus as cd_status
         , gd.status_timestamp as cd_status_date
         , gd.registration_date
         , CASE WHEN gd.status_code IN ('10','12') THEN gd.status_timestamp ELSE null END as out_date
         , gdg.good_number
         , gdg.tnved_code
         , gdg.status_code as good_status_code
         , gdg.status_code || ' ' || good_statuses.description_rus as good_status
         , cast( gdg.status_timestamp as date) as good_status_date
         , cast( gdg.status_timestamp as time) as good_status_time
         , gdg.nett_weight as good_net_weight
         , gdg.gross_weight as good_gross_weight
         , gdg.description as good_desc
         , gdg_charges.base as accrual_basis_payment
         , gdg_charges.rate as customs_rate
         , gdg_charges.tax as customs_tax
         , gdg_docs.doc_02011_nums_dates as cd_consignment_info
         , gdg_docs.doc_02017_nums_dates as cd_airbill_info
         , gdg_docs.doc_02013_nums_dates as cd_railbill_info
         , gdg_docs.doc_02015_nums_dates as cd_trnsprtbill_info
         , gdg_docs.doc_02016_nums_dates as cd_another_trnsprtbill_info
         , gdg_docs.doc_02091_nums_dates as cd_doc_info
         , true as dbf_customs_declaration
    FROM goodsdeclarations_ods.v_goods_declarations gd  --Таможенная декларация
             left join goodsdeclarations_ods.v_goods_declaration_goods gdg --Товары таможенной декларации
                       on gdg.goods_declaration_number =gd."number"
                           and gdg.is_actual ='1'
             left join tmp_goods_declaration_good_documents gdg_docs
                       on gdg_docs.goods_declaration_number =gd."number"
                           and gdg_docs.good_number =gdg.good_number
        -- Посмотреть структуру справочника
             left join rdm_goods_declarations_statuses decl_statuses on gd.status_code = decl_statuses.status_code::text
             left join rdm_goods_declarations_statuses good_statuses on gdg.status_code = good_statuses.status_code::text
             left join tmp_goods_declaration_good_charges gdg_charges
                       on gdg_charges.goods_declaration_number =gd."number"
                           and gdg_charges.good_number =gdg.good_number
             left join tmp_goods_declaration_invoices docs
                       on docs.goods_declaration_number =gdg.goods_declaration_number
                           and docs.good_number =gdg.good_number
             left join ( select goods_declaration_number, good_number
                              , array_agg(number || ': ' || status) as containers
                         from goodsdeclarations_ods.v_goods_declaration_good_containers
                         where is_actual ='1'
                         group by goods_declaration_number, good_number
    ) cont on cont.goods_declaration_number =gd.number
        and cont.good_number =gdg.good_number
    where gd.is_actual ='1'  ;

    return 0;
end;
$function$;
