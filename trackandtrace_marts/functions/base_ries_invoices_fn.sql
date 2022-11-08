--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.base_ries_invoices_fn

CREATE OR REPLACE FUNCTION base_ries_invoices_fn()
    RETURNS boolean
    LANGUAGE plpgsql
AS $function$
begin


    truncate table base_ries_invoices;
    insert into base_ries_invoices (invoice_id, invoice_status, container_id, oc_id, order_id, shipment_id, rms_id
                                   , adeo_order_number, delivery_terms, invoice_date, payment_date, seal, container_type, invoice_total_volume
                                   , invoice_qty, invoice_qty_pkgs, invoice_total_amount, invoice_net_weight, invoice_gross_weight, invoice_pallets
                                   , adeo_code, lm_code, ean_code, invoice_price, invoice_curr, invoice_hs_code, dbf_base_ries_invoices)
    select b.invoice_number as invoice_id---->>	Kalypso / RIES (инвойс) contractual_order_shipment_articles->invoice_number
         , b.status as invoice_status ---->>	RIES base_invoices -> status
         , b.container_number as container_id
         , b.oc_number as oc_id
         , b.order_number as order_id
         , b.shipment_number as shipment_id
         , b.rms_id
         , b.adeo_order_number
         , ries_inv.delivery_terms::text ---->> Incoterms ->>order_confirmations / ries_invoices --> delivery_terms / delivery_terms
         , ries_inv.invoice_date::date ---->> invoice_date -> contractual_order_shipment_articles / ries_invoices  --->selling_invoice_date / invoice_date
         , ries_inv.payment_date::date
         , ries_inv.seal::text
         , ries_inv.container_type::text ---->> contractual_order_containers / ries_invoice / containers --->> 1leg->2leg->if change / container_type / type
         , ries_inv.total_volume::numeric	 	 as invoice_total_volume
         , ries_inv.total_quantity::numeric	 	 as invoice_qty
         , ries_inv.total_packages::numeric	 	 as invoice_qty_pkgs
         , ries_inv.total_amount::numeric	 	 as invoice_total_amount
         , ries_inv.total_weight_net::numeric	 	 as invoice_net_weight
         , ries_inv.total_weight_gross::numeric	 	 as invoice_gross_weight
         , ries_inv.total_pallets::numeric	 	 as invoice_pallets
         , bia.adeo_code as adeo_code
         , bia.lm_code as lm_code
         , ries_inv_arts.ean_code as ean_code
         , ries_inv_arts.price::numeric	 as invoice_price
         , ries_inv_arts.currency as invoice_curr
         , ries_inv_arts.hs_code::text  as invoice_hs_code
         , true as dbf_base_ries_invoices
    from (select b.invoice_number
               , b.status
               , b.container_number
               , b.oc_number
               , b.order_number
               , b.shipment_number
               , b.rms_id
               , b.adeo_order_number
               , row_number() over(partition by id order by "version" desc) as rn
          from ries_entities_ods.v_base_invoices b) b
             left join ries_entities_ods.v_base_invoice_articles bia on b.invoice_number = bia.invoice_number
             left join ries_entities_ods.v_ries_invoices ries_inv on ries_inv.invoice_number =b.invoice_number
        and ries_inv.oc_number =b.oc_number
             left join ries_entities_ods.v_ries_invoice_articles	ries_inv_arts on ries_inv_arts.invoice_number =b.invoice_number
        and ries_inv_arts.lm_code = bia.lm_code
    where b.rn=1;
    return 0;
end;
$function$;
