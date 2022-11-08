--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.kalypso_containers_fn

CREATE OR REPLACE FUNCTION kalypso_containers_fn()
RETURNS boolean
LANGUAGE plpgsql
AS $function$
begin

truncate table kalypso_containers;
insert into kalypso_containers(  oc_id, order_id, rms_order_id, adeo_order_id, invoice_id, container_id1, container_id2, container_creating_date, adeo_code, adeo_dep_name
	, delivery_unit_ordered_unit_code, delivery_type, production_diff, purchasing_incoterm_code, purchasing_incoterm_city_code, country_of_loading_code
	, country_of_loading_name, city_of_discharge_code, city_of_discharge_name, status_kalypso, shipped_container_number, delivered_container_number
	, shipped_information_number_of_units, to_be_shipped_information_number_of_units, to_be_shipped_information_volume, shipped_information_volume
	, updated_delivery_date, updated_shipping_date, shipment_comments, confirmed_delivery_date, confirmed_shipping_date, shipment_release_date, selling_invoice_date
	, container_type_1st_leg, loading_type_1st_leg, container_type_2nd_leg, loading_type_2nd_leg, forwarder_id, main_transport_company_id, bu_name, dbf_kalypso_containers)
select ord_arts.bu_contract as oc_id
   , ord_arts.contractual_order_number as order_id -- номер заказа в калипсо
   , ord_arts.bu_order as rms_order_id -- rms order
   , ord."number" as adeo_order_id  --order Adeo
   , ship_arts.invoice_number as invoice_id -- invoiceNumber
   , ship_arts.shipped_container_number as container_id1
   , ship_arts.delivered_container_number as container_id2
   , coalesce(cont2.container_creating_date, cont1.container_creating_date) as container_creating_date
   , ord_arts.international_product_item_id as adeo_code
   ---< contractual_ord_arts >----
   , ord_arts.international_department_name as adeo_dep_name
   , ord_arts.delivery_unit_ordered_unit_code ---
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
   , ship_arts.order_line_status_name as status_kalypso
   , ship_arts.shipped_container_number
   , ship_arts.delivered_container_number
   , ship_arts.shipped_information_number_of_units::numeric
   , ship_arts.to_be_shipped_information_number_of_units::numeric
   , ship_arts.to_be_shipped_information_volume
   , ship_arts.shipped_information_volume
   , ship_arts.updated_delivery_date
   , ship_arts.updated_shipping_date
   , ship_arts."comments" as shipment_comments
   , ship_arts.confirmed_delivery_date
   , ship_arts.confirmed_shipping_date
   , ship_arts.shipment_release_date
   , ship_arts.selling_invoice_date
   ---< contractual_order_containers 1 leg >----
   , cont1.category_type as container_type_1st_leg     --Тип контейнера на 1 плече
   , cont1.loading_category as loading_type_1st_leg    --Тип загрузки (CFS/LCL/LM3/FCL)
   ---< contractual_order_containers 2 leg >----
   , cont2.category_type as container_type_2nd_leg     --Тип контейнера на 2 плече
   , cont2.loading_category as loading_type_2nd_leg    --Тип загрузки (CFS/LCL/LM3/FCL)
   , cont2.forwarder_id      --Экспедитор АДЕО
   , cont2.main_transport_company_id      --Компания, которая осуществляет перевозку
   ---< contractual_orders >----
   , ord.bu_name
   , true as dbf_kalypso_containers
from ries_report_ods.v_contractual_orders ord
 left join ries_report_ods.v_contractual_order_shipment_articles ship_arts
    				 on ship_arts.contractual_order_number = ord."number"
    			    and ship_arts.order_line_status_name not in ('CANCELLED', 'NOT CONFIRMED')
    			    and ship_arts.is_actual='1'
 left join ( select bu_contract, contractual_order_number, bu_order , international_product_item_id ,international_department_name
	  		 	, max(delivery_unit_ordered_unit_code) as delivery_unit_ordered_unit_code -- Зачем? Почему?
		   from ries_report_ods.v_contractual_order_articles where is_actual='1'
		   group by 1,2,3,4,5
	  ) ord_arts on ord_arts.contractual_order_number =ord."number"
		        and ord_arts.international_product_item_id =ship_arts.international_product_item_id
 left join ries_report_ods.v_contractual_order_transport_legs legs
 					on legs.contractual_order_number =ord."number"
 				 and legs.is_actual='1'
 left join ries_report_ods.v_contractual_order_containers cont1
		  			on cont1."number" =ship_arts.shipped_container_number
			       and cont1.contractual_order_number =ord."number"
			       and cont1.is_actual='1'
 left join ries_report_ods.v_contractual_order_containers cont2
 					on cont2."number" =ship_arts.delivered_container_number
 				   and cont1.container_creating_date =cont2.container_creating_date -- Зачем? Почему?
 				   and cont2.contractual_order_number =ord."number"
 				   and cont2.is_actual ='1'
where ord.is_actual ='1';

return 0;
end;
$function$;
