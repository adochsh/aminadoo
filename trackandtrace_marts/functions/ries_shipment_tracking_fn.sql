--liquibase formatted sql
--changeset 60115905:create:trackandtrace_marts.ries_shipment_tracking_fn

CREATE OR REPLACE FUNCTION ries_shipment_tracking_fn()
RETURNS boolean
LANGUAGE plpgsql
AS $function$
begin

truncate table ries_shipment_tracking;
insert into ries_shipment_tracking ( oc_id, invoice_id, actual_container, container_id, swb, modified_container
	, loading_type, container_type, custom_in_plan, destination_rail_station, truck_arr_pol_fact, rdd_pol, etd_pod_plan_fwr
	, rta_pod, etd_pod_plan, rdd_pod, rdd_rail_station, drop_off, rta_rail_station, arrival_wh_plan, arrival_wh_fact
	, forwarder_comments, tu, release_to, release_to_modified, transport_from_pod, loading_percent, fwr_appl_create_date
	, fwr_appl_confirm_date, customs_terminal, customs_broker, comments_broker, custom_in_fact, import_specialist
	, cd_specialist, certification_specialist, port_of_discharge, etd_pol_plan, dbf_ries_shipment_tracking
)
select oc_id, invoice_id, actual_container, container_id, swb, modified_container
       , loading_type, container_type, custom_in_plan, destination_rail_station, truck_arr_pol_fact, rdd_pol, etd_pod_plan_fwr
       , rta_pod, etd_pod_plan, rdd_pod, rdd_rail_station, drop_off, rta_rail_station, arrival_wh_plan, arrival_wh_fact
       , forwarder_comments, tu, release_to, release_to_modified, transport_from_pod, loading_percent, fwr_appl_create_date
       , fwr_appl_confirm_date, customs_terminal, customs_broker, comments_broker, custom_in_fact, import_specialist
       , cd_specialist, certification_specialist, port_of_discharge, etd_pol_plan, dbf_ries_shipment_tracking from (
    select ship."orderConfirmationOrProformaInvoiceNumber"    as oc_id
	  , ship."invoiceNumber"    as invoice_id
	  , ship."actualContainerNumber"    as actual_container
	  , ship."containerNumber" as container_id
	  , ship."number" as  swb
	  , ship."modifiedContainerNumber"  as  modified_container
	  , ship."loadingType"  as  loading_type
	  , containers."type" as  container_type
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
      , true as dbf_ries_shipment_tracking
      , row_number() over(partition by ship.id  order by ship.version desc) as rn
from ries_report_ods.v_shipments ship
left join ries_report_ods.v_containers containers on ship."actualContainerNumber"=containers.number and containers.is_actual ='1'
where ship.is_actual ='1') t
where t.rn = 1;

return 0;
end;
$function$;
