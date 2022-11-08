create or replace view v_rms_orders_shipments as
 select order_id
   , invoice_id
   , lm_code
   , adeo_code
   , invoice_upload
   , receiving_date1
   , receiving_date2
   , to_loc
   , ship_expected
   , ship_received
   , dbf_rms_orders_shipments
 from rms_orders_shipments;
