create or replace view v_rms_orders as
 select order_id
   , lm_code
   , supplier_code
   , supplier_name
   , v_loc
   , loc
   , loc_name
   , order_date
   , status
   , unit_price
   , dpac_item
   , order_qty
   , received_qty
   , order_dpac_amount
   , received_dpac_amount
   , order_amount
   , received_amount
   , status_oc
   , dbf_rms_orders
 from rms_orders;
