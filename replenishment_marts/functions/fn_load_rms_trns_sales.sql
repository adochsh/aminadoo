--liquibase formatted sql
--changeset 60098727:create:function:fn_load_rms_trns_sales
CREATE OR REPLACE FUNCTION fn_load_rms_trns_sales(period_start date, period_end date)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
declare
    v_counter_inserted int;
begin

    CREATE TEMP TABLE potok_stock_dict (lm_code text);

    INSERT INTO potok_stock_dict (lm_code)
    SELECT  cast(uil.item as text)
    FROM rms_p009qtzb_rms_ods.v_uda_item_lov uil
    WHERE uil.uda_id = 12 AND uil.is_actual = '1';

    raise notice '==================================== START =====================================';
    raise notice '[%] Inserting into temp tables' , date_trunc('second' , clock_timestamp())::text;

    DELETE FROM rms_trns_sales
    WHERE opened_date BETWEEN period_start AND period_end;

    INSERT INTO rms_trns_sales (lm_code, opened_date, store_num, line_type, qty_sold, price, unit_cost, ca_ttc)
    SELECT line_item_id::bigint AS lm_code,
              cast(opened_date as date) AS opened_date,
              store_id AS store_num,
              line_type,
              sum(line_quantity) AS qty_sold,
              sum(line_item_price) AS price,
              sum(line_item_cost) AS unit_cost,
              sum(line_total_sum_after_pricecorrection_with_vat) AS ca_ttc
    FROM dds.v_receipt_lines rl
    JOIN potok_stock_dict stock ON stock.lm_code = rl.line_item_id
    JOIN rms_p009qtzb_rms_ods.v_uda_item_lov uil ON uil.item = rl.line_item_id
    JOIN rms_p009qtzb_rms_ods.v_uda_values uv ON  uil.uda_id = uv.uda_id AND uil.uda_value = uv.uda_value AND uil.uda_id = 5
    WHERE cast(opened_date as date) BETWEEN period_start AND period_end
          AND line_type IN ('Sales', 'Returns', 'pickedUp orders')
          AND coalesce(tpnet_line_operation_cancel_flag , 0) <> -1
          AND line_item_type = 'Normal'
          AND  (tpnet_receipt_type IN ('RT', 'RR')
                    OR tpnet_receipt_type IN ('SA', 'FI')
                    OR (tpnet_receipt_type= 'NM' AND
                        tpnet_receipt_operation_type <> 'EFT_MAINT'))
          AND uv.uda_value_desc <> 'S'
          AND uil.is_actual = '1' AND uv.is_actual = '1'
    GROUP BY line_item_id::bigint,
          cast(opened_date as date),
          store_id,
          line_type;

    get diagnostics v_counter_inserted = row_count;
    raise notice '[%] Inserted % rows into replenishment_marts.rms_trns_sales' , date_trunc('second' , clock_timestamp())::text , v_counter_inserted::text;
    perform public.fn_analyze_table('replenishment_marts','rms_trns_sales');
    raise notice '[%] Function finished.' , date_trunc('second' , clock_timestamp())::text;
    raise notice '==================================== FINISH =====================================';
    return 0;
end;
$function$
;
