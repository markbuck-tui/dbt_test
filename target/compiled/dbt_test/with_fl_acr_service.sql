

    -- ,unique_key=concat('sk_service_id', '|', 'service_version') -- bk in the target table

SELECT
  ser_1.sk_service_id
  ,ser_1.atcom_ser_id
  ,ser_1.atcom_dep_point_id
  ,ser_1.service_version
  ,ser_1.service_status
  ,ser_1.direction
  ,ser_1.sell_type
  ,ser_1.service_type
  ,ser_1.flight_type_code
  ,ser_1.service_start_date1
  ,ser_1.service_end_date1
  ,ser_1.departure_flight_number
  ,ser_1.atcom_arr_point_id
  ,ser_1.source_stock_type_code
  ,ser_1.file_dt
FROM opa_stg_uk.fl_acr_service_stream_test ser_1
WHERE ser_1.file_dt = (SELECT MAX(ser_2.file_dt) FROM opa_stg_uk.fl_acr_service ser_2 WHERE ser_1.sk_service_id = ser_2.sk_service_id AND ser_1.service_version = ser_2.service_version)



-- Incremental filters

  -- this filter will only be applied on an incremental run
  AND file_dt >= (SELECT MAX(file_dt) FROM opa_stg_uk.fl_acr_service_stream_test)
