

    -- ,unique_key='sk_booking_service_id' -- bk in the target table

SELECT
  bk_ser_1.sk_booking_service_id
  ,bk_ser_1.sk_booking_id
  ,bk_ser_1.sk_service_id
  ,bk_ser_1.service_version
  ,bk_ser_1.booking_version
  ,bk_ser_1.file_dt
FROM opa_stg_uk.fl_acr_booking_service_stream_test bk_ser_1
-- WHERE bk_ser_1.file_dt = (SELECT MAX(bk_ser_2.file_dt) FROM opa_stg_uk.fl_acr_booking_service bk_ser_2 WHERE bk_ser_1.sk_booking_service_id = bk_ser_2.sk_booking_service_id)
-- AND bk_ser_1.booking_version = (SELECT MAX(bk_ser_3.booking_version) FROM opa_stg_uk.fl_acr_booking_service bk_ser_3 WHERE bk_ser_1.sk_booking_id = bk_ser_3.sk_booking_id)
-- AND bk_ser_1.service_version = (SELECT MAX(bk_ser_4.service_version) FROM opa_stg_uk.fl_acr_booking_service bk_ser_4 WHERE bk_ser_1.sk_service_id = bk_ser_4.sk_service_id)


-- Incremental filters

  -- this filter will only be applied on an incremental run
  WHERE file_dt >= (SELECT MAX(file_dt) FROM opa_stg_uk.fl_acr_booking_service_stream_test)
