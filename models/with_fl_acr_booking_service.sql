{{
  config(materialized='view')
}}

SELECT
  bk_ser_1.sk_booking_service_id
  ,bk_ser_1.sk_booking_id
  ,bk_ser_1.sk_service_id
  ,bk_ser_1.service_version
  ,bk_ser_1.booking_version
FROM opa_stg_uk.fl_acr_booking_service bk_ser_1
WHERE bk_ser_1.file_dt = (SELECT MAX(bk_ser_2.file_dt) FROM opa_stg_uk.fl_acr_booking_service bk_ser_2 WHERE bk_ser_1.sk_booking_service_id = bk_ser_2.sk_booking_service_id)
AND bk_ser_1.booking_version = (SELECT MAX(bk_ser_3.booking_version) FROM opa_stg_uk.fl_acr_booking_service bk_ser_3 WHERE bk_ser_1.sk_booking_id = bk_ser_3.sk_booking_id)
AND bk_ser_1.service_version = (SELECT MAX(bk_ser_4.service_version) FROM opa_stg_uk.fl_acr_booking_service bk_ser_4 WHERE bk_ser_1.sk_service_id = bk_ser_4.sk_service_id)

-- To be removed when running against all bookings
AND bk_ser_1.sk_booking_id IN ('380402','975528','10016009','10063844','15994298','22568921','25059884','27813713','28536240','30846203','33404409','20348866','31280892','35353771')
