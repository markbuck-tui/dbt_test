create or replace transient table OPA_DEV.DBT_TEST.with_fl_acr_booking
      as (

    -- ,unique_key=sk_booking_id

SELECT
  bk_1.sk_booking_id as sk_booking_id
  ,bk_1.booking_version
  ,bk_1.atcom_res_id
  ,bk_1.atcom_res_version
  ,bk_1.atcom_market_id -- CG ADDED in V1.06 for new Source Market Derivation
  ,bk_1.number_of_adults
  ,bk_1.number_of_children
  ,bk_1.number_of_infants
  ,bk_1.number_of_passengers
  ,bk_1.sk_season_id
  ,bk_1.booking_status
  ,bk_1.atcom_agent_id
  ,bk_1.atcom_sell_currency_id
  ,bk_1.season_date
  ,bk_1.confirmed_on
  ,bk_1.cancelled_on
  ,bk_1.source_created_on
  ,bk_1.modified_on
  ,bk_1.effective_from
  ,DATEADD('second', -1, LEAD(bk_1.effective_from) OVER (PARTITION BY bk_1.sk_booking_id ORDER BY bk_1.booking_version)) AS lead_effective_from
  ,bk_1.effective_to
  ,bk_1.dwh_created_on
  ,bk_1.dwh_modified_on

FROM opa_stg_uk.fl_acr_booking bk_1
WHERE bk_1.file_dt = (SELECT MAX(bk_2.file_dt) FROM opa_stg_uk.fl_acr_booking bk_2 WHERE bk_1.sk_booking_id = bk_2.sk_booking_id AND bk_1.booking_version = bk_2.booking_version)
AND bk_1.booking_version = (SELECT MAX(bk_3.booking_version) FROM opa_stg_uk.fl_acr_booking bk_3 WHERE bk_1.sk_booking_id = bk_3.sk_booking_id)
AND (bk_1.sk_season_id > 201701 OR bk_1.sk_booking_id IS NULL)

-- To be removed when running against all bookings
AND bk_1.sk_booking_id IN ('380402','975528','10016009','10063844','15994298','22568921','25059884','27813713','28536240','30846203','33404409','20348866','31280892','35353771')


-- Incremental filters

-- GROUP BY 1
      );
    