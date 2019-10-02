create or replace transient table OPA_DEV.DBT_TEST.with_fl_acr_service_element
      as (

    -- ,unique_key='sk_service_element_id' -- bk in the target table

SELECT
  ser_e_1.sk_service_element_id
  ,ser_e_1.sk_service_id
  ,ser_e_1.service_version
  ,ser_e_1.atcom_sub_ser_id
FROM opa_stg_uk.fl_acr_service_element ser_e_1
WHERE ser_e_1.file_dt = (SELECT MAX(ser_e_2.file_dt) FROM opa_stg_uk.fl_acr_service_element ser_e_2 WHERE ser_e_1.sk_service_element_id = ser_e_2.sk_service_element_id)


-- Incremental filters

-- GROUP BY 1
      );
    