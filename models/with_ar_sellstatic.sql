{{
  config(materialized='ephemeral')
}}

SELECT
  sls_1.sell_stc_id
  ,sls_1.stc_stk_id
FROM opa_stg_uk.ar_sellstatic sls_1
WHERE sls_1.file_dt = (SELECT MAX(sls_2.file_dt) FROM opa_stg_uk.ar_sellstatic sls_2 WHERE sls_1.sell_stc_id = sls_2.sell_stc_id)
