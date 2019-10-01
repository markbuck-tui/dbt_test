{{
  config(materialized='view')
}}

SELECT
  sr.stc_rm_id
  ,sr.stc_stk_id
  ,sr.rm_id
FROM opa_stg_uk.ar_staticroom sr
WHERE sr.file_dt = (SELECT MAX(sr_2.file_dt) FROM opa_stg_uk.ar_staticroom sr_2 WHERE sr.stc_rm_id = sr_2.stc_rm_id)
