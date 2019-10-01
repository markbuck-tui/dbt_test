{{
  config(materialized='view')
}}

SELECT DISTINCT
  mk_1.mkt_id
  ,mk_1.off_id
FROM opa_stg_uk.ar_market mk_1
WHERE mk_1.file_dt = (SELECT MAX(mk_2.file_dt) FROM opa_stg_uk.ar_market mk_2 WHERE mk_1.mkt_id = mk_2.mkt_id)