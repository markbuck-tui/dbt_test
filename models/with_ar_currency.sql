{{
  config(materialized='ephemeral')
}}

SELECT DISTINCT
  cur_1.cur_id
  ,cur_1.cd
  ,cur_1.name
FROM opa_stg_uk.ar_currency cur_1
WHERE cur_1.file_dt = (SELECT MAX(cur_2.file_dt) FROM opa_stg_uk.ar_currency cur_2 WHERE cur_1.cur_id = cur_2.cur_id)
