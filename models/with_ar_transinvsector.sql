{{
  config(materialized='view')
}}

SELECT
  tis.trans_inv_sec_id
  ,tis.dep_dt_tm
FROM opa_stg_uk.ar_transinvsector tis
WHERE tis.file_dt = (SELECT MAX(tis_2.file_dt) FROM opa_stg_uk.ar_transinvsector tis_2 WHERE tis.trans_inv_sec_id = tis_2.trans_inv_sec_id)
