{{
  config(materialized='view')
}}

SELECT
  tirs.trans_inv_route_id
  ,tirs.trans_inv_sec_id
  ,tirs.trans_inv_route_sec_id
FROM opa_stg_uk.ar_transinvroutesector tirs
WHERE tirs.file_dt = (SELECT MAX(tirs_2.file_dt) FROM opa_stg_uk.ar_transinvroutesector tirs_2 WHERE tirs.trans_inv_route_sec_id = tirs_2.trans_inv_route_sec_id)
