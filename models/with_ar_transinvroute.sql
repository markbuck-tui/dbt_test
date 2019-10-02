{{
  config(materialized='ephemeral')
}}

SELECT
  tir.trans_inv_route_id
  ,tir.trans_route_id
FROM opa_stg_uk.ar_transinvroute tir
WHERE tir.file_dt = (SELECT MAX(tir_2.file_dt) FROM opa_stg_uk.ar_transinvroute tir_2 WHERE tir.trans_inv_route_id = tir_2.trans_inv_route_id)
