

SELECT DISTINCT
  ag_1.agt_id
  ,ag_1.def_mkt_id
  ,ag_1.agt_tp_id
FROM opa_stg_uk.ar_agent ag_1
WHERE ag_1.file_dt = (SELECT MAX(ag_2.file_dt) FROM opa_stg_uk.ar_agent ag_2 WHERE ag_1.agt_id = ag_2.agt_id)