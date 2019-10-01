

SELECT
  ss.stc_stk_id
  ,ss.cd
FROM opa_stg_uk.ar_staticstock ss
WHERE ss.file_dt = (SELECT MAX(ss_2.file_dt) FROM opa_stg_uk.ar_staticstock ss_2 WHERE ss.stc_stk_id = ss_2.stc_stk_id)