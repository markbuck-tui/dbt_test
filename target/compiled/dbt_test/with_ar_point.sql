

SELECT
  p.pt_id
  ,p.pt_cd
FROM opa_stg_uk.ar_point p
WHERE p.file_dt = (SELECT MAX(p_2.file_dt) FROM opa_stg_uk.ar_point p_2 WHERE p.pt_id = p_2.pt_id)