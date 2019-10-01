{{
  config(materialized='view')
}}

SELECT DISTINCT
  uc_1.user_cd_id
  ,uc_1.cd
  ,uc_1.name
FROM opa_stg_uk.ar_usercodes uc_1
WHERE uc_1.file_dt = (SELECT MAX(uc_2.file_dt) FROM opa_stg_uk.ar_usercodes uc_2 WHERE uc_1.user_cd_id = uc_2.user_cd_id)
