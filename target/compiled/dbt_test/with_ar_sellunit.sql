

SELECT
  su.sell_unit_id
  ,su.rm_id
  ,su.bb_cd_id
FROM opa_stg_uk.ar_sellunit su
WHERE su.file_dt = (SELECT MAX(su_2.file_dt) FROM opa_stg_uk.ar_sellunit su_2 WHERE su.sell_unit_id = su_2.sell_unit_id)