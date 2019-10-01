

SELECT DISTINCT
  ofn_1.off_name_id
  ,ofn_1.cd
  ,ofn_1.name
  -- ,sm.source_market_code
FROM opa_stg_uk.ar_officename ofn_1
-- LEFT OUTER JOIN opa_stg_all.source_market sm ON 'UKATCOM|' || ofn_1.cd = bk_source_market
WHERE ofn_1.file_dt = (SELECT MAX(ofn_2.file_dt) FROM opa_stg_uk.ar_officename ofn_2 WHERE ofn_1.off_name_id = ofn_2.off_name_id)