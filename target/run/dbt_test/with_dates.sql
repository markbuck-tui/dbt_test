create or replace view OPA_DEV.DBT_TEST.with_dates as (
    


SELECT
	dd.bk_date
	,dd.group_season_code
FROM opa_stg_all.date_dim dd
  );