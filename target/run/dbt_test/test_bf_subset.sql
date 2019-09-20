create or replace view OPA_DEV.DBT_TEST.test_bf_subset as (
    SELECT *
FROM opa_stg_uk.fl_acr_booking
limit 10
  );