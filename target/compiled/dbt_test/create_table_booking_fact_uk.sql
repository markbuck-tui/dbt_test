

with view_booking_fact as (
  select * from OPA_DEV.DBT_TEST.v_booking_fact_uk
)
SELECT * from view_booking_fact