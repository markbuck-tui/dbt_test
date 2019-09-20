{{
  config(materialized='table')
}}

with view_booking_fact as (
  select * from {{ref('v_booking_fact_uk')}}
)
SELECT * from view_booking_fact
