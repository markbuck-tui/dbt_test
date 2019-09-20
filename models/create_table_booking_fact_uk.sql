{{
  config(materialized='table')
}}

with view_booking_fact as (
  select * from {{ref('create_view_v_booking_fact_uk')}}
)
SELECT * from view_booking_fact
