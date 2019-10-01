create or replace view OPA_DEV.DBT_TEST.with_booking_fact_margin as (
    

SELECT
  bff_1.bk_booking
  ,bff_1.ffd_flag
  ,bff_1.sm_currency
  ,bff_1.sm_revenue
  ,bff_1.sm_cnx_and_amend_revenue
  ,bff_1.sm_accommodation_costs
  ,bff_1.sm_early_booking_discounts
  ,bff_1.sm_late_booking_discounts
  ,bff_1.sm_flying_costs
  ,bff_1.sm_other_costs
  ,bff_1.sm_distribution_costs
  ,bff_1.sm_non_margin_items
  ,bff_1.sm_margin
  ,bff_1.smg_currency
  ,bff_1.smg_revenue
  ,bff_1.smg_cnx_and_amend_revenue
  ,bff_1.smg_accommodation_costs
  ,bff_1.smg_early_booking_discounts
  ,bff_1.smg_late_booking_discounts
  ,bff_1.smg_flying_costs
  ,bff_1.smg_other_costs
  ,bff_1.smg_distribution_costs
  ,bff_1.smg_non_margin_items
  ,bff_1.smg_margin
  ,bff_1.rep_currency
  ,bff_1.rep_revenue
  ,bff_1.rep_cnx_and_amend_revenue
  ,bff_1.rep_accommodation_costs
  ,bff_1.rep_early_booking_discounts
  ,bff_1.rep_late_booking_discounts
  ,bff_1.rep_flying_costs
  ,bff_1.rep_other_costs
  ,bff_1.rep_distribution_costs
  ,bff_1.rep_non_margin_items
  ,bff_1.rep_margin
FROM opa_fl_uk.v_booking_fact_margin_uk bff_1
  );