{{
  config(
    materialized='table',
    schema='ANALYTICS'
  )
}}

-- fct_retention_cohorts
--
-- Computes point-in-time retention at 7, 30, and 90 days after signup.
-- A member is "retained at D7" if they have activity between days 6-8,
-- not just any activity before day 7 (which would always be 100%).
-- Segmented by plan type and acquisition channel.

with members as (
    select
        m.member_id,
        m.plan_type,
        m.acquisition_channel,
        date_trunc('week', m.signup_date) as cohort_week,
        m.signup_date
    from {{ source('raw', 'members') }} m
),
activity as (
    select member_id, metric_date
    from {{ source('raw', 'daily_metrics') }}
),
retention as (
    select
        mem.plan_type,
        mem.acquisition_channel,
        mem.cohort_week,
        count(distinct mem.member_id) as cohort_size,
        -- point-in-time: active around day 7 (days 6-8)
        count(distinct case when act.metric_date between mem.signup_date + interval '6 day'
                                                    and mem.signup_date + interval '8 day'
                            then mem.member_id end) as active_d7,
        -- point-in-time: active around day 30 (days 28-32)
        count(distinct case when act.metric_date between mem.signup_date + interval '28 day'
                                                    and mem.signup_date + interval '32 day'
                            then mem.member_id end) as active_d30,
        -- point-in-time: active around day 90 (days 88-92)
        count(distinct case when act.metric_date between mem.signup_date + interval '88 day'
                                                    and mem.signup_date + interval '92 day'
                            then mem.member_id end) as active_d90
    from members mem
    left join activity act
      on act.member_id = mem.member_id
    group by 1,2,3
)
select
    plan_type,
    acquisition_channel,
    cohort_week,
    cohort_size,
    active_d7,
    active_d30,
    active_d90,
    active_d7 / nullif(cohort_size, 0) as retention_d7,
    active_d30 / nullif(cohort_size, 0) as retention_d30,
    active_d90 / nullif(cohort_size, 0) as retention_d90
from retention;
