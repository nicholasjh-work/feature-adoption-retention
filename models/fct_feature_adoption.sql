{{
  config(
    materialized='table',
    schema='ANALYTICS'
  )
}}

-- fct_feature_adoption
--
-- This model produces weekly adoption metrics for each feature in the product.
-- It counts the number of unique users who interact with a feature,
-- identifies those users who are new adopters (first ever interaction in the
-- week) and repeat users (had used the feature in a prior week).  The model
-- can be extended to include additional dimensions such as plan type or
-- acquisition channel by joining to the members table.

with events as (
    select
        member_id,
        event_date,
        feature,
        date_trunc('week', event_date) as week_start
    from {{ source('raw', 'feature_events') }}
),
first_event as (
    select
        member_id,
        feature,
        min(event_date) as first_event_date
    from {{ source('raw', 'feature_events') }}
    group by member_id, feature
),
metrics as (
    select
        e.week_start,
        e.feature,
        count(distinct e.member_id) as unique_users,
        -- new adopters are members whose first ever event occurs within the week
        count(distinct case when fe.first_event_date >= e.week_start
                            and fe.first_event_date < e.week_start + interval '7 day'
                            then e.member_id end) as new_adopters,
        -- repeat users had their first event before the week
        count(distinct case when fe.first_event_date < e.week_start then e.member_id end) as repeat_users
    from events e
    join first_event fe
      on fe.member_id = e.member_id
     and fe.feature = e.feature
    group by 1,2
)
select * from metrics;