# KPI Definitions – Feature Adoption & Retention

This document elaborates on the metrics calculated in this repository.  It supplements the general KPI dictionary from the infrastructure project with adoption‑specific definitions.

## Feature adoption metrics

- **Unique feature users** – the number of distinct members who trigger an event associated with a feature in a given week.  Measuring weekly active users aligns with industry practice.
- **New adopters** – members for whom the first recorded interaction with a feature falls within the current week.  This cohort represents fresh uptake and can highlight the success of onboarding campaigns.
- **Repeat users** – members who interacted with the feature before the current week and again within the week.  A high ratio of repeat to new users indicates stickiness.

## Retention metrics

- **Cohort retention** – the proportion of members who remain active 7, 30 and 90 days after signing up.  Activity is defined as the presence of at least one daily metric record.  Retention is segmented by plan type and acquisition channel to surface differences in churn behaviour.
- **Churn** – the complement of retention; members who do not record any activity within the time window are considered churned.

## Data quality checks

All models include dbt tests to enforce uniqueness and non‑null constraints.  The `fct_feature_adoption` table is unique on the combination of week and feature, while the retention cohorts are unique on the combination of plan, channel and cohort week.