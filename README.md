<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/nh-logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="assets/nh-logo-light.svg">
    <img alt="NH" src="assets/nh-logo-dark.svg" width="80">
  </picture>
</p>

<h1 align="center">Feature Adoption & Retention</h1>
<p align="center">
  <strong>Measure how members adopt product features and how long they stay engaged</strong>
</p>

<p align="center">
  <a href="https://nicholasjh-work.github.io/feature-adoption-retention/"><img src="https://img.shields.io/badge/Live_Demo-4285F4?style=for-the-badge&logo=googlechrome&logoColor=white" alt="Demo"></a>&nbsp;
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue?style=for-the-badge" alt="License"></a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white" alt="dbt">
  <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white" alt="Snowflake">
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white" alt="Pandas">
  <img src="https://img.shields.io/badge/Matplotlib-11557C?style=flat&logo=matplotlib&logoColor=white" alt="Matplotlib">
</p>

---

dbt models and analytics for measuring feature adoption curves and retention cohorts. Builds on the synthetic data from [Infra-data-pipelines](https://github.com/nicholasjh-work/Infra-data-pipelines).

---

### Quick start

```bash
git clone https://github.com/nicholasjh-work/feature-adoption-retention.git
cd feature-adoption-retention
pip install -r requirements.txt

# Requires infra-data-pipelines demo to have run first (PostgreSQL loaded)
python demo.py
```

---

### Demo output

#### Feature Adoption (Weekly Unique Users)

![Feature Adoption](screenshots/feature_adoption.png)

Top features by total unique users:

```
heart_rate_alert                18,928
subscription_management         18,869
account_settings                18,752
health_goal                     18,742
coaching                        18,724
```

#### Cohort Retention Heatmap

![Retention Heatmap](screenshots/retention_heatmap.png)

Point-in-time retention using activity windows (not cumulative):

```
Overall retention:
  D7:  98.7%
  D30: 92.8%
  D90: 65.4%
```

#### Retention by Plan Type

![Retention by Plan](screenshots/retention_by_plan.png)

---

### dbt models

#### fct_feature_adoption

| Column | Description |
|--------|-------------|
| `week_start` | Start of ISO week |
| `feature` | Feature name (sleep_tracking, coaching, etc.) |
| `unique_users` | Distinct members who used the feature that week |
| `new_adopters` | Members whose first-ever interaction occurred that week |
| `repeat_users` | Members who returned from a prior week |

#### fct_retention_cohorts

Point-in-time retention at 7, 30, and 90 days after signup.

| Column | Description |
|--------|-------------|
| `plan_type` | free, pro, enterprise |
| `acquisition_channel` | organic, social, paid, referral |
| `cohort_week` | Week of member signup |
| `cohort_size` | Members in the cohort |
| `retention_d7` / `retention_d30` / `retention_d90` | Point-in-time retention rates |

---

### Running with Snowflake (Production)

```bash
cp profiles.yml.example ~/.dbt/profiles.yml
cp .env.example .env
dbt build
```

---

### Related repos

- [Infra-data-pipelines](https://github.com/nicholasjh-work/Infra-data-pipelines) - Data generation and ingestion
- [Experimentation-segmentation](https://github.com/nicholasjh-work/Experimentation-segmentation) - A/B testing and user segmentation
- [subscription-financial-model](https://github.com/nicholasjh-work/subscription-financial-model) - Churn, retention, and revenue analytics

---

<p align="center">
  <a href="https://linkedin.com/in/nicholashidalgo"><img src="https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white" alt="LinkedIn"></a>&nbsp;
  <a href="https://nicholashidalgo.com"><img src="https://img.shields.io/badge/Website-000000?style=for-the-badge&logo=googlechrome&logoColor=white" alt="Website"></a>&nbsp;
  <a href="mailto:analytics@nicholashidalgo.com"><img src="https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white" alt="Email"></a>
</p>
