# Event Schema – Feature Adoption & Retention

The event structures relevant to this repository originate from the synthetic data generator.  They represent user interactions with product features, session boundaries and subscription changes.  The same definitions are used across the analytics ecosystem.

| Event Name | Description | Key fields | Notes |
|-----------|-------------|-----------|------|
| `feature_event` | Emitted when a member uses a feature such as setting a health goal or completing onboarding. | `member_id`, `event_date`, `feature`, `event_name` | Used for adoption metrics. |
| `session_start` / `session_end` | Denotes the start and end of an app session. | `member_id`, `session_start`, `session_end`, `device_type` | Useful for DAU/WAU/MAU calculations. |
| `experiment_assignment` | Records a member’s variant assignment in an A/B test. | `member_id`, `experiment_id`, `variant`, `assignment_date` | Joined to outcome metrics in the experimentation repository. |
| `subscription_update` | Captures subscription lifecycle events such as upgrades and cancellations. | `member_id`, `plan_type`, `start_date`, `end_date`, `auto_renew` | Used to segment retention cohorts. |

Events destined for Amplitude are batched in groups of ten to respect the API’s recommendations.  Each event includes an `insert_id` to guarantee idempotency.