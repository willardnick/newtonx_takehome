# NewtonX Analytics Engineer Take-Home

## Project Overview

This repository contains a complete dbt-based analytics solution for NewtonX's expert supply funnel, structured as a production-ready analytics engineering project.

**Author:** Nick Willard  
**Time Spent:** ~3 hours  
**Stack:** dbt (BigQuery dialect), HTML/JS (Chart.js dashboard), docx-js (memo)

---

## Repository Structure

```
├── README.md                          ← You are here
├── dbt_project/
│   ├── dbt_project.yml                ← dbt configuration
│   ├── models/
│   │   ├── staging/                   ← Source definitions + light cleaning
│   │   │   ├── src_newtonx.yml        ← Source declarations & tests
│   │   │   ├── stg_newtonx.yml        ← Staging model tests
│   │   │   ├── stg_experts.sql
│   │   │   ├── stg_funnel_events.sql
│   │   │   ├── stg_engagements.sql
│   │   │   └── stg_daily_channel_spend.sql
│   │   ├── intermediate/              ← Reusable business logic
│   │   │   ├── int_newtonx.yml
│   │   │   ├── int_expert_funnel_summary.sql    ← Pivoted funnel (backbone)
│   │   │   ├── int_expert_with_engagement.sql   ← Expert + funnel + payouts
│   │   │   └── int_channel_spend_summary.sql    ← Aggregated spend
│   │   └── marts/                     ← Final analytical outputs
│   │       ├── mart_newtonx.yml
│   │       ├── mart_funnel_conversion.sql       ← Part 1.1
│   │       ├── mart_cohort_conversion.sql       ← Part 1.2
│   │       ├── mart_period_comparison.sql       ← Part 1.3
│   │       ├── mart_channel_performance.sql     ← Part 2.1
│   │       └── mart_campaign_deep_dive.sql      ← Part 2.2
│   ├── tests/                         ← Custom data quality tests
│   │   ├── test_null_utm_impact.sql
│   │   ├── test_funnel_timestamp_ordering.sql
│   │   ├── test_no_activated_rejected_experts.sql
│   │   └── test_null_payout_impact.sql
│   └── analysis/                      ← Ad-hoc edge case queries
│       └── edge_case_analysis.sql
├── dashboard/
│   └── growth_pod_dashboard.html      ← Part 3: Interactive dashboard
├── memo/
│   ├── growth_pod_memo.docx           ← Part 4: Written memo
│   └── generate_memo.js               ← Memo generation script
└── data/                              ← Raw CSVs (for reference)
    ├── experts.csv
    ├── engagements.csv
    ├── funnel_events.csv
    └── daily_channel_spend.csv
```

---

## Setup Instructions

### Prerequisites
- dbt-bigquery (or dbt-core for local development)
- A BigQuery project with the raw CSVs loaded as tables

### Running the dbt Project

```bash
# 1. Clone and navigate
git clone https://github.com/willardnick/newtonx_takehome.git
cd newtonx_takehome/dbt_project

# 2. Configure your BigQuery connection in ~/.dbt/profiles.yml:
# newtonx_analytics:
#   target: dev
#   outputs:
#     dev:
#       type: bigquery
#       method: oauth
#       project: your-project-id
#       dataset: analytics_dev
#       threads: 4

# 3. Load CSVs into BigQuery as raw tables (raw_data schema)
# Tables: raw_data.experts, raw_data.funnel_events, raw_data.engagements, raw_data.daily_channel_spend

# 4. Run dbt
dbt deps
dbt run          # Builds all models
dbt test         # Runs all tests (source + custom)

# 5. View the dashboard
open ../dashboard/growth_pod_dashboard.html
```

---

## Architecture & Design Decisions

### Layer Design (Staging → Intermediate → Marts)

**Staging:** One view per source table. Handles type casting, lowercase standardization, and defensive trimming. No business logic.

**Intermediate:** Three reusable models that encode the core business logic:
- `int_expert_funnel_summary` — Pivots the event stream into one row per expert with reached-flags. This is the backbone model, reused by 4 of the 5 mart models.
- `int_expert_with_engagement` — Joins expert attributes, funnel progression, and payout data. Reused by 4 mart models.
- `int_channel_spend_summary` — Aggregates daily spend to channel and campaign level. Reused by 2 mart models.

**Marts:** One table per analytical question. Materialized as tables for dashboard performance.

### Key Assumptions & Trade-offs

1. **"Activated" = first_engagement_completed AND NOT rejected.** Rejected experts are excluded from Verified and Activated counts per the assignment requirements.

2. **60-day activation window** for cohort analysis. This creates right-censoring for recent cohorts (Nov–Dec 2024). The memo flags this explicitly.

3. **Unknown channel treatment:** Included in all reports for transparency but flagged as unreliable for investment decisions. No spend is allocated to unknown (it would be arbitrary).

4. **Organic/Referral cost metrics:** Shown as NULL, not $0. These channels have zero ad spend by definition, so CPS/CPA/LTV:CAC are undefined (not "free" — there are implicit costs like referral incentives and SEO investment that aren't in the spend data).

5. **Granularity mismatch (spend → experts):** daily_channel_spend is at the campaign-day level; experts are attributed at the channel level. We aggregate spend to channel totals for Part 2.1, and join on campaign_name for Part 2.2. This is appropriate because expert attribution is channel-level (signup_source), not impression-level.

6. **LTV proxy:** Per the assignment, we use 12-month total payout as the LTV proxy. In reality, this underestimates true LTV for recently activated experts who haven't had 12 months of engagement yet.

7. **Statistical significance (Part 1.3):** We use a two-proportion z-test for activation rate differences between Q3 and Q4. For days-to-activate, we note that a proper Welch's t-test would need per-expert variance — we flag the direction and magnitude instead.

### Edge Cases Handled

- **Experts with signup only (no further events):** 3,235 experts. Counted at signup stage, null for all subsequent stages.
- **Stage-skipping experts:** Handled by using event_name rather than timestamp ordering to assign stages.
- **Out-of-order timestamps:** MIN(timestamp) per event_name ensures we get the earliest occurrence regardless of insertion order.
- **Duplicate events:** MIN() aggregation naturally deduplicates.
- **Verification rejected:** Tracked as a boolean flag; these experts are excluded from Verified/Activated counts.

### Testing Strategy

**Source-level tests (src_newtonx.yml):** Uniqueness, not-null, accepted_values, and referential integrity for all four source tables.

**Staging tests (stg_newtonx.yml):** Validates that cleaning logic preserves data integrity.

**Custom tests:**
- `test_null_utm_impact` — Flags if >5% of paid-channel experts have null UTM fields (indicates tracking failure).
- `test_funnel_timestamp_ordering` — Monitors out-of-order event timestamps (known issue; fails if >5% affected).
- `test_no_activated_rejected_experts` — Business rule: no rejected expert should reach activation.
- `test_null_payout_impact` — Flags null/zero payouts that would distort LTV calculations.

---

## What I'd Do With More Time

1. **Incremental models:** Convert intermediate and mart models to incremental materialization with proper merge keys for production efficiency.

2. **dbt exposures:** Define exposures for the dashboard and memo to document lineage from raw data to business deliverables.

3. **Deeper attribution modeling:** The current approach is last-touch (signup_source). A multi-touch model incorporating utm_medium and utm_source could better credit the full acquisition journey, especially for the "unknown" bucket.

4. **Retention analysis:** The current LTV proxy (total payout) doesn't capture churn. A survival analysis on expert engagement frequency would give a more accurate LTV estimate.

5. **Proper BI tool:** The HTML dashboard works but a Looker/Metabase deployment would provide drill-down capability, scheduled refreshes, and proper access control.

6. **dbt macros:** Extract common patterns (safe_divide, stage counting) into reusable macros.

7. **CI/CD:** Add a GitHub Actions workflow that runs `dbt build` and `dbt test` on every PR.

8. **Unknown source attribution:** Build a probabilistic model using country/industry/signup_date patterns to re-attribute unknown experts to likely channels.
