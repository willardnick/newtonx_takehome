# NewtonX Sr. Data Analyst/Engineer Take-Home

## Project Overview

This repository contains a complete dbt-based analytics solution for NewtonX's expert supply funnel, structured as a production-ready analytics engineering project.

**Author:** Nick Willard  
**Time Spent:** ~3 hours  
**Stack:** dbt (BigQuery dialect), HTML/JS (Chart.js dashboard), docx-js (memo), Claude.ai  
**Link to Claude.Ai (Opus) Conversation:** https://claude.ai/share/fefa4b48-e063-4ab5-98e6-b1dbee7bed7a
**Instructions:** https://drive.google.com/drive/u/0/folders/1oToTCKZvkDCjQRuin4ZWxw0h-A40_z3R

**Author Commentary:** I spent the vast majority of my time checking the work produced via Claude, questioning assumptions it made, and digging into specifics around sessions and time between steps. There were simple misteps by Claude, such as firstly claiming that the step with the largest volume drop was the most critical step. A cursory analysis showed that this was _not_ the case, and I iterated the analyses and dashboard creation around the core concepts of time between steps. I then walked through a number of steps searching for silver bullets that might point to website or tooling outages or marketing channel issues, of which Claude found none.

Interesting take home- more comprehensive than any other I've had the opportunity to take on.

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
│   │   │   ├── int_channel_spend_summary.sql    ← Aggregated spend
│   │   │   ├── int_expert_sessions.sql          ← Sessionized events (30-min threshold)
│   │   │   └── int_funnel_step_sessions.sql     ← Step-pair timing + session breaks
│   │   └── marts/                     ← Final analytical outputs
│   │       ├── mart_newtonx.yml
│   │       ├── mart_funnel_conversion.sql       ← Part 1.1
│   │       ├── mart_cohort_conversion.sql       ← Part 1.2
│   │       ├── mart_period_comparison.sql       ← Part 1.3
│   │       ├── mart_channel_performance.sql     ← Part 2.1
│   │       ├── mart_campaign_deep_dive.sql      ← Part 2.2
│   │       └── mart_funnel_step_analysis.sql    ← Session + timing analysis
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

**Intermediate:** Five reusable models that encode the core business logic:
- `int_expert_funnel_summary` — Pivots the event stream into one row per expert with reached-flags. This is the backbone model, reused by 5 of the 6 mart models.
- `int_expert_with_engagement` — Joins expert attributes, funnel progression, and payout data. Reused by 4 mart models.
- `int_channel_spend_summary` — Aggregates daily spend to channel and campaign level. Reused by 2 mart models.
- `int_expert_sessions` — Sessionizes raw funnel events using a 30-minute inactivity threshold. Assigns a `session_number` to every event. Reused by `int_funnel_step_sessions` and `mart_funnel_step_analysis`.
- `int_funnel_step_sessions` — Pairs consecutive funnel steps per expert, computing step duration and whether the transition required a new session. Powers the step-level analysis in `mart_funnel_step_analysis`.

**Marts:** Six tables, one per analytical question. Materialized as tables for dashboard performance.

### Session Analysis Design

Sessions are defined by a **24-hour inactivity threshold**: if 24+ hours pass between two consecutive events for the same expert, the second event starts a new session.

**Why 24 hours, not 30 minutes:** The funnel events are server-side milestone events (account created, profile saved, verification processed), not client-side clickstream data. There are no intermediate page views or button clicks between milestones. The minimum inter-step gap is typically 2–48 hours, so a 30-minute threshold would place every single step in its own session (100% break rate at every transition), which is technically correct but analytically useless. The 24-hour window groups same-day activity into one session and reveals which transitions genuinely require a return visit on a different day.

**Key findings with 24h threshold:**

| Transition | Session Break Rate | Avg Hours | Interpretation |
|---|---|---|---|
| Signup → Profile Started | 49.6% | 24.4h | Half complete same-day |
| Profile Started → Completed | 67.7% | 37.1h | Most return next day |
| **Profile Completed → Verif Submitted** | **0.0%** | **12.6h** | **Now-or-never** |
| Verif Submitted → Approved | 97.7% | 47.8h | Async review (expected) |
| Approved → Activated | 98.1% | 538.1h | Demand matching delay |

**The critical insight:** Profile Completed → Verification Submitted has a **0% session break rate**. Experts who submit do so in the same session as completing their profile. The 2,321 experts (38.3%) who don't submit in that moment never come back. This is not procrastination — it is permanent abandonment, and it represents the single most actionable intervention point in the funnel because:
1. It is entirely within the expert's control (unlike verification or activation).
2. The step is trivially fast for those who complete it (median 13 hours).
3. The abandoned experts have completed profiles and can be re-contacted.
4. The drop-off rate varies by channel (referral: 24.7%, paid social: 52.3%), suggesting that expert intent/quality, not UX friction, is the primary driver.

The average activated expert requires **4 sessions** to complete the full funnel.

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
