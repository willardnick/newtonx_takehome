/*
    edge_case_analysis.sql
    ----------------------
    Diagnostic queries to identify edge cases and data quality issues.
    Run these as ad-hoc queries; they are NOT materialized models.
*/

-- ============================================================
-- 1. EXPERTS WITH SIGNUP BUT NO SUBSEQUENT FUNNEL EVENTS
-- ============================================================
-- These experts created accounts but never started their profile.
-- ~27% of signups — high drop-off worth investigating.
-- Possible causes: poor onboarding UX, bot signups, incomplete tracking.

select
    e.signup_source,
    count(*)                                                    as signup_only_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 2)         as pct_of_total

from {{ ref('stg_experts') }} e
left join {{ ref('int_expert_funnel_summary') }} f
    on e.expert_id = f.expert_id
where f.profile_started_at is null
group by e.signup_source
order by signup_only_count desc;


-- ============================================================
-- 2. EXPERTS WHO SKIPPED FUNNEL STAGES
-- ============================================================
-- e.g., went from signup directly to profile_completed without profile_started.
-- Could indicate event tracking failures or product changes.

select
    'signup_to_completed_skip' as edge_case,
    count(*) as expert_count
from {{ ref('int_expert_funnel_summary') }}
where profile_completed_at is not null
  and profile_started_at is null

union all

select
    'verification_without_profile' as edge_case,
    count(*)
from {{ ref('int_expert_funnel_summary') }}
where verification_submitted_at is not null
  and profile_completed_at is null

union all

select
    'activated_without_verification' as edge_case,
    count(*)
from {{ ref('int_expert_funnel_summary') }}
where first_engagement_at is not null
  and verification_approved_at is null;


-- ============================================================
-- 3. EVENTS WITH TIMESTAMPS BEFORE SIGNUP
-- ============================================================
-- Should never happen — indicates instrumentation bugs.

select
    fe.expert_id,
    fe.event_name,
    fe.event_at             as event_timestamp,
    e.signup_at             as signup_timestamp,
    timestamp_diff(fe.event_at, e.signup_at, hour) as hours_before_signup

from {{ ref('stg_funnel_events') }} fe
inner join {{ ref('stg_experts') }} e
    on fe.expert_id = e.expert_id
where fe.event_at < e.signup_at
  and fe.event_name != 'signup'
order by hours_before_signup
limit 50;


-- ============================================================
-- 4. DUPLICATE FUNNEL EVENTS (same expert + same event_name)
-- ============================================================
-- The int_expert_funnel_summary handles this via MIN(), but we should
-- know how prevalent duplicates are.

select
    event_name,
    count(*) as duplicate_pairs

from (
    select
        expert_id,
        event_name,
        count(*) as event_count
    from {{ ref('stg_funnel_events') }}
    group by expert_id, event_name
    having count(*) > 1
)
group by event_name
order by duplicate_pairs desc;


-- ============================================================
-- 5. SPEND DATA COVERAGE GAPS
-- ============================================================
-- Days where specific campaigns have zero spend might indicate
-- paused campaigns or missing data.

select
    campaign_name,
    min(spend_date) as first_spend_date,
    max(spend_date) as last_spend_date,
    count(*)        as days_with_data,
    date_diff(max(spend_date), min(spend_date), day) + 1 as expected_days,
    date_diff(max(spend_date), min(spend_date), day) + 1 - count(*) as missing_days

from {{ ref('stg_daily_channel_spend') }}
group by campaign_name
having date_diff(max(spend_date), min(spend_date), day) + 1 - count(*) > 0
order by missing_days desc;


-- ============================================================
-- 6. UNKNOWN SOURCE INVESTIGATION
-- ============================================================
-- Profiles the "unknown" segment to look for patterns that might
-- help attribute them to real channels.

select
    country,
    industry,
    count(*)                                            as expert_count,
    countif(reached_activated)                          as activated_count,
    round(safe_divide(countif(reached_activated), count(*)) * 100, 2)
                                                        as activation_rate

from {{ ref('int_expert_with_engagement') }}
where signup_source = 'unknown'
group by country, industry
order by expert_count desc
limit 20;
