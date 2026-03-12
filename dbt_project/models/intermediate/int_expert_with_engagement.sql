/*
    int_expert_with_engagement.sql
    ------------------------------
    Joins the expert profile with their funnel summary and engagement revenue.
    Produces one row per expert with all the attributes needed for channel
    attribution (Part 2) and cohort analysis (Part 1.2).

    This model is reused by:
    - mart_channel_performance (2.1)
    - mart_campaign_deep_dive (2.2)
    - mart_cohort_conversion (1.2)
    - mart_period_comparison (1.3)
*/

with expert_payouts as (
    select
        expert_id,
        count(*)                        as total_engagements,
        sum(payout_amount)              as total_payout,
        min(engaged_at)                 as first_engagement_date,

        /* 12-month payout as LTV proxy (per assignment instructions) */
        sum(payout_amount)              as ltv_12m_proxy

    from {{ ref('stg_engagements') }}
    group by expert_id
)

select
    e.expert_id,
    e.signup_at,
    e.signup_date,
    e.signup_week,
    e.signup_source,
    e.utm_campaign,
    e.is_paid_channel,
    e.country,
    e.industry,

    f.reached_signup,
    f.reached_profile_started,
    f.reached_profile_completed,
    f.reached_verification_submitted,
    f.reached_verified,
    f.reached_activated,
    f.is_rejected,
    f.days_to_activate,
    f.signup_at          as funnel_signup_at,
    f.first_engagement_at,

    coalesce(p.total_engagements, 0)    as total_engagements,
    coalesce(p.total_payout, 0)         as total_payout,
    coalesce(p.ltv_12m_proxy, 0)        as ltv_12m_proxy

from {{ ref('stg_experts') }} e
left join {{ ref('int_expert_funnel_summary') }} f
    on e.expert_id = f.expert_id
left join expert_payouts p
    on e.expert_id = p.expert_id
