/*
    mart_channel_performance.sql
    ----------------------------
    Part 2.1: Channel Performance Query

    Calculates per signup_source:
    - Total signups
    - Activation rate (% reaching first engagement)
    - Total expert payouts (proxy for expert quality)
    - Average payout per activated expert
    - Cost per signup (CPS)
    - Cost per activated expert (CPA)
    - Estimated LTV:CAC (12-month payout as LTV proxy)

    Handling the "unknown" channel:
    -----------------------------------------------------------------
    ~15% of experts have signup_source = 'unknown' due to tracking gaps.
    We include "unknown" as its own row for completeness and transparency,
    but it should NOT be used for channel investment decisions because:
      1. It's a mix of multiple real channels — attribution is impossible.
      2. Any cost allocation would be arbitrary.
    Recommendation: Investigate tracking gaps (UTM parameter stripping,
    redirect issues, ad blocker impact) and fix instrumentation to reduce
    the unknown bucket over time.

    Organic and referral channels:
    -----------------------------------------------------------------
    These have $0 ad spend by definition. CPS and CPA are shown as NULL
    (not $0) to avoid misleading comparisons. LTV:CAC is shown as NULL
    as well since the denominator (CAC) is effectively zero — though these
    channels are clearly highly efficient from an acquisition cost standpoint.

    Granularity mismatch note:
    -----------------------------------------------------------------
    daily_channel_spend is at the campaign-day level; we aggregate to channel
    level to join with expert-level data. This is appropriate because experts
    are attributed to channels (signup_source), not individual ad impressions.
*/

with channel_experts as (
    select
        signup_source,
        count(*)                                                as total_signups,
        countif(reached_activated)                              as activated_count,
        round(safe_divide(countif(reached_activated), count(*)) * 100, 2)
                                                                as activation_rate_pct,
        sum(total_payout)                                       as total_payouts,
        round(safe_divide(
            sum(case when reached_activated then total_payout else 0 end),
            countif(reached_activated)
        ), 2)                                                   as avg_payout_per_activated,

        /* LTV proxy: average 12-month payout for activated experts */
        round(safe_divide(
            sum(case when reached_activated then ltv_12m_proxy else 0 end),
            countif(reached_activated)
        ), 2)                                                   as avg_ltv_per_activated

    from {{ ref('int_expert_with_engagement') }}
    group by signup_source
),

channel_spend as (
    select
        channel                     as signup_source,
        sum(channel_total_spend)    as total_spend
    from {{ ref('int_channel_spend_summary') }}
    /* Deduplicate: channel_total_spend repeats per campaign row */
    group by channel
)

select
    ce.signup_source,
    ce.total_signups,
    ce.activation_rate_pct,
    ce.total_payouts,
    ce.avg_payout_per_activated,

    /* For paid channels, compute cost metrics.
       For organic/referral/unknown, show NULL (not 0) since spend is undefined. */
    round(safe_divide(cs.total_spend, ce.total_signups), 2)     as cost_per_signup,
    round(safe_divide(cs.total_spend, ce.activated_count), 2)   as cost_per_activated,

    cs.total_spend,

    /* LTV:CAC — only meaningful for paid channels with non-zero spend */
    round(safe_divide(ce.avg_ltv_per_activated, safe_divide(cs.total_spend, ce.activated_count)), 2)
                                                                as ltv_to_cac_ratio

from channel_experts ce
left join channel_spend cs
    on ce.signup_source = cs.signup_source

order by ce.total_signups desc
