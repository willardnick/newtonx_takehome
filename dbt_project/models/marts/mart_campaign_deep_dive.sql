/*
    mart_campaign_deep_dive.sql
    ---------------------------
    Part 2.2: Campaign-Level Deep Dive (Paid Channels Only)

    Identifies:
    - Top 3 campaigns by volume
    - Top 3 campaigns by activation rate (minimum 100 signups)
    - Top 3 campaigns by LTV:CAC efficiency

    Deliverable: Rankings + a short recommendation paragraph (in the memo).
*/

with campaign_experts as (
    select
        signup_source,
        utm_campaign,
        count(*)                                                    as total_signups,
        countif(reached_activated)                                  as activated_count,
        round(safe_divide(countif(reached_activated), count(*)) * 100, 2)
                                                                    as activation_rate_pct,
        sum(case when reached_activated then ltv_12m_proxy else 0 end)
                                                                    as total_ltv_activated

    from {{ ref('int_expert_with_engagement') }}
    where is_paid_channel
      and utm_campaign is not null
    group by signup_source, utm_campaign
),

campaign_with_spend as (
    select
        ce.signup_source,
        ce.utm_campaign,
        ce.total_signups,
        ce.activated_count,
        ce.activation_rate_pct,
        ce.total_ltv_activated,

        cs.campaign_spend,

        round(safe_divide(cs.campaign_spend, ce.total_signups), 2)      as cost_per_signup,
        round(safe_divide(cs.campaign_spend, ce.activated_count), 2)    as cost_per_activated,
        round(safe_divide(
            safe_divide(ce.total_ltv_activated, nullif(ce.activated_count, 0)),
            safe_divide(cs.campaign_spend, nullif(ce.activated_count, 0))
        ), 2) as ltv_to_cac_ratio

    from campaign_experts ce
    left join {{ ref('int_channel_spend_summary') }} cs
        on ce.signup_source = cs.channel
       and ce.utm_campaign  = cs.campaign_name
),

/* Rank campaigns across three dimensions */
ranked as (
    select
        *,
        row_number() over (order by total_signups desc)                             as rank_by_volume,
        row_number() over (
            order by case when total_signups >= 100 then activation_rate_pct end desc nulls last
        )                                                                           as rank_by_activation,
        row_number() over (order by ltv_to_cac_ratio desc nulls last)               as rank_by_ltv_cac

    from campaign_with_spend
)

select
    signup_source,
    utm_campaign,
    total_signups,
    activated_count,
    activation_rate_pct,
    campaign_spend,
    cost_per_signup,
    cost_per_activated,
    ltv_to_cac_ratio,
    rank_by_volume,
    rank_by_activation,
    rank_by_ltv_cac,

    /* Flag if campaign appears in any top-3 list */
    (rank_by_volume <= 3 or rank_by_activation <= 3 or rank_by_ltv_cac <= 3)
        as is_top_campaign

from ranked
order by rank_by_volume
