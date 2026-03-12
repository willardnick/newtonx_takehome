/*
    int_channel_spend_summary.sql
    -----------------------------
    Aggregates daily_channel_spend to channel-level and campaign-level totals.
    Used by mart_channel_performance and mart_campaign_deep_dive.

    Note: Only paid channels (paid_search, paid_social, linkedin_outreach) appear
    in this table. Organic, referral, and unknown have $0 spend by definition.
*/

with campaign_totals as (
    select
        channel,
        campaign_name,
        sum(spend_usd)      as total_spend,
        sum(impressions)    as total_impressions,
        sum(clicks)         as total_clicks

    from {{ ref('stg_daily_channel_spend') }}
    group by channel, campaign_name
),

channel_totals as (
    select
        channel,
        sum(total_spend)        as channel_total_spend,
        sum(total_impressions)  as channel_total_impressions,
        sum(total_clicks)       as channel_total_clicks

    from campaign_totals
    group by channel
)

select
    ct.channel,
    ct.campaign_name,
    ct.total_spend          as campaign_spend,
    ct.total_impressions    as campaign_impressions,
    ct.total_clicks         as campaign_clicks,
    ch.channel_total_spend,
    ch.channel_total_impressions,
    ch.channel_total_clicks

from campaign_totals ct
inner join channel_totals ch
    on ct.channel = ch.channel
