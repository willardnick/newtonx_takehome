/*
    stg_daily_channel_spend.sql
    ---------------------------
    Cleans the raw daily_channel_spend table.
    - Casts types.
    - Standardizes channel and campaign_name to lowercase for join consistency.
*/

with source as (
    select * from {{ source('raw', 'daily_channel_spend') }}
),

cleaned as (
    select
        cast(date as date)                          as spend_date,
        lower(trim(channel))                        as channel,
        lower(trim(campaign_name))                  as campaign_name,
        cast(spend_usd as numeric)                  as spend_usd,
        cast(impressions as int64)                   as impressions,
        cast(clicks as int64)                        as clicks

    from source
)

select * from cleaned
