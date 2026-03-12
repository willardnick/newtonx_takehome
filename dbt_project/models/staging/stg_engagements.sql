/*
    stg_engagements.sql
    -------------------
    Cleans the raw engagements table.
    - Casts types.
    - Adds engagement_month for time-series aggregation.
*/

with source as (
    select * from {{ source('raw', 'engagements') }}
),

cleaned as (
    select
        engagement_id,
        expert_id,
        lower(trim(engagement_type))                as engagement_type,
        cast(engagement_date as timestamp)           as engaged_at,
        cast(engagement_date as date)                as engagement_date,
        cast(payout_amount as numeric)               as payout_amount,
        trim(client_industry)                        as client_industry

    from source
)

select * from cleaned
