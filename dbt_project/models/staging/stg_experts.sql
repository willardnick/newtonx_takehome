/*
    stg_experts.sql
    ---------------
    Light cleaning of the raw experts table.
    - Casts types explicitly for BigQuery.
    - Standardizes signup_source to lowercase (defensive).
    - Preserves nullable UTM fields as-is; downstream models decide how to handle.
*/

with source as (
    select * from {{ source('raw', 'experts') }}
),

cleaned as (
    select
        expert_id,
        cast(signup_date as timestamp)                          as signup_at,
        cast(signup_date as date)                               as signup_date,
        date_trunc(cast(signup_date as date), week(monday))     as signup_week,
        lower(trim(signup_source))                              as signup_source,
        lower(trim(utm_campaign))                               as utm_campaign,
        lower(trim(utm_medium))                                 as utm_medium,
        lower(trim(utm_source))                                 as utm_source,
        trim(country)                                           as country,
        trim(industry)                                          as industry,

        /* Flag whether this expert came through a paid channel */
        case
            when lower(trim(signup_source)) in ('paid_search', 'paid_social', 'linkedin_outreach')
            then true
            else false
        end as is_paid_channel

    from source
)

select * from cleaned
