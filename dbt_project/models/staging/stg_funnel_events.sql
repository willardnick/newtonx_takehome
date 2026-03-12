/*
    stg_funnel_events.sql
    ---------------------
    Cleans the raw funnel_events table.
    - Casts timestamps.
    - Assigns a numeric stage_order for deterministic funnel ordering,
      which is critical for handling out-of-order timestamps (a known data quality issue).
    - Keeps verification_rejected separate from verification_approved with distinct ordinals.
*/

with source as (
    select * from {{ source('raw', 'funnel_events') }}
),

cleaned as (
    select
        event_id,
        expert_id,
        lower(trim(event_name))                     as event_name,
        cast(event_timestamp as timestamp)           as event_at,
        cast(event_timestamp as date)                as event_date,
        event_properties,

        /* Canonical ordering for the funnel.
           verification_rejected is a terminal branch at the same level as approved. */
        case lower(trim(event_name))
            when 'signup'                       then 1
            when 'profile_started'              then 2
            when 'profile_completed'            then 3
            when 'verification_submitted'       then 4
            when 'verification_approved'        then 5
            when 'verification_rejected'        then 5  -- same tier, different outcome
            when 'first_engagement_completed'   then 6
        end as stage_order,

        /* Boolean flags for easy downstream filtering */
        lower(trim(event_name)) = 'verification_rejected' as is_rejected

    from source
)

select * from cleaned
