/*
    mart_funnel_conversion.sql
    --------------------------
    Part 1.1: Full-Funnel Conversion Report

    Shows conversion rates between each funnel stage:
      Signup → Profile Started → Profile Completed → Verification Submitted → Verified → Activated

    Requirements addressed:
    - Stage-to-stage AND overall conversion from signup.
    - Experts who skip stages are counted at whatever stages they did reach.
    - Rejected experts are excluded from "Verified" and "Activated" counts
      (handled in int_expert_funnel_summary via the reached_verified / reached_activated flags).

    Output columns: stage, experts_reached, conversion_from_previous, conversion_from_signup
*/

with stage_counts as (
    select
        'signup'                    as stage,
        1                           as stage_order,
        countif(reached_signup)     as experts_reached
    from {{ ref('int_expert_funnel_summary') }}

    union all

    select
        'profile_started',
        2,
        countif(reached_profile_started)
    from {{ ref('int_expert_funnel_summary') }}

    union all

    select
        'profile_completed',
        3,
        countif(reached_profile_completed)
    from {{ ref('int_expert_funnel_summary') }}

    union all

    select
        'verification_submitted',
        4,
        countif(reached_verification_submitted)
    from {{ ref('int_expert_funnel_summary') }}

    union all

    select
        'verified',
        5,
        countif(reached_verified)
    from {{ ref('int_expert_funnel_summary') }}

    union all

    select
        'activated',
        6,
        countif(reached_activated)
    from {{ ref('int_expert_funnel_summary') }}
),

with_signup_base as (
    select
        sc.*,
        first_value(experts_reached) over (order by stage_order) as signup_total,
        lag(experts_reached) over (order by stage_order)         as previous_stage_count

    from stage_counts sc
)

select
    stage,
    experts_reached,

    round(
        safe_divide(experts_reached, previous_stage_count) * 100, 2
    ) as conversion_from_previous,

    round(
        safe_divide(experts_reached, signup_total) * 100, 2
    ) as conversion_from_signup

from with_signup_base
order by stage_order
