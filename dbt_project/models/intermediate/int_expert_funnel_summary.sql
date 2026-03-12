/*
    int_expert_funnel_summary.sql
    -----------------------------
    Pivots the funnel event stream into one row per expert with the earliest
    timestamp for each funnel stage. This is the backbone model reused across
    Part 1 (funnel analysis) and Part 2 (channel attribution).

    Design decisions:
    - Uses MIN(event_at) per stage to handle duplicate events gracefully.
    - Keeps verification_rejected as a boolean flag rather than a stage timestamp,
      since rejected experts should be excluded from "Verified" and "Activated" counts.
    - Computes days_to_activate for cohort analysis.

    Edge cases handled:
    - Experts who signed up but never progressed (signup_at populated, all others null).
    - Out-of-order timestamps: we rely on event_name to define the stage, not timestamp order.
    - Experts with verification_rejected are flagged; they will never have verification_approved.
*/

with pivoted as (
    select
        expert_id,
        min(case when event_name = 'signup'                     then event_at end) as signup_at,
        min(case when event_name = 'profile_started'            then event_at end) as profile_started_at,
        min(case when event_name = 'profile_completed'          then event_at end) as profile_completed_at,
        min(case when event_name = 'verification_submitted'     then event_at end) as verification_submitted_at,
        min(case when event_name = 'verification_approved'      then event_at end) as verification_approved_at,
        min(case when event_name = 'verification_rejected'      then event_at end) as verification_rejected_at,
        min(case when event_name = 'first_engagement_completed' then event_at end) as first_engagement_at,

        max(case when event_name = 'verification_rejected' then true else false end) as is_rejected

    from {{ ref('stg_funnel_events') }}
    group by expert_id
),

enriched as (
    select
        p.*,

        /* Reached flags: an expert "reached" a stage if the timestamp is non-null.
           For verified and activated, also require NOT rejected. */
        signup_at                    is not null as reached_signup,
        profile_started_at           is not null as reached_profile_started,
        profile_completed_at         is not null as reached_profile_completed,
        verification_submitted_at    is not null as reached_verification_submitted,
        (verification_approved_at    is not null and not is_rejected) as reached_verified,
        (first_engagement_at         is not null and not is_rejected) as reached_activated,

        /* Days from signup to first engagement (activation) */
        case
            when first_engagement_at is not null and signup_at is not null
            then date_diff(cast(first_engagement_at as date), cast(signup_at as date), day)
        end as days_to_activate

    from pivoted p
)

select * from enriched
