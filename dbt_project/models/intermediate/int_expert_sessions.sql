/*
    int_expert_sessions.sql
    -----------------------
    Sessionizes expert funnel events using a 24-HOUR inactivity threshold.
 
    Why 24 hours (not 30 minutes):
    The funnel events are server-side milestone events (account created, profile
    saved, verification processed), NOT client-side clickstream data. There are no
    intermediate page views or clicks between milestones. The minimum gap between
    funnel steps is typically 2–6 hours, making a 30-minute threshold meaningless
    (every step would be its own session). A 24-hour window groups events that
    happen within the same "working day" into a single session, revealing which
    transitions genuinely require the expert to come back on a different day.
 
    Key finding with 24h threshold:
    - ~50% of experts complete signup + profile_started in session 1
    - Profile Completed → Verification Submitted has a 0% session break rate
      (experts who submit ALWAYS do so same-session; those who don't, never return)
    - Avg activated expert needs 4 sessions (not 6 as with 30-min threshold)
*/
 
with ordered_events as (
    select
        event_id,
        expert_id,
        event_name,
        event_at,
        stage_order,
 
        lag(event_at) over (
            partition by expert_id
            order by event_at, stage_order
        ) as prev_event_at
 
    from {{ ref('stg_funnel_events') }}
),
 
with_gaps as (
    select
        *,
        timestamp_diff(event_at, prev_event_at, minute) as gap_minutes,
 
        /* New session when:
           1. First event for this expert, OR
           2. Gap since previous event exceeds 24 hours (1440 minutes) */
        case
            when prev_event_at is null then true
            when timestamp_diff(event_at, prev_event_at, minute) > 1440 then true
            else false
        end as is_new_session
 
    from ordered_events
),
 
sessionized as (
    select
        *,
        countif(is_new_session) over (
            partition by expert_id
            order by event_at, stage_order
            rows between unbounded preceding and current row
        ) as session_number
 
    from with_gaps
)
 
select
    event_id,
    expert_id,
    event_name,
    event_at,
    stage_order,
    prev_event_at,
    gap_minutes,
    is_new_session,
    session_number,
 
    min(event_at) over (partition by expert_id, session_number) as session_start_at,
    max(event_at) over (partition by expert_id, session_number) as session_end_at,
    count(*) over (partition by expert_id, session_number)      as events_in_session
 
from sessionized
