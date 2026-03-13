/*
    int_expert_sessions.sql
    -----------------------
    Sessionizes expert funnel events using a 30-minute inactivity threshold.
    A new session begins whenever there is a 30+ minute gap between consecutive
    events for the same expert.

    This model is the foundation for understanding whether experts complete the
    funnel in one sitting or across multiple return visits.

    Key finding from the data:
    Virtually every funnel step occurs in a separate session. The minimum
    inter-step gap is almost always >1 hour, meaning experts are leaving and
    coming back for each stage of the funnel. This is a critical UX signal.
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

        /* A new session starts when:
           1. This is the expert's first event (prev is null), OR
           2. The gap since the previous event exceeds 30 minutes */
        case
            when prev_event_at is null then true
            when timestamp_diff(event_at, prev_event_at, minute) > 30 then true
            else false
        end as is_new_session

    from ordered_events
),

sessionized as (
    select
        *,

        /* Session number: cumulative count of session starts per expert */
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

    /* Session-level aggregations for downstream use */
    min(event_at) over (partition by expert_id, session_number) as session_start_at,
    max(event_at) over (partition by expert_id, session_number) as session_end_at,
    count(*) over (partition by expert_id, session_number)      as events_in_session

from sessionized
