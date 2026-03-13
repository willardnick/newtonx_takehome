/*
    int_funnel_step_sessions.sql
    ----------------------------
    Analyzes each funnel step transition to determine:
    1. Whether completing the next step required a new session (user left and came back).
    2. How long it took to reach the next step (step completion time).
    3. Which session each step lands in.

    This is the core model for understanding WHERE in the funnel experts walk away
    and have to return in a separate session.

    Reuses int_expert_sessions for session assignments and int_expert_funnel_summary
    for the deduped per-expert step timestamps.
*/

with step_sessions as (
    /* Get the session number for each expert's FIRST occurrence of each event */
    select
        expert_id,
        event_name,
        event_at,
        session_number,
        stage_order

    from {{ ref('int_expert_sessions') }}
    qualify row_number() over (
        partition by expert_id, event_name
        order by event_at
    ) = 1
),

step_pairs as (
    /* For each step, find the next step in the funnel and compute timing + session gap */
    select
        curr.expert_id,
        curr.event_name                     as current_step,
        curr.stage_order                    as current_stage_order,
        curr.event_at                       as current_step_at,
        curr.session_number                 as current_session,

        nxt.event_name                      as next_step,
        nxt.event_at                        as next_step_at,
        nxt.session_number                  as next_session,

        /* Time to complete this step (reach the next one) */
        timestamp_diff(nxt.event_at, curr.event_at, minute)     as step_duration_minutes,
        round(timestamp_diff(nxt.event_at, curr.event_at, minute) / 60.0, 1)
                                                                 as step_duration_hours,

        /* Did the expert have to come back in a new session to complete the next step? */
        (nxt.session_number > curr.session_number)              as required_new_session,
        (nxt.session_number - curr.session_number)              as sessions_gap

    from step_sessions curr
    inner join step_sessions nxt
        on curr.expert_id = nxt.expert_id
       and nxt.stage_order = curr.stage_order + 1
    /* Only forward transitions in the happy path (exclude rejected → activated) */
    where curr.event_name in (
        'signup', 'profile_started', 'profile_completed',
        'verification_submitted', 'verification_approved'
    )
    and nxt.event_name in (
        'profile_started', 'profile_completed', 'verification_submitted',
        'verification_approved', 'first_engagement_completed'
    )
)

select * from step_pairs
