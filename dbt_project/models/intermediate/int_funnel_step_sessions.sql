/*
    mart_funnel_step_analysis.sql
    -----------------------------
    Aggregated funnel step analysis combining:
    - Conversion rates between stages (% change, not volume)
    - Step completion times (avg, median, p75, p90)
    - Session break rates using 24-HOUR inactivity threshold
    - Average sessions to complete the full funnel
 
    Critical insight surfaced by 24h sessionization:
    Profile Completed → Verification Submitted has a 0% session break rate.
    Experts who submit do so in the same session as completing their profile.
    The 38.3% who drop off at this step NEVER COME BACK — they are lost
    permanently, not temporarily. This makes it the most actionable drop-off
    in the funnel because:
      1. It is entirely within the expert's control (unlike verification approval)
      2. The experts have already invested time completing their profile
      3. They can be re-contacted (we have their profile data)
 
    "Biggest drop-off" is measured by % lost at each step transition.
*/
 
with funnel_stages as (
    select 'signup' as stage, 1 as stage_order, countif(reached_signup) as experts_reached
    from {{ ref('int_expert_funnel_summary') }}
    union all
    select 'profile_started', 2, countif(reached_profile_started) from {{ ref('int_expert_funnel_summary') }}
    union all
    select 'profile_completed', 3, countif(reached_profile_completed) from {{ ref('int_expert_funnel_summary') }}
    union all
    select 'verification_submitted', 4, countif(reached_verification_submitted) from {{ ref('int_expert_funnel_summary') }}
    union all
    select 'verified', 5, countif(reached_verified) from {{ ref('int_expert_funnel_summary') }}
    union all
    select 'activated', 6, countif(reached_activated) from {{ ref('int_expert_funnel_summary') }}
),
 
step_timing_raw as (
    select
        current_step,
        next_step,
        count(*)                                                        as experts_with_transition,
        round(avg(step_duration_hours), 1)                              as avg_hours,
        approx_quantiles(step_duration_hours, 100)[offset(50)]          as median_hours,
        approx_quantiles(step_duration_hours, 100)[offset(75)]          as p75_hours,
        approx_quantiles(step_duration_hours, 100)[offset(90)]          as p90_hours,
        round(countif(required_new_session) * 100.0 / count(*), 1)      as session_break_rate_pct,
        round(avg(sessions_gap), 2)                                     as avg_sessions_gap
    from {{ ref('int_funnel_step_sessions') }}
    group by current_step, next_step
),
 
step_timing as (
    select
        case next_step
            when 'profile_started'              then 'profile_started'
            when 'profile_completed'            then 'profile_completed'
            when 'verification_submitted'       then 'verification_submitted'
            when 'verification_approved'        then 'verified'
            when 'first_engagement_completed'   then 'activated'
        end as dest_stage,
        current_step as from_step,
        avg_hours, median_hours, p75_hours, p90_hours,
        session_break_rate_pct, avg_sessions_gap
    from step_timing_raw
),
 
with_conversion as (
    select
        stage,
        stage_order,
        experts_reached,
 
        round(safe_divide(
            experts_reached,
            lag(experts_reached) over (order by stage_order)
        ) * 100, 1) as conversion_from_previous_pct,
 
        round(safe_divide(
            experts_reached,
            first_value(experts_reached) over (order by stage_order)
        ) * 100, 1) as conversion_from_signup_pct,
 
        round(100.0 - safe_divide(
            experts_reached,
            lag(experts_reached) over (order by stage_order)
        ) * 100, 1) as dropoff_rate_pct
 
    from funnel_stages
),
 
sessions_to_complete as (
    select
        round(avg(max_session), 2)                          as avg_sessions_to_activate,
        approx_quantiles(max_session, 100)[offset(50)]      as median_sessions_to_activate
    from (
        select expert_id, max(session_number) as max_session
        from {{ ref('int_expert_sessions') }}
        where expert_id in (
            select expert_id from {{ ref('int_expert_funnel_summary') }} where reached_activated
        )
        group by expert_id
    )
)
 
select
    c.stage,
    c.stage_order,
    c.experts_reached,
    c.conversion_from_previous_pct,
    c.conversion_from_signup_pct,
    c.dropoff_rate_pct,
 
    st.from_step,
    st.avg_hours                    as avg_hours_to_reach,
    st.median_hours                 as median_hours_to_reach,
    st.p75_hours                    as p75_hours_to_reach,
    st.p90_hours                    as p90_hours_to_reach,
 
    st.session_break_rate_pct,
    st.avg_sessions_gap,
 
    sc.avg_sessions_to_activate,
    sc.median_sessions_to_activate
 
from with_conversion c
left join step_timing st
    on c.stage = st.dest_stage
cross join sessions_to_complete sc
order by c.stage_order
