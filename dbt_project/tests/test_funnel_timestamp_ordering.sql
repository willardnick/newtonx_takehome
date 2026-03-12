/*
    test_funnel_timestamp_ordering.sql
    -----------------------------------
    Detects experts whose funnel events have timestamps that violate the
    expected logical ordering (e.g., profile_completed before profile_started).

    This is a known data quality issue per the assignment notes. This test
    surfaces the count so we can monitor the severity. It fails if MORE than
    5% of experts with 2+ events have ordering violations.

    This does NOT break the analysis — int_expert_funnel_summary uses event_name
    (not timestamp order) to assign stages — but it flags instrumentation issues.
*/

with event_pairs as (
    select
        expert_id,
        event_name,
        event_at,
        stage_order,
        lag(stage_order) over (partition by expert_id order by event_at) as prev_stage_order

    from {{ ref('stg_funnel_events') }}
),

violations as (
    select
        expert_id,
        count(*) as violation_count
    from event_pairs
    where prev_stage_order is not null
      and stage_order < prev_stage_order
    group by expert_id
),

summary as (
    select
        (select count(distinct expert_id) from {{ ref('stg_funnel_events') }}) as total_experts,
        count(*) as experts_with_violations
    from violations
)

select *
from summary
where safe_divide(experts_with_violations, total_experts) > 0.05
