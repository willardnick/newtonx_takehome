/*
    test_no_activated_rejected_experts.sql
    --------------------------------------
    Validates a critical business rule: no expert who was verification_rejected
    should appear as "activated" (reached first_engagement_completed).

    This would indicate either a data integrity bug or a bypass of the
    verification gate. Fails if ANY such expert exists.
*/

select
    expert_id,
    is_rejected,
    reached_activated

from {{ ref('int_expert_funnel_summary') }}
where is_rejected = true
  and first_engagement_at is not null
