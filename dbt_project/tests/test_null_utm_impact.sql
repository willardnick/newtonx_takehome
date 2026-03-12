/*
    test_null_utm_impact.sql
    ------------------------
    Tests the impact of NULL utm_campaign, utm_medium, and utm_source fields.
    
    Flags a FAILURE if more than 35% of experts have all three UTM fields null
    AND their signup_source is NOT one of [organic, referral, unknown] — which would
    indicate a tracking gap in paid channels where UTMs should always be populated.

    Rationale: Organic/unknown/referral experts are expected to have null UTMs.
    Paid channel experts with null UTMs represent a data quality problem.
*/

with null_check as (
    select
        count(*) as total_experts,

        countif(
            utm_campaign is null
            and utm_medium is null
            and utm_source is null
            and signup_source not in ('organic', 'referral', 'unknown')
        ) as paid_with_all_nulls

    from {{ ref('stg_experts') }}
)

select *
from null_check
where safe_divide(paid_with_all_nulls, total_experts) > 0.05
