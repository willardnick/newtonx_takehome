/*
    test_null_payout_impact.sql
    ---------------------------
    Tests whether any engagements have NULL or zero payout amounts,
    which would distort LTV calculations and channel ROI metrics.

    Fails if more than 1% of engagements have null/zero payouts.
*/

with payout_check as (
    select
        count(*)                                                as total_engagements,
        countif(payout_amount is null or payout_amount = 0)     as null_or_zero_payouts

    from {{ ref('stg_engagements') }}
)

select *
from payout_check
where safe_divide(null_or_zero_payouts, total_engagements) > 0.01
