/*
    mart_cohort_conversion.sql
    --------------------------
    Part 1.2: Cohort Conversion Analysis (Time Series)

    Weekly cohort analysis showing:
    - Signup week (cohort)
    - Conversion to "Activated" status
    - Days-to-activate distribution (median, p75, p90)

    Requirements addressed:
    - signup_date defines cohorts (via signup_week from stg_experts).
    - Only counts activations within 60 days of signup.
    - Week-over-week trend via LAG window function.
*/

with cohort_base as (
    select
        signup_week,
        expert_id,
        reached_activated,
        days_to_activate,

        /* Only count activations that occurred within 60 days of signup */
        case
            when reached_activated and days_to_activate <= 60
            then true
            else false
        end as activated_within_60d

    from {{ ref('int_expert_with_engagement') }}
),

cohort_stats as (
    select
        signup_week,
        count(*)                            as total_signups,
        countif(activated_within_60d)       as activated_count,

        round(
            safe_divide(countif(activated_within_60d), count(*)) * 100, 2
        ) as activation_rate_pct,

        /* Days-to-activate distribution (only for those activated within 60d) */
        approx_quantiles(
            case when activated_within_60d then days_to_activate end,
            100 ignore nulls
        )[offset(50)] as median_days_to_activate,

        approx_quantiles(
            case when activated_within_60d then days_to_activate end,
            100 ignore nulls
        )[offset(75)] as p75_days_to_activate,

        approx_quantiles(
            case when activated_within_60d then days_to_activate end,
            100 ignore nulls
        )[offset(90)] as p90_days_to_activate

    from cohort_base
    group by signup_week
)

select
    signup_week,
    total_signups,
    activated_count,
    activation_rate_pct,
    median_days_to_activate,
    p75_days_to_activate,
    p90_days_to_activate,

    /* Week-over-week change in activation rate */
    round(
        activation_rate_pct - lag(activation_rate_pct) over (order by signup_week), 2
    ) as wow_activation_rate_change

from cohort_stats
order by signup_week
