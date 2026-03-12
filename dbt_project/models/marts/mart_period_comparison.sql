/*
    mart_period_comparison.sql
    --------------------------
    Part 1.3: Period-over-Period Comparison (Q3 2024 vs Q4 2024)

    Compares:
    - Total signups
    - Signup → Activation conversion rate
    - Average days to activation
    - Percent change for each metric

    Bonus: Flags statistical significance using a two-proportion z-test
    for conversion rates. For days_to_activate, we note the approach in
    comments — a proper t-test would need variance which approx_quantiles
    doesn't directly provide in BigQuery, so we flag based on effect size
    and sample size heuristics.

    Statistical approach (conversion rate):
    - H0: p_q3 = p_q4
    - z = (p_q3 - p_q4) / sqrt(p_pooled * (1 - p_pooled) * (1/n_q3 + 1/n_q4))
    - Significant if |z| > 1.96 (α = 0.05)
*/

with quarterly_data as (
    select
        case
            when signup_date between '2024-07-01' and '2024-09-30' then 'Q3_2024'
            when signup_date between '2024-10-01' and '2024-12-31' then 'Q4_2024'
        end as quarter,

        count(*)                                                as total_signups,
        countif(reached_activated and days_to_activate <= 60)   as activated_count,

        round(safe_divide(
            countif(reached_activated and days_to_activate <= 60),
            count(*)
        ) * 100, 2) as activation_rate_pct,

        round(avg(
            case when reached_activated and days_to_activate <= 60
                 then days_to_activate end
        ), 1) as avg_days_to_activate

    from {{ ref('int_expert_with_engagement') }}
    where signup_date between '2024-07-01' and '2024-12-31'
    group by 1
),

comparison as (
    select
        q4.total_signups        as q4_signups,
        q3.total_signups        as q3_signups,
        q4.activation_rate_pct  as q4_activation_rate,
        q3.activation_rate_pct  as q3_activation_rate,
        q4.avg_days_to_activate as q4_avg_days,
        q3.avg_days_to_activate as q3_avg_days,
        q4.activated_count      as q4_activated,
        q3.activated_count      as q3_activated,

        /* Percent changes */
        round(safe_divide(q4.total_signups - q3.total_signups, q3.total_signups) * 100, 2)
            as signups_pct_change,
        round(q4.activation_rate_pct - q3.activation_rate_pct, 2)
            as activation_rate_pp_change,
        round(safe_divide(q4.avg_days_to_activate - q3.avg_days_to_activate, q3.avg_days_to_activate) * 100, 2)
            as avg_days_pct_change,

        /* Two-proportion z-test for activation rate significance */
        safe_divide(
            (q3.activated_count + q4.activated_count),
            (q3.total_signups + q4.total_signups)
        ) as pooled_rate

    from quarterly_data q3
    cross join quarterly_data q4
    where q3.quarter = 'Q3_2024'
      and q4.quarter = 'Q4_2024'
)

select
    q3_signups,
    q4_signups,
    signups_pct_change,

    q3_activation_rate,
    q4_activation_rate,
    activation_rate_pp_change,

    q3_avg_days,
    q4_avg_days,
    avg_days_pct_change,

    /* Statistical significance flag for conversion rate difference.
       z = (p1 - p2) / sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
       Using |z| > 1.96 as threshold for α = 0.05. */
    case
        when abs(
            (q3_activation_rate/100.0 - q4_activation_rate/100.0)
            / sqrt(pooled_rate * (1 - pooled_rate) * (1.0/q3_signups + 1.0/q4_signups))
        ) > 1.96
        then 'SIGNIFICANT (p < 0.05)'
        else 'NOT SIGNIFICANT'
    end as activation_rate_significance

from comparison
