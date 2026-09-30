-- Multi-factor store tiering. Each input is standardized (z-score) so the weights are comparable,
-- then combined into one composite score. Per-factor contributions are kept on every row so any
-- individual store's tier can be explained to a stakeholder.
{% set weights = {
    'revenue': 0.35,
    'revenue_growth': 0.20,
    'accessory_attach_rate': 0.15,
    'activation_rate': 0.15,
    'avg_basket': 0.15,
} %}
with f as (
    select *, ln(revenue) as log_revenue from {{ ref('store_features') }}
),
z as (
    select
        store_id, revenue, revenue_growth, accessory_attach_rate, activation_rate, avg_basket,
        (log_revenue - avg(log_revenue) over ()) / nullif(stddev_samp(log_revenue) over (), 0) as z_revenue,
        {% for k in weights if k != 'revenue' %}
        ({{ k }} - avg({{ k }}) over ()) / nullif(stddev_samp({{ k }}) over (), 0) as z_{{ k }}{{ "," if not loop.last }}
        {% endfor %}
    from f
),
scored as (
    select
        z.*,
        {% for k, w in weights.items() %}
        round(({{ w }} * coalesce(z_{{ k }}, 0))::numeric, 4) as contrib_{{ k }},
        {% endfor %}
        {% for k, w in weights.items() %}
        {{ w }} * coalesce(z_{{ k }}, 0){{ " +" if not loop.last }}
        {% endfor %} as composite_score
    from z
)
select
    *,
    percent_rank() over (order by composite_score desc) as score_percentile,
    case
        when percent_rank() over (order by composite_score desc) < 0.10 then 'Platinum'
        when percent_rank() over (order by composite_score desc) < 0.35 then 'Gold'
        when percent_rank() over (order by composite_score desc) < 0.70 then 'Silver'
        else 'Bronze'
    end as tier
from scored
