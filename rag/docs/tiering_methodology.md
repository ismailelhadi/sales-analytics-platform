# Store Tiering Methodology

## Purpose
Store tiering ranks every store into one of four tiers (Platinum, Gold, Silver, Bronze) so sales, marketing and inventory teams can prioritise support and allocation. The model lives in the dbt model `store_tiering`.

## Inputs and weights
The composite score combines five weighted inputs, each measured over the scoring window:
- Revenue (log-transformed): weight 0.35
- Revenue growth: weight 0.20
- Accessory attach rate: weight 0.15
- Activation rate: weight 0.15
- Average basket: weight 0.15
The weights sum to 1.0. Revenue is log-transformed so a few very large flagship stores do not dominate the score.

## Standardisation
Each input is converted to a z-score (subtract the mean across stores, divide by the standard deviation) before weighting, so inputs on different scales are comparable. The composite score is the weighted sum of the z-scores.

## Tier thresholds
Stores are ranked by composite score and assigned by percentile rank:
- Platinum: top 10% of stores
- Gold: next 25% (10% to 35%)
- Silver: next 35% (35% to 70%)
- Bronze: bottom 30%

## Scoring window
The default scoring window is 2026-01-01 to 2026-04-30. It is set with the dbt variables `scoring_start` and `scoring_end`.

## Explaining a store's tier
Every row of `store_tiering` keeps the contribution of each input (`contrib_revenue`, `contrib_revenue_growth`, and so on). To explain why a store landed in its tier, look at which contributions are largest and which are negative. A negative contribution means the store is below the average store on that input.

## Validation
Tiers are validated on the period after the scoring window (2026-05-01 onward). A useful tiering should show Platinum stores earning more future revenue on average than Gold, Gold more than Silver, and Silver more than Bronze. The validation script `pipeline/evaluate.py` reports rank correlation and average future revenue by tier.
