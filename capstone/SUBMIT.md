# Masar Capstone: Submission

Name: Sara Mohammed Alhamdan  
Branch: main  
Link: https://github.com/SaraAlhamdan/masar-lakehouse/tree/main

## What I built

1. **Bronze:** Loaded raw trips, drivers, and GPS data into Delta tables.
2. **Silver (dbt):** Cleaned and transformed the data, removed duplicates, and passed all dbt tests and seven quality checks.
3. **Delta control:** Applied data constraints, tested invalid writes, and demonstrated schema evolution using a scratch table.
4. **Gold:** Built hourly demand aggregations and point-in-time trip features.
5. **BI-safe table:** Protected personal identifiers through pseudonymization and location coarsening.
6. **Pipeline:** Successfully executed the complete pipeline using `make pipeline` from a clean state.
7. **Proof:** Verified row counts and demonstrated Delta Lake time travel.

## Counts per layer

Pipeline output before the erasure demonstration:

| Table | Rows |
|---|---:|
| bronze/trips | 48,200 |
| bronze/drivers | 1,850 |
| bronze/gps_pings | 120,000 |
| silver/trips | 44,371 |
| gold/zone_hourly_demand | 120 |
| gold/trip_features | 44,371 |
| gold/bi_trips_safe | 44,371 |

## Time travel

- Version 0: 44,371 trips.
- Latest version (v5): 44,364 trips.
- Versions 0–4 contained seven trips for rider_00042.
- Version 5 contained zero trips for that rider after erasure.

## Steps the trainer handed over

None.

## One thing I would improve

I would improve the erasure process across all layers to prevent deleted records from returning when the pipeline runs again.