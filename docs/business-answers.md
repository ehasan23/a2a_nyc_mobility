# Business answers — for Northbridge Mobility Advisory

Every number comes from the reconciled **prd** gold schema, queried with full entitlement.
SQL for every result: [`src/analysis/business_questions.sql`](../src/analysis/business_questions.sql).
Dashboard: `[fill link]`.

**How to write each answer** (Section 7 of the brief): say what the number is, what it is compared with, and
how sure you are. *"Trips touching the zone fell 9% while trips elsewhere fell 2%"* is an answer;
*"trips: 1,234,567"* is not. State every definition you chose.

## Definitions used throughout

| Term | Definition |
|---|---|
| Zone (CRZ) | Pickup zones in `gold_crz_zones` with `in_crz = true`: ≥ 90% of their pickups from 6 Jan to 28 Feb 2025 carry a congestion fee, and ≥ 200 such pickups. `[fill: N zones, all Manhattan, south of ~60th St? yes/no]` |
| touches_zone | Pickup **or** dropoff in one of those zones |
| outside | Every other trip, including unknown zones 264 / 265 |
| Before / after (BQ1) | 1 Nov – 31 Dec 2024 vs 5 Jan – 28 Feb 2025. 1–4 January are in neither: January, but before the toll |
| Fee era (BQ2, BQ3) | `pre_toll` = pickup before 5 Jan 2025; `toll` = from 5 Jan |
| Driver share | sum(driver_pay) / sum(base_passenger_fare). The congestion fee is a pass-through and is not added to the fare |
| Wait | pickup − request, HVFHV only; negative waits excluded from percentiles and counted |
| Weather class | Central Park: snow if snow > 0; wet if precipitation ≥ 5 mm; otherwise dry |

---

## BQ1 · Did trips touching the zone fall after 5 January?

**Result** (paste the four-row table):

| Service | Segment | Trips/day before | Trips/day after | % change |
|---|---|---|---|---|
| hvfhv | outside | [fill] | [fill] | [fill] |
| hvfhv | touches_zone | [fill] | [fill] | [fill] |
| yellow | outside | [fill] | [fill] | [fill] |
| yellow | touches_zone | [fill] | [fill] | [fill] |

**Gap** (touches_zone % change − outside % change): HVFHV `[fill]` points, yellow `[fill]` points.
**Sensitivity** (excluding 20 Dec – 5 Jan): HVFHV `[fill]`, yellow `[fill]` — the conclusion `[holds / does not hold]`.

**Chart:** daily trips by segment, colour changing on 5 January (dashboard tiles "BQ1").

**In plain language** (2–3 sentences): `[fill]`

> Why the gap and not the raw change: holidays and winter hit both segments; the toll hits only one.
> The difference between the segments is what points at the zone. It is still an association over two months,
> not proof of cause — say so.

## BQ2 · Who pays the toll, and what happened to driver pay?

**Fee incidence, toll era, HVFHV:**

| Platform | Trips | % with a fee | Average fee when charged |
|---|---|---|---|
| [fill] | | | |

**Before vs after, by platform and segment:**

| Platform | Segment | Era | Avg base fare | Avg driver pay | Driver share |
|---|---|---|---|---|---|
| [fill] | | | | | |

**One sentence per platform:** `[fill]`
**Denominator used for driver share:** base passenger fare (no tolls, taxes, surcharges, tips or the congestion fee).

## BQ3 · Did service levels change?

**p50 / p90 wait by platform and segment (all hours):**

| Platform | Segment | Era | p50 (min) | p90 (min) | Negative waits excluded |
|---|---|---|---|---|---|
| [fill] | | | | | |

**Chart:** p90 wait by hour of day, before and after, touches_zone segment.
**Negative waits excluded in total:** `[fill]` of `[fill]` trips (`[fill]`%).

**In plain language:** `[fill]` — the p90 tells a dispatcher more than the mean: it is the wait that one rider
in ten exceeds.

## BQ4 · How much does weather move demand?

**Trips per day by weather class, within each month:**

| Month | Service | Class | Days | Trips/day | vs dry days, same month |
|---|---|---|---|---|---|
| [fill] | | | | | |

**Chart:** trips per day by weather class and service, month by month.

**In plain language:** `[fill]` — say how many days fall in each class. With one or two snow days a month,
say that the snow estimate rests on very few days. Weekday/weekend mix also differs between classes.

---

## What the client should take away

`[fill: three bullets for the committee, three for the operator's policy team]`
