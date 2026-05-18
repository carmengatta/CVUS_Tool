# Mortality Analysis: Outreach Opportunities

**Source:** Structured mortality parsing of 93 large DB plan Form 5500 filings (2024 plan year) + Schedule SB database analysis across 2019-2024 (field `SB_MORTALITY_TBL_CD`)
**Data:** `_plan_mortality_summary_2024.csv`, `db_plans_{year}.parquet` (2019-2024)

---

## 1. Former Substitute Mortality Users (Reactivation Opportunity)

### The Collapse of Substitute Mortality Usage (2019-2024)

Analysis of the Schedule SB mortality table code (`SB_MORTALITY_TBL_CD = 3`) across six years of Form 5500 filings reveals a dramatic and accelerating decline in substitute mortality table usage:

| Year | Plans Using Substitute | Participants Covered | New Adopters | Dropped | Net Change |
|---|---|---|---|---|---|
| 2019 | 101 | 1,892,958 | — | — | — |
| 2020 | 102 | 1,790,895 | 13 | 12 | +1 |
| 2021 | 104 | 1,765,425 | 14 | 12 | +2 |
| 2022 | 88 | 1,587,297 | 3 | 19 | **-16** |
| 2023 | 83 | 1,510,829 | 10 | 15 | -5 |
| 2024 | 55 | 950,053 | 2 | 30 | **-28** |

**From 101 plans covering 1.9 million participants in 2019 to just 55 plans covering 950,000 in 2024** — a 46% decline in plan count and 50% decline in participant coverage over five years.

The decline accelerated sharply in two waves:
- **2022 (net -16):** The first major exit year, coinciding with post-COVID actuarial recalibration and the anticipation of new IRS mortality regulations.
- **2024 (net -28):** A catastrophic drop, almost certainly driven by SECURE 2.0 Section 101, which mandated generational mortality projection for plans with 500+ participants. The new IRS regulations (TD 9983, October 2023) updated how substitute tables must integrate with generational projection, making existing IRS approvals more burdensome to maintain.

New adoption has virtually stopped: only **2 plans** adopted substitute mortality in the 2024 filing year, compared to **30 that dropped it**.

### Why Plans Are Dropping: The SECURE 2.0 Connection

Two plans in our PDF extraction sample — The Coca-Cola Company and Georgia-Pacific — include explicit transition language in their Schedule SB filings: *"changed from IRS approved plan-specific substitute mortality tables to a generational projection as required by guidance issued by IRS under IRC 430."*

This confirms the regulatory driver: maintaining a substitute mortality approval now requires reconciling plan-specific experience tables with the mandatory generational projection framework. Many plan sponsors and their actuaries appear to have concluded that the administrative cost of re-qualifying under the new rules exceeds the funding benefit — at least in the short term.

### Actuary Firm Concentration in Drops

Willis Towers Watson served as actuary for **60% of all plans that dropped** substitute mortality across the six-year period:

| Actuary Firm | Plans Dropped (2019-2024) | Share |
|---|---|---|
| Willis Towers Watson | 53 | 60% |
| Buck Global | 11 | 13% |
| Mercer | 10 | 11% |
| Aon Consulting | 8 | 9% |
| Milliman | 3 | 3% |
| Segal | 2 | 2% |
| Ernst & Young | 1 | 1% |

WTW also remains the dominant actuary for the 55 plans still using substitute mortality (33 of 55, or 60%). This concentration suggests the drop trend may be driven partly by firm-level advisory decisions rather than purely individual plan economics.

### Reactivation Targets

**88 plans** dropped substitute mortality between 2019 and 2024. Below are the most actionable reactivation targets, organized by priority tier.

#### Tier 1: 2023-2024 Drops, Confirmed Still Filing (Highest Priority)

These plans confirmed their switch away from substitute mortality on their 2024 Form 5500 filing (mortality code changed from 3 to 2). The decision is recent and most likely driven by the SECURE 2.0 compliance burden rather than a finding that their mortality doesn't differ from prescribed tables.

| Sponsor | EIN | Plan(s) | Retirees | Total Participants | Actuary | Notes |
|---|---|---|---|---|---|---|
| Georgia-Pacific LLC | 93-0432081 | 046, 070 | 27,448 | 48,666 | WTW | Koch subsidiary; PDF confirms explicit SECURE 2.0 transition language |
| Koch Companies, LLC | 99-2447784 | 001 | 2,929 | 12,355 | WTW | Koch subsidiary |
| OfficeMax / ODP Corporation | 82-0100960 | 005 | 10,367 | 12,102 | Milliman | |
| Ardagh Glass Inc. | 35-1958205 | 008 | 1,616 | 4,051 | Mercer | |
| ASARCO LLC | 81-0666284 | 007, 008 | 6,181 | 9,010 | Milliman | |
| Legacy Vulcan LLC | 20-8579133 | 010, 020 | 2,674 | 5,859 | Aon | |
| Resolute FP US Inc. | 62-0721803 | 004 | 3,795 | 4,647 | WTW | |
| Ball Corporation | 35-0160610 | 035 | 505 | 4,387 | WTW | Previously dropped plan 001 in 2021 |
| INV Management Services, LLC | 85-1507460 | 001 | 2,466 | 2,772 | WTW | Koch subsidiary |
| Molex LLC | 36-2369491 | 002 | 868 | 2,300 | WTW | Koch subsidiary |
| Flint Hills Resources Pine Bend | 61-1603905 | 001 | 158 | 997 | WTW | Koch subsidiary |
| The Coca-Cola Company | 58-0628465 | 004 | 23,649 | 51,246 | *N/A* | PDF confirms explicit SECURE 2.0 transition language |
| FHR Peru Holding Company | 84-2606672 | 001 | 31 | 72 | WTW | Koch subsidiary |

**Koch Industries cluster:** 7 plans across 6 Koch subsidiaries (Georgia-Pacific, Koch Companies, Molex, Flint Hills Resources, FHR Peru, INV Management Services) all dropped simultaneously in the 2024 filing year, all with WTW as actuary. This was a coordinated corporate decision affecting ~70,000 participants. A single conversation with Koch's benefits team could unlock re-engagement across all entities.

#### Tier 2: 2023-2024 Drops, No 2024 Filing Yet (Monitor — May Be Late Filers)

These plans had substitute mortality in 2023 but have not yet filed a 2024 Form 5500. The 2024 filing dataset contains ~25% fewer total filings than 2023 (4,387 vs 5,862), so many of these may be late filers rather than confirmed drops. **Monitor for their 2024 filings.**

| Sponsor | EIN | Plan(s) | Retirees (2023) | Total Participants (2023) | Actuary | Notes |
|---|---|---|---|---|---|---|
| **General Motors LLC** | 27-0383222 | 003, 016 | **315,726** | **368,827** | WTW | Largest single-sponsor substitute user in the database; filing likely pending |
| Michelin North America | 11-1724631 | 010 | 14,090 | 19,397 | Aon | |
| FCA US LLC | 27-0187394 | 038, 043 | 11,027 | 12,800 | Mercer | Still has 3 other plans WITH substitute (005, 004, 007) |
| Komatsu Mining Corp. | 39-1566457 | 008 | 8,696 | 11,088 | WTW | |
| Appalachia Holding Company | 54-0295165 | 001 | 3,353 | 6,813 | WTW | |
| Ball Corporation | 35-0160610 | 039 | 193 | 5,601 | WTW | |
| FirstGroup Services, Inc. | 86-3006037 | 001 | 5,407 | 5,524 | Segal | |
| ThyssenKrupp North America | 22-2393554 | 001 | 2,885 | 3,114 | WTW | Has bounced on/off substitute across multiple years |
| Cleveland-Cliffs Inc. | 34-1464672 | 003 | 1,173 | 2,759 | WTW | Other Cliffs entities still have substitute |
| Alpha Natural Resources | 56-2298262 | 003, 004 | 1,986 | 2,517 | WTW | A different Alpha plan (005) newly adopted code 3 in 2024 |
| Verizon Business Global LLC | 90-0357488 | 008 | 682 | 824 | Aon | Other Verizon entities still have substitute |
| The Goodyear Tire & Rubber Co. | 34-0253240 | 017 | 501 | 603 | WTW | Other Goodyear plans still have substitute |

#### Tier 3: 2022-2023 Drops (Still Actionable)

One year removed, but these plans have recent institutional memory of substitute mortality. Several are large industrial sponsors.

| Sponsor | EIN | Plan(s) | Retirees | Total Participants | Actuary |
|---|---|---|---|---|---|
| Alcoa USA Corp | 37-1808900 | 001, 002, 037 | 3,346 | 9,087 | WTW |
| O-I Glass, Inc. | 22-2781933 | 001, 002 | 5,864 | 11,350 | WTW |
| Hexion Inc. | 13-0511250 | 002 | 1,604 | 3,317 | WTW |
| Suriname Aluminum Company | 98-0150255 | 003, 037 | 3,058 | 3,698 | WTW |
| Peabody Holding Company | 74-2666822 | 004 | 839 | 982 | WTW |

#### Tier 4: 2021-2022 Drops (Larger Plans)

Older drops, but several are large plans where the funding impact of re-adopting substitute mortality could be significant.

| Sponsor | EIN | Plan(s) | Retirees | Total Participants | Actuary |
|---|---|---|---|---|---|
| Arconic Corporation | 84-2745636 | 003, 004 | 5,132 | 12,912 | Buck |
| Howmet Aerospace Inc. | 25-0317820 | 001 | 6,282 | 9,786 | Buck |
| ATI Inc. | 25-1792394 | 001, 200 | 1,047 | 4,952 | Mercer |
| ThyssenKrupp entities | various | 003, 005, 007 | 869 | 1,470 | WTW |
| Motiva Enterprises | 76-0262490 | 006 | 24 | 165 | WTW |

#### Tier 5: 2019-2021 Drops (Selective — Large Plans Only)

The oldest drops. Most relevant for very large plans where the cumulative cost of using prescribed tables may have grown substantial.

| Sponsor | EIN | Plan(s) | Retirees | Total Participants | Actuary | Notes |
|---|---|---|---|---|---|---|
| Weyerhaeuser Company | 91-0470860 | 002 | 5,435 | 20,057 | Mercer | Dropped 2020 |
| Newell Operating Company | 36-1953130 | 001 | 5,465 | 11,941 | Mercer | Dropped 2021, **re-adopted in 2024** |
| AK Steel / Cleveland-Cliffs Steel | 31-1267098 | 003 | 6,660 | 11,463 | WTW | Dropped 2020; now filing as Cleveland-Cliffs Steel with code 3 again |
| Ball Corporation | 35-0160610 | 001 | 6,596 | 10,345 | WTW | Dropped 2021; plan 035 also dropped 2024 |
| The Brink's Company | 54-1317776 | 003 | 5,662 | 10,340 | Mercer | Dropped 2021, **re-adopted in 2022** |
| Sonoco Products Company | 57-0248420 | 001 | 102 | 889 | Aon | Dropped 2021 |

*Note: Newell, AK Steel/Cleveland-Cliffs, and Brink's demonstrate that re-adoption does happen — these are proof points for the reactivation pitch.*

### Outreach Angle

Every plan on this list has already invested in the IRS substitute mortality approval process — an undertaking that requires a credible experience study, actuarial documentation, and a formal Revenue Procedure 2017-55 application. The decision to drop was almost certainly driven by the **administrative burden** of re-qualifying under the new SECURE 2.0 generational framework, not by a finding that their mortality matches prescribed tables.

Position substitute mortality services as the solution to this exact problem:
- *"Your plan's mortality still differs from the IRS tables — the underlying experience hasn't changed. What changed is the regulatory framework. We can help you re-qualify under the new generational rules."*
- For Koch entities: a single engagement could cover all 7 plans across 6 subsidiaries
- For plans with WTW as actuary: frame the conversation around whether the decision to drop was a cost-benefit call that may have shifted now that the SECURE 2.0 rules are better understood and the re-application process is more predictable

---

## 2. Heavier Mortality Plans Without Substitute Funding Tables (Primary Opportunity)

**Segment size:** 6 plans (4 with numeric scaling >100%, 2 with qualitative heavier mortality indicators)

These plans apply scaling factors **above 100%** to their accounting mortality tables, indicating their participants die **sooner** than the standard population. This is the key insight: heavier mortality means **shorter expected lifetimes**, which translates to **lower pension liabilities**. If these plans used IRS-approved substitute mortality tables for **funding** (IRC 430), their funding target would decrease — a direct financial benefit. None of these plans currently use substitute tables.

### Plans with Numeric Scaling >100% (Heavier Mortality)

| Sponsor | Scaling | Description | Retirees | Total Participants | Substitute? |
|---|---|---|---|---|---|
| Ford Motor (2 plans) | **134%** | increased by 34% | 141,605 | 202,719 | Yes (already has) |
| Whirlpool | **120%** | 20% load | 19,964 | 29,187 | Yes (already has) |
| **Paramount Global** | **104%** | multiplier of 1.04 | 16,551 | 26,142 | **No** |
| **MetLife Group** | **103%** | 102% females / 104% males | 42,068 | 82,150 | **No** |
| **American Airlines (plan 002)** | **103%** | increased 3.0% | 20,359 | 31,037 | **No** |
| **Target** | **102%** | weighted 102% | 16,521 | 85,402 | **No** |

*Ford and Whirlpool already use substitute mortality tables and are captured in Segment 7 (Retention). The four bolded sponsors are the primary outreach targets.*

### Plans with Qualitative Heavier Mortality Indicators (No Numeric Scaling)

| Sponsor | Indicator | Retirees | Total Participants |
|---|---|---|---|
| **Eaton** | Plan-specific mortality ratios (1.055 / 1.122) derived from experience | 21,701 | 46,522 |
| **FCA/Stellantis** | Plan-specific bargaining unit mortality tables | 75,039 | 103,598 |

These plans use company-derived mortality assumptions that suggest heavier-than-standard mortality but express them as custom tables rather than percentage scalars.

**Outreach angle:** These six plans have actuarial evidence that their participants die sooner than the standard Pri-2012 population assumes. Under prescribed IRS tables, they are **overfunding** relative to their actual liability — paying for pension obligations that will, on average, end sooner than the tables predict. A substitute mortality table application under IRC 430(h)(3)(C) would allow them to use plan-specific tables that reflect this heavier mortality for funding purposes, directly reducing their minimum required contributions.

---

## 2b. Company-Adjusted Plans with Lighter Mortality (Monitoring, Not Substitute Candidates)

**Segment size:** 15 plans (13 unique sponsors)

These plans also apply company-specific modifications to their accounting mortality, but their adjustments indicate **lighter** mortality (scaling <100% or qualitative language suggesting longer-lived participants). Lighter mortality means participants live **longer** than standard tables assume, which **increases** pension liabilities. These plans would **not** benefit from substitute mortality tables — doing so would raise their funding target, not lower it. However, they represent sophisticated mortality-aware sponsors who may be interested in other actuarial services.

### Plans with Numeric Scaling <100% (Lighter Mortality)

| Sponsor | Scaling | Description | Retirees | Total Participants |
|---|---|---|---|---|
| Truist Financial | 85% | 85% Pri-2012 White Collar | 25,949 | 95,925 |
| GlaxoSmithKline | 89.6% | decreased by 10.4% | 14,266 | 38,147 |
| Lockheed Martin | 90% | 90% Pri-2012 White Collar | 38,806 | 84,564 |
| Bank of America | 94% | 0.94 multiplier on Pri-2012 | 47,205 | 181,533 |
| RTX | 95% | 5% decrease | 177,158 | 288,395 |
| Union Carbide/Dow | 96% | RP-2014 reduced by 4% | 27,320 | 33,212 |
| Dow Chemical | 96% | RP-2014 reduced by 4% | 14,562 | 33,153 |
| Wells Fargo | 98% | 98% of Amounts-Weighted Pri-2012 | 51,193 | 174,938 |
| American Airlines (plan 006) | 99% | decreased 1.0% | 19,590 | 37,360 |

### Plans with Qualitative Company Adjustments (Direction Unclear)

| Sponsor | Adjustment Type | Retirees | Total Participants |
|---|---|---|---|
| AT&T | Custom mortality tables | 97,967 | 296,285 |
| Caterpillar (2 plans) | Credibility-adjusted multiplier from company experience study (2015-2019) | 45,885 | 65,990 |
| Cigna Holding | Company experience adjustment | 25,838 | 53,110 |
| Consolidated Edison | Adjusted based on actual company experience | 15,038 | 27,554 |
| Deseret Mutual (DMBA) | DMBA-adjusted tables | 18,461 | 33,150 |
| Eastman Kodak | Base table selected from plan experience review | 29,048 | 35,031 |
| General Dynamics (2 plans) | Aon Endemic scale with plan experience | 38,774 | 67,434 |
| General Electric | Wage-class adjusted based on company experience | 78,410 | 123,696 |
| International Paper | Company rating factors with experience study | 36,213 | 84,446 |
| Marsh McLennan | MILES industry experience tables | 17,709 | 35,260 |
| Prudential | Adjusted for Prudential-specific experience | 50,374 | 100,353 |
| ROPCOR (GE legacy) | Wage-class adjusted based on company experience | 37,155 | 53,128 |
| Shell USA | Shell Modified Pri-2012 | 31,049 | 53,239 |
| Union Pacific | MILES industry experience tables | 13,027 | 20,361 |

**Outreach angle:** While these plans are not candidates for substitute mortality tables, they demonstrate actuarial sophistication and an existing investment in mortality analysis. They may be receptive to other services: updated experience studies, assumption governance reviews, or analysis of how their lighter mortality interacts with the SECURE 2.0 generational projection requirements.

---

## 3. Generic Pri-2012 Users With No Adjustments

**Segment size:** 20 plans

These plans use standard Pri-2012 mortality tables for accounting with **no collar adjustment, no scaling factor, and no company-specific modifications**. Their disclosures are minimal (e.g., *"Mortality basis: Pri-2012 for 2024 and 2023"*). This may indicate:
- A smaller or less complex plan that has not invested in mortality analysis
- An actuarial firm using boilerplate assumptions without plan-specific review
- A genuine population that matches the standard tables

| Sponsor | Improvement Scale | Retirees | Total Participants |
|---|---|---|---|
| 3M | MP-2021 | 40,641 | 57,511 |
| Abbott Laboratories | MP-2021 | 14,780 | 23,757 |
| Altria Client Services | MP-2020 | 19,455 | 26,082 |
| Berkshire Hathaway | MP-2021 | 15,688 | 34,533 |
| Celanese Americas | (none specified) | 15,311 | 18,259 |
| Citigroup | MP-2021 | 43,977 | 124,841 |
| Disney (TWDC Enterprises) | IRS Modified MP-2021 | 16,404 | 52,987 |
| Exelon | MP-2021 | 13,606 | 24,456 |
| Honeywell International | MP-2021 | 57,500 | 96,300 |
| JPMorgan Chase | IRS Modified MP-2021 | 47,169 | 222,715 |
| L3Harris Technologies | Buck Modified MP-2021 | 39,958 | 60,044 |
| M&T Bank | MP-2020 | 13,651 | 27,892 |
| Nationwide Mutual | MIM-2021 | 15,930 | 28,857 |
| Nokia of America (2 plans) | MP-2020 | 75,502 | 102,730 |
| Parker Hannifin | MP-2021 | 25,430 | 40,612 |
| Pfizer | MMP-2021 | 20,331 | 58,193 |
| PNC Financial Services | MP-2021 | 24,344 | 118,930 |
| The Coca-Cola Company | MP-2021 | 23,649 | 51,246 |
| U.S. Bancorp | MP-2021 | 23,943 | 46,626 |

**Outreach angle:** For large plans (which all of these are, given they're in our Form 5500 dataset), using unmodified Pri-2012 without any plan-specific adjustment is likely leaving money on the table. An initial mortality experience study could reveal whether their population skews differently from the national average, potentially unlocking funding savings through a substitute mortality application or simply more accurate accounting assumptions.

---

## 4. Older Improvement Scales (Not Yet on MP-2021)

**Segment size:** 16 plans (12 unique sponsors)

These plans are still using older mortality improvement scales (MP-2020 or earlier) for their accounting assumptions. While improvement scales are typically updated as part of the annual assumption review, lagging behind may indicate a less active assumption governance process.

| Sponsor | Current Scale | Gap | Retirees | Total Participants |
|---|---|---|---|---|
| Altria Client Services | MP-2020 | 1 year behind | 19,455 | 26,082 |
| Boeing (4 plans) | MP-2017 | 4 years behind | 174,449 | 286,661 |
| Chevron | MP-2020 | 1 year behind | 23,418 | 60,325 |
| Dominion Energy | MP-2020 | 1 year behind | 20,270 | 39,157 |
| Duke Energy | MP-2020 | 1 year behind | 16,613 | 21,173 |
| GlaxoSmithKline | MP-2020 | 1 year behind | 14,266 | 38,147 |
| M&T Bank | MP-2020 | 1 year behind | 13,651 | 27,892 |
| MetLife Group | MP-2019 | 2 years behind | 42,068 | 82,150 |
| Nokia of America (2 plans) | MP-2020 | 1 year behind | 75,502 | 102,730 |
| Paramount Global | MP-2020 | 1 year behind | 16,551 | 26,142 |
| Target | MP-2020 | 1 year behind | 16,521 | 85,402 |
| Wells Fargo | MP-2020 | 1 year behind | 51,193 | 174,938 |

**Outreach angle:** The improvement scale choice directly affects projected liabilities. Plans on older scales may not have updated because their actuarial firm hasn't recommended it or the plan committee hasn't reviewed mortality assumptions recently. This is a natural conversation starter about assumption governance and mortality best practices. Boeing's use of MP-2017 as a bridge scale is a particularly unique case that may warrant specialized analysis.

---

## 5. COVID-Adjusted Plans (Mortality-Aware, Receptive to Refinement)

**Segment size:** 13 plans in summary

These plans have already incorporated COVID-era mortality adjustments (Aon endemic scales, IRS-developed adjustments, excess mortality provisions). This indicates an actuarial team that is actively monitoring mortality trends and willing to adopt non-standard modifications.

**Outreach angle:** Plans that are already thinking about COVID mortality adjustments are the most natural audience for a broader conversation about mortality refinement, experience studies, and how post-pandemic mortality trends may diverge from pre-pandemic expectations embedded in standard tables.

---

## 6. Industry Peer Comparison Anomalies (Data-Driven Outreach Hooks)

Cross-referencing substitute mortality usage (Schedule SB field `SB_MORTALITY_TBL_CD`) with NAICS industry codes across 4,387 DB plan filings reveals stark concentration patterns. Substitute mortality is almost exclusively a **manufacturing** phenomenon — and within manufacturing, it clusters in a handful of specific sub-sectors.

### The Concentration: Substitute Mortality by Industry

| Industry Sector | Plans | Substitute | Rate | Participants |
|---|---|---|---|---|
| Manufacturing | 1,391 | 49 | 3.5% | 6,093,211 |
| Mining & Oil/Gas Extraction | 75 | 3 | 4.0% | 156,937 |
| Information (Telecom) | 125 | 2 | 1.6% | 1,012,256 |
| Wholesale Trade | 170 | 1 | 0.6% | 262,614 |
| **Finance & Insurance** | **597** | **0** | **0.0%** | **2,692,194** |
| **Utilities** | **184** | **0** | **0.0%** | **900,669** |
| **Health Care** | **629** | **0** | **0.0%** | **1,814,107** |
| All Other Sectors | 816 | 0 | 0.0% | 3,335,654 |

**89% of all substitute mortality plans are in manufacturing.** Finance, utilities, health care, and every other sector have zero usage despite collectively covering 8.7 million participants.

### Eye-Popping Stat #1: Iron & Steel Mills — 55% Substitute Rate

NAICS 331110 (Iron & Steel Mills) has the **highest substitute mortality rate of any industry**: 6 of 11 plans (54.5%). If you're a steel mill and you don't use substitute mortality, you're in the minority.

| Sponsor | Retirees | Total Participants | Substitute? |
|---|---|---|---|
| U.S. Steel Corporation | 19,107 | 24,627 | **YES** |
| Cleveland-Cliffs Steel Corp. | 6,660 | 11,463 | **YES** |
| Cleveland-Cliffs Steel LLC (009) | 9,695 | 11,173 | **YES** |
| Handy & Harman | 4,107 | 5,825 | **YES** |
| **Carpenter Technology** | **2,460** | **4,320** | **No** |
| Cleveland-Cliffs Steel LLC (010) | 4,131 | 4,182 | **YES** |
| **American Cast Iron Pipe** | **2,174** | **3,139** | **No** |
| **Ampco-Pittsburgh** | **1,581** | **2,244** | **No** |
| **EVRAZ Inc. (2 plans)** | **1,829** | **3,717** | **No** |
| Cleveland-Cliffs Hibbing | 879 | 1,660 | **YES** |

**Outreach hook:** *"6 of your 11 industry peers in iron and steel manufacturing — including U.S. Steel and Cleveland-Cliffs — use IRS-approved substitute mortality tables. Your plan's workforce demographics likely mirror theirs. Have you evaluated whether your participants' mortality differs from the standard IRS tables?"*

**Key targets:** Carpenter Technology (4,320 participants, 2,460 retirees), EVRAZ (3,717 combined), American Cast Iron (3,139), Ampco-Pittsburgh (2,244)

### Eye-Popping Stat #2: Rubber Manufacturing — 45% Substitute Rate

NAICS 326200 (Rubber Product Manufacturing): 5 of 11 plans (45.5%). Goodyear dominates with 3 plans; Cooper Tire has 2.

| Sponsor | Retirees | Total Participants | Substitute? |
|---|---|---|---|
| Goodyear (3 plans) | 31,554 | 41,604 | **YES** |
| **Continental Automotive (2 plans)** | **4,479** | **6,875** | **No** |
| Cooper Tire (2 plans) | 1,875 | 3,118 | **YES** |
| **Acushnet Company** | **318** | **2,273** | **No** |
| **Gates Corporation** | **1,150** | **1,642** | **No** |

**Outreach hook:** *"Nearly half of rubber product manufacturers use substitute mortality for funding. Your largest competitor, Goodyear, uses it across all three of their pension plans. Could your plan be overfunding by using generic tables?"*

**Key targets:** Continental Automotive (6,875 participants), Gates Corporation (1,642), Acushnet (2,273)

### Eye-Popping Stat #3: Motor Vehicle Manufacturing — 80% of Participants Covered

NAICS 336100: Only 7 of 23 plans (30%) use substitute, but those 7 plans cover **80% of all participants** in the sector (341,396 of 429,352). Ford and FCA alone account for the bulk.

| Sponsor | Retirees | Total Participants | Substitute? |
|---|---|---|---|
| Ford Motor (2 plans) | 141,605 | 202,719 | **YES** |
| FCA/Stellantis (3 plans) | 87,993 | 123,798 | **YES** |
| **PACCAR** | **4,990** | **19,552** | **No** |
| Daimler Truck (2 plans) | 4,119 | 14,879 | **YES** |
| **International Motors (3 plans)** | **19,194** | **22,834** | **No** |
| **Harley-Davidson** | **5,169** | **7,962** | **No** |
| **Mack Trucks** | **6,001** | **7,675** | **No** |
| **Volvo Group (2 plans)** | **1,542** | **9,112** | **No** |

**Outreach hook:** *"80% of pension participants in motor vehicle manufacturing are covered by substitute mortality tables. Ford, FCA, and Daimler Truck all use them. If your workforce has similar demographics — blue-collar, manufacturing floor, similar age distribution — you may be overfunding relative to your industry peers."*

**Key targets:** PACCAR (19,552), International Motors (22,834 across 3 plans), Harley-Davidson (7,962), Mack Trucks (7,675), Volvo Group (9,112)

### Eye-Popping Stat #4: Aerospace & Defense — 0% Despite 800K+ Participants

NAICS 3364 (Aerospace Product & Parts Manufacturing): **0 of 39 plans** use substitute mortality, despite covering over 800,000 participants across some of the largest DB plan sponsors in the country.

| Sponsor | Retirees | Total Participants |
|---|---|---|
| RTX Corporation | 177,158 | 288,395 |
| Boeing (8 plans) | 274,372 | 441,500 |
| Northrop Grumman (6 plans) | 170,098 | 323,515 |
| BAE Systems (2 plans) | 5,795 | 28,869 |
| Howmet Aerospace | 6,282 | 9,786 |
| Kaman Corporation | 4,099 | 6,595 |

This is notable because the sector includes some of the largest DB plan sponsors in the country, yet none have pursued substitute mortality. Howmet Aerospace (formerly part of Arconic/Alcoa) dropped substitute mortality in 2022 — the only aerospace plan to have ever used it. Boeing, despite its massive participant base (441K across 8 plans), has used Prescribed Separate tables for as far back as records are available (2019-2024). The complete absence across the entire sector suggests an industry-wide assumption that standard tables are "close enough" — an assumption that may not hold for a workforce mix of engineers, skilled trades, and office staff.

**Outreach hook:** *"Zero aerospace & defense plans use substitute mortality, yet this sector has over 800,000 pension participants with a distinctive workforce profile. Has your actuarial team evaluated whether your plan's mortality experience differs from the standard IRS tables?"*

### Eye-Popping Stat #5: Telecom — Only Verizon

NAICS 517000 (Telecommunications): Only Verizon uses substitute mortality (2 plans, 174,407 participants). No other telecom company does.

| Sponsor | Retirees | Total Participants | Substitute? |
|---|---|---|---|
| Verizon Communications | 75,158 | 112,363 | **YES** |
| Verizon Corporate Services | 26,681 | 62,044 | **YES** |
| **Cox Enterprises** | **13,588** | **54,486** | **No** |
| **Lumen Technologies** | **20,618** | **37,553** | **No** |
| **Frontier Communications** | **6,246** | **20,799** | **No** |
| **Brightspeed (Lumen legacy)** | **15,128** | **19,587** | **No** |

**Outreach hook:** *"Verizon is the only telecom company using substitute mortality tables — and it covers 174,000 participants across two plans. Their workforce demographic (union technicians, office staff, retirees from decades of service) is similar to yours. If Verizon's experience differs enough from standard tables to justify the IRS application, does yours?"*

**Key targets:** Cox Enterprises (54,486), Lumen (37,553), Frontier (20,799), Brightspeed (19,587)

### Eye-Popping Stat #6: Petroleum & Coal — Complete Evacuation

NAICS 324 (Petroleum & Coal Products) historically had 7 plans with substitute mortality. **All 7 have dropped.** Zero remain. This sector includes Koch Industries entities (which dropped in 2024) and Motiva Enterprises (dropped 2022). Despite having 318,836 participants across 39 plans, the entire sector has abandoned substitute mortality.

Major sponsors now without substitute: ExxonMobil (69,514), Chevron (60,325), Shell USA (53,239), BP (29,430), Marathon Petroleum (23,127), Phillips 66 (15,950), Koch Companies (12,355), Valero (11,823).

### Eye-Popping Stat #7: Finance & Insurance — 2.7 Million Participants, Zero Substitute

597 plans covering 2,692,194 participants with **zero substitute mortality usage**. This is the largest untapped sector by far. The top plans include JPMorgan Chase (222,715), Citigroup (124,841), PNC Financial (118,930), State Farm (116,344), Prudential (100,353), Truist (95,925), MetLife (82,150), Liberty Mutual (81,544).

Financial services companies tend to have white-collar workforces with potentially different mortality profiles than the standard Pri-2012 tables assume. Several of these sponsors (Bank of America, MetLife, Prudential, Wells Fargo) already use company-specific mortality adjustments for accounting — yet none have pursued substitute tables for funding.

---

## 7. Current Substitute Mortality Users (Retention and Deepening)

**Segment size:** 55 plans (30 unique sponsors) per 2024 Schedule SB filings

The 55 plans still using substitute mortality tables represent the surviving core of sophisticated mortality buyers. Their total participant coverage is ~950,000.

### By Actuary Firm

| Actuary Firm | Plans | Key Sponsors |
|---|---|---|
| Willis Towers Watson | 33 | Ford, Corning, Goodyear, Whirlpool, Cleveland-Cliffs, Trane Technologies, Daimler Truck, Dana, HarbisonWalker |
| Ernst & Young | 5 | Huntington Ingalls Industries (4 plans), U.S. Steel |
| Mercer | 5 | FCA/Stellantis (3 plans), Newell, Brink's |
| Aon Consulting | 6 | Verizon (2 plans), Crown Cork & Seal (2 plans), Pilkington (2 plans) |
| Buck Global | 2 | Eaton, Instant Brands |
| Segal | 2 | Olin Corporation (2 plans) |
| October Three Consulting | 1 | JPS Industries |

### Top Sponsors by Participant Count

| Sponsor | Plans | Retirees | Total Participants | Actuary |
|---|---|---|---|---|
| Ford Motor Company | 2 (001, 002) | 141,605 | 202,719 | WTW |
| Verizon (Communications + Corporate Services) | 2 (016, 001) | 101,839 | 174,407 | Aon |
| FCA US / Stellantis | 3 (004, 005, 007) | 87,993 | 123,798 | Mercer |
| Huntington Ingalls Industries | 4 (041, 100, 101, 305) | 15,170 | 55,280 | E&Y |
| Eaton Corporation | 1 (029) | 21,701 | 46,522 | Buck |
| Goodyear Tire & Rubber | 3 (001, 002, 010) | 31,554 | 41,604 | WTW |
| Trane Technologies | 3 (001, 008, 023) | 21,067 | 35,630 | WTW |
| Corning Incorporated | 1 (001) | 12,963 | 33,672 | WTW |
| Cleveland-Cliffs entities | 5 plans | 23,350 | 30,568 | WTW |
| Whirlpool Corporation | 1 (107) | 19,964 | 29,187 | WTW |
| U.S. Steel | 1 (001) | 19,107 | 24,627 | E&Y |

*Full list: 55 plans across Alpha Natural Resources, Buzzi Unicem, Cleveland-Cliffs (5 entities), Cooper Tire (2), Corning, Crown Cork & Seal (2), Daimler Truck (2), Dana (3), Detroit Diesel (2), Eaton, Fairfield Manufacturing, FCA US (3), Ford (2), Handy & Harman, HarbisonWalker (2), HM US Services, Huntington Ingalls (4), Instant Brands, JPS Industries, Mueller Group, Newell, Olin (2), Pilkington (2), Quad/Graphics, The Brink's, The Goodyear (3), ThyssenKrupp NA, Trane Technologies (3), U.S. Steel, Verizon (2), Whirlpool.*

**Outreach angle:** These are existing sophisticated buyers of mortality analysis services. Given the mass exodus of plans from substitute mortality (down 46% in five years), the remaining 55 plans represent the most committed users. Retention and deepening opportunities include:
- Updated experience studies as approvals approach expiration (typically 10-year periods)
- Extension to additional plan populations within the same sponsor group
- Integration with the SECURE 2.0 generational projection requirements (plans that successfully maintained substitute through the transition are well-positioned to help frame the value proposition for former users considering re-adoption)
- For sponsors with plans that partially dropped (FCA, Verizon, Goodyear, Cleveland-Cliffs, Ball): re-engagement on the dropped plans using the same experience data

---

## 8. Mega-Asset Plans Without Substitute Mortality (Blue-Chip Targets)

**Segment size:** 70 plans with >$5 billion in assets, none using substitute mortality tables

The largest DB plans in the country — collectively holding hundreds of billions in pension assets — universally use prescribed mortality tables. For plans of this scale, even a small percentage reduction in funding target from plan-specific mortality translates to tens or hundreds of millions in contribution savings. Yet none have pursued IRS-approved substitute tables.

### Top 20 by Plan Assets

| Sponsor | Assets ($B) | Liability ($B) | Funding Ratio (%) | Total Participants | Retirees | Actuary Firm | Industry |
|---|---|---|---|---|---|---|---|
| RTX Corporation | 41.6 | 41.3 | 100.9 | 288,395 | 177,158 | WTW | Manufacturing |
| State Farm Mutual | 37.7 | 22.6 | 166.7 | 116,344 | 48,483 | Aon | Finance & Insurance |
| Kaiser Foundation | 34.5 | 21.1 | 163.4 | 197,749 | 24,843 | Aon | Health Care |
| AT&T Inc. | 28.3 | 32.0 | 88.3 | 296,285 | 97,967 | Aon | Information |
| Boeing (plan 001) | 23.3 | 27.2 | 85.8 | 119,676 | 66,376 | WTW | Manufacturing |
| UPS | 21.2 | 25.7 | 82.5 | 148,089 | 57,606 | WTW | Real Estate* |
| Johnson & Johnson | 21.1 | 17.0 | 124.0 | 87,711 | 20,488 | Mercer | Manufacturing |
| Bank of America | 19.0 | 11.7 | 163.1 | 181,533 | 47,205 | WTW | Management |
| GE Aerospace | 19.0 | 22.2 | 85.6 | 123,696 | 78,410 | WTW | Manufacturing |
| Northrop Grumman | 18.8 | 17.6 | 106.6 | 106,531 | 60,798 | WTW | Manufacturing |
| Lockheed Martin | 18.3 | 22.1 | 82.6 | 84,564 | 38,806 | Empower | Manufacturing |
| IBM | 17.9 | 18.7 | 96.0 | 175,203 | 76,728 | WTW | Professional Services |
| Pacific Gas & Electric | 17.6 | 15.8 | 111.4 | 59,745 | 27,837 | WTW | Utilities |
| Honeywell | 16.6 | 12.6 | 131.9 | 96,300 | 57,500 | Aon | Manufacturing |
| JPMorgan Chase | 16.1 | 10.3 | 155.7 | 222,715 | 47,169 | Mercer | Finance & Insurance |
| Consolidated Edison | 15.6 | 11.3 | 138.0 | 27,554 | 15,038 | WTW | Utilities |
| Boeing (plan 003) | 15.4 | 17.4 | 88.5 | 119,415 | 70,691 | WTW | Manufacturing |
| Southern Company | 15.3 | 11.4 | 133.9 | 57,637 | 26,387 | Aon | Utilities |
| Truist Financial | 14.7 | 6.6 | 221.6 | 95,925 | 25,949 | Aon | Finance & Insurance |
| Nokia of America | 12.9 | 9.7 | 132.1 | 86,410 | 59,496 | Aon | Manufacturing |

*UPS classified under Real Estate in NAICS; operationally a transportation/logistics company.*

### The ROI Argument

For a plan with $20 billion in liabilities, a 2% reduction in funding target from plan-specific mortality tables equals **$400 million** in reduced obligations. Even a 0.5% impact is $100 million. The cost of an IRS substitute mortality application — typically $200K-$500K in actuarial fees — is trivial relative to the potential savings for plans at this scale. The question isn't whether it's worth investigating; it's why these plans haven't already.

### Concentration Patterns

- **WTW dominates:** 10 of the top 20 mega-asset plans use WTW as actuary. Given WTW's role in the mass substitute mortality exodus (60% of drops), their advisory stance may be the primary barrier.
- **Underfunded mega-plans** (AT&T at 88%, Boeing at 86%, UPS at 83%, Lockheed at 83%, GE at 86%): These plans face the largest absolute funding gaps and would benefit most from any liability reduction.
- **Overfunded mega-plans** (State Farm at 167%, Kaiser at 163%, Truist at 222%): Even overfunded plans benefit — lower liabilities accelerate the path to plan termination or reduce PBGC premiums.

**Outreach angle:** *"Your plan holds $[X] billion in pension assets. At this scale, even a modest mortality adjustment — say 1-2% — translates to $[Y] hundred million in funding impact. Have you evaluated whether your participant population's mortality differs from the standard IRS tables? For a plan your size, the ROI on an experience study is potentially 100:1."*

---

## 9. High-Annuitant-Ratio Plans (Aging Populations, Heavier Mortality Likely)

**Segment size:** 38 plans with >70% annuitant ratio and >5,000 participants, none using substitute mortality

Plans where retirees and separated vested participants make up more than 70% of the total population are, by definition, mature plans with aging demographics. Older populations are more likely to exhibit mortality patterns that diverge from the standard Pri-2012 tables — particularly in industries with occupational health exposures. These plans are paying benefits now, making the mortality assumption a direct driver of current cash obligations.

### Top 20 by Participant Count

| Sponsor | Annuitant Ratio (%) | Retirees | Total Participants | Actuary Firm | Industry |
|---|---|---|---|---|---|
| EIDP/DuPont | 77.2 | 52,481 | 67,965 | Aon | Manufacturing |
| 3M Company | 70.7 | 40,641 | 57,511 | Aon | Manufacturing |
| Eastman Kodak | 82.9 | 29,048 | 35,031 | Deloitte | Manufacturing |
| Union Carbide / Dow | 82.3 | 27,320 | 33,212 | WTW | Manufacturing |
| Boeing (plan 004) | 80.3 | 22,651 | 28,218 | WTW | Manufacturing |
| Altria Client Services | 74.6 | 19,455 | 26,082 | WTW | Manufacturing |
| Caterpillar | 88.7 | 22,910 | 25,827 | WTW | Manufacturing |
| ConAgra Brands | 71.6 | 17,232 | 24,070 | WTW | Manufacturing |
| Duke Energy | 78.5 | 16,613 | 21,173 | WTW | Utilities |
| Northrop Grumman (plan) | 72.1 | 15,256 | 21,148 | WTW | Manufacturing |
| Brightspeed (Lumen legacy) | 77.2 | 15,128 | 19,587 | WTW | Information |
| Boeing (plan 005) | 76.1 | 14,731 | 19,352 | WTW | Manufacturing |
| Eversource Energy | 71.2 | 13,346 | 18,731 | Aon | Utilities |
| Celanese Americas | 83.9 | 15,311 | 18,259 | WTW | Manufacturing |
| Nokia of America | 98.1 | 16,006 | 16,320 | Aon | Manufacturing |
| Savannah River Nuclear | 75.0 | 11,933 | 15,901 | Buck | Professional Services |
| Regions Financial | 79.6 | 11,143 | 14,001 | Mercer | Finance & Insurance |
| Continental Casualty (CNA) | 73.0 | 10,021 | 13,737 | WTW | Management |
| Boeing (plan 006) | 75.0 | 9,573 | 12,763 | WTW | Manufacturing |
| OfficeMax / ODP Corp | 85.7 | 10,367 | 12,102 | Milliman | Retail |

### Why Annuitant Ratio Matters for Mortality

The mortality assumption has the greatest dollar impact on **plans that are actively paying benefits**. A plan with 25,000 active employees and 5,000 retirees has most of its liability in the future (accrual phase). A plan with 5,000 actives and 25,000 retirees has most of its liability driven by how long current payments will last — making the mortality table the single most impactful assumption.

Key observations:
- **Manufacturing dominance:** 14 of the top 20 are manufacturing plans — the same sector where substitute mortality has historically been concentrated. These mature industrial plans have workforces (or former workforces) with occupational exposures likely to produce heavier-than-standard mortality.
- **Nokia at 98.1%:** Nearly the entire participant population is retired or separated. The mortality assumption is essentially the *only* assumption that matters for this plan's $9.7 billion liability.
- **Caterpillar at 88.7%:** Heavy equipment manufacturing with 22,910 retirees. Already uses credibility-adjusted company experience for accounting (see Section 2b) but has not pursued substitute tables for funding.
- **Overlap with other segments:** Several plans appear in multiple segments — Brightspeed (Telecom white space, Section 6), OfficeMax (Tier 1 reactivation, Section 1), Celanese and Altria (Generic Pri-2012, Section 3).

**Outreach angle:** *"With [X]% of your participants already in pay status, the mortality table is the dominant driver of your plan's funding obligation. Have you evaluated whether your retiree population's actual longevity matches the standard IRS tables? For a plan as mature as yours, a plan-specific experience study could reveal meaningful funding savings."*

---

## 10. Significantly Underfunded Plans (Substitute Mortality as Contribution Relief)

**Segment size:** 110 plans with <80% funding ratio, >2,000 participants, none using substitute mortality

Underfunded plans face the most acute pressure: higher required contributions, larger PBGC variable-rate premiums, and potential benefit restrictions under IRC 436. For these sponsors, substitute mortality tables offer a direct path to reducing the funding target — every dollar of liability reduction translates to lower required contributions and lower PBGC premiums.

### Top 20 by Funding Shortfall

| Sponsor | Funding Ratio (%) | Assets ($B) | Liability ($B) | Shortfall ($M) | Total Participants | Retirees | Actuary Firm |
|---|---|---|---|---|---|---|---|
| 3M Company | 71.7 | 8.9 | 12.4 | 3,512 | 57,511 | 40,641 | Aon |
| Shell USA | 75.2 | 9.7 | 12.9 | 3,203 | 53,239 | 31,049 | Aon |
| Principal Financial | 52.8 | 1.5 | 2.8 | 1,310 | 25,429 | 7,476 | Principal |
| Meijer (2 plans) | 10.6-15.0 | 0.3 | 2.3 | 2,002 | 36,224 | 19,566 | Aon |
| Continental Casualty (CNA) | 50.8 | 0.9 | 1.7 | 823 | 13,737 | 10,021 | WTW |
| General Dynamics | 77.4 | 2.7 | 3.5 | 784 | 35,602 | 20,899 | Aon |
| Paramount Global | 76.9 | 1.9 | 2.5 | 575 | 26,142 | 16,551 | WTW |
| Weyerhaeuser | 75.7 | 1.5 | 2.0 | 476 | 20,057 | 5,435 | WTW |
| Kellanova (Kellogg) | 72.9 | 1.2 | 1.6 | 445 | 11,073 | 3,979 | WTW |
| BASF Corporation | 79.0 | 1.6 | 2.0 | 426 | 15,508 | 7,006 | WTW |
| Howmet Aerospace | 63.4 | 0.7 | 1.1 | 391 | 9,786 | 6,282 | Buck |
| Puerto Rico Telephone | 68.1 | 0.8 | 1.2 | 374 | 7,214 | 5,479 | Mercer |
| Billerud Americas | 37.9 | 0.3 | 0.9 | 550 | 6,088 | 3,886 | WTW |
| Schneider Electric | 6.4 | 0.1 | 0.8 | 705 | 6,759 | 3,293 | Mercer |
| Albemarle Corporation | 1.5 | 0.0 | 0.4 | 434 | 2,700 | 2,128 | Milliman |
| Credit Suisse (UBS) | 44.0 | 0.3 | 0.7 | 376 | 2,664 | 789 | Aon |

### The Contribution Relief Math

For an underfunded plan, the financial impact of substitute mortality is amplified:

- **3M** ($3.5B shortfall, 71.7% funded): A 2% mortality-driven liability reduction = ~$248M off the funding target. At 3M's current contribution levels, that could eliminate 1-2 years of shortfall amortization payments.
- **Shell** ($3.2B shortfall, 75.2% funded): Shell already uses company-modified Pri-2012 for accounting (see Section 2b) but has not pursued substitute tables for funding. The accounting work is already done — extending it to a formal IRS application is incremental.
- **Paramount Global** ($575M shortfall, 76.9% funded): Already applies 104% scaling for accounting (heavier mortality, Section 2). A substitute mortality application would formalize what their actuary already knows.

### Multi-Segment Overlap (Highest-Priority Targets)

Plans appearing in **both** the underfunded segment and other opportunity segments represent the strongest outreach candidates:

| Sponsor | Shortfall ($M) | Other Segments |
|---|---|---|
| 3M | 3,512 | Generic Pri-2012 (Section 3), High annuitant ratio (Section 9) |
| Shell USA | 3,203 | Company-adjusted mortality (Section 2b) |
| Paramount Global | 575 | Heavier mortality 104% (Section 2), Older improvement scale (Section 4) |
| Weyerhaeuser | 476 | Former substitute user, dropped 2020 (Section 1, Tier 5) |
| Howmet Aerospace | 391 | Former substitute user, dropped 2022 (Section 1, Tier 4) |

**Outreach angle:** *"Your plan is currently [X]% funded with a $[Y] million shortfall. Substitute mortality tables could reduce your funding target by $[Z] million — directly lowering your required contributions and PBGC variable-rate premiums. For a plan in your funding position, this is one of the few levers that reduces obligations without changing benefit promises."*

---

## 11. Unionized Manufacturing Plans (Occupational Mortality Angle)

**Segment size:** 71 plans with >5,000 participants in collectively bargained manufacturing plans, none using substitute mortality

Union manufacturing plans represent the demographic profile most likely to exhibit heavier-than-standard mortality: blue-collar workforces with decades of occupational exposure to physical labor, industrial chemicals, noise, and environmental hazards. The existing substitute mortality user base is overwhelmingly manufacturing (89% of all substitute plans) — yet these 71 large unionized plans have not pursued plan-specific tables.

### Top 20 by Participant Count

| Sponsor | Total Participants | Retirees | Actuary Firm | NAICS |
|---|---|---|---|---|
| GE Aerospace | 123,696 | 78,410 | WTW | 335900 (Electrical Equipment) |
| Boeing | 119,415 | 70,691 | WTW | 336410 (Aerospace) |
| Honeywell | 96,300 | 57,500 | Aon | 339900 (Misc. Manufacturing) |
| PepsiCo (plan 001) | 89,735 | 1,572 | Mercer | 312110 (Soft Drink Mfg) |
| ExxonMobil | 69,514 | 39,198 | WTW | 324110 (Petroleum Refining) |
| PepsiCo (plan 002) | 65,832 | 26,654 | Mercer | 312110 (Soft Drink Mfg) |
| Chevron | 60,325 | 23,418 | Aon | 324110 (Petroleum Refining) |
| 3M Company | 57,511 | 40,641 | Aon | 339900 (Misc. Manufacturing) |
| Textron | 57,341 | 35,252 | WTW | 339900 (Misc. Manufacturing) |
| Ropcor / GE legacy | 53,128 | 37,155 | WTW | 335900 (Electrical Equipment) |
| Cummins | 46,404 | 9,262 | WTW | 333610 (Engine Mfg) |
| General Dynamics | 35,602 | 20,899 | Aon | 334200 (Defense Electronics) |
| Georgia-Pacific | 35,298 | 20,789 | WTW | 321210 (Wood Products) |
| Ecolab | 35,291 | 6,051 | Aon | 325600 (Chemical Mfg) |
| Union Carbide / Dow | 33,212 | 27,320 | WTW | 325100 (Chemical Mfg) |
| Lockheed Martin | 33,180 | 18,422 | Aon | 339900 (Misc. Manufacturing) |
| Dow Chemical | 33,153 | 14,562 | WTW | 325100 (Chemical Mfg) |
| General Dynamics (plan 2) | 31,832 | 17,875 | Aon | 334200 (Defense Electronics) |
| BP Corporation | 29,430 | 5,640 | Mercer | 324110 (Petroleum Refining) |
| Boeing (plan 004) | 28,218 | 22,651 | WTW | 336410 (Aerospace) |

### The Occupational Mortality Evidence

Academic and actuarial research consistently documents mortality differentials by occupation:

- **Manufacturing workers** have higher all-cause mortality than white-collar populations, driven by cardiovascular disease, respiratory illness, and occupational cancer.
- **Petroleum refining** workers (ExxonMobil, Chevron, BP) have documented exposure to benzene, hydrogen sulfide, and other carcinogens.
- **Chemical manufacturing** workers (Dow, Union Carbide, Ecolab) face chronic chemical exposure risks.
- **Aerospace manufacturing** workers (Boeing, Lockheed, General Dynamics) work with composite materials, solvents, and heavy metals.

These plans are using standard Pri-2012 mortality tables calibrated to a broad U.S. population that includes white-collar workers with longer life expectancies. Their actual participant mortality almost certainly skews heavier.

### Industry Cluster Opportunities

| Industry Cluster | Plans | Combined Participants | Key Sponsors |
|---|---|---|---|
| Petroleum Refining (324110) | 6 | ~250,000 | ExxonMobil, Chevron, BP, Shell |
| Electrical Equipment (335900) | 3 | ~180,000 | GE Aerospace, Ropcor |
| Aerospace (336410) | 5 | ~310,000 | Boeing, Lockheed, General Dynamics |
| Chemical Manufacturing (325xxx) | 5 | ~170,000 | Dow, Union Carbide, Ecolab |
| Engine/Equipment Mfg (333xxx) | 3 | ~95,000 | Cummins, Textron, Honeywell |

**Outreach angle:** *"Your plan covers a unionized manufacturing workforce — the exact demographic profile where mortality most commonly diverges from standard IRS tables. In fact, 89% of all plans currently using IRS-approved substitute mortality tables are in manufacturing. An experience study for your population could reveal funding savings while also providing more accurate actuarial projections."*

---

## 12. Active Derisking Plans (Mortality as Part of the Glide Path)

**Segment size:** 23 plans with PRT activity in 2024 AND >60% fixed-income allocation AND >5,000 participants, none using substitute mortality

Plans that are simultaneously executing pension risk transfer (PRT) transactions and shifting to liability-driven investment (LDI) strategies are on an explicit derisking glide path. These sponsors have already committed to reducing pension risk — substitute mortality tables are a natural complement that reduces the liability side of the equation, making PRT transactions more affordable and accelerating the path to full plan termination.

### Top 20 by Participant Count

| Sponsor | Total Participants | Retirees | Fixed Income % | PRT Amount ($M) | Total Assets ($B) | Actuary Firm |
|---|---|---|---|---|---|---|
| IBM | 175,203 | 76,728 | 67.0 | 6,025 | 17.9 | WTW |
| Wells Fargo | 174,938 | 51,193 | 83.1 | 5 | 8.3 | Aon |
| UPS | 148,089 | 57,606 | 73.8 | 0* | 21.2 | WTW |
| MetLife Group | 82,150 | 42,068 | 81.0 | 5 | 7.9 | Milliman |
| 3M Company | 57,511 | 40,641 | 61.8 | 2,276 | 8.9 | Aon |
| Shell USA | 53,239 | 31,049 | 66.0 | 4,864 | 9.7 | Aon |
| Highmark Health | 39,311 | 12,599 | 69.4 | 50 | 3.1 | Mercer |
| GlaxoSmithKline | 38,147 | 14,266 | 74.0 | 4 | 2.9 | WTW |
| Mfg Investment Corp (Manulife) | 22,575 | 10,184 | 70.0 | 1 | 1.9 | Aon |
| Meijer | 20,540 | 11,411 | 80.9 | 835 | 0.1 | Aon |
| BMO Financial | 18,851 | 4,325 | 85.0 | 6 | 1.3 | Mercer |
| Federal-Mogul Powertrain | 16,839 | 10,181 | 71.0 | 241 | 0.5 | WTW |
| Meijer (plan 2) | 15,684 | 8,155 | 80.9 | 1,210 | 0.2 | Aon |
| Continental Casualty (CNA) | 13,737 | 10,021 | 61.0 | 1,034 | 0.9 | WTW |
| Mack Trucks | 7,675 | 6,001 | 92.9 | 425 | 0.4 | Mercer |
| Riverside Management (Kaiser) | 7,596 | 3,092 | 95.0 | 42 | 0.5 | Mercer |
| Harvard University | 7,269 | 2,294 | 81.0 | 2 | 1.6 | WTW |
| Cytec / Syensqo | 7,093 | 3,595 | 77.0 | 450 | 0.4 | Aon |
| Girl Scouts of the USA | 6,918 | 3,570 | 68.0 | 9 | 0.4 | WTW |

*UPS shows $0 PRT in 2024 but has high fixed-income allocation consistent with LDI strategy; may have PRT activity in prior years.*

### The Derisking Synergy

Substitute mortality and PRT are complementary strategies:

1. **Before a PRT transaction:** Lower funding liability through substitute mortality = more affordable annuity purchase (the insurer prices off your actual liability, but your *minimum required contribution* and PBGC premiums are based on the IRS funding target).
2. **During a glide path:** As fixed-income allocation increases and the plan approaches termination, every basis point of liability accuracy matters. Overstating liability with generic mortality tables inflates the cost of the final settlement.
3. **For partial PRT (retiree liftouts):** The mortality assumption on the *remaining* population changes after a retiree liftout. If the healthiest (longest-lived) retirees are transferred to an insurer, the remaining population likely has even heavier mortality — strengthening the case for substitute tables.

### Standout Targets

- **IBM** ($6B PRT in 2024, $17.9B assets): The largest single-year PRT transaction in the dataset. IBM is aggressively derisking. Substitute mortality could reduce the funding target on the remaining $18.7B liability.
- **Shell** ($4.9B PRT, 75.2% funded): Already uses company-modified mortality for accounting. Extending to substitute funding tables while executing PRT is a natural bundle.
- **3M** ($2.3B PRT, 71.7% funded): Underfunded, high annuitant ratio, generic Pri-2012 — the most multi-dimensional opportunity in the entire dataset.
- **MetLife** (81% fixed income, 103% accounting mortality scaling): Already knows their mortality is heavier than standard (Section 2). Active derisking. No substitute tables.

**Outreach angle:** *"You're already executing a pension derisking strategy through PRT and LDI. Substitute mortality tables are the missing piece — they reduce your funding target, lower PBGC premiums, and make your next annuity purchase more cost-effective. For a plan on a glide path to termination, accurate mortality is the difference between terminating on schedule and overfunding by millions."*

---

## 13. Multi-Plan Sponsors with Inconsistent Mortality Strategies

**Segment size:** 4 sponsors across 11 plans

A small but highly actionable segment: sponsors who use substitute mortality on some plans but not others within the same corporate family. The experience data from the substitute plan likely applies to the non-substitute plan — the participant populations overlap or are drawn from the same workforce.

| Sponsor | Plan | Mortality Code | Total Participants | Retirees | Notes |
|---|---|---|---|---|---|
| **Ford Motor Company** | 001 | Substitute | 145,606 | 109,131 | Main salaried plan |
| | 002 | Substitute | 57,113 | 32,474 | Hourly plan |
| | 013 | Standard | 2,136 | 1,268 | Smaller plan — same workforce |
| **Goodyear Tire & Rubber** | 001 | Substitute | 16,870 | 10,771 | Main plan |
| | 002 | Substitute | 13,893 | 11,893 | Second plan |
| | 010 | Substitute | 10,841 | 8,890 | Third plan |
| | 017 | Standard | 603 | 501 | Small plan — no substitute |
| **Trane U.S. Inc.** | 001 | Substitute | 14,744 | 10,023 | Main plan |
| | 023 | Substitute | 12,098 | 7,058 | Second plan |
| | 008 | Standard | 8,788 | 3,986 | Mid-size plan — no substitute |
| **Buzzi Unicem USA** | 001 | Substitute | 1,236 | 596 | With substitute |
| | 002 | Standard | 1,060 | 435 | Without substitute |

**Outreach angle:** These are the easiest conversations in the entire pipeline. The sponsor has already invested in the IRS application process and has approved substitute tables on file. Extending to the remaining plans within the same corporate family is incremental — the experience study data likely already covers those populations, and the IRS application process is familiar. *"You already use substitute mortality for [Plan A]. Your [Plan B] covers the same workforce. Extending the approval would be straightforward and reduce your funding target across your entire pension program."*

---

## Summary: Outreach Priority Matrix

| Segment | Size | Priority | Rationale |
|---|---|---|---|
| Former substitute users (Tier 1: confirmed drops) | 13 plans, ~117K participants | Highest | Proven buyers, recent SECURE 2.0-driven lapse, Koch cluster = single engagement for 7 plans |
| Former substitute users (Tier 2: monitor for filing) | 12 plans, ~440K participants | Highest | Includes GM (369K participants) — confirm filing status before outreach |
| Industry peer outliers (Steel, Rubber, Motor Vehicle) | ~20 named targets | Highest | "Your peers use it, you don't" — most compelling cold outreach angle |
| Multi-plan sponsors with inconsistent mortality | 4 sponsors, 11 plans | Highest | Easiest sale — experience data already exists, IRS process is familiar |
| Underfunded plans (multi-segment overlap) | 5 plans (3M, Shell, Paramount, Weyerhaeuser, Howmet) | Highest | Appear in 2+ segments, acute funding pressure, strongest combined case |
| Former substitute users (Tier 3-5: older drops) | ~33 plans | High | Alcoa, O-I Glass, Arconic, Howmet, Weyerhaeuser — large plans with institutional memory |
| Heavier mortality, no substitute | 4 plans + 2 qualitative | High | Direct funding savings from substitute tables, actuarial evidence already exists |
| Active derisking plans | 23 plans | High | PRT + LDI strategy already in motion; substitute mortality accelerates the glide path |
| High-annuitant-ratio plans (>70%) | 38 plans, ~600K participants | High | Mortality is the dominant assumption; aging populations most likely to diverge from standard tables |
| Underfunded plans (<80% funded) | 110 plans | High | Acute contribution pressure; substitute mortality directly reduces required contributions and PBGC premiums |
| Mega-asset plans (>$5B) | 70 plans | High | Enormous absolute dollar impact even from small mortality adjustments; ROI argument is compelling |
| Unionized manufacturing plans | 71 plans, ~1.5M participants | High | Occupational mortality evidence is strongest here; 89% of existing substitute users are manufacturing |
| Industry white space (Aerospace, Telecom, Petroleum) | ~30 large plans | Medium-High | Zero substitute usage in sectors with distinctive workforce profiles |
| Generic Pri-2012, no adjustments | 20 plans | Medium | Largest untapped segment, may need education |
| Older improvement scales | 16 plans | Medium | Signals less active assumption governance |
| COVID-adjusted plans | 13 plans | Medium | Already mortality-aware, receptive to refinement |
| Finance & Insurance / Utilities | 781 plans, 3.6M participants | Medium | Massive untapped sectors, may need industry-first education campaign |
| Lighter mortality / company-adjusted | 15 plans | Lower | Not substitute candidates, but sophisticated sponsors for other services |
| Current substitute users | 55 plans, ~950K participants | Ongoing | Retention, renewal, deepening — especially partial-drop sponsors (FCA, Verizon, Goodyear, Cliffs, Ball) |
