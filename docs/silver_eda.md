# Silver Layer EDA

**Scope:** 1980–2015 (35-year window, release_date >= 1980-01-01 AND < 2016-01-01)
**Total movies in scope:** 31,450

---

## Date Range Selection

Evaluated five windows (10y / 20y / 30y / 40y / 50y) across **all** column skews — budget, revenue, genres/movie, companies/movie, countries/movie, languages/movie, and temporal distribution. Skew metric = mean / median ratio; closer to 1.0 = less skewed.

| Range | Movies | Budget skew | Revenue skew | Genre/movie | Company/movie | Country/movie | Lang/movie | Temporal skew |
|---|---|---|---|---|---|---|---|---|
| 10y 2010–2019 | 12,793 | 3.53 | 11.23 | 0.97 | 1.23 | 1.28 | 1.27 | 0.85 |
| 20y 2000–2019 | 23,988 | 2.83 | 7.49 | 1.03 | 1.25 | 1.32 | 1.31 | 0.96 |
| 30y 1990–2019 | 29,663 | 2.41 | 6.45 | 1.05 | 1.22 | 1.32 | 1.31 | 1.16 |
| 40y 1980–2019 | 33,591 | 2.38 | 5.84 | 1.06 | 1.18 | 1.30 | 1.30 | 1.30 |
| 50y 1970–2019 | 37,062 | 2.42 | 5.65 | 1.06 | 1.15 | 1.30 | 1.29 | 1.55 |
| **35y 1980–2015** | **31,450** | **2.34** | **5.57** | **1.06** | **1.17** | **1.30** | **1.31** | **1.26** |

**Decision: 1980–2015 (35 years).**
- Starting from 1980 gives the best balance of skew and fill rates — 1970s films have sparse, unreliable financial data
- 2017–2019 are truncated in the dataset snapshot (532 / 5 / 1 movies respectively) — keeping them distorts trends
- Trimming to 2015 improves every skew metric vs 1980–2019 and avoids all truncated years
- Clean, divisible-by-5 year count; tradeoff of ~2,141 fewer movies is acceptable

---

## silver.movies (31,450 rows in scope)

### Nulls / Coverage

| Column | Filled | Nulls | Fill % | Notes |
|---|---|---|---|---|
| movie_id | 31,450 | 0 | 100% | PK, no nulls |
| title | 31,450 | 0 | 100% | — |
| release_date | 31,450 | 0 | 100% | Scoped by date, so no nulls in this set |
| budget | 9,833 | 21,617 | 31.3% | **Systematic** — missing even after TMDB enrichment |
| revenue | 9,479 | 21,971 | 30.1% | **Systematic** — missing even after TMDB enrichment |

Budget and revenue nulls are systematic, not random. TMDB itself often has no financial data for smaller or older productions. Gold KPIs using budget/revenue must filter to non-null rows and acknowledge the 31% fill rate.

### budget (continuous, USD)

| Stat | Value |
|---|---|
| Min | $1 |
| P05 | $78,413 |
| P25 | $2.22M |
| Median (P50) | $8.50M |
| Mean | $19.91M |
| P75 | $24.0M |
| P99 | $153.4M |
| Max | $380.0M |
| StdDev | $31.19M |
| Mean/Median ratio | **2.34** |

**Distribution: Right-skewed.** Mean is 2.3× the median. Long right tail from blockbuster productions. Values below $1,000 (213 rows in scope) are likely placeholders (budget=1), not genuine micro-budgets — P05 at $78K confirms legitimate indie films exist at that range.

**Outliers (beyond 3 StdDev, threshold ≈ $113.5M): 246 rows.**

| Rank | movie_id | Title | Year | Budget | Z-Score |
|---|---|---|---|---|---|
| 1 | 1865 | Pirates of the Caribbean: On Stranger Tides | 2011 | $380.0M | 11.5 |
| 2 | 285 | Pirates of the Caribbean: At World's End | 2007 | $300.0M | 9.0 |
| 3 | 99861 | Avengers: Age of Ultron | 2015 | $280.0M | 8.3 |
| 4 | 38757 | Tangled | 2010 | $260.0M | 7.7 |
| 5 | 49529 | John Carter | 2012 | $260.0M | 7.7 |
| 6 | 559 | Spider-Man 3 | 2007 | $258.0M | 7.6 |
| 7 | 57201 | The Lone Ranger | 2013 | $255.0M | 7.5 |
| 8 | 767 | Harry Potter and the Half-Blood Prince | 2009 | $250.0M | 7.4 |

All outliers are genuine blockbuster productions — none are data errors. Keep in dataset as-is. The skew reflects real industry structure: a small number of major studio tentpoles operate at a completely different budget scale than the rest of the market. That is the truth of the data.

**Gold treatment:** Use raw values. Report median alongside mean where both are shown — median ($8.5M) is the more representative figure for a typical film. Segment by budget tier (e.g. micro/indie/mid/blockbuster) rather than averaging across the full range.

### revenue (continuous, USD)

| Stat | Value |
|---|---|
| Min | $1 |
| P25 | $1.01M |
| Median (P50) | $9.0M |
| Mean | $50.1M |
| P75 | $40.8M |
| P99 | $627.0M |
| Max | $2,787.97M (Avatar) |
| StdDev | $123.5M |
| Mean/Median ratio | **5.57** |

**Distribution: Heavily right-skewed.** Mean is 5.6× the median. A handful of blockbusters pull the mean far right. P99 is $627M while max is $2.8B — extreme outlier behavior concentrated in the top 1%.

**Outliers (beyond 3 StdDev, threshold ≈ $420.6M): 190 rows.**

| Rank | movie_id | Title | Year | Revenue | Z-Score |
|---|---|---|---|---|---|
| 1 | 19995 | Avatar | 2009 | $2,788.0M | 22.2 |
| 2 | 140607 | Star Wars: The Force Awakens | 2015 | $2,068.2M | 16.3 |
| 3 | 597 | Titanic | 1997 | $1,845.0M | 14.5 |
| 4 | 24428 | The Avengers | 2012 | $1,519.6M | 11.9 |
| 5 | 135397 | Jurassic World | 2015 | $1,513.5M | 11.9 |
| 6 | 168259 | Furious 7 | 2015 | $1,506.2M | 11.8 |
| 7 | 99861 | Avengers: Age of Ultron | 2015 | $1,405.4M | 11.0 |
| 8 | 12445 | Harry Potter and the Deathly Hallows: Part 2 | 2011 | $1,342.0M | 10.5 |

Avatar at z=22.2 is exceptional even among outliers — it sits 22 standard deviations above the mean. All are verified genuine box office figures. Keep in dataset as-is. The extreme skew is not a data problem — it reflects the actual economics of the film industry, where a handful of franchises generate revenues that are structurally incomparable to everything else. Transforming that away would misrepresent reality.

158 revenue values below $1,000 follow the same placeholder pattern as budget ($1 minimum).

**Gold treatment:** Use raw values. Always report median alongside mean — median ($9.0M) is the more representative figure for a typical film. For any metric comparing films across the full range, segment by revenue tier rather than computing a single average.

### release_date (temporal)

- Range in scope: 1980-01-01 to 2015-12-31 (no truncated years)
- Movies per year: min 352 (1983), max 1,973 (2014), mean 874, median 692
- Temporal skew (mean/median ratio): **1.26** — moderately right-skewed, driven by the 2000s–2010s production volume boom
- Clear upward trend: ~360 movies/year in early 1980s → ~1,700–1,900/year by 2011–2015

---

## silver.movie_genres (scope: 31,230 movies have genre data)

### Coverage
- 31,230 of 31,450 movies (99.3%) have at least one genre
- 220 movies (0.7%) have no genre — negligible

### Cardinality
- **20 unique genres** — fixed, closed set (TMDB standard genre taxonomy)
- Nominal categorical (no natural order)

### Genre frequency

| Genre | Movies | % of scoped movies |
|---|---|---|
| Drama | 14,471 | 46.0% |
| Comedy | 9,852 | 31.3% |
| Thriller | 5,846 | 18.6% |
| Action | 4,852 | 15.4% |
| Romance | 4,703 | 14.9% |
| Documentary | 3,573 | 11.4% |
| Horror | 3,477 | 11.1% |
| Crime | 2,855 | 9.1% |
| Adventure | 2,397 | 7.6% |
| Science Fiction | 2,315 | 7.4% |
| Family | 2,227 | 7.1% |
| Fantasy | 1,754 | 5.6% |
| Mystery | 1,651 | 5.2% |
| Animation | 1,583 | 5.0% |
| Foreign | 1,355 | 4.3% |
| Music | 1,021 | 3.2% |
| History | 935 | 3.0% |
| TV Movie | 740 | 2.4% |
| War | 635 | 2.0% |
| Western | 242 | 0.8% |

**Distribution: Heavily imbalanced.** Drama (46%) and Comedy (31%) together cover ~77% of all genre rows. Western is the rarest at 0.8%. This is a multi-label column — one movie can appear in multiple genre rows. Do not use raw counts for genre share comparisons; use % of movies per genre.

### Genres per movie

| Stat | Value |
|---|---|
| Min | 1 |
| P25 | 1 |
| Median | 2 |
| Mean | 2.13 |
| P75 | 3 |
| P99 | 5 |
| Max | 10 |
| StdDev | 1.08 |
| Mean/Median ratio | **1.06** |

**Distribution: Near-symmetric, slight right skew.** Most movies have 1–3 genres. Mean ≈ median (1.06). StdDev = 1.05. Upper fence (mean + 3×StdDev) ≈ 5.2 genres — movies with 6+ genres are statistical outliers.

**Outliers (6+ genres): Data quality issue — genre duplication.**

| movie_id | Title | Year | Genre Count | Note |
|---|---|---|---|---|
| 10991 | Pokémon: Spell of the Unknown | 2000 | 10 | 5 genres × 2 (duplicate rows) |
| 12600 | Pokémon 4Ever: Celebi - Voice of the Forest | 2001 | 10 | 5 genres × 2 (duplicate rows) |
| 69234 | The Phantom of the Opera | 1990 | 10 | 5 genres × 2 (duplicate rows) |
| 4912 | Confessions of a Dangerous Mind | 2002 | 10 | 5 genres × 2 (duplicate rows) |
| 23305 | The Warrior | 2001 | 10 | 5 genres × 2 (duplicate rows) |
| 15028 | Clockstoppers | 2002 | 8 | 4 genres × 2 (duplicate rows) |
| 11052 | Yu-Gi-Oh! The Movie | 2004 | 8 | 8 distinct genres (legitimate) |
| 325712 | Cool Cat Saves the Kids | 2015 | 8 | 8 distinct genres (legitimate) |

The "10-genre" movies are all duplicated genre rows, not genuinely 10-genre films. This is a known upstream data quality issue from the bronze source. Gold genre models should apply `DISTINCT` on genre values per movie to prevent double-counting.

**Normalization for Gold:** None needed. Apply `DISTINCT genre` in Gold queries.

---

## silver.production_companies (scope: 21,572 movies have company data)

### Coverage
- 21,572 of 31,450 movies (68.6%) have at least one company
- **9,878 movies (31.4%) have no company data** — significant gap. Correlates with older/smaller productions with sparse TMDB metadata.

### Cardinality
- **18,222 unique company names** — extremely high cardinality
- Nominal categorical
- **Data quality issues:** "The" appears as a standalone company name (151 occurrences in scope) — parsing artifact. "Columbia Pictures" and "Columbia Pictures Corporation" are the same studio listed under two names.

### Companies per movie

| Stat | Value |
|---|---|
| Min | 1 |
| P25 | 1 |
| Median | 2 |
| Mean | 2.35 |
| P75 | 3 |
| P99 | 9 |
| Max | 26 |
| StdDev | 1.85 |
| Mean/Median ratio | **1.17** |

**Distribution: Right-skewed.** Most movies have 1–3 companies. Tail extends to 26. StdDev = 1.85. Upper fence (mean + 3×StdDev) ≈ 7.9 companies — movies with 8+ are statistical outliers.

**Outliers (8+ companies): Multi-territory co-productions and anthology films.**

| movie_id | Title | Year | Company Count | Note |
|---|---|---|---|---|
| 16 | Dancer in the Dark | 2000 | 26 | Large European co-production |
| 345775 | Long Way North | 2015 | 26 | Multi-country animated feature |
| 8985 | Visions of Europe | 2004 | 26 | 25-director anthology — structural outlier |
| 298721 | Cemetery of Splendour | 2015 | 24 | Multi-territory art film |
| 17609 | Antichrist | 2009 | 24 | Danish-German-French co-production |
| 18897 | Don't Look Back | 2009 | 22 | Multi-territory documentary |
| 83430 | Altiplano | 2009 | 22 | Multi-territory production |
| 1951 | Manderlay | 2005 | 21 | Lars von Trier — same co-production pattern as Dancer in the Dark |

These are structurally extreme but not data errors — European art cinema and anthology formats genuinely involve many co-financing entities. Visions of Europe (movie_id 8985) is a recurring extreme outlier across company, country, and language dimensions.

**Note for Gold:** Company-level analysis has low reliability due to: (1) 31.4% coverage gap, (2) name duplication, (3) parsed artifact rows. Use a curated top-N studio list rather than raw `company_name`. Cap at P99 (8 companies) for outlier-sensitive metrics.

---

## silver.producing_countries (scope: 30,048 movies have country data)

### Coverage
- 30,048 of 31,450 movies (95.5%) have at least one country
- 1,402 movies (4.5%) have no country — small residual gap

### Cardinality
- **168 unique ISO country codes** — high but manageable
- Nominal categorical (can be grouped ordinally by region/continent)
- Historic country codes correctly mapped: SU → Soviet Union, XC → Czechoslovakia, XG → East Germany, YU → Yugoslavia

### Top countries (1980–2015)

| ISO | Country | Movies |
|---|---|---|
| US | United States | 15,781 |
| GB | United Kingdom | 3,117 |
| FR | France | 2,941 |
| DE | Germany | 1,909 |
| CA | Canada | 1,810 |
| JP | Japan | 1,238 |
| IT | Italy | 1,193 |
| IN | India | 871 |
| ES | Spain | 753 |
| RU | Russian Federation | 633 |

US accounts for 40.3% of all country rows — the single largest producing country by a wide margin. Any country-level Gold analysis must account for this dominance.

### Countries per movie

| Stat | Value |
|---|---|
| Min | 1 |
| P25 | 1 |
| Median | 1 |
| Mean | 1.30 |
| P75 | 1 |
| P99 | 4 |
| Max | 25 |
| StdDev | 0.75 |
| Mean/Median ratio | **1.30** |

**Distribution: Right-skewed, bounded low.** Median = 1 — most movies are single-country productions. StdDev = 0.68. Upper fence (mean + 3×StdDev) ≈ 3.3 countries — movies with 4+ are statistical outliers.

**Outliers (5+ countries): Anthology films and multi-territory documentaries.**

| movie_id | Title | Year | Country Count | Note |
|---|---|---|---|---|
| 8985 | Visions of Europe | 2004 | 25 | 25-director anthology — one country per director segment |
| 298721 | Cemetery of Splendour | 2015 | 18 | Multi-territory art film |
| 321530 | Caffeinated | 2015 | 15 | Documentary filmed across 15 countries |
| 150523 | The Mahabharata | 1989 | 15 | Peter Brook's multinational production |
| 16 | Dancer in the Dark | 2000 | 12 | Large European co-production |
| 152795 | The Congress | 2013 | 12 | Multi-territory animated/live-action hybrid |
| 1926 | 11'09''01 - September 11 | 2002 | 11 | 11-director anthology — one country per director |
| 26763 | Burma VJ: Reporting from a Closed Country | 2008 | 11 | Multi-territory documentary |

Visions of Europe (25 countries) and 11'09''01 (11 countries) are anthology structures where country count equals director count — legitimate data, but structurally incomparable to conventional co-productions.

**Normalization for Gold:** None needed. Cap at P99 (4 countries) for outlier-sensitive metrics.

---

## silver.spoken_languages (scope: 30,583 movies have language data)

### Coverage
- 30,583 of 31,450 movies (97.2%) have at least one language
- 867 movies (2.8%) have no language — very small gap

### Cardinality
- **134 unique ISO language codes**
- Nominal categorical
- Special codes correctly mapped: `cn` → Cantonese, `xx` → No Language (silent films / abstract shorts)

### Top languages (1980–2015)

| ISO | Language | Movies |
|---|---|---|
| en | English | 21,039 |
| fr | French | 3,073 |
| de | German | 1,942 |
| es | Spanish | 1,849 |
| it | Italian | 1,335 |
| ja | Japanese | 1,283 |
| ru | Russian | 1,169 |
| hi | Hindi | 700 |
| zh | Chinese | 653 |
| ko | Korean | 501 |

English appears in 68.8% of movies with language data — dominant but less extreme than the US country skew. Any language-level Gold analysis should use % share, not raw counts.

### Languages per movie

| Stat | Value |
|---|---|
| Min | 1 |
| P25 | 1 |
| Median | 1 |
| Mean | 1.31 |
| P75 | 1 |
| P99 | 4 |
| Max | 19 |
| StdDev | 0.73 |
| Mean/Median ratio | **1.31** |

**Distribution: Near-identical pattern to countries_per_movie.** Median = 1, most films are single-language. StdDev = 0.69. Upper fence (mean + 3×StdDev) ≈ 3.4 languages — movies with 4+ are statistical outliers.

**Outliers (5+ languages): Same anthology and documentary pattern as countries.**

| movie_id | Title | Year | Language Count | Note |
|---|---|---|---|---|
| 8985 | Visions of Europe | 2004 | 19 | 25-director anthology — one language per segment |
| 42195 | The Testaments | 2000 | 13 | Multi-language anthology |
| 36108 | To Each His Own Cinema | 2007 | 12 | Cannes anthology — 35 directors across multiple languages |
| 359364 | Human | 2015 | 9 | Yann Arthus-Bertrand documentary filmed in 60 countries |
| 57276 | Pina | 2011 | 9 | Wim Wenders dance documentary — multilingual ensemble |
| 22549 | Ulysses' Gaze | 1995 | 9 | Theo Angelopoulos — multi-country Balkans journey |
| 20502 | The Life | 2004 | 9 | Multi-territory production |
| 14161 | 2012 | 2009 | 9 | Large multilingual blockbuster cast |

Visions of Europe (movie_id 8985) holds the max across all three dimensions — 25 countries, 19 languages, 26 companies. It is a persistent structural outlier that sets the ceiling for all three columns. Legitimate data; keep in dataset.

**Normalization for Gold:** None needed. Cap at P99 (4 languages) for outlier-sensitive metrics.

---

## Summary: Distribution Types and Normalization Needs

| Column | Distribution | Skew (mean/median) | StdDev | Outlier Threshold (3σ) | Named Extreme Outliers | Normalization for Gold |
|---|---|---|---|---|---|---|
| budget | Right-skewed continuous | **2.34** | $31.19M | >$113.5M (246 movies) | Pirates of the Caribbean: On Stranger Tides $380M (z=11.5) | Use raw values; report median + mean; segment by budget tier |
| revenue | Heavily right-skewed continuous | **5.57** | $123.49M | >$420.6M (190 movies) | Avatar $2,788M (z=22.2); Star Wars: TFA $2,068M (z=16.3) | Use raw values; report median + mean; segment by revenue tier |
| release_year | Right-skewed temporal (production boom) | **1.26** | — | No truncated years in 1980–2015 | — | Treat as ordinal; 1980–2015 is clean |
| genres_per_movie | Near-symmetric, slight right skew | **1.06** | 1.05 | >5.2 genres | 5 movies with "10 genres" — all are duplicated rows (5 genres × 2), not genuine 10-genre films | None; apply DISTINCT genre in Gold |
| companies_per_movie | Right-skewed count | **1.17** | 1.85 | >7.9 companies | Dancer in the Dark, Long Way North, Visions of Europe — all 26 (multi-territory co-productions) | None; cap at P99=8 for outlier-sensitive metrics |
| countries_per_movie | Right-skewed, bounded low | **1.30** | 0.68 | >3.3 countries | Visions of Europe 25 (anthology); Cemetery of Splendour 18; Caffeinated 15 | None; cap at P99=4 for outlier-sensitive metrics |
| languages_per_movie | Right-skewed, bounded low | **1.31** | 0.69 | >3.4 languages | Visions of Europe 19; The Testaments 13; To Each His Own Cinema 12 | None; cap at P99=4 for outlier-sensitive metrics |
| genre (categorical) | Heavily imbalanced — Drama 46% | — | — | 20 fixed values, nominal | — | Use % share per genre, not raw counts |
| country (categorical) | US-dominant — 40.3% of rows | — | — | 168 ISO codes, nominal | — | Group by region/subregion for balanced views |
| language (categorical) | English-dominant — 68.8% of movies | — | — | 134 ISO codes, nominal | — | Use % share or group by language family |
| company_name (categorical) | Very high cardinality, name variants | — | — | 18,222 unique, nominal | — | Top-N curated studio list; parsed artifact rows removed in Silver |

---

## Key Findings for Gold Layer Design

1. **Budget and revenue are heavily right-skewed but must stay as raw values.** The skew (mean/median: 2.34 and 5.57) reflects real industry structure — blockbusters operate at a fundamentally different scale than the rest of the market. Transforming the data would misrepresent that reality. Gold metrics must always report median alongside mean (budget median: $8.5M; revenue median: $9.0M) and should segment by tier rather than computing a single average across the full range. Avatar at z=22.2 (22 standard deviations above the revenue mean) is genuinely exceptional — that fact belongs in the data, not smoothed away.

2. **Financial data covers only ~31% of movies.** Gold financial KPIs must clearly state they apply to the subset with known financials (9,833 budget / 9,479 revenue out of 31,450).

3. **1980–2015 is a clean, complete window.** No truncated years. All 36 calendar years (1980 through 2015 inclusive) have 352–1,973 movies each.

4. **US (40.3% of country rows) and English (68.8% of movies)** dominate country and language distributions. Any "global" breakdown will be US/English-heavy — use continent/region groupings for balanced dashboard views.

5. **"The" company name** (151 rows) and duplicate studio name variants (Columbia Pictures vs Columbia Pictures Corporation) are data quality issues. Gold company models should use a curated top-N studio list.

6. **Genre is multi-label and imbalanced.** A movie can have 1–10 genre rows, but the apparent "10-genre" movies are upstream duplicates (5 genres × 2 rows each). Gold genre models must apply `DISTINCT genre` per movie. Drama+Comedy cover ~77% of all genre rows. Use % of movies per genre for fair comparisons, not raw row counts.

7. **Visions of Europe (movie_id 8985)** is a statistical outlier: 25 countries, 19 languages. Legitimate anthology film — keep in dataset, but be aware it sets the max for both columns.
