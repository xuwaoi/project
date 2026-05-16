#  Urban Analytics Platform — Microsoft Fabric
### Платформа городской аналитики на Microsoft Fabric

> Unified analytics platform combining NYC  Taxi mobility data, OpenAQ air quality measurements, World Bank GDP and ECB FX rates - built on Microsoft Fabric with Medallion architecture.
>
> Единая аналитическая платформа, объединяющая данные о поездках такси NYC, качестве воздуха OpenAQ, ВВП Всемирного банка и курсах валют ЕЦБ - построена на Microsoft Fabric с архитектурой Medallion.

---

## Table of Contents / Содержание

- [Overview / Обзор](#overview--обзор)
- [Architecture / Архитектура](#architecture--архитектура)
- [Data Sources / Источники данных](#data-sources--источники-данных)
- [Project Structure / Структура проекта](#project-structure--структура-проекта)
- [ETL Pipeline — Step by Step](#etl-pipeline--step-by-step)
- [Data Model / Модель данных](#data-model--модель-данных)
- [Dashboards / Дашборды](#dashboards--дашборды)
- [Key Findings / Ключевые выводы](#key-findings--ключевые-выводы)
- [Technologies / Технологии](#technologies--технологии)
- [Prerequisites / Предварительные условия](#prerequisites--предварительные-условия)

---

## Overview / Обзор

**EN:**
This project builds a unified analytical platform on **Microsoft Fabric** that integrates three open data domains and answers cross-domain analytical questions:

-  **Mobility** — NYC Taxi trip records (2022–2024), sourced as Parquet files from the NYC TLC
-  **Environment** — Daily air quality measurements for NYC (PM2.5, PM10, O3, etc.) from OpenAQ API, collected via Python/requests
-  **Economics** — Annual US GDP from World Bank API + daily USD/EUR exchange rates from ECB API, loaded via Dataflow Gen2

**RU:**
Проект создаёт единую аналитическую платформу на **Microsoft Fabric**, объединяющую три открытых источника данных:

-  **Мобильность** — поездки такси NYC (Yellow + Green, 2022–2024), файлы Parquet с TLC
-  **Окружающая среда** — ежедневные измерения качества воздуха по NYC через API OpenAQ, собраны через Python/requests
-  **Экономика** — годовой ВВП США + ежедневные курсы USD/EUR, загружены через Dataflow Gen2

---

## Architecture / Архитектура

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│                                                                     │
│  NYC TLC Parquet files    OpenAQ REST API    World Bank + ECB APIs  │
│  (Yellow + Green taxi)    (Python/requests)  (Dataflow Gen2)        │
└────────┬──────────────────────┬──────────────────────┬─────────────┘
         │                      │                      │
         ▼                      ▼                      ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER  (Raw, Delta)                       │
│                                                                     │
│  Files/taxi_22_23/      bronze.aq_table      bronze.fx_table        │
│  Files/taxi_data_2024/  bronze.aq2_table     bronze.gdp             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  taxi.ipynb (PySpark)
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER  (Cleaned, Delta)                   │
│                                                                     │
│                    silver.taxi_enriched                             │
│      (unified Yellow+Green, zone names, coordinates, duration)      │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  taxi.ipynb + aq.ipynb
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    GOLD LAYER  (Star Schema, Delta)                 │
│                                                                     │
│   dim_date   dim_zone   (dim_fx via bronze.fx_table)                │
│   fact_taxi_daily   fact_air   fact_combined   fact_final           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  analysis.ipynb + Power BI
                               ▼
                    ┌──────────────────────┐
                    │   Power BI Report    │
                    │   (2 pages)          │
                    └──────────────────────┘
```

The platform follows the **Medallion architecture** (Bronze → Silver → Gold), where each layer progressively refines data quality and adds business meaning.

---

## Data Sources / Источники данных

| Source | Format | Period | Tool used | Link |
|--------|--------|--------|-----------|------|
| NYC TLC Yellow Taxi | Parquet | 2022–2024 | Files uploaded to Lakehouse | [TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| NYC TLC Green Taxi | Parquet | 2022–2024 | Files uploaded to Lakehouse | [TLC Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| OpenAQ Air Quality | JSON API | 2022–2023 | Python (requests) → CSV → Lakehouse | [OpenAQ Docs](https://docs.openaq.org) |
| World Bank GDP (USA) | JSON API | Multi-year | Dataflow Gen2 (`World bank`) | [WB API](https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json) |
| ECB FX Rates USD/EUR | CSV API | Daily | Dataflow Gen2 (`FX`) | [ECB API](https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata) |

### Note on OpenAQ ingestion / Примечание по загрузке OpenAQ

**EN:** OpenAQ data was collected via a custom Python script using the OpenAQ v3 API. The script:
1. Queries all sensor locations within NYC bounding box (`bbox: -74.3, 40.5, -73.6, 41`)
2. Extracts sensor IDs from location responses
3. Fetches daily measurements per sensor for 2022–2023
4. Merges coordinates from location metadata
5. Exports to CSV → uploaded to Lakehouse as `bronze.aq_table` / `bronze.aq2_table`

**RU:** Данные OpenAQ собирались через Python-скрипт с API OpenAQ v3:
1. Запрос всех станций мониторинга в границах NYC 
2. Извлечение ID сенсоров из ответа
3. Загрузка ежедневных измерений на каждый сенсор за 2022–2023 гг.
4. Добавление координат из метаданных
5. Экспорт в CSV → загрузка в Lakehouse как `bronze.aq_table` и `bronze.aq2_table`

---

## Project Structure / Структура проекта

```
urban-analytics-fabric/
│
├── README.md
│
├── notebooks/
│   ├── taxi.ipynb          # Bronze→Silver→Gold: Taxi ETL + star schema build
│   ├── aq.ipynb            # Bronze: Air quality union + fact_air aggregation
│   └── analysis.ipynb      # Gold: Cross-domain analysis & correlations
│
├── scripts/
│   └── 2023-2023aq.ipynb # Python: collect OpenAQ data via API v3 (2022-2023)
|   |__ aq_2.ipynb # Python: collect OpenAQ data via API v3 (2024)
│
└── powerbi/
    └── dashboard.pbix      # Power BI report 
```

---

## ETL Pipeline — Step by Step

### Phase 1 — Bronze: Raw Ingestion

**Taxi data** — monthly Parquet files downloaded from NYC TLC, uploaded to Lakehouse:
- `Files/taxi_22_23/` — Yellow and Green taxi, 2022–2023
- `Files/taxi_data_2024/` — Yellow and Green taxi, 2024

**Air quality** — collected via `2023-2023aq.ipynb` and `aq_2.ipynb`, exported as CSV, uploaded as `bronze.aq_table` and `bronze.aq2_table`.

**FX and GDP** — loaded automatically via Dataflow Gen2 items into `bronze.fx_table` and `bronze.gdp`.

---

### Phase 2 — Silver: Cleaning & Standardisation (`taxi.ipynb`)

**Step 1 — Unify Yellow and Green schemas**

Yellow taxi uses `tpep_pickup_datetime`, Green uses `lpep_pickup_datetime`. Both are renamed to `pickup_time` / `dropoff_time` to enable a union.

**Step 2 — Cast column types**

All numeric fields are cast to consistent types: `PULocationID` / `DOLocationID` → `long`; `passenger_count`, `trip_distance`, `fare_amount`, `total_amount` → `double`.

**Step 3 — Combine all years**

`taxi_22_23` and `taxi_2024` DataFrames are merged with `unionByName`.

**Step 4 — Filter bad records**
- Remove trips with zero or null `passenger_count`
- Remove trips with zero `trip_distance`
- Remove trips with zero or negative `trip_duration_min`
- Keep only trips within 2022-01-01 → 2025-01-01

**Step 5 — Enrich with zone names and coordinates**

Two lookup files are joined:
- `taxi_zone_lookup.csv` → adds `PU_Zone`, `PU_Borough`, `DO_Zone`, `DO_Borough`
- `zone_coordinates.csv` → adds `lat`, `lon` per pickup zone

Coordinates are also rounded to 2 decimal places (`lat_r`, `lon_r`) for spatial matching with air quality sensors.

**Step 6 — Write to Silver and build dim_zone**

```
silver.taxi_enriched  ← full cleaned and enriched taxi table
gold.dim_zone         ← unique zones deduplicated by rounded coordinate
```

---

### Phase 3 — Gold: Star Schema (`taxi.ipynb` + `aq.ipynb`)

**`fact_taxi_daily`** — daily aggregation per location:
```
GROUP BY date, lat_r, lon_r
→ trip_count, total_revenue, avg_distance, avg_duration
```

**`fact_air`** — daily average pollution per location and pollutant type:
```
GROUP BY date, lat, lon, parameter
→ avg_pollution
```
Built in `aq.ipynb` by unioning `bronze.aq_table` and `bronze.aq2_table` then aggregating.

**`fact_combined`** — taxi daily joined with air quality on `date`, `lat_r`, `lon_r`.

**`fact_final`** — the master wide table joining everything:

```
fact_combined
  JOIN bronze.fx_table ON date  → adds rate, revenue_eur = total_revenue × rate
  JOIN bronze.gdp ON year       → adds gdp
  JOIN gold.dim_zone ON coord   → adds PU_Zone, PU_Borough
```

Final columns: `date, year, PU_Zone, PU_Borough, coord, lat_r, lon_r, trip_count, total_revenue, revenue_eur, rate, avg_distance, avg_duration, avg_pollution, parameter, gdp`

**`dim_date`** — calendar dimension from distinct dates in `silver.taxi_enriched`:
```
date, year, month, day, day_of_week
```

---

### Phase 4 — Analysis (`analysis.ipynb`)

Queries `gold.fact_final` and computes:

| Analysis | Result |
|----------|--------|
| Yearly trends | trips, revenue, avg_pollution per year |
| Monthly trends | trips, pollution per month/year |
| Top pickup zones | top 10 by trip_count |
| Pollution hotspots | top 10 locations by avg_pollution |
| FX revenue comparison | avg USD vs avg EUR revenue |
| **Corr: trips vs pollution** | **−0.00887** |
| Corr: revenue vs pollution | computed |
| Corr: GDP vs trips | computed |
| Corr: GDP vs revenue | computed |

---

## Data Model / Модель данных

```
        dim_date                    dim_zone
      ┌───────────┐              ┌───────────────┐
      │ date  (PK)│              │ coord    (PK) │
      │ year      │              │ PULocationID  │
      │ month     │              │ PU_Zone       │
      │ day       │              │ PU_Borough    │
      │ day_of_wk │              │ lat, lon      │
      └─────┬─────┘              └───────┬───────┘
            │                            │
            └────────────┬───────────────┘
                         │
                  ┌──────┴──────────┐
                  │   fact_final    │
                  │─────────────────│
                  │ date            │──► dim_date
                  │ year            │
                  │ coord           │──► dim_zone
                  │ PU_Zone         │
                  │ PU_Borough      │
                  │ trip_count      │
                  │ total_revenue   │
                  │ revenue_eur     │ = total_revenue × rate
                  │ rate            │◄── bronze.fx_table
                  │ avg_distance    │
                  │ avg_duration    │
                  │ avg_pollution   │◄── fact_air (date + coord)
                  │ parameter       │
                  │ gdp             │◄── bronze.gdp (year)
                  └─────────────────┘
```

**Key design decisions:**

- Zones are matched by **rounded coordinates** (`lat_r`, `lon_r`) rather than `LocationID`, enabling spatial join with OpenAQ sensors which have their own lat/lon
- `revenue_eur` is computed at the fact level (`total_revenue × rate`) so Power BI can aggregate both currencies without extra calculation
- `fact_final` is a wide denormalised table — intentional choice for Power BI performance

---

## Dashboards / Дашборды

### Page 1 — Mobility & Economics

| Visual | What it shows |
|--------|---------------|
| KPI: Total Revenue | **$2.898bn** across 2022–2024 |
| KPI: Total Trips | **112M** trips |
| KPI: Average Pollution | 163.85 (all pollutants selected) |
| KPI: Average Fare | **$25.80** per trip |
| Line: Revenue USD vs EUR | Monthly revenue in both currencies — shows EUR line diverging from USD due to FX rate movement |
| Bar: Top Pickup Zones | Midtown Center #1 (~10M+), Penn Station/Madison Square, JFK Airport, Upper East Side |
| Line: Daily Trip Volume | Day-by-day from Jan 2022 to Dec 2024 — seasonal dips visible in summer, crashes visible (lockdown tail / COVID recovery pattern) |
| Line: GDP Trend | US GDP in billions, growing steadily 2022–2024 |

Slicers: `parameter` (filter pollutant type), `year`

### Page 2 — Air Quality & Correlation

| Visual | What it shows |
|--------|---------------|
| KPI: Average Pollution | 149.89 |
| KPI: Total Trips | 113M |
| KPI: Correlation Trips vs Pollution | **−0.00887** — near-zero |
| Line: Air Quality Trends | All pollutants over time: o3, pm1, pm10, pm25, relativehumidity, temperature, um003 |
| Bar: Pollution Hotspots | Queensboro Hill, Flatiron, Gravesend, Bayside top the list |
| Overlay: Trips vs Pollution | Dual-axis chart: dark blue = trip count, light blue = avg pollution — visually confirms the weak relationship |

---

## Key Findings / Ключевые выводы

**1. 🚕 Scale of NYC Taxi Mobility**
112 million trips over three years, average fare $25.80. Midtown Center is the undisputed #1 pickup zone, followed by Penn Station and JFK — confirming that business districts and transit hubs drive the majority of taxi demand.

**2. 💱 FX Impact on Revenue**
Revenue in EUR consistently diverges from USD. This is visible in the line chart — when the dollar is strong against the euro, USD revenue stays flat while EUR revenue drops. This matters for international benchmarking and policy comparison.

**3. 🌫️ Pollution Hotspots Are Not Where You Expect**
The highest average pollution is in Queensboro Hill, Flatiron, Gravesend, and Bayside — not Midtown or downtown Manhattan. This suggests that proximity to highways, industrial zones, and specific borough geography drives pollution more than taxi pickup density.

**4. 🔗 Taxis Don't Drive Air Pollution**
The computed Pearson correlation between daily taxi trip count and average pollution is **−0.00887** — statistically zero. Taxis are not a meaningful driver of NYC air quality. Pollution is shaped by total traffic volume, truck freight, weather, wind, and industrial emissions. Policy targeting only yellow cabs would have negligible environmental impact.

**5. 📈 GDP Growth Context**
US GDP grew consistently across 2022–2024. Some of the taxi revenue growth over this period reflects broader economic expansion rather than pure demand shift.

---

## Technologies / Технологии

| Layer | Technology |
|-------|------------|
| Platform | Microsoft Fabric (Trial workspace) |
| Storage | OneLake, Delta Lake format |
| Raw files | Parquet (taxi), CSV (zone lookups) |
| API ingestion — FX, GDP | Dataflow Gen2 |
| API ingestion — OpenAQ | Python (`requests`, `pandas`) |
| Transformation | PySpark in Fabric Notebooks |
| Data model | Delta tables across Bronze / Silver / Gold schemas |
| Visualisation | Power BI embedded in Fabric (2-page report) |
| Architecture | Medallion (Bronze → Silver → Gold) |

---

## Prerequisites / Предварительные условия

To reproduce this project:

1. **Microsoft Fabric** workspace (49-day free Trial is sufficient)
2. Fabric **Lakehouse** named `my_project` inside a workspace named `final_project`
3. Dataflow Gen2 items for `FX` and `World bank` (importable from `/dataflows/`)
4. **OpenAQ API key** — free registration at [openaq.org](https://openaq.org)
5. NYC TLC Parquet files for 2022–2024 uploaded to `Files/taxi_22_23/` and `Files/taxi_data_2024/`
6. `taxi_zone_lookup.csv` and `zone_coordinates.csv` uploaded to `Files/`

### Run order / Порядок запуска

```
1. Dataflows:          FX  →  World bank  →  aq_NY
                       (populates bronze.fx_table, bronze.gdp, bronze.aq_*)

2. openaq_collector.py
                       (produces openaq2.csv → upload to Lakehouse → bronze.aq_table / aq2_table)

3. Notebook: taxi.ipynb
                       (Files/* → silver.taxi_enriched → gold.dim_zone, dim_date,
                        fact_taxi_daily, fact_combined, fact_final)

4. Notebook: aq.ipynb
                       (bronze.aq_* → gold.fact_air)

5. Notebook: analysis.ipynb
                       (gold.fact_final → printed analysis + correlation results)

6. Power BI: dashboard.pbix
                       (connects to gold.fact_final → 2-page interactive report)
```
