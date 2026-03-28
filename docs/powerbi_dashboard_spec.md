# Power BI Dashboard Specification

Use the Hive/Spark views below as the model source:
- `bi_fact_traffic`
- `bi_dim_zone`
- `bi_dim_road`
- `v_hourly_congestion`
- `v_road_performance`
- `v_weather_impact`
- `v_speed_distribution`
- `v_zone_kpis`
- `v_live_summary`

Recommended visuals:
1. KPI cards: total events, average speed, average congestion, unique vehicles.
2. Line chart: hourly congestion by zone (`v_hourly_congestion`).
3. Bar chart: road performance by average congestion (`v_road_performance`).
4. Donut or stacked bar: weather impact (`v_weather_impact`).
5. Stacked bar: speed distribution per zone (`v_speed_distribution`).
6. Matrix / table: zone KPI summary (`v_zone_kpis`).

Suggested slicers:
- city_zone
- road_id
- weather
- event_date
- speed_band

This file is included so the repo is presentation-ready even when the `.pbix` file is not committed.
