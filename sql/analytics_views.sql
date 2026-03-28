-- ════════════════════════════════════════════════════════════════════
--  Advanced Analytics Views  ·  mobility-analytics
-- ════════════════════════════════════════════════════════════════════
-- Run AFTER create_tables.sql inside the spark-sql shell.
-- These views power the dashboard widgets.

USE mobility;

-- ── 1. Hourly Average Congestion by Zone ──────────────────────────
-- Used for: Line chart - congestion trend over the day
CREATE OR REPLACE VIEW v_hourly_congestion AS
SELECT
    f.event_hour,
    z.zone_type,
    f.city_zone,
    ROUND(AVG(f.congestion_level), 2)  AS avg_congestion,
    ROUND(AVG(f.speed),            1)  AS avg_speed,
    COUNT(*)                           AS event_count
FROM bi_fact_traffic f
JOIN bi_dim_zone z ON f.city_zone = z.city_zone
GROUP BY f.event_hour, z.zone_type, f.city_zone;


-- ── 2. Road Performance Summary ───────────────────────────────────
-- Used for: Bar chart - which roads are most congested
CREATE OR REPLACE VIEW v_road_performance AS
SELECT
    f.road_id,
    r.road_type,
    r.speed_limit,
    ROUND(AVG(f.speed),            1)  AS avg_speed,
    ROUND(AVG(f.congestion_level), 2)  AS avg_congestion,
    COUNT(*)                           AS total_events,
    SUM(f.peak_flag)                   AS peak_hour_events,
    ROUND(
        100.0 * SUM(f.peak_flag) / COUNT(*), 1
    )                                  AS peak_pct
FROM bi_fact_traffic f
JOIN bi_dim_road r ON f.road_id = r.road_id
GROUP BY f.road_id, r.road_type, r.speed_limit;


-- ── 3. Weather Impact on Traffic ──────────────────────────────────
-- Used for: Donut/Bar chart - how weather correlates to congestion
CREATE OR REPLACE VIEW v_weather_impact AS
SELECT
    weather,
    ROUND(AVG(congestion_level), 2)  AS avg_congestion,
    ROUND(AVG(speed),            1)  AS avg_speed,
    COUNT(*)                         AS event_count,
    ROUND(
        100.0 * SUM(CASE WHEN speed_band = 'LOW'    THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                AS pct_low_speed,
    ROUND(
        100.0 * SUM(CASE WHEN speed_band = 'HIGH'   THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                AS pct_high_speed
FROM bi_fact_traffic
GROUP BY weather;


-- ── 4. Speed Band Distribution ────────────────────────────────────
-- Used for: Stacked bar chart - vehicle speed distribution
CREATE OR REPLACE VIEW v_speed_distribution AS
SELECT
    city_zone,
    speed_band,
    COUNT(*)                         AS vehicle_count,
    ROUND(AVG(congestion_level), 2)  AS avg_congestion
FROM bi_fact_traffic
GROUP BY city_zone, speed_band;


-- ── 5. Zone Risk Dashboard KPIs ───────────────────────────────────
-- Used for: KPI cards and zone risk table
CREATE OR REPLACE VIEW v_zone_kpis AS
SELECT
    f.city_zone,
    z.zone_type,
    z.traffic_risk,
    ROUND(AVG(f.speed),            1)  AS avg_speed,
    ROUND(AVG(f.congestion_level), 2)  AS avg_congestion,
    COUNT(DISTINCT f.vehicle_id)       AS unique_vehicles,
    COUNT(*)                           AS total_events,
    SUM(f.peak_flag)                   AS peak_events,
    MIN(f.event_time)                  AS first_event,
    MAX(f.event_time)                  AS last_event
FROM bi_fact_traffic f
JOIN bi_dim_zone z ON f.city_zone = z.city_zone
GROUP BY f.city_zone, z.zone_type, z.traffic_risk;


-- ── 6. Live Traffic Summary (all-up KPI) ─────────────────────────
-- Used for: Top-level dashboard KPI cards
CREATE OR REPLACE VIEW v_live_summary AS
SELECT
    COUNT(DISTINCT vehicle_id)       AS total_unique_vehicles,
    ROUND(AVG(speed),            1)  AS avg_speed_kmh,
    ROUND(AVG(congestion_level), 2)  AS avg_congestion,
    COUNT(*)                         AS total_events,
    SUM(peak_flag)                   AS peak_hour_events,
    ROUND(
        100.0 * SUM(CASE WHEN speed_band = 'LOW' THEN 1 ELSE 0 END) / COUNT(*), 1
    )                                AS pct_low_speed_traffic
FROM bi_fact_traffic;



-- ── 7. Reject Monitoring ───────────────────────────────────────────
CREATE OR REPLACE VIEW v_reject_monitoring AS
SELECT
    reject_reason,
    COUNT(*) AS rejected_rows,
    MIN(rejected_at) AS first_seen,
    MAX(rejected_at) AS last_seen
FROM bi_rejects
GROUP BY reject_reason;
