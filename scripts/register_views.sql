CREATE DATABASE IF NOT EXISTS mobility;
USE mobility;

CREATE TABLE IF NOT EXISTS fact_traffic USING DELTA LOCATION '/opt/spark/warehouse/fact_traffic';
CREATE TABLE IF NOT EXISTS dim_zone USING DELTA LOCATION '/opt/spark/warehouse/dim_zone';
CREATE TABLE IF NOT EXISTS dim_road USING DELTA LOCATION '/opt/spark/warehouse/dim_road';

CREATE OR REPLACE VIEW bi_fact_traffic AS
SELECT
    CAST(vehicle_id       AS STRING)    AS vehicle_id,
    CAST(road_id          AS STRING)    AS road_id,
    CAST(city_zone        AS STRING)    AS city_zone,
    CAST(speed            AS DOUBLE)    AS speed,
    CAST(congestion_level AS INT)       AS congestion_level,
    CAST(event_ts         AS TIMESTAMP) AS event_time,
    CAST(peak_flag        AS INT)       AS peak_flag,
    CAST(speed_band       AS STRING)    AS speed_band,
    CAST(weather          AS STRING)    AS weather,
    CAST(event_date       AS DATE)      AS event_date,
    CAST(hour             AS INT)       AS event_hour,
    CAST(ingested_at      AS TIMESTAMP) AS ingested_at
FROM fact_traffic;

CREATE OR REPLACE VIEW bi_dim_zone AS
SELECT
    CAST(city_zone    AS STRING) AS city_zone,
    CAST(zone_type    AS STRING) AS zone_type,
    CAST(traffic_risk AS STRING) AS traffic_risk
FROM dim_zone;

CREATE OR REPLACE VIEW bi_dim_road AS
SELECT
    CAST(road_id     AS STRING) AS road_id,
    CAST(road_type   AS STRING) AS road_type,
    CAST(speed_limit AS INT)    AS speed_limit
FROM dim_road;

CREATE OR REPLACE VIEW v_hourly_congestion AS
SELECT f.event_hour, z.zone_type, f.city_zone,
    ROUND(AVG(f.congestion_level), 2) AS avg_congestion,
    ROUND(AVG(f.speed), 1)            AS avg_speed,
    COUNT(*)                          AS event_count
FROM bi_fact_traffic f
JOIN bi_dim_zone z ON f.city_zone = z.city_zone
GROUP BY f.event_hour, z.zone_type, f.city_zone;

CREATE OR REPLACE VIEW v_road_performance AS
SELECT f.road_id, r.road_type, r.speed_limit,
    ROUND(AVG(f.speed), 1)            AS avg_speed,
    ROUND(AVG(f.congestion_level), 2) AS avg_congestion,
    COUNT(*)                          AS total_events,
    SUM(f.peak_flag)                  AS peak_hour_events,
    ROUND(100.0 * SUM(f.peak_flag) / COUNT(*), 1) AS peak_pct
FROM bi_fact_traffic f
JOIN bi_dim_road r ON f.road_id = r.road_id
GROUP BY f.road_id, r.road_type, r.speed_limit;

CREATE OR REPLACE VIEW v_weather_impact AS
SELECT weather,
    ROUND(AVG(congestion_level), 2) AS avg_congestion,
    ROUND(AVG(speed), 1)            AS avg_speed,
    COUNT(*)                        AS event_count,
    ROUND(100.0 * SUM(CASE WHEN speed_band = 'LOW'  THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_low_speed,
    ROUND(100.0 * SUM(CASE WHEN speed_band = 'HIGH' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_high_speed
FROM bi_fact_traffic
GROUP BY weather;

CREATE OR REPLACE VIEW v_speed_distribution AS
SELECT city_zone, speed_band,
    COUNT(*)                        AS vehicle_count,
    ROUND(AVG(congestion_level), 2) AS avg_congestion
FROM bi_fact_traffic
GROUP BY city_zone, speed_band;

CREATE OR REPLACE VIEW v_zone_kpis AS
SELECT f.city_zone, z.zone_type, z.traffic_risk,
    ROUND(AVG(f.speed), 1)            AS avg_speed,
    ROUND(AVG(f.congestion_level), 2) AS avg_congestion,
    COUNT(DISTINCT f.vehicle_id)      AS unique_vehicles,
    COUNT(*)                          AS total_events,
    SUM(f.peak_flag)                  AS peak_events,
    MIN(f.event_time)                 AS first_event,
    MAX(f.event_time)                 AS last_event
FROM bi_fact_traffic f
JOIN bi_dim_zone z ON f.city_zone = z.city_zone
GROUP BY f.city_zone, z.zone_type, z.traffic_risk;

CREATE OR REPLACE VIEW v_live_summary AS
SELECT
    COUNT(DISTINCT vehicle_id)        AS total_unique_vehicles,
    ROUND(AVG(speed), 1)              AS avg_speed_kmh,
    ROUND(AVG(congestion_level), 2)   AS avg_congestion,
    COUNT(*)                          AS total_events,
    SUM(peak_flag)                    AS peak_hour_events,
    ROUND(100.0 * SUM(CASE WHEN speed_band = 'LOW' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_low_speed_traffic
FROM bi_fact_traffic;
