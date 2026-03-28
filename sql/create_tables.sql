-- ════════════════════════════════════════════════════════════════════
--  Hive Metastore DDL  ·  mobility-analytics
-- ════════════════════════════════════════════════════════════════════
-- Run this inside the spark-sql shell (see commands.txt for the
-- full spark-sql connection command with delta packages).
--
-- These statements:
--   1. Create a dedicated database
--   2. Register Delta tables by LOCATION (external tables that auto-
--      refresh as the streaming jobs write new data)
--   3. Create BI views with explicit casts (safe for PowerBI/Tableau)
-- ════════════════════════════════════════════════════════════════════


-- ─── Database ─────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS mobility;

USE mobility;


-- ═══════════════════════════════════════════════════════════════════
-- EXTERNAL DELTA TABLES
-- ═══════════════════════════════════════════════════════════════════
-- These tables do NOT copy data. They point to the Delta Lake
-- storage location.  When the Spark streaming jobs add new Delta
-- files, SELECT queries here automatically see the new rows.

CREATE TABLE IF NOT EXISTS fact_traffic
USING DELTA
LOCATION '/opt/spark/warehouse/fact_traffic';

CREATE TABLE IF NOT EXISTS dim_zone
USING DELTA
LOCATION '/opt/spark/warehouse/dim_zone';

CREATE TABLE IF NOT EXISTS dim_road
USING DELTA
LOCATION '/opt/spark/warehouse/dim_road';

CREATE TABLE IF NOT EXISTS traffic_rejects
USING DELTA
LOCATION '/opt/spark/warehouse/traffic_rejects';


-- ═══════════════════════════════════════════════════════════════════
-- VERIFICATION QUERIES
-- ═══════════════════════════════════════════════════════════════════
SELECT COUNT(*)  AS fact_row_count FROM fact_traffic;
SELECT *         FROM dim_zone;
SELECT *         FROM dim_road;
SELECT COUNT(*)  AS reject_row_count FROM traffic_rejects;


-- ═══════════════════════════════════════════════════════════════════
-- BI VIEWS  (for PowerBI / Tableau / direct JDBC access)
-- ═══════════════════════════════════════════════════════════════════
-- IMPORTANT: Use CREATE OR REPLACE VIEW; CREATE TABLE is NOT used
-- here because PowerBI cannot query these tables directly — only views.
--
-- All columns are explicitly CAST so downstream tools always see the
-- correct data type, regardless of any upstream inference drift.


-- ── Fact Traffic View ──────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_fact_traffic AS
SELECT
    CAST(vehicle_id       AS STRING)    AS vehicle_id,
    CAST(road_id          AS STRING)    AS road_id,
    CAST(city_zone        AS STRING)    AS city_zone,
    CAST(speed            AS DOUBLE)    AS speed,
    CAST(congestion_level AS INT)       AS congestion_level,
    CAST(event_ts         AS TIMESTAMP) AS event_time,
    CAST(peak_flag        AS INT)       AS peak_flag,      -- FIX: was STRING in original
    CAST(speed_band       AS STRING)    AS speed_band,
    CAST(weather          AS STRING)    AS weather,
    CAST(event_date       AS DATE)      AS event_date,
    CAST(hour             AS INT)       AS event_hour,
    CAST(ingested_at      AS TIMESTAMP) AS ingested_at
FROM fact_traffic;


-- ── Dimension Zone View ────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_dim_zone AS
SELECT
    CAST(city_zone     AS STRING) AS city_zone,
    CAST(zone_type     AS STRING) AS zone_type,
    CAST(traffic_risk  AS STRING) AS traffic_risk
FROM dim_zone;


-- ── Dimension Road View ────────────────────────────────────────────
CREATE OR REPLACE VIEW bi_dim_road AS
SELECT
    CAST(road_id     AS STRING) AS road_id,
    CAST(road_type   AS STRING) AS road_type,
    CAST(speed_limit AS INT)    AS speed_limit
FROM dim_road;



-- ── Reject Monitoring View ───────────────────────────────────────────
CREATE OR REPLACE VIEW bi_rejects AS
SELECT
    CAST(vehicle_id AS STRING) AS vehicle_id,
    CAST(road_id AS STRING) AS road_id,
    CAST(city_zone AS STRING) AS city_zone,
    CAST(reject_reason AS STRING) AS reject_reason,
    CAST(rejected_at AS TIMESTAMP) AS rejected_at,
    CAST(raw_json AS STRING) AS raw_json
FROM traffic_rejects;
