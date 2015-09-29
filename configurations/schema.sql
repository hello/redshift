-- CREATE USER kingshy WTIH PASSWORD 'give-a-password' IN GROUP data;
CREATE GROUP data;

-- Interleaved key problem, use COMPOUND
CREATE TABLE device_sensors_par_2015_02 (
    id BIGINT PRIMARY KEY,
    account_id BIGINT,
    device_id BIGINT,
    ambient_temp INTEGER,
    ambient_light INTEGER,
    ambient_humidity INTEGER,
    ambient_air_quality INTEGER,
    ts TIMESTAMP,
    local_utc_ts TIMESTAMP,
    offset_millis INTEGER,
    ambient_light_variance INTEGER,
    ambient_light_peakiness INTEGER,
    ambient_air_quality_raw INTEGER,
    ambient_dust_variance INTEGER,
    ambient_dust_min INTEGER,
    ambient_dust_max INTEGER,
    firmware_version INTEGER DEFAULT 0,
    wave_count INTEGER DEFAULT 0,
    hold_count INTEGER DEFAULT 0,
    audio_num_disturbances INTEGER DEFAULT 0,
    audio_peak_disturbances_db INTEGER DEFAULT 0,
    audio_peak_background_db INTEGER DEFAULT 0,
    UNIQUE (account_id, device_id, ts)
)
DISTSTYLE KEY DISTKEY (account_id)
COMPOUND SORTKEY (account_id, local_utc_ts);

GRANT ALL ON device_sensors_par_2015_02 TO migrator;
GRANT ALL ON device_sensors_par_2015_02 TO tim;

CREATE TABLE IF NOT EXISTS device_sensors_par_2015_03 (LIKE device_sensors_par_2015_02);
GRANT ALL ON device_sensors_par_2015_03 TO migrator;
GRANT ALL ON device_sensors_par_2015_03 TO tim;

CREATE TABLE IF NOT EXISTS device_sensors_par_2015_04 (LIKE device_sensors_par_2015_02);
GRANT ALL ON device_sensors_par_2015_04 TO migrator;
GRANT ALL ON device_sensors_par_2015_04 TO tim;

CREATE TABLE IF NOT EXISTS device_sensors_par_2015_05 (LIKE device_sensors_par_2015_02);
GRANT ALL ON device_sensors_par_2015_05 TO migrator;
GRANT ALL ON device_sensors_par_2015_05 TO tim;

CREATE TABLE IF NOT EXISTS device_sensors_par_2015_06 (LIKE device_sensors_par_2015_02);
GRANT ALL ON device_sensors_par_2015_06 TO migrator;
GRANT ALL ON device_sensors_par_2015_06 TO tim;

CREATE TABLE IF NOT EXISTS device_sensors_par_2015_07 (LIKE device_sensors_par_2015_02);
GRANT ALL ON device_sensors_par_2015_07 TO migrator;
GRANT ALL ON device_sensors_par_2015_07 TO tim;

CREATE TABLE IF NOT EXISTS device_sensors_par_2015_08 (LIKE device_sensors_par_2015_02);
GRANT ALL ON device_sensors_par_2015_08 TO migrator;
GRANT ALL ON device_sensors_par_2015_08 TO tim;

CREATE TABLE IF NOT EXISTS device_sensors_par_2015_09 (LIKE device_sensors_par_2015_02);
GRANT ALL ON device_sensors_par_2015_09 TO migrator;
GRANT ALL ON device_sensors_par_2015_09 TO tim;
GRANT ALL ON device_sensors_par_2015_09 TO kingshy;

CREATE TABLE IF NOT EXISTS device_sensors_par_2015_10 (LIKE device_sensors_par_2015_02);
GRANT ALL ON device_sensors_par_2015_10 TO migrator;
GRANT ALL ON device_sensors_par_2015_10 TO tim;

--update device_sensor tables every month

-- tracker motion
CREATE TABLE tracker_motion_par_2015_02(
    id BIGINT PRIMARY KEY,
    account_id BIGINT,
    tracker_id BIGINT,
    svm_no_gravity INTEGER,
    ts TIMESTAMP,
    offset_millis INTEGER,
    local_utc_ts TIMESTAMP,
    motion_range BIGINT,
    kickoff_counts INTEGER,
    on_duration_seconds INTEGER,
    UNIQUE (account_id, tracker_id, ts)
)
DISTSTYLE KEY DISTKEY (account_id)
COMPOUND SORTKEY (local_utc_ts, account_id);

GRANT ALL ON tracker_motion_par_2015_02 TO migrator;
GRANT ALL ON tracker_motion_par_2015_02 TO tim;

CREATE TABLE tracker_motion_par_2015_03 (LIKE tracker_motion_par_2015_02);
GRANT ALL ON tracker_motion_par_2015_03 TO migrator;
GRANT ALL ON tracker_motion_par_2015_03 TO tim;

CREATE TABLE tracker_motion_par_2015_04 (LIKE tracker_motion_par_2015_02);
GRANT ALL ON tracker_motion_par_2015_04 TO migrator;
GRANT ALL ON tracker_motion_par_2015_04 TO tim;

CREATE TABLE tracker_motion_par_2015_05 (LIKE tracker_motion_par_2015_02);
GRANT ALL ON tracker_motion_par_2015_05 TO migrator;
GRANT ALL ON tracker_motion_par_2015_05 TO tim;

CREATE TABLE tracker_motion_par_2015_06 (LIKE tracker_motion_par_2015_02);
GRANT ALL ON tracker_motion_par_2015_06 TO migrator;
GRANT ALL ON tracker_motion_par_2015_06 TO tim;

CREATE TABLE tracker_motion_par_2015_07 (LIKE tracker_motion_par_2015_02);
GRANT ALL ON tracker_motion_par_2015_07 TO migrator;
GRANT ALL ON tracker_motion_par_2015_07 TO tim;

CREATE TABLE tracker_motion_par_2015_08 (LIKE tracker_motion_par_2015_02);
GRANT ALL ON tracker_motion_par_2015_08 TO migrator;
GRANT ALL ON tracker_motion_par_2015_08 TO tim;

CREATE TABLE tracker_motion_par_2015_09 (LIKE tracker_motion_par_2015_02);
GRANT ALL ON tracker_motion_par_2015_09 TO migrator;
GRANT ALL ON tracker_motion_par_2015_09 TO tim;

CREATE TABLE tracker_motion_par_2015_10 (LIKE tracker_motion_par_2015_02);
GRANT ALL ON tracker_motion_par_2015_10 TO migrator;
GRANT ALL ON tracker_motion_par_2015_10 TO tim;

-- Update tracker motion tables monthly

GRANT SELECT ON ALL TABLES IN SCHEMA public TO GROUP data;


-- need to update each month
CREATE OR REPLACE VIEW tracker_motion_master AS 
SELECT * FROM tracker_motion_par_2015_02 UNION ALL 
SELECT * FROM tracker_motion_par_2015_03 UNION ALL 
SELECT * FROM tracker_motion_par_2015_04 UNION ALL 
SELECT * FROM tracker_motion_par_2015_05 UNION ALL 
SELECT * FROM tracker_motion_par_2015_06 UNION ALL 
SELECT * FROM tracker_motion_par_2015_07 UNION ALL 
SELECT * FROM tracker_motion_par_2015_08 UNION ALL 
SELECT * FROM tracker_motion_par_2015_09 UNION ALL 
SELECT * FROM tracker_motion_par_2015_10 ORDER BY local_utc_ts;

GRANT SELECT ON tracker_motion_master TO GROUP data;
GRANT ALL ON tracker_motion_master TO tim;
GRANT ALL ON tracker_motion_master TO migrator;

-- need to update each month
CREATE OR REPLACE VIEW device_sensors_master AS 
SELECT * FROM device_sensors_par_2015_02 UNION ALL 
SELECT * FROM device_sensors_par_2015_03 UNION ALL 
SELECT * FROM device_sensors_par_2015_04 UNION ALL 
SELECT * FROM device_sensors_par_2015_05 UNION ALL 
SELECT * FROM device_sensors_par_2015_06 UNION ALL 
SELECT * FROM device_sensors_par_2015_07 UNION ALL 
SELECT * FROM device_sensors_par_2015_08 UNION ALL 
SELECT * FROM device_sensors_par_2015_09 UNION ALL 
SELECT * FROM device_sensors_par_2015_10 ORDER BY ts;

GRANT SELECT ON device_sensors_master TO GROUP data;
GRANT ALL ON device_sensors_master TO tim;
GRANT ALL ON device_sensors_master TO migrator;

