CREATE TABLE device_sensors_master (
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
INTERLEAVED SORTKEY (local_utc_ts, account_id);

CREATE TABLE device_sensors_par_2015_02 (LIKE device_sensors_master);
GRANT ALL ON device_sensors_par_2015_02 TO migrator;

CREATE TABLE device_sensors_par_2015_03 (LIKE device_sensors_master);
GRANT ALL ON device_sensors_par_2015_03 TO migrator;

CREATE TABLE device_sensors_par_2015_04 (LIKE device_sensors_master);
GRANT ALL ON device_sensors_par_2015_04 TO migrator;

CREATE TABLE device_sensors_par_2015_05 (LIKE device_sensors_master);
GRANT ALL ON device_sensors_par_2015_05 TO migrator;

CREATE TABLE device_sensors_par_2015_06 (LIKE device_sensors_master);
GRANT ALL ON device_sensors_par_2015_06 TO migrator;

CREATE TABLE device_sensors_par_2015_07 (LIKE device_sensors_master);
GRANT ALL ON device_sensors_par_2015_07 TO migrator;

CREATE TABLE device_sensors_par_2015_08 (LIKE device_sensors_master);
GRANT ALL ON device_sensors_par_2015_08 TO migrator;
