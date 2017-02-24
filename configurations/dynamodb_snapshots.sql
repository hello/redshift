
--
-- prod_insights (subset)
-- run: python snapshot_ddb.py prod_insights
--

CREATE TABLE prod_insights (
  account_id BIGINT,
  category INTEGER,
  date_category VARCHAR(20),
  insight_type VARCHAR(32),
  time_period VARCHAR(32),
  timestamp_utc TIMESTAMP
) DISTSTYLE KEY DISTKEY (account_id)
COMPOUND SORTKEY (timestamp_utc, category);

GRANT ALL ON prod_insights TO migrator;
GRANT SELECT ON prod_insights TO GROUP data;
ALTER TABLE prod_insights OWNER to migrator;


--
-- prod_sleep_stats (subset)
--
CREATE TABLE prod_sleep_stats_v_0_2(
  account_id BIGINT,
  avg_motion_amplitude FLOAT4,
  awake_score INTEGER,
  awake_time VARCHAR(20),
  date VARCHAR(12),
  day_of_week INTEGER,
  duration_score INTEGER,
  env_score INTEGER,
  fall_asleep_time VARCHAR(20),
  light_sleep INTEGER,
  max_motion_amplitude INTEGER,
  medium_sleep INTEGER,
  motion_period_mins INTEGER,
  motion_score INTEGER,
  num_motions INTEGER,
  offset_millis INTEGER,
  score INTEGER,
  sleep_duration INTEGER,
  sleep_motion_count INTEGER,
  sleep_onset_minutes INTEGER,
  sound_sleep INTEGER,
  type VARCHAR(15),
  version VARCHAR(15)
) DISTSTYLE even
COMPOUND SORTKEY(account_id, date);

GRANT ALL ON prod_sleep_stats_v_0_2 TO migrator;
GRANT SELECT ON prod_sleep_stats_v_0_2 TO GROUP data;
ALTER TABLE prod_sleep_stats_v_0_2 OWNER to migrator;


--
-- prod_pill_heartbeat
--
CREATE TABLE prod_pill_heartbeat(
  battery_level INTEGER,
  fw_version INTEGER,
  pill_id VARCHAR(100),
  uptime BIGINT,
  utc_dt TIMESTAMP WITHOUT TIME ZONE
) DISTSTYLE KEY DISTKEY (pill_id)
COMPOUND SORTKEY (pill_id, battery_level);

GRANT ALL ON prod_pill_heartbeat TO migrator;
GRANT SELECT ON prod_pill_heartbeat TO GROUP data;
ALTER TABLE prod_pill_heartbeat OWNER to migrator;


--
-- prod_app_stats
--
CREATE TABLE prod_app_stats (
  account_id BIGINT,
  insights_last_viewed BIGINT,
  questions_last_viewed BIGINT
) DISTSTYLE KEY DISTKEY (account_id)
COMPOUND SORTKEY (insights_last_viewed, questions_last_viewed);

GRANT ALL ON prod_app_stats TO migrator;
GRANT SELECT ON prod_app_stats TO GROUP data;
ALTER TABLE prod_app_stats OWNER to migrator;


--
-- prod_timezone_history
--
CREATE TABLE prod_timezone_history (
    account_id BIGINT,
    updated_at_server_time_millis BIGINT,
    time_zone_name VARCHAR(64)
) DISTSTYLE even
COMPOUND SORTKEY(account_id);

GRANT ALL ON prod_timezone_history TO migrator;
GRANT SELECT ON prod_timezone_history TO GROUP data;
ALTER TABLE prod_timezone_history OWNER to migrator;


--
-- prod_ring_history_by_account (subset)
--
CREATE TABLE prod_ring_history_by_account (
  account_id BIGINT,
  actual_ring_time BIGINT,
  created_at_utc BIGINT,
  device_id VARCHAR(20),
  expected_ring_time BIGINT,
  ring_time_object VARCHAR(256)
) DISTSTYLE KEY DISTKEY (account_id)
COMPOUND SORTKEY (account_id, created_at_utc);

GRANT ALL ON prod_ring_history_by_account TO migrator;
GRANT SELECT ON prod_ring_history_by_account TO GROUP data;
ALTER TABLE prod_ring_history_by_account OWNER to migrator;


--
-- prod_sense_last_seen
--
CREATE TABLE prod_sense_last_seen (
  dust INTEGER,
  fw_version INTEGER,
  humidity INTEGER,
  light INTEGER,
  sense_id VARCHAR(100),
  sound INTEGER,
  temp INTEGER,
  updated_at_utc TIMESTAMP WITHOUT TIME ZONE
) DISTSTYLE KEY DISTKEY (sense_id)
COMPOUND SORTKEY (sense_id, updated_at_utc);

GRANT ALL ON prod_sense_last_seen TO migrator;
GRANT SELECT ON prod_sense_last_seen TO GROUP data;
ALTER TABLE prod_sense_last_seen OWNER to migrator;


--
-- prod_pill_last_seen
--
CREATE TABLE prod_pill_last_seen(
  battery_level INTEGER,
  fw_version INTEGER,
  pill_id VARCHAR(100),
  updated_at_utc TIMESTAMP WITHOUT TIME ZONE,
  uptime BIGINT
) DISTSTYLE KEY DISTKEY (pill_id)
COMPOUND SORTKEY (pill_id, battery_level);

GRANT ALL ON prod_pill_last_seen TO migrator;
GRANT SELECT ON prod_pill_last_seen TO GROUP data;
ALTER TABLE prod_pill_last_seen OWNER to migrator;

--
-- prod_agg_stats_v_0_1
--
CREATE TABLE prod_agg_stats (
  aid INTEGER,
  avg_day_dust_density INTEGER,
  avg_day_humid INTEGER,
  avg_day_temp INTEGER,
  "date_local|sense_id" VARCHAR(32),
  deve_data_count INTEGER,
  dow INTEGER,
  max_day_temp INTEGER,
  min_day_temp INTEGER,
  sum_count_mlux_hrs_map VARCHAR(512),
  tracker_motion_count INTEGER
) DISTSTYLE KEY DISTKEY (aid)
COMPOUND SORTKEY (aid, "date_local|sense_id");

GRANT ALL ON prod_agg_stats TO migrator;
GRANT SELECT ON prod_agg_stats TO GROUP data;
ALTER TABLE prod_agg_stats OWNER to migrator;


--
-- speech_results (dev and prod)
--
CREATE TABLE prod_speech_results (
  cmd VARCHAR(64),
  cmd_result VARCHAR(32),
  conf NUMERIC,
  created_utc TIMESTAMP WITHOUT TIME ZONE,
  handler_type VARCHAR(64),
  resp_text VARCHAR(512),
  service VARCHAR(32),
  text VARCHAR(1024),
  updated TIMESTAMP WITHOUT TIME ZONE,
  uuid VARCHAR(64),
  wake_id INTEGER
) DISTSTYLE EVEN
COMPOUND SORTKEY (created_utc, cmd_result, text);

GRANT ALL ON prod_speech_results to migrator;
GRANT SELECT ON prod_speech_results to tim;
ALTER TABLE prod_speech_results OWNER to migrator;

CREATE TABLE speech_results (like prod_speech_results);
GRANT SELECT ON speech_results to tim;
ALTER TABLE speech_results OWNER to migrator;


CREATE TABLE prod_speech_timeline (
  account_id INTEGER,
  sense_id VARCHAR(100),
  ts TIMESTAMP WITHOUT TIME ZONE
) DISTSTYLE KEY DISTKEY (account_id)
COMPOUND SORTKEY (account_id, sense_id, ts);

GRANT ALL ON prod_speech_timeline to migrator;
GRANT SELECT ON prod_speech_timeline to tim;
ALTER TABLE prod_speech_timeline OWNER to migrator;

-- add fw version to prod_speech_results
ALTER TABLE prod_speech_results ADD COLUMN fw INTEGER DEFAULT 0;


-- 2017-01-30
CREATE TABLE key_store (
  aes_key VARCHAR(256),
  created_at TIMESTAMP WITHOUT TIME ZONE,
  device_id VARCHAR(64),
  hw_version INTEGER,
  metadata VARCHAR(512),
  note VARCHAR(512)
) DISTSTYLE KEY DISTKEY (device_id)
INTERLEAVED SORTKEY (device_id, metadata);

GRANT ALL ON key_store TO group ops;
GRANT ALL ON key_store to migrator;
ALTER TABLE key_store OWNER to migrator;

CREATE TABLE key_store_admin (
  created_at TIMESTAMP WITHOUT TIME ZONE,
  device_id VARCHAR(64),
  hw_version INTEGER,
  metadata VARCHAR(512),
  note VARCHAR(512)
) DISTSTYLE KEY DISTKEY (device_id)
INTERLEAVED SORTKEY (device_id, metadata);

ALTER TABLE key_store_admin OWNER to migrator;
GRANT SELECT ON key_store_admin to admin_tool;


CREATE TABLE pill_key_store (
  aes_key VARCHAR(256),
  created_at TIMESTAMP WITHOUT TIME ZONE,
  device_id VARCHAR(64),
  metadata VARCHAR(512),
  note VARCHAR(512)
) DISTSTYLE KEY DISTKEY (device_id)
INTERLEAVED SORTKEY (device_id, metadata);

GRANT ALL ON pill_key_store TO group ops;
ALTER TABLE pill_key_store OWNER to migrator;

CREATE TABLE prod_alarm (
  account_id BIGINT,
  alarm_templates VARCHAR(65535),
  updated_at BIGINT
) DISTSTYLE KEY DISTKEY (account_id)
COMPOUND SORTKEY (account_id, updated_at);

GRANT ALL ON prod_alarm TO group ops;
ALTER TABLE prod_alarm OWNER to migrator;

