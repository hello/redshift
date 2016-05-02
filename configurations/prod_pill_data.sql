CREATE TABLE dev_pill_data (
  account_id BIGINT,
  tracker_id BIGINT,
  external_tracker_id VARCHAR(100),
  svm_no_gravity INTEGER,
  ts TIMESTAMP WITHOUT TIME ZONE,
  offset_millis INTEGER,
  local_utc_ts TIMESTAMP WITHOUT TIME ZONE,
  motion_range BIGINT,
  kickoff_counts INTEGER,
  on_duration_seconds INTEGER
) DISTSTYLE KEY DISTKEY (account_id)
COMPOUND SORTKEY (local_utc_ts, account_id);

GRANT ALL ON dev_pill_data TO migrator;
GRANT SELECT ON dev_pill_data TO GROUP data;

-- run this sql later when ready for prod
-- CREATE TABLE prod_pill_data (like dev_pill_data);
