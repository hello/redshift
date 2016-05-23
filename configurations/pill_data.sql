
-- dev version
CREATE TABLE dev_pill_data (
  account_id BIGINT,
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

ALTER TABLE dev_pill_data OWNER TO migrator;


-- prod version

CREATE TABLE prod_pill_data (
  account_id BIGINT,
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

GRANT ALL ON prod_pill_data TO migrator;
GRANT SELECT ON prod_pill_data TO GROUP data;
GRANT SELECT ON prod_pill_data TO GROUP prod;

ALTER TABLE prod_pill_data OWNER TO migrator;
