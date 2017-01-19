
--
-- table to get account_id, action, count(*)
-- Notes:
-- * default null_string is "\N" -- any Optional.absent() values should be replaced with this
--   http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-null-as
-- * use interleaved sortkey which is better for ad-hoc queries.
--

CREATE TABLE account_actions (
  account_id INTEGER,
  action VARCHAR(256) NOT NULL,
  category VARCHAR(256) DEFAULT NULL, -- ops, app, mobile, fw, ds, biz etc...
  result VARCHAR(256) DEFAULT NULL, -- result of action if exist. e.g. pass/fail/value
  sense_id VARCHAR(64) DEFAULT NULL,
  fw INTEGER DEFAULT NULL, -- firmware version
  hw INTEGER DEFAULT NULL, -- hardware version  1 or 4
  ts TIMESTAMP WITHOUT TIME ZONE, -- utc 
  offset_millis INTEGER DEFAULT NULL -- local offset if available
) DISTYLE KEY DISTKEY (account_id)
INTERLEAVED SORTKEY (account_id, ts, action, category, sense_id, fw_version);

GRANT ALL ON account_actions to migrator;
GRANT ALL ON account_actions to group ops;

