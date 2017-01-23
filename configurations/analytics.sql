
--
-- table to get account_id, action, count(*)
-- Notes:
-- * default null_string is "\N" -- any Optional.absent() values should be replaced with this
--   http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-null-as
-- * use interleaved sortkey which is better for ad-hoc queries.
--

CREATE TABLE actions (
  account_id INTEGER,
  action VARCHAR(256) NOT NULL,      -- action that user is performing
  result VARCHAR(256) DEFAULT NULL,  -- result of action if exist. e.g. pass/fail/value
  ts TIMESTAMP WITHOUT TIME ZONE,    -- utc 
  offset_millis INTEGER DEFAULT NULL -- local offset if available
) DISTYLE KEY DISTKEY (account_id)
INTERLEAVED SORTKEY (account_id, ts, action);

GRANT ALL ON account_actions to migrator;
GRANT ALL ON account_actions to group ops;

