
--
-- table to get account_id, action, count(*)
-- Notes:
-- * default null_string is "\N" -- any Optional.absent() values should be replaced with this
--   http://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-null-as
-- * use interleaved sortkey which is better for ad-hoc queries.
--

CREATE TABLE actions (
  account_id BIGINT,
  action VARCHAR(256) NOT NULL,      -- action that user is performing
  result VARCHAR(256) DEFAULT NULL,  -- result of action if exist. e.g. pass/fail/value
  ts TIMESTAMP WITHOUT TIME ZONE,    -- utc 
  offset_millis INTEGER DEFAULT NULL -- local offset if available
) DISTSTYLE KEY DISTKEY (account_id)
INTERLEAVED SORTKEY (account_id, ts, action);

GRANT ALL ON actions to migrator;
GRANT ALL ON actions to group ops;

