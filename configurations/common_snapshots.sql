
--
-- commondb questions
--
CREATE TABLE questions(
  id INTEGER PRIMARY KEY,
  parent_id INTEGER,
  question_text VARCHAR(256),
  lang VARCHAR(8),
  frequency VARCHAR(32),
  response_type VARCHAR(32),
  dependency INTEGER,
  ask_time VARCHAR(32),
  account_info VARCHAR(32),
  created TIMESTAMP,
  category VARCHAR(32)
) DISTSTYLE even
COMPOUND SORTKEY (id);

GRANT ALL ON questions to migrator;
GRANT SELECT ON questions to GROUP data;


---- export from postres
-- \copy (SELECT id, parent_id, question_text, lang, frequency, response_type, dependency, ask_time, account_info, created, category FROM questions order by id) TO '/home/ubuntu/snapshots/commondb/questions.csv' CSV

---- upload to S3 
-- aws s3 cp questions.csv s3://hello-db-exports/snapshots/questions.csv

---- download to redshift on migrator3
-- ~/misc/Analytics/copy_questions.sh


--
-- commondb account_questions
--
CREATE TABLE account_questions(
  id INTEGER PRIMARY KEY,
  account_id BIGINT,
  question_id INTEGER,
  created_local_utc_ts TIMESTAMP,
  expires_local_utc_ts TIMESTAMP,
  created TIMESTAMP
) DISTSTYLE even
COMPOUND SORTKEY (account_id, question_id);

GRANT ALL ON account_questions to migrator;
GRANT SELECT ON account_questions to GROUP data;

---- common-db export
-- \copy (SELECT * FROM account_questions) TO '/home/ubuntu/snapshots/commondb/account_questions.csv' CSV;

---- upload to S3
-- gzip account_questions.csv
-- aws s3 cp account_questions.csv.gz s3://hello-db-exports/snapshots/account_questions.csv.gz

---- redshift
-- copy_account_questions.sh


--
-- commondb responses
--
CREATE TABLE responses(
id BIGINT PRIMARY KEY,
account_id BIGINT,
question_id INTEGER,
account_question_id BIGINT,
response_id INTEGER,
skip VARCHAR(2),
question_freq VARCHAR(32),
created TIMESTAMP
) DISTSTYLE even
COMPOUND SORTKEY (account_id, question_id);

GRANT ALL ON responses to migrator;
GRANT SELECT ON responses to GROUP data;

---- common-db export
-- \copy (SELECT * FROM responses) TO '/home/ubuntu/snapshots/commondb/responses.csv' CSV;

---- upload to S3
-- gzip responses.csv
-- aws s3 cp responses.csv.gz s3://hello-db-exports/snapshots/responses.csv.gz

---- redshift
-- copy_responses.sh


--
-- commondb response_choices
--
CREATE TABLE response_choices(
  id INTEGER PRIMARY KEY,
  question_id INTEGER,
  response_text VARCHAR(64),
  created TIMESTAMP
) DISTSTYLE even
COMPOUND SORTKEY(question_id);

GRANT ALL ON response_choices to migrator;
GRANT SELECT ON response_choices to GROUP data;

---- common-db export
-- \copy (SELECT * FROM response_choices) TO '/home/ubuntu/snapshots/commondb/response_choices.csv' CSV;

---- upload to S3
-- aws s3 cp responses.csv.gz s3://hello-db-exports/snapshots/response_choices.csv

---- redshift
-- copy_responses.sh


