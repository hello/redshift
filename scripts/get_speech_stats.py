import os
from datetime import datetime
import json
import arrow
import psycopg2
import psycopg2.extras
import requests

REDSHIT_HOSTNAME = 'sensors2.cy7n0vzxfedi.us-east-1.redshift.amazonaws.com'
REDSHIT_USER = 'migrator'
REDSHIT_DB = 'sensors1'

SLACK_TOKEN = "T024FJP19/B30RWGBB9/KtT7y8rXKGhaH3yAduGuPgGh"
SLACK_URL = "https://hooks.slack.com/services/%s" % SLACK_TOKEN
HEADERS = {'content-type': 'application/json'}

def get_db():
    """Connects to the specific database."""
    conn = psycopg2.connect(
        database=REDSHIT_DB,
        user=REDSHIT_USER,
        password=os.getenv('REDSHIT_PASS'),
        host=REDSHIT_HOSTNAME,
        port=5439
    )
    return conn

def post_slack(slack_string, title):
    attachments = {
        "fallback": "Voice command stats",
        "fields": [
            {
                "title": title,
                "value": slack_string
            }
        ]
    }

    payload = {
        'channel': '#voice-stats',
        'username': 'shout',
        "attachments": [attachments]
    }

    requests.post(SLACK_URL, data=json.dumps(payload), headers=HEADERS)


def main():
    utc_now = arrow.get(datetime.utcnow())
    week_ago = utc_now.replace(days=-7)
    date_string = week_ago.format('YYYY-MM-DD')

    # number of commands per day
    query = """SELECT date_trunc('day', created_utc) AS day, COUNT(*) AS c
        FROM prod_speech_results 
        WHERE created_utc >= %s GROUP BY day ORDER BY day"""

    stats = {}
    days = []
    db = get_db()
    with db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, (date_string,))
        rows = cur.fetchall()
        slack_string = ""
        for row in rows:
            # slack_string += "\n%s | %s" % (str(row['day'])[:10], row['c'])
            days.append(row['day'])
            stats.setdefault(row['day'], {'c':0, 'u': 0})
            stats[row['day']]['c'] = row['c']

        # if slack_string != "":
        #     post_slack(slack_string, "Commands per day (testing)")


    # number of distinct account-ids using speech
    query = """SELECT date_trunc('day', ts) AS day,
        COUNT(distinct account_id) AS c 
        FROM prod_speech_timeline 
        WHERE ts >= %s GROUP BY day ORDER BY day"""

    with db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, (date_string,))
        rows = cur.fetchall()
        slack_string = ""
        for row in rows:
            # slack_string += "\n%s | %s" % (str(row['day'])[:10], row['c'])
            if row['day'] in stats:
                stats[row['day']]['u'] = row['c']

        # if slack_string != "":
        #     post_slack(slack_string,
        #             "Distinct accounts using per day (testing)")

    if stats:
        slack_string = ""
        for day in days:
            slack_string += "\n%s | %s | %s" % (
                day, stats[day]['c'], stats[day]['u'])

        if slack_string != "":
            post_slack(slack_string, "Commands and distinct accounts per day")

    cmd_query = """
        SELECT date, fw,
          ROUND((1.0*empty_text)/total, 2) as empty_text_perc,
          ROUND((1.0*text_try_again)/total, 2) as text_try_again_perc,
          ROUND((1.0*text_reject)/total, 2) as text_reject_perc,
          ROUND((1.0*ok_cmds)/total, 2) as ok_commands,
          total AS total_commands
        FROM (
            SELECT DATE_TRUNC('day', created_utc) AS date, fw,
              SUM(CASE WHEN (text != '' AND cmd_result='TRY_AGAIN')  THEN 1 ELSE 0 END) AS text_try_again,
              SUM(CASE WHEN (text != '' AND cmd_result='REJECTED')  THEN 1 ELSE 0 END) AS text_reject,
              SUM(CASE WHEN (text = '')  THEN 1 ELSE 0 END) as empty_text,
              SUM(CASE WHEN (cmd != '')  THEN 1 ELSE 0 END) as ok_cmds,
              COUNT(*) AS total
            FROM prod_speech_results WHERE created_utc >= current_date - INTERVAL '7 day'
            GROUP BY date, fw ORDER BY date, fw
        ) ORDER BY date, fw
    """
    with db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(cmd_query, (date_string,))
        rows = cur.fetchall()
        slack_title = "Date       | empty_text   | reject     | try_again  | ok_cmd | total "
        num_data = 0
        slack_data = {}
        for row in rows:
            fw_version = str(row['fw'])
            if fw_version is None:
                fw_version = 'null'
            if fw_version in ['-1', '0', '1', 'None']:
                continue
            slack_data.setdefault(fw_version, [])
            num_data += 1
            slack_string = "\n%s | %s         | %s       | %s        | %s      | %s" % (
                str(row['date'])[:10], 
                row['empty_text_perc'],
                row['text_reject_perc'],
                row['text_try_again_perc'],
                row['ok_commands'],
                row['total_commands'])
            slack_data[fw_version].append(slack_string)


        for fw in sorted(slack_data):
            slack_string = slack_title
            for row in slack_data[fw]:
                slack_string += row
            post_slack(slack_string, "Commands breakdown by FW version %s" % (fw))

if __name__ == "__main__":
    main()
