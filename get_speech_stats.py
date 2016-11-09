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

SLACK_URL = "https://hooks.slack.com/services/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"
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

def main():
    utc_now = arrow.get(datetime.utcnow())
    week_ago = utc_now.replace(days=-7)
    date_string = week_ago.format('YYYY-MM-DD')
    query = """SELECT date_trunc('day', created_utc) AS day, COUNT(*) AS c 
        FROM prod_speech_results 
        WHERE created_utc >= %s GROUP BY day ORDER BY day"""

    db = get_db()
    with db.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(query, (date_string,))
        rows = cur.fetchall()
        slack_string = ""
        for row in rows:
            slack_string += "\n%s | %s" % (str(row['day'])[:10], row['c'])

    attachments = {
        "fallback": "Voice command stats",
        "fields": [
            {
                "title": "Commands per Day",
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

if __name__ == "__main__":
    main()
