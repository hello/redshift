
import requests
import os
import json

data = {"text": "testing webhook", "channel": "#ops", "username": "redshit"}
slack_token = os.getenv('REDSHIFT_SLACK')
if slack_token:
    slack_url = "https://hooks.slack.com/services/%s" % (slack_token)
    headers={'content-type': 'application/json'}
    r = requests.post(slack_url, json.dumps(data), headers=headers)
