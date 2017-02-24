"""Slack"""
import os
import json
import logging
import requests

SLACK_TOKEN = os.getenv('REDSHIFT_SLACK')
SLACK_URL = "https://hooks.slack.com/services/%s" % (SLACK_TOKEN)
HEADERS = {'content-type': 'application/json'}

def post(table, date, text):
    """Post a status message to ops"""
    if not SLACK_TOKEN:
        logging.error("Wrong url")
        return

    prefix = "*Copy %s %s:* " % (table, date)
    payload = {'text': prefix + text,
        'channel': '#ops', 'username': 'redshit'}
    try:
        requests.post(SLACK_URL, data=json.dumps(payload), headers=HEADERS)
    except requests.exceptions.RequestException as err:
        logging.error("Fail to send to Slack")
        logging.error(err)
