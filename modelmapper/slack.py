import json
import requests


def slack(text,
          username,
          channel,
          slack_http_endpoint):
    url = slack_http_endpoint if slack_http_endpoint.startswith('https') else "https://{}".format(slack_http_endpoint)
    payload = {
        "channel": channel,
        "username": username,
        "text": text,
        'parse': 'full'
    }
    return requests.post(url, data=json.dumps(payload))
