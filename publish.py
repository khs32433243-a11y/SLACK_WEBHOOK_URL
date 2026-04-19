import os, json, urllib.request

def post_to_slack(text: str):
    url = os.environ.get("SLACK_WEBHOOK_URL")
    payload = {"text": text}
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req)
