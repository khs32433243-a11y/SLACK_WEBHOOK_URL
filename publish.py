import os, json, urllib.request

def post_to_slack(text: str):
    """성공 리포트를 슬랙으로 전송"""
    url = os.environ.get("SLACK_WEBHOOK_URL")
    payload = {"text": f"✅ *주간 수익률 분석 완료*\n\n{text}"}
    
    req = urllib.request.Request(
        url, 
        data=json.dumps(payload).encode("utf-8"), 
        headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req)
    except Exception as e:
        print(f"Slack 전송 실패: {e}")

def post_failure_alert(error_msg: str):
    """에러 발생 시 알림 전송 (이름을 main.py와 맞춤)"""
    url = os.environ.get("SLACK_WEBHOOK_URL")
    payload = {"text": f"⚠️ *수익률 리포트 가동 실패*\n\n*사유:* {error_msg}"}
    
    req = urllib.request.Request(
        url, 
        data=json.dumps(payload).encode("utf-8"), 
        headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req)
    except Exception as e:
        print(f"에러 알림 전송 실패: {e}")
