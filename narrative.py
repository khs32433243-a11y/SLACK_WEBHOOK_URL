import anthropic
import os

def generate_narrative(analysis: dict) -> str:
    """
    AI 리포트 생성 함수 (이름을 main.py와 맞춤)
    """
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    
    # 분석 데이터를 요약하여 프롬프트 작성
    prompt = f"다음 주간 수익률 분석 데이터를 바탕으로 마케팅 인사이트 리포트를 작성해줘: {analysis}"
    
    message = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text
