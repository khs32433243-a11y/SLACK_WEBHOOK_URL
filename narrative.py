import anthropic
import os

def generate_report(analysis: dict) -> str:
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))
    prompt = f"다음 수익률 분석 데이터를 바탕으로 성과 보고서를 작성해줘: {analysis}"
    message = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    return message.content[0].text
