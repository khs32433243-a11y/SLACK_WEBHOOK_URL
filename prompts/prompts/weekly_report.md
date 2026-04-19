name: Weekly profit report

on:
  # 매주 월 06:00 KST (일 21:00 UTC) 정기 실행
  schedule:
    - cron: "0 21 * * 0"
  # data/latest.xlsx 를 커밋하면 즉시 실행
  push:
    paths:
      - "data/**"
  # 수동 트리거 (테스트용)
  workflow_dispatch:

jobs:
  report:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    permissions:
      contents: write

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run weekly report
        run: python main.py
        env:
          ANTHROPIC_API_KEY: ${{ secrets.ANTHROPIC_API_KEY }}
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}

      - name: Commit archive
        run: |
          git config user.email "bot@blitz.local"
          git config user.name "report-bot"
          git add archive/
          if git diff --staged --quiet; then
            echo "No archive changes"
          else
            git commit -m "archive: $(date -u +%Y-W%V)"
            git push
          fi
