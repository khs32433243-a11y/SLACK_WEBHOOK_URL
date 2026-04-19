"""Slack publisher using Incoming Webhooks.

Stdlib only — no requests dependency. Splits long bodies across multiple
blocks because Slack caps each section at ~3000 chars.
"""

import json
import os
import urllib.request
import urllib.error


_BLOCK_CHAR_LIMIT = 2900  # leave some headroom


def _split_into_blocks(body: str) -> list[dict]:
    """Split markdown body into section blocks respecting Slack's char limit."""
    blocks: list[dict] = []
    remaining = body
    while remaining:
        if len(remaining) <= _BLOCK_CHAR_LIMIT:
            blocks.append({"type": "section",
                           "text": {"type": "mrkdwn", "text": remaining}})
            break
        # Split on the last blank line before the limit
        cut = remaining.rfind("\n\n", 0, _BLOCK_CHAR_LIMIT)
        if cut == -1:
            cut = _BLOCK_CHAR_LIMIT
        blocks.append({"type": "section",
                       "text": {"type": "mrkdwn", "text": remaining[:cut]}})
        remaining = remaining[cut:].lstrip()
    return blocks


def post_to_slack(title: str, body: str, webhook_url: str | None = None) -> None:
    url = webhook_url or os.environ["SLACK_WEBHOOK_URL"]
    payload = {
        "blocks": [
            {"type": "header",
             "text": {"type": "plain_text", "text": title, "emoji": False}},
            *_split_into_blocks(body),
        ]
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                raise RuntimeError(f"Slack returned {resp.status}: {resp.read()!r}")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Slack webhook failed: {e.code} {e.read()!r}") from e


def post_failure_alert(error: Exception, webhook_url: str | None = None) -> None:
    """Send a short alert when the pipeline itself failed."""
    post_to_slack(
        title="⚠️ 주간 리포트 실패",
        body=f"```{type(error).__name__}: {error}```\n리포 로그 확인 요망.",
        webhook_url=webhook_url,
    )
