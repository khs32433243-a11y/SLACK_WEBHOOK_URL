"""LLM narrative generator.

Takes the structured decomposition dict and produces a Korean markdown report
suitable for direct posting to Slack.
"""

import json
import os
from pathlib import Path

import anthropic


DEFAULT_MODEL = "claude-opus-4-7"


def generate_narrative(
    decomposition: dict,
    prompt_template: Path,
    brand_context: Path,
    model: str = DEFAULT_MODEL,
    max_tokens: int = 2000,
) -> str:
    """Render a weekly report from the decomposition.

    The system prompt carries the brand-manager rules (강북점 취급, 플랫폼비 중복
    금지 등). The user prompt carries the task + the data.
    """
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    rules = brand_context.read_text(encoding="utf-8")
    task = prompt_template.read_text(encoding="utf-8")

    system = (
        "You are a senior data analyst for Blitz Dynamics, a 10-store delivery "
        "kitchen operator. Respond in Korean. Follow the brand-manager rules "
        "strictly.\n\n"
        f"<brand_manager_rules>\n{rules}\n</brand_manager_rules>"
    )
    user_msg = task.replace(
        "{{DATA}}",
        json.dumps(decomposition, ensure_ascii=False, indent=2, default=str),
    )

    resp = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[{"role": "user", "content": user_msg}],
    )
    # Concatenate text blocks
    return "".join(b.text for b in resp.content if b.type == "text")
