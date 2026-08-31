"""Orchestrator: Groq LLM + direct tool-calling (in-process, no subprocess).

Flow per turn:
  1. Send messages + tools schema to Groq
  2. If Groq returns tool_calls → execute each via TOOLS_FN → append results
  3. Repeat up to MAX_TOOL_CALLS times, then get final text response
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from groq import Groq
from hr_etl.mcp.server import TOOLS_FN, TOOLS_SCHEMA

_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.1"))
_MAX_TOKENS = int(os.getenv("LLM_MAX_TOKENS", "1024"))
_MAX_TOOL_CALLS = int(os.getenv("ASSISTANT_MAX_TOOL_CALLS", "4"))

_SYSTEM_PROMPT = """Eres el Asistente Técnico Oficial de HR Insights ETL.

REGLAS:
1. Usa las herramientas disponibles para responder. Cuando una herramienta devuelva un campo "content", usa ese texto como base de tu respuesta.
2. Si la pregunta no encaja con ninguna herramienta, declina amablemente e indica qué sí puedes hacer.
3. Cuando una herramienta devuelva el campo "_pii_warning", DEBES incluirlo al final de tu respuesta.
4. Responde siempre en español.
"""


def is_available() -> bool:
    return bool(os.getenv("GROQ_API_KEY"))


def chat(user_message: str, history: list[dict]) -> str:
    """Run one turn: user_message + history → assistant reply."""
    client = Groq(api_key=os.getenv("GROQ_API_KEY"))
    model = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")

    messages = [{"role": "system", "content": _SYSTEM_PROMPT}]
    for msg in history:
        if msg["role"] in ("user", "assistant"):
            messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": user_message})

    for _ in range(_MAX_TOOL_CALLS):
        response = client.chat.completions.create(
            model=model,
            temperature=_TEMPERATURE,
            max_tokens=_MAX_TOKENS,
            messages=messages,
            tools=TOOLS_SCHEMA,
            tool_choice="auto",
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            return msg.content or _final_response(client, messages, model)

        messages.append({"role": "assistant", "content": msg.content or "", "tool_calls": [
            {"id": tc.id, "type": "function", "function": {"name": tc.function.name, "arguments": tc.function.arguments}}
            for tc in msg.tool_calls
        ]})

        for tc in msg.tool_calls:
            result = _call_tool(tc.function.name, tc.function.arguments)
            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(result, ensure_ascii=False),
            })

        # Tools executed — force plain text response now
        return _final_response(client, messages, model)

    return _final_response(client, messages, model)


def _final_response(client: Groq, messages: list[dict], model: str) -> str:
    """Call Groq without tools to force a plain text response."""
    r = client.chat.completions.create(
        model=model,
        temperature=_TEMPERATURE,
        max_tokens=_MAX_TOKENS,
        messages=messages,
    )
    return r.choices[0].message.content or "Sin respuesta."


def _call_tool(name: str, arguments: str) -> dict:
    fn = TOOLS_FN.get(name)
    if fn is None:
        return {"error": f"Tool '{name}' no encontrada."}
    try:
        kwargs = json.loads(arguments) if arguments else {}
        # Some models send malformed args like {"":{}}, filter them out
        kwargs = {k: v for k, v in kwargs.items() if k}
        return fn(**kwargs)
    except TypeError:
        # Fallback: call with no args if kwargs don't match signature
        try:
            return fn()
        except Exception as exc:
            return {"error": str(exc)}
    except Exception as exc:
        return {"error": str(exc)}
