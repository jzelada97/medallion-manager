"""MCP tools as plain functions — no FastMCP, no decorators.

Each function calls the existing FastAPI and returns a dict.
The orchestrator calls these directly (in-process).
"""

from __future__ import annotations

import os
from pathlib import Path

import requests

_API_URL = os.getenv("API_URL", "http://localhost:8000")
_KNOWLEDGE = Path(__file__).parent.parent.parent.parent / "frontend" / "assistant" / "knowledge"


def _api(path: str, params: dict | None = None) -> dict:
    resp = requests.get(f"{_API_URL}{path}", params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _read_md(name: str) -> str:
    p = _KNOWLEDGE / f"{name}.md"
    return p.read_text(encoding="utf-8") if p.exists() else f"(contenido {name} no disponible)"


# ── datos / métricas ────────────────────────────────────────────────────────

def get_stats() -> dict:
    """Totales y KPIs del warehouse: personas, con banco, top ciudad, top empresa."""
    return _api("/stats")


def top_cities(limit: int = 10) -> dict:
    """Ranking de las ciudades con más personas consolidadas."""
    data = _api("/stats")
    cities = data.get("top_cities", [])[:limit]
    return {"top_cities": cities}


def top_companies(limit: int = 10) -> dict:
    """Ranking de las empresas con más personas consolidadas."""
    data = _api("/stats")
    companies = data.get("top_companies", [])[:limit]
    return {"top_companies": companies}


def completeness_distribution() -> dict:
    """Distribución de completitud de registros (gold layer)."""
    try:
        return _api("/gold/completeness")
    except Exception:
        return {"error": "Gold layer no disponible. Ejecuta el DAG de Airflow primero."}


def duplicate_candidates(limit: int = 20) -> dict:
    """Pares de registros candidatos a duplicado con score de confianza."""
    try:
        return _api("/candidates", {"limit": limit})
    except Exception:
        return {"candidates": [], "note": "Sin candidatos o endpoint no disponible."}


def search_person(
    q: str | None = None,
    city: str | None = None,
    company: str | None = None,
    job: str | None = None,
    page: int = 1,
) -> dict:
    """Busca personas con filtros opcionales. Devuelve lista paginada."""
    params: dict = {"limit": 10, "offset": (page - 1) * 10}
    if q:
        params["q"] = q
    if city:
        params["city"] = city
    if company:
        params["company"] = company
    if job:
        params["job"] = job
    result = _api("/persons", params)
    result["_pii_warning"] = (
        "⚠️ Datos sintéticos de demostración. En un sistema real estos campos "
        "(passport, IBAN, salario, email, teléfono) NO se mostrarían en un chat."
    )
    return result


# ── explicación del proyecto (contenido curado) ─────────────────────────────

def explain_project() -> dict:
    """Qué es HR Insights ETL, objetivo y niveles implementados."""
    return {"content": _read_md("project")}


def explain_architecture() -> dict:
    """Arquitectura del sistema: Kafka→MongoDB→Redis→PostgreSQL→API→Streamlit."""
    return {"content": _read_md("architecture")}


def explain_matching() -> dict:
    """Estrategia de matching sin ID global: passport > nombre > dirección."""
    return {"content": _read_md("matching")}


def explain_how_built() -> dict:
    """Decisiones técnicas, stack, tests y CI del proyecto."""
    return {"content": _read_md("how_built")}


# ── catálogo para el orquestador ────────────────────────────────────────────

TOOLS_FN: dict[str, callable] = {
    "get_stats": get_stats,
    "top_cities": top_cities,
    "top_companies": top_companies,
    "completeness_distribution": completeness_distribution,
    "duplicate_candidates": duplicate_candidates,
    "search_person": search_person,
    "explain_project": explain_project,
    "explain_architecture": explain_architecture,
    "explain_matching": explain_matching,
    "explain_how_built": explain_how_built,
}

TOOLS_SCHEMA: list[dict] = [
    {
        "type": "function",
        "function": {
            "name": "get_stats",
            "description": "Totales y KPIs del warehouse: total personas, con banco, top ciudad, top empresa.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "top_cities",
            "description": "Ranking de ciudades con más personas consolidadas.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "description": "Número de ciudades a devolver (default 10)"}},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "top_companies",
            "description": "Ranking de empresas con más personas consolidadas.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "description": "Número de empresas a devolver (default 10)"}},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "completeness_distribution",
            "description": "Distribución de completitud de registros del gold layer.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "duplicate_candidates",
            "description": "Pares de registros candidatos a duplicado con score de confianza.",
            "parameters": {
                "type": "object",
                "properties": {"limit": {"type": "integer", "description": "Número máximo de candidatos (default 20)"}},
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_person",
            "description": "Busca personas por nombre, ciudad, empresa o puesto.",
            "parameters": {
                "type": "object",
                "properties": {
                    "q": {"type": "string", "description": "Búsqueda libre (nombre, email, passport)"},
                    "city": {"type": "string", "description": "Filtrar por ciudad"},
                    "company": {"type": "string", "description": "Filtrar por empresa"},
                    "job": {"type": "string", "description": "Filtrar por puesto"},
                    "page": {"type": "integer", "description": "Página de resultados (default 1)"},
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_project",
            "description": "Explica qué es HR Insights ETL, su objetivo y niveles implementados.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_architecture",
            "description": "Explica la arquitectura del sistema: Kafka, MongoDB, Redis, PostgreSQL, API, Streamlit.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_matching",
            "description": "Explica la estrategia de matching sin ID global: passport, nombre, dirección, cross-linking.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_how_built",
            "description": "Explica las decisiones técnicas, stack, tests y CI del proyecto.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
]
