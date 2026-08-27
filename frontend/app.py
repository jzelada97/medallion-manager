"""Streamlit frontend for HR Insights.

A simple but polished dashboard to explore the consolidated HR data served by the
FastAPI backend. It uses the API's search, filters, pagination and /stats endpoint.
"""

from __future__ import annotations

import os

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")
PAGE_SIZE = 25

st.set_page_config(page_title="HR Insights", page_icon="👥", layout="wide")


# --------------------------------------------------------------------------- #
# API helpers
# --------------------------------------------------------------------------- #
def api_get(path: str, params: dict | None = None) -> dict | None:
    """GET a JSON resource from the API, showing a friendly error on failure."""
    try:
        resp = requests.get(f"{API_URL}{path}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        st.error(f"No se pudo conectar con la API en {API_URL}{path}: {exc}")
        return None


# --------------------------------------------------------------------------- #
# Header + health
# --------------------------------------------------------------------------- #
st.title("👥 HR Insights")
st.caption("Consulta de personas consolidadas — HR Pro")

health = api_get("/health")
if health is None:
    st.stop()

# --------------------------------------------------------------------------- #
# Stats overview
# --------------------------------------------------------------------------- #
stats = api_get("/stats") or {}
c1, c2, c3 = st.columns(3)
c1.metric("Personas totales", stats.get("total_persons", 0))
c2.metric("Con datos bancarios", stats.get("with_bank", 0))
top_cities = stats.get("top_cities", [])
c3.metric("Ciudades distintas (top)", len(top_cities))

col_a, col_b = st.columns(2)
with col_a:
    st.subheader("Top ciudades")
    if top_cities:
        df_cities = pd.DataFrame(top_cities).rename(
            columns={"value": "ciudad", "count": "personas"}
        )
        st.bar_chart(df_cities.set_index("ciudad"))
    else:
        st.info("Sin datos todavía.")
with col_b:
    st.subheader("Top empresas")
    top_companies = stats.get("top_companies", [])
    if top_companies:
        df_comp = pd.DataFrame(top_companies).rename(
            columns={"value": "empresa", "count": "personas"}
        )
        st.bar_chart(df_comp.set_index("empresa"))
    else:
        st.info("Sin datos todavía.")

st.divider()

# --------------------------------------------------------------------------- #
# Search + filters
# --------------------------------------------------------------------------- #
with st.sidebar:
    st.header("Búsqueda y filtros")
    q = st.text_input("Buscar (nombre, empresa, email)")
    city = st.text_input("Ciudad")
    company = st.text_input("Empresa")
    job = st.text_input("Puesto")
    page = st.number_input("Página", min_value=1, value=1, step=1)

params: dict[str, object] = {"limit": PAGE_SIZE, "offset": (int(page) - 1) * PAGE_SIZE}
if q:
    params["q"] = q
if city:
    params["city"] = city
if company:
    params["company"] = company
if job:
    params["job"] = job

data = api_get("/persons", params) or {"total": 0, "count": 0, "items": []}
total = data.get("total", 0)
items = data.get("items", [])

st.subheader("Personas")
st.caption(f"{total} resultado(s) · mostrando {len(items)} (página {int(page)})")

if items:
    df = pd.DataFrame(items)
    preferred = ["id", "full_name", "city", "company", "job", "email", "phone", "iban", "salary"]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    st.dataframe(df[cols], use_container_width=True, hide_index=True)

    # --- Detail view ---
    st.subheader("Ficha de persona")
    ids = [row["id"] for row in items]
    selected = st.selectbox("Selecciona un id para ver el detalle", ids)
    if selected is not None:
        detail = api_get(f"/persons/{selected}")
        if detail:
            st.json(detail)
else:
    st.info(
        "No hay personas que coincidan con la búsqueda. "
        "Si el pipeline aún no ha procesado datos, la tabla estará vacía."
    )
