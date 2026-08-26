"""Streamlit frontend to query consolidated HR data via the API."""

from __future__ import annotations

import os

import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(page_title="HR Insights", layout="wide")
st.title("HR Insights — Consulta de personas")

with st.sidebar:
    st.header("Filtros")
    city = st.text_input("Ciudad")
    company = st.text_input("Empresa")
    limit = st.slider("Máx. resultados", 10, 500, 50, step=10)

params: dict[str, object] = {"limit": limit}
if city:
    params["city"] = city
if company:
    params["company"] = company

try:
    resp = requests.get(f"{API_URL}/persons", params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    st.caption(f"{data.get('count', 0)} resultados")
    st.dataframe(data.get("items", []), use_container_width=True)
except requests.RequestException as exc:
    st.error(f"No se pudo conectar con la API en {API_URL}: {exc}")
