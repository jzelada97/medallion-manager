"""Streamlit frontend for HR Insights.

A simple but polished dashboard to explore the consolidated HR data served by the
FastAPI backend. It uses the API's search, filters, pagination and /stats endpoint,
plus a duplicate-candidates view backed by the batch reconciliation (/candidates).
"""

from __future__ import annotations

import os

import pandas as pd
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")
# The API caps `limit` at 500 per request, so page sizes stay within that.
PAGE_SIZE_OPTIONS = [25, 50, 100, 250, 500]
DEFAULT_PAGE_SIZE = 25

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
# Tabs: Personas | Duplicados
# --------------------------------------------------------------------------- #
tab_persons, tab_dupes = st.tabs(["👤 Personas", "🔗 Duplicados"])


# --------------------------------------------------------------------------- #
# Tab 1: Personas (search + filters + detail)
# --------------------------------------------------------------------------- #
with tab_persons:
    # Page number lives in session_state so the Prev/Next buttons can update it.
    if "persons_page" not in st.session_state:
        st.session_state.persons_page = 1

    # --- Search bar (prominent) + advanced filters in an expander ---
    search_col, size_col = st.columns([4, 1])
    with search_col:
        q = st.text_input(
            "🔎 Buscar persona",
            placeholder="Nombre, empresa o email…",
            label_visibility="collapsed",
        )
    with size_col:
        page_size = st.selectbox(
            "Por página",
            PAGE_SIZE_OPTIONS,
            index=PAGE_SIZE_OPTIONS.index(DEFAULT_PAGE_SIZE),
            label_visibility="collapsed",
        )

    with st.expander("Filtros avanzados"):
        fc1, fc2, fc3 = st.columns(3)
        city = fc1.text_input("Ciudad")
        company = fc2.text_input("Empresa")
        job = fc3.text_input("Puesto")

    # Any change in the search/filters resets to page 1 so results make sense.
    filter_signature = (q, city, company, job, page_size)
    if st.session_state.get("persons_filter_sig") != filter_signature:
        st.session_state.persons_filter_sig = filter_signature
        st.session_state.persons_page = 1

    page = st.session_state.persons_page

    params: dict[str, object] = {
        "limit": int(page_size),
        "offset": (int(page) - 1) * int(page_size),
    }
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
    total_pages = max(1, (int(total) + int(page_size) - 1) // int(page_size))

    # Clamp page if filters shrank the result set below the current page.
    if page > total_pages:
        st.session_state.persons_page = total_pages
        page = total_pages

    st.subheader("Personas")
    st.caption(f"{total} resultado(s) · {int(page_size)} por página")

    if items:
        df = pd.DataFrame(items)
        preferred = [
            "id", "full_name", "city", "company", "job", "email", "phone", "iban", "salary",
        ]
        cols = [c for c in preferred if c in df.columns] + [
            c for c in df.columns if c not in preferred
        ]
        st.dataframe(df[cols], use_container_width=True, hide_index=True)

        # --- Pagination controls (Prev / page indicator / Next) ---
        prev_col, ind_col, next_col = st.columns([1, 2, 1])
        with prev_col:
            if st.button("◀ Anterior", disabled=(page <= 1), use_container_width=True):
                st.session_state.persons_page = max(1, page - 1)
                st.rerun()
        with ind_col:
            st.markdown(
                f"<div style='text-align:center;padding-top:6px'>"
                f"Página <b>{int(page)}</b> de <b>{total_pages}</b></div>",
                unsafe_allow_html=True,
            )
        with next_col:
            if st.button(
                "Siguiente ▶", disabled=(page >= total_pages), use_container_width=True
            ):
                st.session_state.persons_page = min(total_pages, page + 1)
                st.rerun()

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


# --------------------------------------------------------------------------- #
# Tab 2: Duplicados (match candidates from batch reconciliation)
# --------------------------------------------------------------------------- #
def _person_label(person: dict | None, fallback_id: int) -> str:
    """Compact one-line label for a person, for side-by-side comparison."""
    if not person:
        return f"#{fallback_id} (no encontrado)"
    name = person.get("full_name") or "(sin nombre)"
    bits = [f"#{person.get('id', fallback_id)}", name]
    if person.get("city"):
        bits.append(person["city"])
    if person.get("company"):
        bits.append(person["company"])
    return " · ".join(str(b) for b in bits)


with tab_dupes:
    st.subheader("Posibles duplicados")
    st.caption(
        "Pares de personas con nombres parecidos que podrían ser la misma. "
        "Los genera la reconciliación por lotes y se guardan en `match_candidates`."
    )

    min_conf = st.slider(
        "Confianza mínima",
        min_value=0.0,
        max_value=1.0,
        value=0.5,
        step=0.05,
        help="Solo muestra pares cuya confianza de coincidencia supere este umbral.",
    )
    max_rows = st.number_input(
        "Máx. resultados", min_value=1, max_value=500, value=100, step=25
    )

    dupes = api_get(
        "/candidates", {"limit": int(max_rows), "min_confidence": float(min_conf)}
    ) or {"total": 0, "count": 0, "items": []}
    dup_total = dupes.get("total", 0)
    dup_items = dupes.get("items", [])

    st.caption(
        f"{dup_total} candidato(s) por encima de {min_conf:.2f} · "
        f"mostrando {len(dup_items)}"
    )

    if not dup_items:
        st.info(
            "No hay candidatos a duplicado todavía. Ejecuta la reconciliación "
            "(DAG `hr_etl_reconciliation` en Airflow, o "
            "`python -m hr_etl.processing.reconcile`) y vuelve a mirar."
        )
    else:
        df_dupes = pd.DataFrame(dup_items)
        # Human-friendly column order/labels.
        rename = {
            "id": "id",
            "person_id_a": "persona A",
            "person_id_b": "persona B",
            "confidence": "confianza",
            "reason": "motivo",
        }
        show_cols = [c for c in ["id", "person_id_a", "person_id_b", "confidence", "reason"] if c in df_dupes.columns]
        st.dataframe(
            df_dupes[show_cols].rename(columns=rename),
            use_container_width=True,
            hide_index=True,
            column_config={
                "confianza": st.column_config.ProgressColumn(
                    "confianza", min_value=0.0, max_value=1.0, format="%.2f"
                )
            },
        )

        # --- Side-by-side comparison of a selected pair ---
        st.subheader("Comparar par")
        labels = {
            f"#{r['id']} · A={r['person_id_a']} ↔ B={r['person_id_b']} "
            f"({r.get('confidence', 0):.2f})": r
            for r in dup_items
        }
        choice = st.selectbox("Selecciona un par para compararlo", list(labels.keys()))
        if choice:
            pair = labels[choice]
            pa = api_get(f"/persons/{pair['person_id_a']}")
            pb = api_get(f"/persons/{pair['person_id_b']}")

            st.markdown(
                f"**Confianza:** {pair.get('confidence', 0):.2f}  ·  "
                f"**Motivo:** {pair.get('reason', '—')}"
            )
            colp, colq = st.columns(2)
            with colp:
                st.markdown(f"**Persona A** — {_person_label(pa, pair['person_id_a'])}")
                if pa:
                    st.json(pa)
            with colq:
                st.markdown(f"**Persona B** — {_person_label(pb, pair['person_id_b'])}")
                if pb:
                    st.json(pb)
