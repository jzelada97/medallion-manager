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
def api_get(path: str, params: dict | None = None, timeout: int = 30) -> dict | None:
    """GET a JSON resource from the API, showing a friendly error on failure.

    ``timeout`` defaults to 30s: some endpoints (notably ``/groups``, which bundles the
    duplicate-review groups) run heavy aggregations over the full warehouse and can take
    ~10-15s on the production dataset. A 10s cap timed those out, so the pane fell back to
    its empty state even though the API was healthy. Pass a smaller value for light calls.
    """
    try:
        resp = requests.get(f"{API_URL}{path}", params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        st.error(f"No se pudo conectar con la API en {API_URL}{path}: {exc}")
        return None


def api_post(path: str, json: dict | None = None) -> dict | None:
    """POST a JSON payload to the API. On a 4xx, shows the backend's error detail
    (e.g. "person id(s) not found: [...]") instead of a generic connection error."""
    try:
        resp = requests.post(f"{API_URL}{path}", json=json, timeout=30)
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("detail", resp.text)
            except ValueError:
                detail = resp.text
            st.error(f"La API rechazó la operación ({resp.status_code}): {detail}")
            return None
        return resp.json()
    except requests.RequestException as exc:
        st.error(f"No se pudo conectar con la API en {API_URL}{path}: {exc}")
        return None


@st.cache_data(ttl=30, show_spinner=False)
def api_get_cached(path: str, timeout: int = 30) -> dict | None:
    """Cached GET for CONTEXT data that does not change while the user paginates or
    switches tabs (``/health``, ``/stats``, ``/medallion``). Streamlit reruns the whole
    script on every widget interaction — a Next click, a selectbox — so without a cache
    these fixed calls would hit the API again on each rerun. A 30s TTL means they are
    fetched once and reused for the burst of interactions, then refreshed.

    Kept side-effect free (no ``st.error``) because cached functions should be pure; the
    caller renders a fallback when it returns None. Only for param-less, stable endpoints
    — do NOT use it for the paginated list (its params change every page).
    """
    try:
        resp = requests.get(f"{API_URL}{path}", timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException:
        return None


# --------------------------------------------------------------------------- #
# Header + health
# --------------------------------------------------------------------------- #
st.title("👥 HR Insights")
st.caption("Consulta de personas consolidadas — HR Pro")

health = api_get_cached("/health")
if health is None:
    st.error(f"No se pudo conectar con la API en {API_URL}/health.")
    st.stop()

# --------------------------------------------------------------------------- #
# Stats overview
# --------------------------------------------------------------------------- #
stats = api_get_cached("/stats") or {}
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
tab_medallion, tab_persons, tab_dupes = st.tabs(["🏅 Arquitectura", "👤 Personas", "🔗 Duplicados"])


# --------------------------------------------------------------------------- #
# Tab 0: Medallion architecture (Bronze -> Silver -> Gold, live counts)
# --------------------------------------------------------------------------- #
def _fmt(n) -> str:
    """Thousands-separated count, or a dash when unavailable."""
    return f"{int(n):,}".replace(",", ".") if n is not None else "—"


with tab_medallion:
    st.subheader("Arquitectura Medallion")
    st.caption(
        "Flujo de datos por capas. Cada mensaje crudo (Bronze) se consolida y limpia "
        "en registros de persona (Silver), y de ahí se agregan métricas (Gold)."
    )

    med = api_get_cached("/medallion") or {}
    bronze = med.get("bronze", {})
    silver = med.get("silver", {})
    gold = med.get("gold", {})

    b_col, arrow1, s_col, arrow2, g_col = st.columns([3, 1, 3, 1, 3])
    with b_col:
        st.markdown("### 🥉 Bronze")
        st.metric("Mensajes crudos", _fmt(bronze.get("count")))
        st.caption(f"{bronze.get('store', 'MongoDB')} · data lake")
    with arrow1:
        st.markdown(
            "<div style='text-align:center;font-size:2rem;padding-top:2.2rem'>➜</div>",
            unsafe_allow_html=True,
        )
    with s_col:
        st.markdown("### 🥈 Silver")
        st.metric("Personas consolidadas", _fmt(silver.get("count")))
        st.caption(f"{silver.get('store', 'PostgreSQL')} · limpio y unido")
    with arrow2:
        st.markdown(
            "<div style='text-align:center;font-size:2rem;padding-top:2.2rem'>➜</div>",
            unsafe_allow_html=True,
        )
    with g_col:
        st.markdown("### 🥇 Gold")
        if gold.get("refreshed"):
            st.metric("Personas (agregado)", _fmt(gold.get("total_persons")))
            st.caption(
                f"{gold.get('store', 'PostgreSQL')} · "
                f"{_fmt(gold.get('cross_linked'))} cross-linked · "
                f"completitud {gold.get('avg_completeness', '—')}/8"
            )
        else:
            st.metric("Personas (agregado)", "—")
            st.caption("Gold sin refrescar todavía")

    # Bronze -> Silver funnel: how much raw collapses into consolidated persons.
    if bronze.get("count") and silver.get("count"):
        ratio = bronze["count"] / max(silver["count"], 1)
        st.info(
            f"Cada persona consolidada proviene de ~{ratio:.1f} mensajes crudos "
            f"({_fmt(bronze['count'])} Bronze → {_fmt(silver['count'])} Silver)."
        )

    if not gold.get("refreshed"):
        st.warning(
            "La capa Gold aún no se ha refrescado. Se actualiza con el DAG "
            "`hr_etl_gold_eventdriven` de Airflow o ejecutando "
            "`python -m hr_etl.warehouse.gold_layer`."
        )


# --------------------------------------------------------------------------- #
# Tab 1: Personas (search + filters + detail) — Gold layer only
# --------------------------------------------------------------------------- #
with tab_persons:
    st.caption(
        "Solo se muestran personas de la capa **Gold**: completitud >= 80% "
        "(los 5 campos clave presentes) y nombre único en toda la base. "
        "Las personas incompletas o con nombre ambiguo se revisan en la pestaña "
        "🔗 Duplicados."
    )
    # Keyset pagination state: a stack of cursors, one per page visited. cursors[i] is the
    # `after_id` used to fetch page i+1 (cursors[0] is None -> the first page starts from
    # the beginning). Next pushes the page's next_cursor; Prev pops. This rides the id
    # index, so deep pages are as fast as the first — unlike OFFSET, which scans+discards
    # every earlier row (~10s deep into a 200k-row table).
    if "persons_cursors" not in st.session_state:
        st.session_state.persons_cursors = [None]

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

    # Any change in the search/filters resets pagination to the first page.
    filter_signature = (q, city, company, job, page_size)
    if st.session_state.get("persons_filter_sig") != filter_signature:
        st.session_state.persons_filter_sig = filter_signature
        st.session_state.persons_cursors = [None]

    cursors = st.session_state.persons_cursors
    page_number = len(cursors)  # 1-based, for display only
    after_id = cursors[-1]

    params: dict[str, object] = {"limit": int(page_size)}
    if after_id is not None:
        params["after_id"] = int(after_id)
    # Ask for the exact total only on the first page (a COUNT). Deeper pages rely on
    # has_more, so paging never pays for the COUNT again.
    if page_number == 1:
        params["with_total"] = True
    if q:
        params["q"] = q
    if city:
        params["city"] = city
    if company:
        params["company"] = company
    if job:
        params["job"] = job

    data = api_get("/gold/persons", params) or {
        "total": 0,
        "count": 0,
        "items": [],
        "has_more": False,
        "next_cursor": None,
    }
    items = data.get("items", [])
    has_more = data.get("has_more", False)
    next_cursor = data.get("next_cursor")
    # total is only present on page 1; cache it so later pages can still show it.
    if data.get("total") is not None:
        st.session_state.persons_total = data["total"]
    total = st.session_state.get("persons_total")

    st.subheader("Personas")
    total_txt = f"{total} resultado(s) · " if total is not None else ""
    st.caption(f"{total_txt}{int(page_size)} por página")

    if items:
        df = pd.DataFrame(items)
        preferred = [
            "id",
            "full_name",
            "city",
            "company",
            "job",
            "email",
            "phone",
            "iban",
            "salary",
        ]
        cols = [c for c in preferred if c in df.columns] + [
            c for c in df.columns if c not in preferred
        ]
        st.dataframe(df[cols], use_container_width=True, hide_index=True)

        # --- Pagination controls (Prev / page indicator / Next) ---
        prev_col, ind_col, next_col = st.columns([1, 2, 1])
        with prev_col:
            if st.button("◀ Anterior", disabled=(page_number <= 1), use_container_width=True):
                st.session_state.persons_cursors = cursors[:-1] or [None]
                st.rerun()
        with ind_col:
            st.markdown(
                f"<div style='text-align:center;padding-top:6px'>"
                f"Página <b>{page_number}</b></div>",
                unsafe_allow_html=True,
            )
        with next_col:
            if st.button("Siguiente ▶", disabled=(not has_more), use_container_width=True):
                st.session_state.persons_cursors = cursors + [next_cursor]
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
            "No hay personas Gold que coincidan con la búsqueda. "
            "Si el pipeline aún no ha procesado datos, o la capa Gold no se ha "
            "refrescado todavía (`python -m hr_etl.warehouse.gold_layer`), la tabla "
            "estará vacía."
        )


# --------------------------------------------------------------------------- #
# Tab 2: Duplicados (groups from fuzzy batch reconciliation)
# --------------------------------------------------------------------------- #
def _member_label(m: dict) -> str:
    """Compact one-line label for a group member."""
    bits = [f"#{m.get('person_id')}", m.get("full_name") or "(sin nombre)"]
    if m.get("city"):
        bits.append(m["city"])
    if m.get("company"):
        bits.append(m["company"])
    return " · ".join(str(b) for b in bits)


with tab_dupes:
    st.subheader("Posibles duplicados")
    st.caption(
        "Grupos de personas con nombres parecidos (similitud difusa) que podrían ser "
        "la misma. Los genera la reconciliación por lotes y se guardan en "
        "`duplicate_groups`."
    )

    min_conf = st.slider(
        "Confianza mínima",
        min_value=0.0,
        max_value=1.0,
        value=0.5,
        step=0.05,
        help="Solo muestra grupos cuya similitud de nombre supere este umbral.",
    )
    max_groups = st.number_input("Máx. grupos", min_value=1, max_value=500, value=100, step=25)

    data = api_get("/groups", {"limit": int(max_groups), "min_confidence": float(min_conf)}) or {
        "total_groups": 0,
        "count": 0,
        "groups": [],
    }
    total_groups = data.get("total_groups", 0)
    groups = data.get("groups", [])

    st.caption(
        f"{total_groups} grupo(s) por encima de {min_conf:.2f} · " f"mostrando {len(groups)}"
    )

    if not groups:
        st.info(
            "No hay grupos de duplicados todavía. Ejecuta la reconciliación "
            "(DAG `hr_etl_reconciliation` en Airflow, o "
            "`python -m hr_etl.processing.reconcile`) y vuelve a mirar."
        )
    else:
        # Summary table: one row per group.
        summary = [
            {
                "grupo": g["group_id"],
                "miembros": len(g["members"]),
                "confianza": g["confidence"],
                "motivo": g.get("reason", ""),
            }
            for g in groups
        ]
        st.dataframe(
            pd.DataFrame(summary),
            use_container_width=True,
            hide_index=True,
            column_config={
                "confianza": st.column_config.ProgressColumn(
                    "confianza", min_value=0.0, max_value=1.0, format="%.2f"
                )
            },
        )

        # --- Inspect a selected group: list its members side by side ---
        st.subheader("Inspeccionar grupo")
        labels = {
            f"Grupo #{g['group_id']} · {len(g['members'])} personas " f"({g['confidence']:.2f})": g
            for g in groups
        }
        choice = st.selectbox("Selecciona un grupo", list(labels.keys()))
        if choice:
            group = labels[choice]
            st.markdown(
                f"**Confianza:** {group['confidence']:.2f}  ·  "
                f"**Motivo:** {group.get('reason', '—')}  ·  "
                f"**{len(group['members'])} personas**"
            )
            st.caption(
                "Tres formas de resolver un caso, todas persistentes (sobreviven al "
                "reprocesado y al rebuild de la reconciliación):\n\n"
                "- **Consolidar (exactamente 2):** marca las dos filas que sean la "
                "MISMA persona; se fusionan en una (sobrevive el id más bajo) y se "
                "guarda la traza del merge.\n"
                "- **✅ Aprobar como canónica:** esta fila es la buena y ya está "
                "completa; se promociona a Gold aunque el nombre se repita.\n"
                "- **🔀 Marcar como distinta:** es otra persona real con el mismo "
                "nombre (homónimo); sale de la cola y deja de bloquear a sus pares."
            )

            # --- Detail + per-member actions, one column per member ---
            st.markdown("**Detalle de cada persona**")
            gid = group["group_id"]
            members = group["members"]
            cols = st.columns(min(len(members), 3))

            # Enforce a STRICT cap of 2 selected checkboxes for consolidation. We read the
            # current selection from session_state and disable the unchecked boxes once two
            # are already ticked, so a third can never be selected.
            sel_key = lambda pid: f"dupe_sel_{gid}_{pid}"  # noqa: E731
            currently_selected = [
                m["person_id"] for m in members if st.session_state.get(sel_key(m["person_id"]))
            ]
            cap_reached = len(currently_selected) >= 2

            selected_ids: list[int] = []
            for i, m in enumerate(members):
                pid = m["person_id"]
                with cols[i % len(cols)]:
                    is_selected = st.session_state.get(sel_key(pid), False)
                    checked = st.checkbox(
                        _member_label(m),
                        key=sel_key(pid),
                        # disable the box only if the cap is hit AND this box is unticked
                        disabled=cap_reached and not is_selected,
                    )
                    if checked:
                        selected_ids.append(pid)
                    detail = api_get(f"/persons/{pid}")
                    if detail:
                        st.json(detail)
                    # Per-member review actions (single person each).
                    a_col, d_col = st.columns(2)
                    with a_col:
                        if st.button(
                            "✅ Aprobar",
                            key=f"approve_{gid}_{pid}",
                            help="Marcar como canónica y promover a Gold",
                        ):
                            res = api_post("/review/approve", {"person_id": pid})
                            if res is not None:
                                st.success(
                                    f"#{pid} aprobada como canónica. Entrará a Gold en el "
                                    "próximo refresh."
                                )
                                st.rerun()
                    with d_col:
                        if st.button(
                            "🔀 Distinta",
                            key=f"distinct_{gid}_{pid}",
                            help="Es otra persona con el mismo nombre (homónimo)",
                        ):
                            res = api_post("/review/distinct", {"person_id": pid})
                            if res is not None:
                                st.success(
                                    f"#{pid} marcada como persona distinta. Sale de la cola "
                                    "de duplicados."
                                )
                                st.rerun()

            st.divider()
            n_selected = len(selected_ids)
            merge_col, info_col = st.columns([1, 3])
            with merge_col:
                disabled = n_selected != 2
                if st.button(
                    f"🔗 Consolidar seleccionadas ({n_selected}/2)",
                    disabled=disabled,
                    type="primary",
                    key=f"merge_btn_{gid}",
                ):
                    result = api_post("/consolidate", {"person_ids": selected_ids})
                    if result is not None:
                        st.success(
                            f"Consolidado: {result['merged']} fila(s) fusionada(s) en "
                            f"una sola persona (ids {result['person_ids']}). La "
                            "reconciliación y Gold se actualizarán en el próximo ciclo "
                            "del DAG de mantenimiento."
                        )
                        st.rerun()
            with info_col:
                if n_selected < 2:
                    st.caption("Selecciona exactamente 2 personas para consolidar.")
                elif cap_reached:
                    st.caption("Máximo 2 seleccionadas. Desmarca una para elegir otra.")
