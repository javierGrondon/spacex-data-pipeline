"""
Dashboard de monitoreo del SpaceX Data Pipeline.

Lee dos bases:
  - AIRFLOW_DB_URL: base de metadatos de Airflow (corridas y tareas)
  - WAREHOUSE_URL:  warehouse con los datos cargados y los controles de calidad
"""
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, inspect

AIRFLOW_DB_URL = os.getenv("AIRFLOW_DB_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow")
WAREHOUSE_URL = os.getenv("WAREHOUSE_URL", "postgresql+psycopg2://airflow:airflow@postgres:5432/warehouse")
DAG_ID = os.getenv("DAG_ID", "spacex_pipeline")

# Paleta: estados = colores de status (siempre con texto), magnitud = un solo azul
GOOD, CRITICAL, WARNING, NEUTRAL, BLUE = "#0ca30c", "#d03b3b", "#fab219", "#8a8984", "#2a78d6"
STATE_COLORS = {"success": GOOD, "failed": CRITICAL, "running": BLUE, "up_for_retry": WARNING,
                "upstream_failed": NEUTRAL, "queued": NEUTRAL}
LAUNCH_COLORS = {"Éxito": GOOD, "Falla": CRITICAL, "Próximo": BLUE, "Sin dato": NEUTRAL}

st.set_page_config(page_title="SpaceX Pipeline · Monitoreo", page_icon="🚀", layout="wide")


@st.cache_resource
def engines():
    return create_engine(AIRFLOW_DB_URL), create_engine(WAREHOUSE_URL)


@st.cache_data(ttl=60)
def q(which: str, sql: str, **params) -> pd.DataFrame:
    eng = engines()[0 if which == "af" else 1]
    try:
        return pd.read_sql(sql, eng, params=params or None)
    except Exception as exc:  # noqa: BLE001
        st.session_state.setdefault("errors", []).append(f"{which}: {exc}")
        return pd.DataFrame()


def has_table(which, name):
    try:
        return inspect(engines()[0 if which == "af" else 1]).has_table(name)
    except Exception:  # noqa: BLE001
        return False


def style(fig, height=320):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=80, b=10), title_y=0.97,
                      plot_bgcolor="#fcfcfb", paper_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#52514e"), legend_title_text="",
                      legend=dict(orientation="h", y=1.02, yanchor="bottom", x=0),
                      hoverlabel=dict(bgcolor="white"))
    fig.update_xaxes(showgrid=False, linecolor="#d4d3cf")
    fig.update_yaxes(gridcolor="#ecebe8", zeroline=False)
    fig.update_traces(marker_line_color="#fcfcfb", marker_line_width=2, selector=dict(type="bar"))
    return fig


# ---------------------------------------------------------------- datos
runs = q("af", """
    SELECT run_id, state, execution_date AS logical_date, start_date, end_date, run_type
    FROM dag_run WHERE dag_id = :d ORDER BY start_date DESC LIMIT 60
""", d=DAG_ID)
tasks = q("af", """
    SELECT task_id, run_id, state, duration, try_number, start_date, end_date
    FROM task_instance WHERE dag_id = :d
""", d=DAG_ID)
for df in (runs, tasks):
    for c in ("start_date", "end_date"):
        if c in df:
            df[c] = pd.to_datetime(df[c], utc=True, errors="coerce")
if not runs.empty:
    # Duración real = desde que arrancó la primera tarea hasta que terminó la última
    if not tasks.empty:
        span = tasks.groupby("run_id").agg(t0=("start_date", "min"), t1=("end_date", "max"))
        runs = runs.merge(span, left_on="run_id", right_index=True, how="left")
        runs["start_date"] = runs["t0"].fillna(runs["start_date"])
        runs["end_date"] = runs["t1"].fillna(runs["end_date"])
    runs["duracion_s"] = (runs["end_date"] - runs["start_date"]).dt.total_seconds()
    runs = runs.sort_values("start_date", ascending=False)

quality = q("wh", "SELECT * FROM quality_results") if has_table("wh", "quality_results") else pd.DataFrame()
launches = q("wh", "SELECT * FROM launches") if has_table("wh", "launches") else pd.DataFrame()
rockets = q("wh", "SELECT * FROM rockets") if has_table("wh", "rockets") else pd.DataFrame()

# ---------------------------------------------------------------- header
st.title("🚀 SpaceX Data Pipeline — Monitoreo")
st.caption(f"DAG `{DAG_ID}` · Airflow + Postgres · se actualiza cada 60 s")

if runs.empty:
    st.info("Todavía no hay corridas. Activá el DAG en Airflow (http://localhost:8080) y lanzalo con ▶.")
    st.stop()

last = runs.iloc[0]
done = runs[runs["state"].isin(["success", "failed"])]
ok_rate = (done["state"] == "success").mean() * 100 if len(done) else 0
last_q = quality[quality["run_date"] == quality["run_date"].max()] if not quality.empty else pd.DataFrame()
icon = {"success": "✅", "failed": "❌", "running": "⏳"}.get(last["state"], "•")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Última corrida", f"{icon} {last['state']}",
          last["start_date"].strftime("%d/%m %H:%M UTC") if pd.notna(last["start_date"]) else "")
c2.metric("Tasa de éxito", f"{ok_rate:.0f}%", f"{len(done)} corridas terminadas", delta_color="off")
c3.metric("Duración promedio", f"{runs['duracion_s'].mean():.0f} s" if runs["duracion_s"].notna().any() else "-")
c4.metric("Controles de calidad", f"{int(last_q['passed'].sum())}/{len(last_q)} OK" if len(last_q) else "-",
          f"corrida {last_q['run_date'].iloc[0]}" if len(last_q) else "", delta_color="off")

tab1, tab2, tab3 = st.tabs(["🩺 Salud del pipeline", "🧪 Calidad de datos", "🛰️ Lanzamientos"])

# ---------------------------------------------------------------- salud
with tab1:
    left, right = st.columns([3, 2])
    with left:
        r = runs.dropna(subset=["start_date"]).sort_values("logical_date").tail(30)
        r["corrida"] = pd.to_datetime(r["logical_date"], utc=True).dt.strftime("%d/%m")
        fig = px.bar(r, x="corrida", y="duracion_s", color="state", color_discrete_map=STATE_COLORS,
                     title="Duración de cada corrida (s)", labels={"corrida": "", "duracion_s": "segundos", "state": "Estado"},
                     hover_data={"run_id": True, "duracion_s": ":.1f"},
                     category_orders={"corrida": list(r["corrida"])})
        fig.update_xaxes(type="category")
        st.plotly_chart(style(fig), use_container_width=True)
    with right:
        t = tasks.dropna(subset=["duration"]).groupby("task_id", as_index=False)["duration"].mean()
        fig = px.bar(t.sort_values("duration"), x="duration", y="task_id", orientation="h",
                     title="Duración promedio por tarea (s)", labels={"duration": "segundos", "task_id": ""},
                     color_discrete_sequence=[BLUE])
        fig.update_traces(hovertemplate="%{y}: %{x:.1f} s<extra></extra>")
        st.plotly_chart(style(fig), use_container_width=True)

    st.subheader("Tareas con fallas o reintentos")
    issues = tasks[(tasks["state"].isin(["failed", "up_for_retry", "upstream_failed"])) | (tasks["try_number"] > 1)]
    if issues.empty:
        st.success("Sin fallas ni reintentos registrados 🎉")
    else:
        show = issues.sort_values("start_date", ascending=False)[["start_date", "run_id", "task_id", "state", "try_number", "duration"]]
        show["state"] = show["state"].map(lambda s: {"failed": "❌ failed", "up_for_retry": "🔁 retry",
                                                     "upstream_failed": "⛔ upstream_failed"}.get(s, f"✅ {s}"))
        st.dataframe(show, hide_index=True, use_container_width=True)

# ---------------------------------------------------------------- calidad
with tab2:
    if quality.empty:
        st.info("Todavía no hay resultados de calidad.")
    else:
        left, right = st.columns([2, 3])
        with left:
            st.markdown(f"**Última corrida: {last_q['run_date'].iloc[0]}**")
            tbl = last_q[["check_name", "passed", "detail"]].copy()
            tbl["passed"] = tbl["passed"].map(lambda p: "✅ OK" if p else "❌ Falla")
            st.dataframe(tbl.rename(columns={"check_name": "Control", "passed": "Resultado", "detail": "Detalle"}),
                         hide_index=True, use_container_width=True)
        with right:
            h = quality.assign(passed=quality["passed"].astype(bool)).groupby("run_date", as_index=False).agg(
                ok=("passed", "sum"), total=("passed", "count"))
            h["fallas"] = h["total"] - h["ok"]
            hl = h.melt(id_vars="run_date", value_vars=["ok", "fallas"], var_name="r", value_name="n")
            hl["r"] = hl["r"].map({"ok": "OK", "fallas": "Falla"})
            fig = px.bar(hl, x="run_date", y="n", color="r", color_discrete_map={"OK": GOOD, "Falla": CRITICAL},
                         title="Controles por corrida", labels={"run_date": "", "n": "controles", "r": ""})
            fig.update_xaxes(type="category")
            st.plotly_chart(style(fig), use_container_width=True)

# ---------------------------------------------------------------- negocio
with tab3:
    if launches.empty:
        st.info("Todavía no hay datos cargados.")
    else:
        lab = {"success": "Éxito", "failure": "Falla", "upcoming": "Próximo", "unknown": "Sin dato"}
        launches["Estado"] = launches["status"].map(lab)
        past = launches[launches["status"] != "upcoming"]
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Lanzamientos", len(launches))
        k2.metric("Tasa de éxito histórica", f"{(past['status'] == 'success').mean() * 100:.1f}%")
        k3.metric("Con booster reutilizado", f"{launches['reused_core'].astype(bool).mean() * 100:.0f}%")
        k4.metric("Próximos", int((launches["status"] == "upcoming").sum()))

        left, right = st.columns([3, 2])
        with left:
            y = launches.groupby(["year", "Estado"], as_index=False).size()
            fig = px.bar(y, x="year", y="size", color="Estado", color_discrete_map=LAUNCH_COLORS,
                         title="Lanzamientos por año", labels={"year": "", "size": "lanzamientos"},
                         category_orders={"Estado": list(LAUNCH_COLORS)})
            st.plotly_chart(style(fig, 360), use_container_width=True)
        with right:
            by = past.groupby("rocket_name", as_index=False).agg(
                lanzamientos=("launch_id", "count"), exito=("success", lambda s: s.astype(float).mean() * 100))
            fig = px.bar(by.sort_values("lanzamientos"), x="lanzamientos", y="rocket_name", orientation="h",
                         title="Lanzamientos por cohete", labels={"rocket_name": "", "lanzamientos": ""},
                         text=by.sort_values("lanzamientos")["exito"].map(lambda v: f"{v:.0f}% éxito"),
                         color_discrete_sequence=[BLUE])
            fig.update_traces(textposition="outside", cliponaxis=False)
            fig.update_xaxes(range=[0, by["lanzamientos"].max() * 1.3])
            st.plotly_chart(style(fig, 360), use_container_width=True)

        with st.expander("Ver tabla de lanzamientos"):
            st.dataframe(launches[["flight_number", "name", "date_utc", "rocket_name", "Estado", "reused_core"]]
                         .sort_values("flight_number", ascending=False), hide_index=True, use_container_width=True)

if st.session_state.get("errors"):
    with st.expander("⚠️ Errores de conexión"):
        st.write(st.session_state["errors"])
