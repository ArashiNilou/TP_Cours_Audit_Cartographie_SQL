"""Dashboard interactif Plotly Dash alimenté par la base PostgreSQL du projet.

Les données sont chargées en mémoire puis rafraîchies en arrière-plan toutes
les DATAVIZ_REFRESH_SECONDS secondes. Les filtres travaillent uniquement sur
ce cache : ils répondent sans relancer de requête SQL.
"""

import json
import math
import operator
import os
import re
import threading
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg
from dash import Dash, Input, Output, State, ctx, dash_table, dcc, html, no_update

REFRESH_SECONDS = int(os.getenv("DATAVIZ_REFRESH_SECONDS", "60"))
DATA_LAKE_ROOT = Path(os.getenv("DATA_LAKE_ROOT", "/data-lake"))
FRANCE_TRAVAIL_OFFER_URL = "https://candidat.francetravail.fr/offres/recherche/detail/{}"
TOP_COMPETENCES = 15
TABLE_PAGE_SIZE = 10
TABLE_COLUMNS = [
    {"name": "Offre", "id": "source_offre_id"},
    {"name": "Poste", "id": "libelle_poste"},
    {"name": "Contrat", "id": "type_contrat"},
    {"name": "Commune", "id": "nom_commune"},
    {"name": "Métier ROME", "id": "libelle_fiche_metier"},
    {"name": "Salaire (€ / an)", "id": "salaire", "type": "numeric"},
    {"name": "Publication", "id": "publication"},
]

OFFRES_SQL = """
SELECT
    o.offre_id,
    o.source_offre_id,
    o.libelle_poste,
    o.date_publication,
    o.type_contrat,
    o.salaire_brut_annuel_estime::float AS salaire,
    c.code_insee,
    c.nom_commune,
    c.latitude::float  AS latitude,
    c.longitude::float AS longitude,
    mr.rome_code,
    mr.libelle_fiche_metier,
    mr.domaine_professionnel,
    e.raison_sociale,
    e.entreprise_anonyme
FROM offre o
JOIN commune c      ON c.code_insee = o.code_insee
JOIN metier_rome mr ON mr.rome_code = o.rome_code
JOIN entreprise e   ON e.entreprise_id = o.entreprise_id
"""

COMPETENCES_SQL = """
SELECT
    eo.offre_id,
    cp.libelle_competence,
    cp.type_competence,
    CASE eo.statut_exigence WHEN 'E' THEN 'Exigée' ELSE 'Souhaitée' END AS exigence
FROM exigence_offre eo
JOIN competence cp ON cp.competence_id = eo.competence_id
"""

RUNS_SQL = """
SELECT started_at, raw_count, clean_count, rejected_count
FROM tp2_pipeline_run
WHERE status IN ('success', 'no_input')
ORDER BY started_at
"""


def _connect() -> psycopg.Connection:
    params = {
        "host": os.getenv("PGHOST", "postgres"),
        "port": os.getenv("PGPORT", "5432"),
        "dbname": os.getenv("PGDATABASE", "emploi"),
        "user": os.getenv("PGUSER", "postgres"),
        "connect_timeout": 5,
    }
    password = os.getenv("PGPASSWORD")
    if password:
        params["password"] = password
    return psycopg.connect(**params)


def _query(connection: psycopg.Connection, sql: str) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        columns = [column.name for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None]:
    try:
        with _connect() as connection:
            offres = _query(connection, OFFRES_SQL)
            for column in ("salaire", "latitude", "longitude"):
                offres[column] = pd.to_numeric(offres[column], errors="coerce")
            offres["entreprise_anonyme"] = offres["entreprise_anonyme"].fillna(False).astype(bool)
            offres["date_publication"] = pd.to_datetime(offres["date_publication"], errors="coerce")
            offres["publication"] = offres["date_publication"].dt.strftime("%Y-%m-%d")
            competences = _query(connection, COMPETENCES_SQL)
            try:
                runs = _query(connection, RUNS_SQL)
            except psycopg.errors.UndefinedTable:
                connection.rollback()
                runs = pd.DataFrame(columns=["started_at", "raw_count", "clean_count", "rejected_count"])
        return offres, competences, runs, None
    except psycopg.Error as error:
        empty = pd.DataFrame()
        return empty, empty, empty, f"Base PostgreSQL injoignable : {error}"


_cache_lock = threading.Lock()
_cache: dict = {"data": None}


def _reload_cache() -> None:
    offres, competences, runs, error = load_data()
    with _cache_lock:
        previous = _cache["data"]
        if error and previous is not None:
            # Base momentanément indisponible : on garde les dernières données valides.
            offres, competences, runs = previous[:3]
        _cache["data"] = (offres, competences, runs, error)


def _refresh_loop() -> None:
    while True:
        time.sleep(REFRESH_SECONDS)
        _reload_cache()
        _index_raw_offers()


_SOURCE_ID_PATTERN = re.compile(r'"source_offer_id"\s*:\s*"([^"]+)"')
_raw_index_lock = threading.Lock()
_raw_scan_lock = threading.Lock()
_raw_index: dict[str, Path] = {}
_raw_seen: set[Path] = set()


def _index_raw_offers() -> None:
    """Associe chaque identifiant d'offre à son fichier brut du Data Lake.

    Les fichiers bruts sont écrits avec des clés triées : `source_offer_id`
    se trouve en fin de fichier, on ne lit donc que les derniers octets.
    Seuls les nouveaux fichiers sont lus à chaque passage.
    """
    raw_dir = DATA_LAKE_ROOT / "raw" / "france_travail"
    if not raw_dir.is_dir() or not _raw_scan_lock.acquire(blocking=False):
        return
    try:
        _scan_raw_dir(raw_dir)
    finally:
        _raw_scan_lock.release()


def _scan_raw_dir(raw_dir: Path) -> None:
    found: dict[str, Path] = {}
    for path in sorted(raw_dir.glob("ingestion_date=*/*.json")):
        if path in _raw_seen:
            continue
        try:
            with path.open("rb") as handle:
                handle.seek(max(0, path.stat().st_size - 300))
                match = _SOURCE_ID_PATTERN.search(handle.read().decode("utf-8", "ignore"))
        except OSError:
            continue
        _raw_seen.add(path)
        if match:
            found[match.group(1)] = path
    with _raw_index_lock:
        _raw_index.update(found)


def load_raw_offer(source_offre_id: str) -> dict:
    with _raw_index_lock:
        path = _raw_index.get(source_offre_id)
    if path is None:
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("payload") or {}
    except (OSError, ValueError):
        return {}


def load_offer_text(source_offre_id: str) -> tuple[str | None, str | None]:
    """Description et durée de travail, lues à la demande pour ne pas alourdir le cache."""
    try:
        with _connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT description, duree_travail FROM offre WHERE source_offre_id = %s", (source_offre_id,)
            )
            row = cursor.fetchone()
    except psycopg.Error:
        return None, None
    return (row[0], row[1]) if row else (None, None)


def get_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str | None]:
    """Renvoie les données en mémoire ; le premier appel lance le rafraîchissement."""
    with _cache_lock:
        if _cache["data"] is None:
            _cache["data"] = load_data()
            threading.Thread(target=_index_raw_offers, daemon=True).start()
            threading.Thread(target=_refresh_loop, daemon=True).start()
        return _cache["data"]


def empty_figure(message: str) -> go.Figure:
    figure = go.Figure()
    figure.add_annotation(text=message, showarrow=False, font={"size": 14, "color": "#6b7280"})
    figure.update_layout(xaxis={"visible": False}, yaxis={"visible": False}, template="plotly_white")
    return figure


def kpi_card(label: str, value: str) -> html.Div:
    return html.Div(
        [html.Div(value, className="kpi-value"), html.Div(label, className="kpi-label")],
        className="kpi-card",
    )


app = Dash(
    __name__,
    title="Plateforme Emploi - Data Viz",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
server = app.server


def _graph_card(graph_id: str, title: str) -> html.Div:
    return html.Div(
        [html.H2(title, className="card-title"), dcc.Loading(
            dcc.Graph(
                id=graph_id,
                className="graph",
                config={"responsive": True, "displaylogo": False, "displayModeBar": False},
            ),
            type="circle",
            delay_show=400,
        )],
        className="card",
    )


@server.route("/health")
def health() -> tuple[str, int]:
    return "ok", 200


app.index_string = """<!DOCTYPE html>
<html>
<head>
{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
  * { box-sizing: border-box; }
  body { font-family: Segoe UI, Arial, sans-serif; background: #f3f4f6; margin: 0; overflow-x: hidden; }
  .header { background: #1e3a8a; color: white; padding: 18px 28px; }
  .header h1 { margin: 0; font-size: clamp(18px, 2.4vw, 24px); }
  .header p { margin: 4px 0 0; opacity: .85; font-size: clamp(12px, 1.4vw, 15px); }
  .container { padding: 18px 28px; max-width: 1800px; margin: 0 auto; }
  .filters { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px;
             background: white; padding: 16px; border-radius: 10px; margin-bottom: 16px; }
  .kpis { display: flex; flex-wrap: wrap; gap: 16px; margin-bottom: 16px; }
  .kpi-card { background: white; border-radius: 10px; padding: 14px; text-align: center; min-width: 0;
              flex: 1 1 150px; }
  .kpi-value { font-size: clamp(20px, 2.2vw, 26px); font-weight: 700; color: #1e3a8a; }
  .kpi-label { color: #6b7280; font-size: 13px; }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(100%, 560px), 1fr)); gap: 16px;
          margin-bottom: 16px; }
  .card { background: white; border-radius: 10px; padding: 8px; min-width: 0; }
  .graph { height: 420px; }
  .card-title { font-size: 16px; font-weight: 600; color: #1f2937; margin: 6px 8px 0; line-height: 1.3; }
  .error { background: #fee2e2; color: #991b1b; padding: 10px; border-radius: 8px; margin-bottom: 12px; }
  label { font-weight: 600; font-size: 13px; }
  .table-hint { color: #6b7280; font-size: 13px; margin: 4px 6px 8px; }
  .modal-overlay { position: fixed; inset: 0; background: rgba(17, 24, 39, .55); z-index: 1000;
                   align-items: center; justify-content: center; padding: 24px; }
  .modal-box { background: white; border-radius: 12px; width: min(900px, 100%); max-height: 90vh;
               overflow-y: auto; padding: 24px 28px; position: relative; box-shadow: 0 20px 50px rgba(0,0,0,.3); }
  .modal-close { position: absolute; top: 14px; right: 16px; border: none; background: #e5e7eb;
                 border-radius: 50%; width: 34px; height: 34px; font-size: 18px; cursor: pointer; }
  .modal-close:hover { background: #d1d5db; }
  .modal-box h2 { margin: 0 40px 4px 0; color: #1e3a8a; font-size: 22px; }
  .modal-sub { color: #4b5563; margin-bottom: 14px; }
  .modal-box h3 { color: #1e3a8a; font-size: 15px; margin: 18px 0 8px; border-bottom: 1px solid #e5e7eb;
                  padding-bottom: 4px; }
  .detail-grid { display: grid; grid-template-columns: 220px 1fr; gap: 6px 14px; font-size: 14px; }
  .detail-grid dt { color: #6b7280; }
  .detail-grid dd { margin: 0; color: #111827; }
  .description { white-space: pre-wrap; font-size: 14px; line-height: 1.5; color: #111827; }
  .tag { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 13px; margin: 0 6px 6px 0; }
  .tag-E { background: #fee2e2; color: #991b1b; }
  .tag-S { background: #dbeafe; color: #1e40af; }
  .links { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 6px; }
  .btn-link { display: inline-block; padding: 9px 16px; border-radius: 8px; text-decoration: none;
              font-weight: 600; font-size: 14px; }
  .btn-primary { background: #1e3a8a; color: white; }
  .btn-secondary { background: #16a34a; color: white; }
  .no-link { color: #6b7280; font-size: 14px; font-style: italic; align-self: center; }
  @media (max-width: 768px) {
    .header { padding: 14px 16px; }
    .container { padding: 12px; }
    .filters, .kpis, .grid { gap: 10px; margin-bottom: 10px; }
    .filters { padding: 12px; }
    .kpi-card { padding: 10px 6px; flex-basis: 40%; }
    .card { padding: 4px; }
    .graph { height: 360px; }
    .card-title { font-size: 15px; }
    .modal-overlay { padding: 0; align-items: stretch; }
    .modal-box { width: 100%; max-height: 100vh; height: 100%; border-radius: 0; padding: 18px 16px 28px; }
    .modal-box h2 { font-size: 19px; }
    .detail-grid { grid-template-columns: 1fr; gap: 0; }
    .detail-grid dt { font-size: 12px; margin-top: 8px; }
    .btn-link { width: 100%; text-align: center; }
  }
</style>
</head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>"""

app.layout = html.Div(
    [
        html.Div(
            [
                html.H1("Marché de l'emploi - compétences numériques"),
                html.P(
                    "Données France Travail enrichies avec l'API Géo, nettoyées par PySpark "
                    f"et lues dans PostgreSQL (mise à jour toutes les {REFRESH_SECONDS} s)."
                ),
            ],
            className="header",
        ),
        html.Div(
            [
                html.Div(id="error-banner"),
                html.Div(
                    [
                        html.Div([html.Label("Type de contrat"), dcc.Dropdown(id="filtre-contrat", multi=True, placeholder="Tous")]),
                        html.Div([html.Label("Domaine professionnel"), dcc.Dropdown(id="filtre-domaine", multi=True, placeholder="Tous")]),
                        html.Div([html.Label("Commune"), dcc.Dropdown(id="filtre-commune", multi=True, placeholder="Toutes")]),
                    ],
                    className="filters",
                ),
                html.Div(id="kpis", className="kpis"),
                html.Div(
                    [
                        _graph_card("graph-contrats", "Offres par type de contrat"),
                        _graph_card("graph-competences", f"Top {TOP_COMPETENCES} des compétences demandées"),
                        _graph_card("graph-carte", "Carte des offres par commune"),
                        _graph_card("graph-salaires", "Salaire brut annuel estimé par contrat"),
                        _graph_card("graph-publication", "Offres par date de publication"),
                        _graph_card("graph-pipeline", "Pipeline : offres brutes vs propres par passage Spark"),
                    ],
                    className="grid",
                ),
                html.Div(
                    [
                        html.Div("Cliquez sur une offre pour afficher toute sa fiche et son lien.", className="table-hint"),
                        dcc.Loading(
                            dash_table.DataTable(
                                id="table-offres",
                                columns=TABLE_COLUMNS,
                                page_current=0,
                                page_size=TABLE_PAGE_SIZE,
                                page_action="custom",
                                sort_action="custom",
                                sort_mode="single",
                                sort_by=[],
                                filter_action="custom",
                                filter_query="",
                                filter_options={"case": "insensitive"},
                                style_table={"overflowX": "auto", "minWidth": "100%"},
                                style_cell={"fontFamily": "Segoe UI, Arial", "fontSize": 13, "textAlign": "left",
                                            "cursor": "pointer", "whiteSpace": "normal", "height": "auto",
                                            "minWidth": "90px", "maxWidth": "320px", "padding": "6px 8px"},
                                style_cell_conditional=[
                                    {"if": {"column_id": "libelle_poste"}, "minWidth": "200px"},
                                    {"if": {"column_id": "libelle_fiche_metier"}, "minWidth": "180px"},
                                ],
                                style_header={"fontWeight": "700", "backgroundColor": "#e5e7eb"},
                                style_data_conditional=[
                                    {"if": {"column_id": "libelle_poste"}, "color": "#1d4ed8",
                                     "textDecoration": "underline", "fontWeight": "600"},
                                    {"if": {"state": "active"}, "backgroundColor": "#eff6ff", "border": "1px solid #93c5fd"},
                                ],
                            ),
                            type="circle",
                            delay_show=400,
                        ),
                    ],
                    className="card",
                ),
                html.Div(
                    html.Div(
                        [
                            html.Button("✕", id="modal-fermer", className="modal-close", title="Fermer"),
                            html.Div(id="modal-contenu"),
                        ],
                        className="modal-box",
                    ),
                    id="offre-modal",
                    className="modal-overlay",
                    style={"display": "none"},
                ),
                dcc.Interval(id="refresh", interval=REFRESH_SECONDS * 1000),
            ],
            className="container",
        ),
    ]
)


def _options(series: pd.Series) -> list[dict[str, str]]:
    values = sorted(value for value in series.dropna().unique())
    return [{"label": value, "value": value} for value in values]


def _apply_filters(offres: pd.DataFrame, contrats, domaines, communes) -> pd.DataFrame:
    if contrats:
        offres = offres[offres["type_contrat"].isin(contrats)]
    if domaines:
        offres = offres[offres["domaine_professionnel"].isin(domaines)]
    if communes:
        offres = offres[offres["nom_commune"].isin(communes)]
    return offres


def _figure_layout(figure: go.Figure) -> go.Figure:
    # Titre en HTML et légende horizontale au-dessus du tracé : le tracé garde toute la largeur, même sur mobile.
    figure.update_layout(
        template="plotly_white",
        autosize=True,
        title=None,
        margin={"l": 10, "r": 10, "t": 30, "b": 10},
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.0, "x": 0, "title": {"text": ""}},
    )
    figure.update_xaxes(automargin=True)
    figure.update_yaxes(automargin=True)
    return figure


def _short_label(label: str, limit: int = 32) -> str:
    return label if len(label) <= limit else label[: limit - 1].rstrip() + "…"


@app.callback(
    Output("error-banner", "children"),
    Output("filtre-contrat", "options"),
    Output("filtre-domaine", "options"),
    Output("filtre-commune", "options"),
    Output("graph-pipeline", "figure"),
    Input("refresh", "n_intervals"),
)
def update_context(_n_intervals):
    """Éléments indépendants des filtres : bandeau d'erreur, listes et suivi Spark."""
    offres_all, _competences, runs, error = get_data()
    banner = html.Div(error, className="error") if error else None

    if runs.empty:
        fig_pipeline = empty_figure("Aucun passage Spark enregistré")
    else:
        runs_long = runs.melt(
            id_vars="started_at", value_vars=["raw_count", "clean_count", "rejected_count"],
            var_name="type", value_name="lignes",
        )
        runs_long["type"] = runs_long["type"].map(
            {"raw_count": "Brutes (raw)", "clean_count": "Propres (clean)", "rejected_count": "Rejetées"}
        )
        fig_pipeline = _figure_layout(px.line(
            runs_long, x="started_at", y="lignes", color="type", markers=True,
            title="Pipeline : offres brutes vs propres par passage Spark",
            labels={"started_at": "Passage Spark", "lignes": "Nombre d'offres", "type": ""},
            color_discrete_map={"Brutes (raw)": "#6b7280", "Propres (clean)": "#16a34a", "Rejetées": "#dc2626"},
        ))

    if offres_all.empty:
        return banner, [], [], [], fig_pipeline
    return (
        banner,
        _options(offres_all["type_contrat"]),
        _options(offres_all["domaine_professionnel"]),
        _options(offres_all["nom_commune"]),
        fig_pipeline,
    )


@app.callback(
    Output("kpis", "children"),
    Output("graph-contrats", "figure"),
    Output("graph-competences", "figure"),
    Output("graph-carte", "figure"),
    Output("graph-salaires", "figure"),
    Output("graph-publication", "figure"),
    Input("filtre-contrat", "value"),
    Input("filtre-domaine", "value"),
    Input("filtre-commune", "value"),
    Input("refresh", "n_intervals"),
)
def update_dashboard(contrats, domaines, communes, _n_intervals):
    offres_all, competences, _runs, error = get_data()

    if offres_all.empty:
        empty = empty_figure(error or "Aucune offre en base : attendez un passage de Spark")
        kpis = [kpi_card(label, "0") for label in ("Offres", "Communes", "Entreprises", "Compétences", "Salaire moyen")]
        return kpis, empty, empty, empty, empty, empty

    offres = _apply_filters(offres_all, contrats, domaines, communes)
    if contrats or domaines or communes:
        competences = competences[competences["offre_id"].isin(offres["offre_id"])]

    salaire_moyen = offres["salaire"].mean()
    kpis = [
        kpi_card("Offres", f"{len(offres)}"),
        kpi_card("Communes", f"{offres['code_insee'].nunique()}"),
        kpi_card("Entreprises", f"{offres.loc[~offres['entreprise_anonyme'], 'raison_sociale'].nunique()}"),
        kpi_card("Compétences distinctes", f"{competences['libelle_competence'].nunique()}"),
        kpi_card("Salaire moyen estimé", "n.c." if pd.isna(salaire_moyen) else f"{salaire_moyen:,.0f} €".replace(",", " ")),
    ]

    if offres.empty:
        fig_empty = empty_figure("Aucune offre pour ces filtres")
        return kpis, fig_empty, fig_empty, fig_empty, fig_empty, fig_empty

    par_contrat = offres.groupby("type_contrat").size().reset_index(name="offres").sort_values("offres", ascending=False)
    fig_contrats = px.bar(
        par_contrat, x="type_contrat", y="offres", color="type_contrat", text="offres",
        title="Offres par type de contrat", labels={"type_contrat": "Contrat", "offres": "Nombre d'offres"},
    )
    fig_contrats.update_layout(showlegend=False)

    if competences.empty:
        fig_competences = empty_figure("Aucune compétence renseignée")
    else:
        top = competences["libelle_competence"].value_counts().head(TOP_COMPETENCES).index
        par_comp = (
            competences[competences["libelle_competence"].isin(top)]
            .groupby(["libelle_competence", "exigence"]).size().reset_index(name="offres")
        )
        par_comp["competence"] = par_comp["libelle_competence"].map(_short_label)
        fig_competences = px.bar(
            par_comp, x="offres", y="competence", color="exigence", orientation="h",
            hover_name="libelle_competence", hover_data={"competence": False},
            title=f"Top {TOP_COMPETENCES} des compétences demandées",
            labels={"competence": "", "offres": "Nombre d'offres", "exigence": "Niveau"},
            color_discrete_map={"Exigée": "#dc2626", "Souhaitée": "#2563eb"},
        )
        fig_competences.update_layout(yaxis={"categoryorder": "total ascending", "tickfont": {"size": 11}})

    geo = (
        offres.dropna(subset=["latitude", "longitude"])
        .groupby(["nom_commune", "latitude", "longitude"])
        .agg(offres=("offre_id", "count"), salaire_moyen=("salaire", "mean"))
        .reset_index()
    )
    if geo.empty:
        fig_carte = empty_figure("Aucune commune géolocalisée")
    else:
        fig_carte = px.scatter_map(
            geo, lat="latitude", lon="longitude", size="offres", color="offres",
            hover_name="nom_commune", hover_data={"offres": True, "salaire_moyen": ":.0f", "latitude": False, "longitude": False},
            zoom=4.3, center={"lat": 46.6, "lon": 2.4}, size_max=30, map_style="open-street-map",
            title="Carte des offres par commune", color_continuous_scale="Viridis",
        )
        fig_carte.update_layout(margin={"l": 0, "r": 0, "t": 0, "b": 0}, autosize=True, title=None,
                                coloraxis_colorbar={"thickness": 12, "title": {"text": ""}})

    salaires = offres.dropna(subset=["salaire"])
    if salaires.empty:
        fig_salaires = empty_figure("Aucun salaire exploitable")
    else:
        # Seuls les points atypiques sont dessinés : la figure reste légère.
        fig_salaires = px.box(
            salaires, x="type_contrat", y="salaire", color="type_contrat", points="outliers",
            title="Salaire brut annuel estimé par contrat",
            labels={"type_contrat": "Contrat", "salaire": "Salaire (€ / an)"},
        )
        fig_salaires.update_layout(showlegend=False)

    par_jour = offres.groupby(["date_publication", "type_contrat"]).size().reset_index(name="offres")
    fig_publication = px.bar(
        par_jour, x="date_publication", y="offres", color="type_contrat",
        title="Offres par date de publication",
        labels={"date_publication": "Date", "offres": "Nombre d'offres", "type_contrat": "Contrat"},
    )

    for figure in (fig_contrats, fig_competences, fig_salaires, fig_publication):
        _figure_layout(figure)

    return kpis, fig_contrats, fig_competences, fig_carte, fig_salaires, fig_publication


_TABLE_COLUMN_IDS = [column["id"] for column in TABLE_COLUMNS]
_FILTER_PATTERN = re.compile(
    r"^\{(?P<column>[^}]+)\}\s+(?:[is](?=\S))?(?P<op>>=|<=|!=|=|<|>|ge|le|lt|gt|ne|eq|contains|datestartswith)\s+(?P<value>.+)$"
)
_COMPARATORS = {
    "=": operator.eq, "eq": operator.eq, "!=": operator.ne, "ne": operator.ne,
    "<": operator.lt, "lt": operator.lt, "<=": operator.le, "le": operator.le,
    ">": operator.gt, "gt": operator.gt, ">=": operator.ge, "ge": operator.ge,
}


def _filter_table(table: pd.DataFrame, filter_query: str) -> pd.DataFrame:
    """Applique la syntaxe de filtre DataTable (`{col} op valeur && ...`) côté serveur."""
    for clause in (filter_query or "").split(" && "):
        match = _FILTER_PATTERN.match(clause.strip())
        if not match or match["column"] not in _TABLE_COLUMN_IDS:
            continue
        column, op = match["column"], match["op"]
        value = match["value"].strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'`":
            value = value[1:-1]
        series = table[column]

        if op in ("contains", "datestartswith"):
            text = series.astype("string").fillna("")
            if op == "contains":
                mask = text.str.contains(value, case=False, regex=False)
            else:
                mask = text.str.startswith(value)
        elif column == "salaire":
            try:
                number = float(value)
            except ValueError:
                continue
            mask = _COMPARATORS[op](series, number)
        else:
            text = series.astype("string").fillna("").str.lower()
            mask = _COMPARATORS[op](text, value.lower())
        table = table[mask.fillna(False).astype(bool)]
    return table


@app.callback(
    Output("table-offres", "data"),
    Output("table-offres", "page_count"),
    Output("table-offres", "page_current"),
    Input("filtre-contrat", "value"),
    Input("filtre-domaine", "value"),
    Input("filtre-commune", "value"),
    Input("table-offres", "page_current"),
    Input("table-offres", "page_size"),
    Input("table-offres", "sort_by"),
    Input("table-offres", "filter_query"),
    Input("refresh", "n_intervals"),
)
def update_table(contrats, domaines, communes, page_current, page_size, sort_by, filter_query, _n_intervals):
    """Tableau paginé côté serveur : seules les lignes de la page affichée sont envoyées."""
    offres_all, _competences, _runs, _error = get_data()
    if offres_all.empty:
        return [], 1, 0

    table = _apply_filters(offres_all, contrats, domaines, communes)[_TABLE_COLUMN_IDS]
    table = _filter_table(table, filter_query)

    if sort_by:
        column = sort_by[0]["column_id"]
        table = table.sort_values(column, ascending=sort_by[0]["direction"] == "asc", na_position="last")
    else:
        table = table.sort_values("publication", ascending=False, na_position="last")

    page_size = page_size or TABLE_PAGE_SIZE
    page_count = max(1, math.ceil(len(table) / page_size))
    page_current = min(max(page_current or 0, 0), page_count - 1)
    start = page_current * page_size
    rows = table.iloc[start:start + page_size].assign(id=lambda frame: frame["source_offre_id"])
    return rows.to_dict("records"), page_count, page_current


CONTRATS = {"CDI": "CDI", "CDD": "CDD", "MIS": "Intérim", "SAI": "Saisonnier", "CCE": "Profession commerciale"}
EXIGENCES = {"E": "Exigé", "S": "Souhaité"}


def _clean(value) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    return text or None


def _euros(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    return f"{value:,.0f} € brut / an".replace(",", " ")


def _detail_grid(rows: list[tuple[str, object]]) -> html.Dl | None:
    items = []
    for label, value in rows:
        if value in (None, "", []):
            continue
        items += [html.Dt(label), html.Dd(value)]
    return html.Dl(items, className="detail-grid") if items else None


def _section(title: str, content) -> list:
    return [html.H3(title), content] if content else []


def _labels_with_requirement(items: list[dict], label_key: str = "libelle") -> list[str]:
    labels = []
    for item in items or []:
        label = _clean(item.get(label_key))
        if label:
            requirement = EXIGENCES.get(item.get("exigence"))
            labels.append(f"{label} ({requirement.lower()})" if requirement else label)
    return labels


def build_offer_details(source_offre_id: str) -> list:
    offres, competences, _runs, _error = get_data()
    match = offres[offres["source_offre_id"] == source_offre_id] if not offres.empty else offres
    if match.empty:
        return [html.H2("Offre introuvable"), html.P("Cette offre n'est plus présente dans la base.")]
    offre = match.iloc[0]
    raw = load_raw_offer(source_offre_id)
    description, duree_travail = load_offer_text(source_offre_id)

    entreprise = "Entreprise non communiquée" if offre["entreprise_anonyme"] else _clean(offre["raison_sociale"])
    contrat = _clean(raw.get("typeContratLibelle")) or CONTRATS.get(offre["type_contrat"], offre["type_contrat"])
    sous_titre = " • ".join(filter(None, [entreprise, _clean(offre["nom_commune"]), contrat]))

    salaire_raw = raw.get("salaire") or {}
    salaire_parts: list[str] = []
    for key in ("libelle", "commentaire", "complement1", "complement2"):
        part = _clean(salaire_raw.get(key))
        if part and not any(part in existing for existing in salaire_parts):
            salaire_parts.append(part)
    salaire_texte = " — ".join(salaire_parts)
    duree = _clean(raw.get("dureeTravailLibelle")) or _clean(duree_travail)
    duree_converti = _clean(raw.get("dureeTravailLibelleConverti"))
    if duree and duree_converti and duree_converti not in duree:
        duree = f"{duree} ({duree_converti})"
    lieu = raw.get("lieuTravail") or {}
    publication = offre["date_publication"].strftime("%d/%m/%Y") if pd.notna(offre["date_publication"]) else None

    infos = _detail_grid([
        ("Référence", source_offre_id),
        ("Date de publication", publication),
        ("Type de contrat", contrat),
        ("Durée du travail", html.Span(duree, style={"whiteSpace": "pre-wrap"}) if duree else None),
        ("Salaire indiqué", salaire_texte or "Non précisé"),
        ("Salaire annuel estimé", _euros(offre["salaire"])),
        ("Lieu de travail", _clean(lieu.get("libelle")) or _clean(offre["nom_commune"])),
        ("Nombre de postes", _clean(raw.get("nombrePostes"))),
        ("Expérience", _clean(raw.get("experienceLibelle"))),
        ("Qualification", _clean(raw.get("qualificationLibelle"))),
        ("Déplacements", _clean(raw.get("deplacementLibelle"))),
        ("Alternance", "Oui" if raw.get("alternance") else None),
        ("Accessible aux personnes handicapées", "Oui" if raw.get("accessibleTH") else None),
    ])
    metier = _detail_grid([
        ("Métier (code ROME)", f"{offre['libelle_fiche_metier']} ({offre['rome_code']})"),
        ("Appellation", _clean(raw.get("appellationlibelle"))),
        ("Domaine professionnel", _clean(offre["domaine_professionnel"])),
        ("Entreprise", entreprise),
        ("Secteur d'activité", _clean(raw.get("secteurActiviteLibelle"))),
        ("Taille de l'établissement", _clean(raw.get("trancheEffectifEtab"))),
    ])

    competences_offre = competences[competences["offre_id"] == offre["offre_id"]] if not competences.empty else competences
    tags = [
        html.Span(row.libelle_competence, className=f"tag tag-{'E' if row.exigence == 'Exigée' else 'S'}",
                  title=row.exigence)
        for row in competences_offre.sort_values("exigence").itertuples()
    ]
    if tags:
        tags.append(html.Div("Rouge : exigée • Bleu : souhaitée", className="table-hint"))

    formations = []
    for item in raw.get("formations") or []:
        label = " — ".join(filter(None, [_clean(item.get("niveauLibelle")), _clean(item.get("domaineLibelle"))]))
        if label:
            requirement = EXIGENCES.get(item.get("exigence"))
            formations.append(f"{label} ({requirement.lower()})" if requirement else label)
    qualites = [_clean(item.get("libelle")) for item in raw.get("qualitesProfessionnelles") or []]
    profil = _detail_grid([
        ("Formation", html.Ul([html.Li(f) for f in formations]) if formations else None),
        ("Langues", ", ".join(_labels_with_requirement(raw.get("langues")))),
        ("Permis", ", ".join(_labels_with_requirement(raw.get("permis")))),
        ("Qualités professionnelles", ", ".join(filter(None, qualites))),
    ])

    url_offre = _clean((raw.get("origineOffre") or {}).get("urlOrigine")) or FRANCE_TRAVAIL_OFFER_URL.format(source_offre_id)
    url_postuler = _clean((raw.get("contact") or {}).get("urlPostulation"))
    liens = [html.A("Voir l'offre sur France Travail ↗", href=url_offre, target="_blank", rel="noopener noreferrer",
                    className="btn-link btn-primary")]
    if url_postuler and url_postuler.startswith(("http://", "https://")):
        liens.append(html.A("Postuler sur le site du recruteur ↗", href=url_postuler, target="_blank",
                            rel="noopener noreferrer", className="btn-link btn-secondary"))
    else:
        liens.append(html.Span("Pas de lien de candidature direct : postulez depuis France Travail.", className="no-link"))

    return [
        html.H2(offre["libelle_poste"]),
        html.Div(sous_titre, className="modal-sub"),
        html.Div(liens, className="links"),
        *_section("Informations clés", infos),
        *_section("Description du poste", html.Div(_clean(raw.get("description")) or _clean(description)
                                                     or "Aucune description fournie.", className="description")),
        *_section("Compétences demandées", html.Div(tags) if tags else html.Div("Aucune compétence listée.",
                                                                                  className="no-link")),
        *_section("Profil recherché", profil),
        *_section("Métier et entreprise", metier),
    ]


@app.callback(
    Output("offre-modal", "style"),
    Output("modal-contenu", "children"),
    Output("table-offres", "active_cell"),
    Output("table-offres", "selected_cells"),
    Input("table-offres", "active_cell"),
    Input("modal-fermer", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_offer_modal(active_cell, _close_clicks):
    if ctx.triggered_id == "modal-fermer" or not active_cell:
        return {"display": "none"}, no_update, None, []
    source_offre_id = active_cell.get("row_id")
    if not source_offre_id:
        return no_update, no_update, None, []
    # active_cell est remis à None pour qu'un nouveau clic sur la même ligne rouvre la fiche.
    return {"display": "flex"}, build_offer_details(str(source_offre_id)), None, []


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("DATAVIZ_PORT", "8050")), debug=False)
