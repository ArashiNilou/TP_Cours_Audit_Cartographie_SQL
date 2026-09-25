"""Dashboard interactif Plotly Dash alimenté par la base PostgreSQL du TP.

Toutes les données sont relues dans PostgreSQL à chaque rafraîchissement :
le dashboard montre donc toujours l'état produit par le pipeline Spark.
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg
from dash import Dash, Input, Output, dash_table, dcc, html

REFRESH_SECONDS = int(os.getenv("DATAVIZ_REFRESH_SECONDS", "60"))
TOP_COMPETENCES = 15

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


app = Dash(__name__, title="TP2 - Data Viz Plotly")
server = app.server


@server.route("/health")
def health() -> tuple[str, int]:
    return "ok", 200


app.index_string = """<!DOCTYPE html>
<html>
<head>
{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
  body { font-family: Segoe UI, Arial, sans-serif; background: #f3f4f6; margin: 0; }
  .header { background: #1e3a8a; color: white; padding: 18px 28px; }
  .header h1 { margin: 0; font-size: 24px; }
  .header p { margin: 4px 0 0; opacity: .85; }
  .container { padding: 18px 28px; }
  .filters { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; background: white;
             padding: 16px; border-radius: 10px; margin-bottom: 16px; }
  .kpis { display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 16px; }
  .kpi-card { background: white; border-radius: 10px; padding: 14px; text-align: center; }
  .kpi-value { font-size: 26px; font-weight: 700; color: #1e3a8a; }
  .kpi-label { color: #6b7280; font-size: 13px; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
  .card { background: white; border-radius: 10px; padding: 8px; }
  .error { background: #fee2e2; color: #991b1b; padding: 10px; border-radius: 8px; margin-bottom: 12px; }
  label { font-weight: 600; font-size: 13px; }
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
                        html.Div(dcc.Graph(id="graph-contrats"), className="card"),
                        html.Div(dcc.Graph(id="graph-competences"), className="card"),
                        html.Div(dcc.Graph(id="graph-carte"), className="card"),
                        html.Div(dcc.Graph(id="graph-salaires"), className="card"),
                        html.Div(dcc.Graph(id="graph-publication"), className="card"),
                        html.Div(dcc.Graph(id="graph-pipeline"), className="card"),
                    ],
                    className="grid",
                ),
                html.Div(
                    dash_table.DataTable(
                        id="table-offres",
                        page_size=10,
                        sort_action="native",
                        filter_action="native",
                        style_table={"overflowX": "auto"},
                        style_cell={"fontFamily": "Segoe UI, Arial", "fontSize": 13, "textAlign": "left"},
                        style_header={"fontWeight": "700", "backgroundColor": "#e5e7eb"},
                    ),
                    className="card",
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


@app.callback(
    Output("error-banner", "children"),
    Output("filtre-contrat", "options"),
    Output("filtre-domaine", "options"),
    Output("filtre-commune", "options"),
    Output("kpis", "children"),
    Output("graph-contrats", "figure"),
    Output("graph-competences", "figure"),
    Output("graph-carte", "figure"),
    Output("graph-salaires", "figure"),
    Output("graph-publication", "figure"),
    Output("graph-pipeline", "figure"),
    Output("table-offres", "data"),
    Output("table-offres", "columns"),
    Input("filtre-contrat", "value"),
    Input("filtre-domaine", "value"),
    Input("filtre-commune", "value"),
    Input("refresh", "n_intervals"),
)
def update_dashboard(contrats, domaines, communes, _n_intervals):
    offres_all, competences, runs, error = load_data()
    banner = html.Div(error, className="error") if error else None
    no_data = "Aucune offre pour ces filtres"

    if offres_all.empty:
        empty = empty_figure(error or "Aucune offre en base : attendez un passage de Spark")
        kpis = [kpi_card(label, "0") for label in ("Offres", "Communes", "Entreprises", "Compétences", "Salaire moyen")]
        return banner, [], [], [], kpis, empty, empty, empty, empty, empty, empty, [], []

    offres = _apply_filters(offres_all, contrats, domaines, communes)
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
        fig_empty = empty_figure(no_data)
        fig_contrats = fig_competences = fig_carte = fig_salaires = fig_publication = fig_empty
    else:
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
            fig_competences = px.bar(
                par_comp, x="offres", y="libelle_competence", color="exigence", orientation="h",
                title=f"Top {TOP_COMPETENCES} des compétences demandées",
                labels={"libelle_competence": "", "offres": "Nombre d'offres", "exigence": "Niveau"},
                color_discrete_map={"Exigée": "#dc2626", "Souhaitée": "#2563eb"},
            )
            fig_competences.update_layout(yaxis={"categoryorder": "total ascending"})

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
            fig_carte.update_layout(margin={"l": 0, "r": 0, "t": 40, "b": 0})

        salaires = offres.dropna(subset=["salaire"])
        if salaires.empty:
            fig_salaires = empty_figure("Aucun salaire exploitable")
        else:
            fig_salaires = px.box(
                salaires, x="type_contrat", y="salaire", color="type_contrat", points="all",
                hover_data=["libelle_poste", "nom_commune"],
                title="Salaire brut annuel estimé par contrat",
                labels={"type_contrat": "Contrat", "salaire": "Salaire (€ / an)"},
            )
            fig_salaires.update_layout(showlegend=False)

        par_jour = (
            offres.assign(date_publication=pd.to_datetime(offres["date_publication"]))
            .groupby(["date_publication", "type_contrat"]).size().reset_index(name="offres")
        )
        fig_publication = px.bar(
            par_jour, x="date_publication", y="offres", color="type_contrat",
            title="Offres par date de publication",
            labels={"date_publication": "Date", "offres": "Nombre d'offres", "type_contrat": "Contrat"},
        )

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
        fig_pipeline = px.line(
            runs_long, x="started_at", y="lignes", color="type", markers=True,
            title="Pipeline : offres brutes vs propres par passage Spark",
            labels={"started_at": "Passage Spark", "lignes": "Nombre d'offres", "type": ""},
            color_discrete_map={"Brutes (raw)": "#6b7280", "Propres (clean)": "#16a34a", "Rejetées": "#dc2626"},
        )

    for figure in (fig_contrats, fig_competences, fig_salaires, fig_publication, fig_pipeline):
        figure.update_layout(template="plotly_white", margin={"l": 10, "r": 10, "t": 50, "b": 10})

    table = offres[
        ["source_offre_id", "libelle_poste", "type_contrat", "nom_commune", "libelle_fiche_metier", "salaire", "date_publication"]
    ].sort_values("date_publication", ascending=False)
    table = table.assign(date_publication=table["date_publication"].astype(str))
    columns = [
        {"name": "Offre", "id": "source_offre_id"},
        {"name": "Poste", "id": "libelle_poste"},
        {"name": "Contrat", "id": "type_contrat"},
        {"name": "Commune", "id": "nom_commune"},
        {"name": "Métier ROME", "id": "libelle_fiche_metier"},
        {"name": "Salaire (€ / an)", "id": "salaire", "type": "numeric"},
        {"name": "Publication", "id": "date_publication"},
    ]

    return (
        banner,
        _options(offres_all["type_contrat"]),
        _options(offres_all["domaine_professionnel"]),
        _options(offres_all["nom_commune"]),
        kpis,
        fig_contrats,
        fig_competences,
        fig_carte,
        fig_salaires,
        fig_publication,
        fig_pipeline,
        table.to_dict("records"),
        columns,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("DATAVIZ_PORT", "8050")), debug=False)
