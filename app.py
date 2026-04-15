"""
Databricks Dashboard - Find by Integration Name
=================================================
Uses content.oauth.associations.find_by(name=...) to discover
the Databricks integration GUID by name.

Required env vars:
  DATABRICKS_INTEGRATION_NAME - name of the Databricks integration in Connect
  DATABRICKS_HOST             - Databricks workspace hostname
  DATABRICKS_HTTP_PATH        - Databricks SQL warehouse HTTP path
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql as dbsql
from posit import connect
from shiny import App, Inputs, Outputs, Session, reactive, render, ui
from shinywidgets import output_widget, render_widget


# -- Data helper -------------------------------------------------------------

def fetch_databricks(access_token: str) -> pd.DataFrame:
    conn = dbsql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=access_token,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    t.dateTime,
                    t.product,
                    t.quantity,
                    t.totalPrice,
                    c.continent,
                    c.country,
                    f.name AS franchise_name
                FROM samples.bakehouse.sales_transactions t
                JOIN samples.bakehouse.sales_customers c
                    ON t.customerID = c.customerID
                JOIN samples.bakehouse.sales_franchises f
                    ON t.franchiseID = f.franchiseID
            """)
            rows = cur.fetchall()
            cols = [desc[0] for desc in cur.description]
    finally:
        conn.close()
    df = pd.DataFrame(rows, columns=cols)
    df.columns = df.columns.str.lower()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["month"] = df["datetime"].dt.to_period("M").astype(str)
    return df


# -- UI ----------------------------------------------------------------------

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.tags.h3("Databricks Dashboard"),
        ui.tags.p(
            "OAuth via find_by(name=...)",
            style="color: #6c757d; font-size: 0.85rem; margin: 0 0 1rem 0;",
        ),
        ui.input_action_button("load_data", "Refresh Data", class_="btn-primary w-100"),
        ui.tags.script(
            "setTimeout(function() { document.getElementById('load_data').click(); }, 500);"
        ),
        ui.tags.hr(),
        ui.input_select("continent", "Continent", choices=["All"], selected="All"),
        ui.input_select("franchise", "Franchise", choices=["All"], selected="All"),
        width=260,
    ),
    ui.tags.h4("Databricks - Bakehouse"),
    ui.layout_columns(
        ui.value_box("Revenue", ui.output_text("total_revenue"), theme="primary"),
        ui.value_box("Orders", ui.output_text("total_orders"), theme="info"),
        ui.value_box("Franchises", ui.output_text("franchise_count"), theme="warning"),
        col_widths=[4, 4, 4],
    ),
    ui.layout_columns(
        ui.card(ui.card_header("Revenue by Franchise"), output_widget("chart_franchise")),
        ui.card(ui.card_header("Revenue by Continent"), output_widget("chart_continent")),
        col_widths=[6, 6],
    ),
    ui.layout_columns(
        ui.card(ui.card_header("Monthly Revenue Trend"), output_widget("chart_trend")),
        col_widths=[12],
    ),
    title="Databricks Analytics (find_by name)",
)


# -- Server ------------------------------------------------------------------

def server(i: Inputs, o: Outputs, session: Session):
    raw_data = reactive.Value(None)

    @reactive.effect
    @reactive.event(i.load_data)
    def load_all():
        session_token = session.http_conn.headers.get(
            "Posit-Connect-User-Session-Token"
        )
        if not session_token:
            ui.notification_show(
                "No session token found. Deploy on Posit Connect.", type="error"
            )
            return

        client = connect.Client()
        current_content = client.content.get()

        # Discover integration by name
        integration_name = os.environ["DATABRICKS_INTEGRATION_NAME"]
        db_assoc = current_content.oauth.associations.find_by(
            name=integration_name
        )
        if db_assoc is None:
            ui.notification_show(
                f"No integration found with name: {integration_name}", type="error"
            )
            return

        db_guid = db_assoc.get("oauth_integration_guid")

        try:
            creds = client.oauth.get_credentials(session_token, audience=db_guid)
            df = fetch_databricks(creds["access_token"])
            raw_data.set(df)

            continents = ["All"] + sorted(df["continent"].dropna().unique().tolist())
            franchises = ["All"] + sorted(df["franchise_name"].dropna().unique().tolist())
            ui.update_select("continent", choices=continents, selected="All")
            ui.update_select("franchise", choices=franchises, selected="All")

            ui.notification_show(f"Databricks: {len(df)} rows", type="message")
        except Exception as e:
            ui.notification_show(f"Databricks error: {e}", type="error", duration=10)

    @reactive.calc
    def filtered():
        df = raw_data()
        if df is None:
            return None
        if i.continent() != "All":
            df = df[df["continent"] == i.continent()]
        if i.franchise() != "All":
            df = df[df["franchise_name"] == i.franchise()]
        return df

    @render.text
    def total_revenue():
        df = filtered()
        return f"${df['totalprice'].sum():,.2f}" if df is not None else "--"

    @render.text
    def total_orders():
        df = filtered()
        return f"{len(df):,}" if df is not None else "--"

    @render.text
    def franchise_count():
        df = filtered()
        return str(df["franchise_name"].nunique()) if df is not None else "--"

    @render_widget
    def chart_franchise():
        df = filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = (
            df.groupby("franchise_name", as_index=False)["totalprice"]
            .sum()
            .sort_values("totalprice", ascending=True)
        )
        return px.bar(
            agg, x="totalprice", y="franchise_name", orientation="h",
            labels={"totalprice": "Revenue ($)", "franchise_name": "Franchise"},
        )

    @render_widget
    def chart_continent():
        df = filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = df.groupby("continent", as_index=False)["totalprice"].sum()
        return px.pie(agg, names="continent", values="totalprice")

    @render_widget
    def chart_trend():
        df = filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = df.groupby("month", as_index=False)["totalprice"].sum().sort_values("month")
        return px.line(agg, x="month", y="totalprice", markers=True,
                       labels={"totalprice": "Revenue ($)", "month": "Month"})


app = App(app_ui, server)
