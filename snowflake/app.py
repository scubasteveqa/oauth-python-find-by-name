"""
Snowflake Dashboard - Find by Integration Name
================================================
Uses content.oauth.associations.find_by(name=...) to discover
the Snowflake integration GUID by name. Falls back to
find_by(integration_type=SNOWFLAKE) if the name env var is not set.

Required env vars:
  SNOWFLAKE_ACCOUNT           - Snowflake account identifier
  SNOWFLAKE_WAREHOUSE         - Snowflake warehouse
  SNOWFLAKE_DATABASE          - Snowflake database
  SNOWFLAKE_SCHEMA            - Snowflake schema

Optional env vars:
  SNOWFLAKE_INTEGRATION_NAME  - name of the Snowflake integration in Connect
                                (omit to auto-discover by type)
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from posit import connect
from posit.connect.oauth import types
from shiny import App, Inputs, Outputs, Session, reactive, render, ui
from shinywidgets import output_widget, render_widget


# -- Data helper -------------------------------------------------------------

def fetch_snowflake(access_token: str) -> pd.DataFrame:
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        token=access_token,
        authenticator="oauth",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )
    try:
        df = pd.read_sql("SELECT * FROM SALES", conn)
    finally:
        conn.close()
    df.columns = df.columns.str.upper()
    df["SALE_DATE"] = pd.to_datetime(df["SALE_DATE"])
    df["MONTH"] = df["SALE_DATE"].dt.to_period("M").astype(str)
    return df


# -- UI ----------------------------------------------------------------------

app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.tags.h3("Snowflake Dashboard"),
        ui.tags.p(
            "OAuth via find_by(name=...)",
            style="color: #6c757d; font-size: 0.85rem; margin: 0 0 1rem 0;",
        ),
        ui.input_action_button("load_data", "Refresh Data", class_="btn-primary w-100"),
        ui.tags.script(
            "setTimeout(function() { document.getElementById('load_data').click(); }, 500);"
        ),
        ui.tags.hr(),
        ui.input_select("category", "Category", choices=["All"], selected="All"),
        ui.input_select("region", "Region", choices=["All"], selected="All"),
        width=260,
    ),
    ui.tags.h4("Snowflake - Sales"),
    ui.layout_columns(
        ui.value_box("Total Sales", ui.output_text("total_sales"), theme="primary"),
        ui.value_box("Orders", ui.output_text("total_orders"), theme="info"),
        ui.value_box("Avg Order", ui.output_text("avg_order"), theme="success"),
        col_widths=[4, 4, 4],
    ),
    ui.layout_columns(
        ui.card(ui.card_header("Sales by Category"), output_widget("chart_category")),
        ui.card(ui.card_header("Sales by Region"), output_widget("chart_region")),
        col_widths=[6, 6],
    ),
    ui.layout_columns(
        ui.card(ui.card_header("Monthly Sales Trend"), output_widget("chart_trend")),
        col_widths=[12],
    ),
    title="Snowflake Analytics (find_by name)",
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

        # Discover integration: by name if provided, otherwise by type
        integration_name = os.environ.get("SNOWFLAKE_INTEGRATION_NAME")
        if integration_name:
            sf_assoc = current_content.oauth.associations.find_by(
                name=integration_name
            )
        else:
            sf_assoc = current_content.oauth.associations.find_by(
                integration_type=types.OAuthIntegrationType.SNOWFLAKE
            )

        if sf_assoc is None:
            label = f"name={integration_name}" if integration_name else "type=SNOWFLAKE"
            ui.notification_show(
                f"No Snowflake integration found ({label})", type="error"
            )
            return

        sf_guid = sf_assoc.get("oauth_integration_guid")

        try:
            creds = client.oauth.get_credentials(session_token, audience=sf_guid)
            df = fetch_snowflake(creds["access_token"])
            raw_data.set(df)

            categories = ["All"] + sorted(df["CATEGORY"].dropna().unique().tolist())
            regions = ["All"] + sorted(df["REGION"].dropna().unique().tolist())
            ui.update_select("category", choices=categories, selected="All")
            ui.update_select("region", choices=regions, selected="All")

            ui.notification_show(f"Snowflake: {len(df)} rows", type="message")
        except Exception as e:
            ui.notification_show(f"Snowflake error: {e}", type="error", duration=10)

    @reactive.calc
    def filtered():
        df = raw_data()
        if df is None:
            return None
        if i.category() != "All":
            df = df[df["CATEGORY"] == i.category()]
        if i.region() != "All":
            df = df[df["REGION"] == i.region()]
        return df

    @render.text
    def total_sales():
        df = filtered()
        return f"${df['TOTAL_AMOUNT'].sum():,.2f}" if df is not None else "--"

    @render.text
    def total_orders():
        df = filtered()
        return f"{len(df):,}" if df is not None else "--"

    @render.text
    def avg_order():
        df = filtered()
        if df is None or len(df) == 0:
            return "--"
        return f"${df['TOTAL_AMOUNT'].mean():,.2f}"

    @render_widget
    def chart_category():
        df = filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = df.groupby("CATEGORY", as_index=False)["TOTAL_AMOUNT"].sum()
        return px.bar(
            agg, x="CATEGORY", y="TOTAL_AMOUNT", color="CATEGORY",
            labels={"TOTAL_AMOUNT": "Sales ($)", "CATEGORY": "Category"},
        )

    @render_widget
    def chart_region():
        df = filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = df.groupby("REGION", as_index=False)["TOTAL_AMOUNT"].sum()
        return px.pie(agg, names="REGION", values="TOTAL_AMOUNT")

    @render_widget
    def chart_trend():
        df = filtered()
        if df is None:
            fig = go.Figure()
            fig.update_layout(title="Loading...", template="plotly_white", height=350)
            return fig
        agg = df.groupby("MONTH", as_index=False)["TOTAL_AMOUNT"].sum().sort_values("MONTH")
        return px.line(agg, x="MONTH", y="TOTAL_AMOUNT", markers=True,
                       labels={"TOTAL_AMOUNT": "Sales ($)", "MONTH": "Month"})


app = App(app_ui, server)
