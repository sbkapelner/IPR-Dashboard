import os
from pathlib import Path
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st

TECH_CENTER_ORDER = [1600, 1700, 2100, 2400, 2600, 2800, 2900, 3600, 3700, 3900]
AGE_BUCKET_STARTS = [0, 3, 6, 9, 12, 15, 18, 20]
AGE_BUCKET_LABELS = ["0-3 years", "3-6 years", "6-9 years", "9-12 years", "12-15 years", "15-18 years", "18-20 years", "20+ years"]
INSTITUTION_FY_FLOOR = 2022
INSTITUTION_DATE_FLOOR = date(2021, 10, 1)
AGE_TOTAL_COLOR = "#3b82a0"
AGE_DD_COLOR = "#d97706"
MONTH_TOTAL_COLOR = "#2f855a"
MONTH_DD_COLOR = "#dd6b20"
TECH_TOTAL_COLOR = "#4b5563"
TECH_DD_COLOR = "#2f6f4f"


def get_patent_age_bucket_start(value):
    if value < 3:
        return 0
    if value < 6:
        return 3
    if value < 9:
        return 6
    if value < 12:
        return 9
    if value < 15:
        return 12
    if value < 18:
        return 15
    if value < 20:
        return 18
    return 20


def get_patent_age_bucket_label(value):
    if value < 3:
        return "0-3 years"
    if value < 6:
        return "3-6 years"
    if value < 9:
        return "6-9 years"
    if value < 12:
        return "9-12 years"
    if value < 15:
        return "12-15 years"
    if value < 18:
        return "15-18 years"
    if value < 20:
        return "18-20 years"
    return "20+ years"

def load_dotenv(path=".env"):
    env_path = Path(path)
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def get_connection():
    try:
        secrets = dict(st.secrets)
    except Exception:
        secrets = {}

    required_vars = ["PGHOST", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD"]
    missing = [
        name for name in required_vars
        if not os.environ.get(name) and name not in secrets
    ]
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(f"Set PostgreSQL env vars in .env before running: {missing_list}")

    def get_setting(name):
        if name in secrets:
            return secrets[name]
        return os.environ[name]

    return psycopg.connect(
        host=get_setting("PGHOST"),
        port=get_setting("PGPORT"),
        dbname=get_setting("PGDATABASE"),
        user=get_setting("PGUSER"),
        password=get_setting("PGPASSWORD"),
    )


@st.cache_data(ttl=600)
def get_start_date_label():
    query = """
        SELECT MIN(trial_meta_institution_decision_date)
        FROM proceedings
        WHERE trial_meta_institution_decision_date IS NOT NULL
          AND trial_meta_trial_status_category = 'Discretionary Denial'
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()[0]

    return result.isoformat() if result else "Unknown"


@st.cache_data(ttl=600)
def load_histogram_data():
    query = """
        SELECT
            trial_number,
            patent_owner_patent_number,
            patent_owner_grant_date,
            patent_owner_technology_center_number,
            patent_owner_group_art_unit_number,
            regular_petitioner_real_party_in_interest_name,
            trial_meta_petition_filing_date,
            trial_meta_trial_status_category,
            trial_meta_institution_decision_date AS decision_date
        FROM proceedings
        WHERE trial_meta_trial_type_code = 'IPR'
        ORDER BY decision_date DESC, trial_number
    """

    with get_connection() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=600)
def load_institution_rate_data():
    query = """
        SELECT
            trial_decision_id,
            appeal_number,
            trial_number,
            decision_issue_date,
            trial_outcome_category
        FROM trial_decisions
        WHERE decision_issue_date IS NOT NULL
          AND trial_outcome_category IN ('Institution Denied', 'Institution Granted')
        ORDER BY decision_issue_date DESC, trial_decision_id
    """

    with get_connection() as conn:
        return pd.read_sql(query, conn)


def main():
    load_dotenv()
    start_date_label = get_start_date_label()
    start_date_floor = date.fromisoformat(start_date_label)

    st.set_page_config(
        page_title="IPR Dashboard",
        layout="wide",
    )
    st.title(f"IPR Dashboard")
    st.caption("Data from USPTO Open Data Portal")
    st.markdown(
        """
        <style>
        div[data-testid="stVerticalBlockBorderWrapper"] {
            background-color: #f7fafc;
            border-color: #d9e4ef;
            border-radius: 12px;
        }
        .total-period-box {
            background: var(--secondary-background-color);
            color: var(--text-color);
            border: 1px solid rgba(128, 128, 128, 0.35);
            border-radius: 12px;
            padding: 0.7rem 0.9rem;
            text-align: center;
        }
        .total-period-label {
            font-size: 1.25rem;
            font-weight: 600;
            margin: 0;
        }
        .total-period-value {
            font-size: 1.25rem;
            font-weight: 600;
            margin: 0.15rem 0 0 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    def centered_chart(fig, ratios=(1, 4, 1)):
        left, center, right = st.columns(ratios)
        with center:
            st.plotly_chart(fig, use_container_width=True)

    try:
        institution_df = load_institution_rate_data()
        df = load_histogram_data()
    except Exception as exc:
        st.error(f"Failed to load data: {exc}")
        return

    if institution_df.empty:
        st.warning("No institution-rate records are available in the appeals table.")
        return

    if df.empty:
        st.warning("No IPR records are available.")
        return

    institution_df["decision_issue_date"] = pd.to_datetime(
        institution_df["decision_issue_date"]
    ).dt.date
    institution_df["institution_fiscal_year"] = pd.to_datetime(
        institution_df["decision_issue_date"]
    ).dt.year + (
        pd.to_datetime(institution_df["decision_issue_date"]).dt.month >= 10
    ).astype(int)
    institution_df = institution_df[
        institution_df["institution_fiscal_year"] >= INSTITUTION_FY_FLOOR
    ].copy()
    institution_min_date = max(institution_df["decision_issue_date"].min(), INSTITUTION_DATE_FLOOR)
    institution_max_date = institution_df["decision_issue_date"].max()

    with st.container(border=True):
        st.header("Institution Rate")
        institution_view = st.segmented_control(
            "View",
            options=["Month", "Year"],
            default="Month",
            key="institution_view",
        )

        if institution_view == "Month":
            default_institution_start = max(
                institution_min_date,
                institution_max_date - timedelta(days=183),
            )
            institution_date_col1, institution_date_col2, _ = st.columns([1, 1, 3])
            with institution_date_col1:
                institution_start_date = st.date_input(
                    "Start Date",
                    value=default_institution_start,
                    min_value=institution_min_date,
                    max_value=institution_max_date,
                )
            with institution_date_col2:
                institution_end_date = st.date_input(
                    "End Date",
                    value=institution_max_date,
                    min_value=institution_min_date,
                    max_value=institution_max_date,
                )

            if institution_start_date > institution_end_date:
                st.warning("Institution Start Date must be on or before Institution End Date.")
                return

            institution_plot_df = institution_df[
                (institution_df["decision_issue_date"] >= institution_start_date)
                & (institution_df["decision_issue_date"] <= institution_end_date)
            ].copy()
        else:
            institution_plot_df = institution_df.copy()

        if institution_plot_df.empty:
            st.warning("No institution-rate records fall within the selected date range.")
            return

        institution_plot_df["institution_group"] = institution_plot_df["trial_outcome_category"].map(
            {
                "Institution Granted": "Referred",
                "Institution Denied": "Denied",
            }
        )
        period_column = "decision_month" if institution_view == "Month" else "decision_year"
        period_label = "Month" if institution_view == "Month" else "Year"
        period_title = "Institution Outcomes by Month" if institution_view == "Month" else "Institution Outcomes by Year"
        if institution_view == "Month":
            institution_plot_df[period_column] = pd.to_datetime(
                institution_plot_df["decision_issue_date"]
            ).dt.to_period("M").dt.to_timestamp()
        else:
            institution_plot_df[period_column] = institution_plot_df["institution_fiscal_year"]

        institution_period_totals = (
            institution_plot_df.groupby(period_column, as_index=False)
            .size()
            .rename(columns={"size": "total_outcomes"})
        )
        institution_status_counts = (
            institution_plot_df.groupby([period_column, "institution_group"], as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .merge(institution_period_totals, on=period_column, how="left")
        )
        institution_status_counts["percentage"] = institution_status_counts.apply(
            lambda row: (row["count"] / row["total_outcomes"]) if row["total_outcomes"] else 0,
            axis=1,
        )
        institution_status_counts["percentage_label"] = institution_status_counts["percentage"].map(
            lambda value: f"{value:.1%}"
        )
        institution_fig = px.bar(
            institution_status_counts,
            x=period_column,
            y="count",
            color="institution_group",
            barmode="overlay",
            title=period_title,
            labels={
                period_column: period_label,
                "count": "Petitions",
                "institution_group": "",
            },
            text="percentage_label",
            color_discrete_map={
                "Denied": "#c44e52",
                "Referred": "#4c78a8",
            },
        )
        institution_fig.update_traces(opacity=0.8, textposition="outside")
        institution_fig.update_traces(hovertemplate="%{y}<extra></extra>")
        institution_fig.update_layout(
            yaxis_title="Petitions",
            xaxis_title=period_label,
        )
        if institution_view == "Year":
            fiscal_years = sorted(institution_status_counts[period_column].unique())
            institution_fig.update_xaxes(
                tickmode="array",
                tickvals=fiscal_years,
                ticktext=[f"FY{str(year)[-2:]}" for year in fiscal_years],
            )
        centered_chart(institution_fig)

    with st.container(border=True):
        st.header("Discretionary Denials")

        df = df[df["decision_date"].notna()].copy()
        if df.empty:
            st.warning("No IPR records with institution decision dates are available.")
            return

        df["decision_date"] = pd.to_datetime(df["decision_date"]).dt.date
        df["petition_filing_date"] = pd.to_datetime(df["trial_meta_petition_filing_date"]).dt.date
        min_decision_date = max(df["decision_date"].min(), start_date_floor)
        max_decision_date = df["decision_date"].max()

        dd_date_col1, dd_date_col2, _ = st.columns([1, 1, 3])
        with dd_date_col1:
            start_date = st.date_input(
                "Start Date",
                value=min_decision_date,
                min_value=min_decision_date,
                max_value=max_decision_date,
            )
        with dd_date_col2:
            end_date = st.date_input(
                "End Date",
                value=max_decision_date,
                min_value=min_decision_date,
                max_value=max_decision_date,
            )

        if start_date > end_date:
            st.warning("Start Date must be on or before End Date.")
            return

        filtered_df = df[
            (df["decision_date"] >= start_date) & (df["decision_date"] <= end_date)
        ].copy()

        if filtered_df.empty:
            st.warning("No discretionary-denial records fall within the selected date range.")
            return

        filtered_trial_lookup = (
            filtered_df[
                [
                    "trial_number",
                    "patent_owner_grant_date",
                    "decision_date",
                    "patent_owner_technology_center_number",
                ]
            ]
            .dropna(subset=["trial_number"])
            .drop_duplicates(subset=["trial_number"])
        )
        denied_overlap_df = institution_df[
            (institution_df["trial_outcome_category"] == "Institution Denied")
            & (institution_df["decision_issue_date"] >= start_date)
            & (institution_df["decision_issue_date"] <= end_date)
        ].copy()
        denied_overlap_df = denied_overlap_df.dropna(subset=["trial_number"]).drop_duplicates(
            subset=["trial_number"]
        )
        denied_overlap_df = denied_overlap_df.merge(
            filtered_trial_lookup,
            on="trial_number",
            how="inner",
        )

        discretionary_df = filtered_df[
            filtered_df["trial_meta_trial_status_category"] == "Discretionary Denial"
        ].copy()
        discretionary_df = discretionary_df[
            discretionary_df["patent_owner_grant_date"].notna()
        ].copy()
        discretionary_df["patent_owner_grant_date"] = pd.to_datetime(
            discretionary_df["patent_owner_grant_date"]
        ).dt.date
        discretionary_df = discretionary_df[
            discretionary_df["decision_date"] >= discretionary_df["patent_owner_grant_date"]
        ].copy()
        discretionary_df["patent_age_days"] = (
            pd.to_datetime(discretionary_df["decision_date"])
            - pd.to_datetime(discretionary_df["patent_owner_grant_date"])
        ).dt.days

        discretionary_df["patent_age_years"] = discretionary_df["patent_age_days"] / 365.25
        discretionary_df["patent_age_bucket_start"] = discretionary_df["patent_age_years"].apply(
            get_patent_age_bucket_start
        )
        discretionary_df["patent_age_bucket_label"] = discretionary_df["patent_age_years"].apply(
            get_patent_age_bucket_label
        )

        left, center, right = st.columns([2, 1, 2])
        with center:
            st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)
            st.markdown(
                f"""
                <div class="total-period-box">
                    <div class="total-period-label">Total Per Period</div>
                    <div class="total-period-value">{len(discretionary_df):,}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.markdown("<div style='height: 0.5rem;'></div>", unsafe_allow_html=True)

        denied_age_df = denied_overlap_df[
            denied_overlap_df["patent_owner_grant_date"].notna()
        ].copy()
        denied_age_df["patent_owner_grant_date"] = pd.to_datetime(
            denied_age_df["patent_owner_grant_date"]
        ).dt.date
        denied_age_df = denied_age_df[
            denied_age_df["decision_date"] >= denied_age_df["patent_owner_grant_date"]
        ].copy()
        denied_age_df["patent_age_days"] = (
            pd.to_datetime(denied_age_df["decision_date"])
            - pd.to_datetime(denied_age_df["patent_owner_grant_date"])
        ).dt.days
        denied_age_df["patent_age_years"] = denied_age_df["patent_age_days"] / 365.25
        denied_age_df["patent_age_bucket_start"] = denied_age_df["patent_age_years"].apply(
            get_patent_age_bucket_start
        )
        denied_age_df["patent_age_bucket_label"] = denied_age_df["patent_age_years"].apply(
            get_patent_age_bucket_label
        )

        denied_age_counts = (
            denied_age_df[denied_age_df["patent_age_bucket_start"].notna()]
            .groupby(["patent_age_bucket_start", "patent_age_bucket_label"], as_index=False)
            .size()
            .rename(columns={"size": "total_denials"})
        )
        discretionary_age_counts = (
            discretionary_df[discretionary_df["patent_age_bucket_start"].notna()]
            .groupby(["patent_age_bucket_start", "patent_age_bucket_label"], as_index=False)
            .size()
            .rename(columns={"size": "discretionary_denials"})
        )
        full_age_counts = pd.DataFrame(
            {
                "patent_age_bucket_start": AGE_BUCKET_STARTS,
                "patent_age_bucket_label": AGE_BUCKET_LABELS,
            }
        )
        age_counts = full_age_counts.merge(
            denied_age_counts,
            on=["patent_age_bucket_start", "patent_age_bucket_label"],
            how="left",
        ).merge(
            discretionary_age_counts,
            on=["patent_age_bucket_start", "patent_age_bucket_label"],
            how="left",
        ).fillna({"total_denials": 0, "discretionary_denials": 0})
        age_counts["total_denials"] = age_counts["total_denials"].astype(int)
        age_counts["discretionary_denials"] = age_counts["discretionary_denials"].astype(int)
        age_counts["discretionary_share_of_denials"] = age_counts.apply(
            lambda row: (
                row["discretionary_denials"] / row["total_denials"]
                if row["total_denials"] > 0
                else 0
            ),
            axis=1,
        )
        age_counts_long = age_counts.melt(
            id_vars=["patent_age_bucket_start", "patent_age_bucket_label"],
            value_vars=["total_denials", "discretionary_denials"],
            var_name="series",
            value_name="count",
        )
        age_counts_long["series"] = age_counts_long["series"].map(
            {
                "total_denials": "Total Denials",
                "discretionary_denials": "Discretionary Denials",
            }
        )

        metric_col, series_col = st.columns([1, 1])
        with metric_col:
            age_metric = st.segmented_control(
                "Metric",
                options=["Counts", "Discretionary Share of Institution Denials"],
                default="Counts",
                key="age_metric",
            )
        with series_col:
            if age_metric == "Counts":
                age_series = st.segmented_control(
                    "Series",
                    options=["Total Denials", "Discretionary Denials", "Both"],
                    default="Both",
                    key="age_series",
                )
            else:
                age_series = None

        if age_metric == "Counts":
            if age_series == "Both":
                plot_df = age_counts_long
                age_fig = px.bar(
                    plot_df,
                    x="patent_age_bucket_start",
                    y="count",
                    color="series",
                    title="Age of Patents at Decision",
                    labels={
                        "patent_age_bucket_start": "Patent Age",
                        "count": "Count",
                        "series": "",
                    },
                    color_discrete_map={
                        "Total Denials": AGE_TOTAL_COLOR,
                        "Discretionary Denials": AGE_DD_COLOR,
                    },
                )
                age_fig.update_layout(barmode="overlay")
                age_fig.update_traces(opacity=0.8)
                age_fig.update_traces(hovertemplate="%{y}<extra></extra>")
            else:
                series_map = {
                    "Total Denials": ("total_denials", AGE_TOTAL_COLOR),
                    "Discretionary Denials": ("discretionary_denials", AGE_DD_COLOR),
                }
                value_column, color = series_map[age_series]
                age_fig = px.bar(
                    age_counts,
                    x="patent_age_bucket_start",
                    y=value_column,
                    title="Age of Patents at Decision",
                    labels={
                        "patent_age_bucket_start": "Patent Age",
                        value_column: "Count",
                    },
                )
                age_fig.update_traces(marker_color=color)
                age_fig.update_traces(hovertemplate="%{y}<extra></extra>")

            age_fig.update_layout(
                bargap=0.05,
                xaxis_title="Patent Age",
                yaxis_title="Discretionary Denials",
            )
        else:
            age_fig = px.bar(
                age_counts,
                x="patent_age_bucket_start",
                y="discretionary_share_of_denials",
                title="Discretionary Share of Institution Denials by Patent Age",
                labels={
                    "patent_age_bucket_start": "Patent Age",
                    "discretionary_share_of_denials": "Discretionary Share of Institution Denials",
                },
            )
            age_fig.update_traces(marker_color=AGE_DD_COLOR)
            age_fig.update_traces(hovertemplate="%{y:.0%}<extra></extra>")
            age_fig.update_layout(
                bargap=0.05,
                xaxis_title="Patent Age",
                yaxis_title="Discretionary Share of Institution Denials",
            )
        age_fig.update_xaxes(
            tickmode="array",
            tickvals=AGE_BUCKET_STARTS,
            ticktext=["0-3", "3-6", "6-9", "9-12", "12-15", "15-18", "18-20", "20+"],
            range=[-1.5, 21.5],
        )
        if age_metric == "Discretionary Share of Institution Denials":
            age_fig.update_yaxes(tickformat=".0%")

        centered_chart(age_fig)

        monthly_dd_counts = (
            discretionary_df.assign(
                decision_month=pd.to_datetime(discretionary_df["decision_date"]).dt.to_period("M").dt.to_timestamp()
            )
            .groupby("decision_month", as_index=False)
            .size()
            .rename(columns={"size": "discretionary_denials"})
        )
        monthly_denied_counts = (
            denied_overlap_df.assign(
                decision_month=pd.to_datetime(denied_overlap_df["decision_date"]).dt.to_period("M").dt.to_timestamp()
            )
            .groupby("decision_month", as_index=False)
            .size()
            .rename(columns={"size": "total_denials"})
        )
        monthly_counts = monthly_dd_counts.merge(
            monthly_denied_counts,
            on="decision_month",
            how="outer",
        ).fillna({"discretionary_denials": 0, "total_denials": 0})
        monthly_counts["discretionary_denials"] = monthly_counts["discretionary_denials"].astype(int)
        monthly_counts["total_denials"] = monthly_counts["total_denials"].astype(int)
        monthly_counts["discretionary_share_of_denials"] = monthly_counts.apply(
            lambda row: (
                row["discretionary_denials"] / row["total_denials"]
                if row["total_denials"] > 0
                else 0
            ),
            axis=1,
        )
        monthly_counts_long = monthly_counts.melt(
            id_vars=["decision_month"],
            value_vars=["total_denials", "discretionary_denials"],
            var_name="series",
            value_name="count",
        )
        monthly_counts_long["series"] = monthly_counts_long["series"].map(
            {
                "total_denials": "Total Denials",
                "discretionary_denials": "Discretionary Denials",
            }
        )

        month_metric_col, month_series_col = st.columns([1, 1])
        with month_metric_col:
            month_metric = st.segmented_control(
                "Metric",
                options=["Counts", "Discretionary Share of Institution Denials"],
                default="Counts",
                key="month_metric",
            )
        with month_series_col:
            if month_metric == "Counts":
                month_series = st.segmented_control(
                    "Series",
                    options=["Total Denials", "Discretionary Denials", "Both"],
                    default="Both",
                    key="month_series",
                )
            else:
                month_series = None

        if month_metric == "Counts":
            if month_series == "Both":
                month_fig = px.bar(
                    monthly_counts_long,
                    x="decision_month",
                    y="count",
                    color="series",
                    title="Discretionary Denials by Month",
                    labels={
                        "decision_month": "Month",
                        "count": "Count",
                        "series": "",
                    },
                    color_discrete_map={
                        "Total Denials": MONTH_TOTAL_COLOR,
                        "Discretionary Denials": MONTH_DD_COLOR,
                    },
                )
                month_fig.update_layout(barmode="overlay")
                month_fig.update_traces(opacity=0.8)
                month_fig.update_traces(hovertemplate="%{y}<extra></extra>")
            else:
                month_series_map = {
                    "Total Denials": ("total_denials", MONTH_TOTAL_COLOR),
                    "Discretionary Denials": ("discretionary_denials", MONTH_DD_COLOR),
                }
                value_column, color = month_series_map[month_series]
                month_fig = px.bar(
                    monthly_counts,
                    x="decision_month",
                    y=value_column,
                    title="Discretionary Denials by Month",
                    labels={
                        "decision_month": "Month",
                        value_column: "Count",
                    },
                )
                month_fig.update_traces(marker_color=color)
                month_fig.update_traces(hovertemplate="%{y}<extra></extra>")

            month_fig.update_layout(
                bargap=0.05,
                xaxis_title="Month",
                yaxis_title="Discretionary Denials",
            )
        else:
            month_fig = px.bar(
                monthly_counts,
                x="decision_month",
                y="discretionary_share_of_denials",
                title="Discretionary Share of Institution Denials by Month",
                labels={
                    "decision_month": "Month",
                    "discretionary_share_of_denials": "Discretionary Share of Institution Denials",
                },
            )
            month_fig.update_traces(marker_color=MONTH_DD_COLOR)
            month_fig.update_traces(hovertemplate="%{y:.0%}<extra></extra>")
            month_fig.update_layout(
                bargap=0.05,
                xaxis_title="Month",
                yaxis_title="Discretionary Share of Institution Denials",
            )
            month_fig.update_yaxes(tickformat=".0%")

        centered_chart(month_fig)

        tech_center_df = discretionary_df[
            discretionary_df["patent_owner_technology_center_number"].notna()
        ].copy()
        if not tech_center_df.empty:
            tech_center_counts = (
                tech_center_df.assign(
                    patent_owner_technology_center_number=lambda data: pd.to_numeric(
                        data["patent_owner_technology_center_number"],
                        errors="coerce",
                    )
                )
                .dropna(subset=["patent_owner_technology_center_number"])
                .assign(
                    patent_owner_technology_center_number=lambda data: data[
                        "patent_owner_technology_center_number"
                    ].astype(int)
                )
                .groupby("patent_owner_technology_center_number", as_index=False)
                .size()
                .rename(columns={"size": "discretionary_denials"})
            )
            total_denial_tech_center_counts = (
                denied_overlap_df[
                    denied_overlap_df["patent_owner_technology_center_number"].notna()
                ]
                .assign(
                    patent_owner_technology_center_number=lambda data: pd.to_numeric(
                        data["patent_owner_technology_center_number"],
                        errors="coerce",
                    )
                )
                .dropna(subset=["patent_owner_technology_center_number"])
                .assign(
                    patent_owner_technology_center_number=lambda data: data[
                        "patent_owner_technology_center_number"
                    ].astype(int)
                )
                .groupby("patent_owner_technology_center_number", as_index=False)
                .size()
                .rename(columns={"size": "total_denials"})
            )
            tech_center_counts = tech_center_counts.set_index(
                "patent_owner_technology_center_number"
            ).reindex(TECH_CENTER_ORDER, fill_value=0).reset_index()
            tech_center_counts = tech_center_counts.merge(
                total_denial_tech_center_counts,
                on="patent_owner_technology_center_number",
                how="left",
            ).fillna({"total_denials": 0})
            tech_center_counts["total_denials"] = tech_center_counts["total_denials"].astype(int)
            tech_center_counts["technology_center_label"] = tech_center_counts[
                "patent_owner_technology_center_number"
            ].astype(str)

            tech_center_view = st.segmented_control(
                "Chart Type",
                options=["Bar", "Pie"],
                default="Bar",
                key="tech_center_view",
            )
            if tech_center_view == "Bar":
                tech_center_series = st.segmented_control(
                    "Series",
                    options=["Total Denials", "Discretionary Denials", "Both"],
                    default="Both",
                    key="tech_center_series",
                )
                if tech_center_series == "Both":
                    tech_center_plot_df = tech_center_counts.melt(
                        id_vars=["technology_center_label"],
                        value_vars=["total_denials", "discretionary_denials"],
                        var_name="series",
                        value_name="count",
                    )
                    tech_center_plot_df["series"] = tech_center_plot_df["series"].map(
                        {
                            "total_denials": "Total Denials",
                            "discretionary_denials": "Discretionary Denials",
                        }
                    )
                    tech_center_fig = px.bar(
                        tech_center_plot_df,
                        x="technology_center_label",
                        y="count",
                        color="series",
                        title="Denials by Technology Center Number",
                        labels={
                            "technology_center_label": "Technology Center Number",
                            "count": "Denials",
                            "series": "",
                        },
                        color_discrete_map={
                            "Total Denials": TECH_TOTAL_COLOR,
                            "Discretionary Denials": TECH_DD_COLOR,
                        },
                        category_orders={
                            "technology_center_label": [str(value) for value in TECH_CENTER_ORDER]
                        },
                    )
                    tech_center_fig.update_layout(barmode="overlay")
                    tech_center_fig.update_traces(opacity=0.8)
                else:
                    value_column, color = (
                        ( "total_denials", TECH_TOTAL_COLOR)
                        if tech_center_series == "Total Denials"
                        else ("discretionary_denials", TECH_DD_COLOR)
                    )
                    tech_center_fig = px.bar(
                        tech_center_counts,
                        x="technology_center_label",
                        y=value_column,
                        title="Denials by Technology Center Number",
                        labels={
                            "technology_center_label": "Technology Center Number",
                            value_column: "Denials",
                        },
                        category_orders={
                            "technology_center_label": [str(value) for value in TECH_CENTER_ORDER]
                        },
                    )
                    tech_center_fig.update_traces(marker_color=color)
                tech_center_fig.update_layout(
                    bargap=0.1,
                    xaxis_title="Technology Center Number",
                    yaxis_title="Denials",
                    xaxis=dict(
                        tickmode="array",
                        tickvals=[str(value) for value in TECH_CENTER_ORDER],
                        ticktext=[str(value) for value in TECH_CENTER_ORDER],
                        type="category",
                    ),
                )
            else:
                tech_center_fig = px.pie(
                    tech_center_counts,
                    names="technology_center_label",
                    values="discretionary_denials",
                    title="Discretionary Denials by Technology Center Number",
                )
                tech_center_fig.update_traces(hovertemplate="%{label}<extra></extra>")
            if tech_center_view == "Bar":
                tech_center_fig.update_traces(hovertemplate="%{y}<extra></extra>")
            centered_chart(tech_center_fig)

        repeated_pairs = (
            filtered_df[
                (filtered_df["trial_meta_trial_status_category"] == "Discretionary Denial")
                & filtered_df["regular_petitioner_real_party_in_interest_name"].notna()
                & filtered_df["patent_owner_patent_number"].notna()
            ]
            .groupby(
                [
                    "regular_petitioner_real_party_in_interest_name",
                    "patent_owner_patent_number",
                ],
                as_index=False,
            )
            .agg(
                petition_count=("trial_number", "nunique"),
                trial_numbers=("trial_number", lambda values: sorted(set(values))),
            )
            .query("petition_count > 1")
            .sort_values(
                by=[
                    "petition_count",
                    "regular_petitioner_real_party_in_interest_name",
                    "patent_owner_patent_number",
                ],
                ascending=[False, True, True],
            )
        )
        if not repeated_pairs.empty:
            repeated_pairs["trial_numbers"] = repeated_pairs["trial_numbers"].apply(
                lambda values: ", ".join(values)
            )
            st.subheader("Repeated Petitioner / Patent Pairs")
            st.dataframe(
                repeated_pairs.rename(
                    columns={
                        "regular_petitioner_real_party_in_interest_name": "Petitioner",
                        "patent_owner_patent_number": "Patent Number",
                        "petition_count": "Petition Count",
                        "trial_numbers": "Trial Numbers",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

if __name__ == "__main__":
    main()
