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
MONTH_TOTAL_COLOR = "#c8b089"
MONTH_DD_COLOR = "#7a1f3d"
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
            trial_meta_accorded_filing_date AS accorded_filing_date,
            trial_meta_latest_decision_date AS latest_decision_date,
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


@st.cache_data(ttl=600)
def load_final_written_timing_data():
    query = """
        SELECT
            p.trial_number,
            p.trial_meta_institution_decision_date AS institution_decision_date,
            fwd.document_filing_date AS final_written_decision_date
        FROM proceedings p
        JOIN (
            SELECT DISTINCT trial_number
            FROM trial_decisions
            WHERE trial_outcome_category = 'Institution Granted'
              AND trial_number IS NOT NULL
        ) granted
            ON p.trial_number = granted.trial_number
        JOIN (
            SELECT
                trial_number,
                MIN(document_filing_date) AS document_filing_date
            FROM final_written_decision_documents
            WHERE document_filing_date IS NOT NULL
              AND document_type_description_text = 'Final Written Decision:  original'
            GROUP BY trial_number
        ) fwd
            ON p.trial_number = fwd.trial_number
        WHERE p.trial_meta_trial_status_category = 'Final Written Decision'
          AND p.trial_meta_institution_decision_date IS NOT NULL
        ORDER BY final_written_decision_date DESC, p.trial_number
    """

    with get_connection() as conn:
        return pd.read_sql(query, conn)


@st.cache_data(ttl=600)
def load_discretionary_issue_data():
    query = """
        SELECT
            g.trial_number,
            p.trial_meta_institution_decision_date AS decision_date,
            p.patent_owner_patent_number,
            p.regular_petitioner_real_party_in_interest_name,
            g.parallel_litigation_314a,
            g.serial_petitions_314a,
            g.settled_expectations_314a,
            g.previous_art_or_arguments_325d,
            g.estoppel_315e,
            g.analysis_json
        FROM discretionary_denials_granular g
        JOIN proceedings p
            ON g.trial_number = p.trial_number
        WHERE p.trial_meta_institution_decision_date IS NOT NULL
        ORDER BY p.trial_meta_institution_decision_date DESC, g.trial_number
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
        issue_df = load_discretionary_issue_data()
    except Exception as exc:
        st.error(f"Failed to load data: {exc}")
        return

    try:
        final_written_timing_df = load_final_written_timing_data()
    except Exception:
        final_written_timing_df = pd.DataFrame()

    if institution_df.empty:
        st.warning("No institution-rate records are available in the appeals table.")
        return

    if df.empty:
        st.warning("No IPR records are available.")
        return

    if "accorded_filing_date" in df.columns:
        df["accorded_filing_date"] = pd.to_datetime(df["accorded_filing_date"]).dt.date
    if "latest_decision_date" in df.columns:
        df["latest_decision_date"] = pd.to_datetime(df["latest_decision_date"]).dt.date

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
        st.header("Institution Rates and Timing")
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
                key="institution_start_date",
            )
        with institution_date_col2:
            institution_end_date = st.date_input(
                "End Date",
                value=institution_max_date,
                min_value=institution_min_date,
                max_value=institution_max_date,
                key="institution_end_date",
            )

        if institution_start_date > institution_end_date:
            st.warning("Institution Start Date must be on or before Institution End Date.")
            return

        institution_view = st.segmented_control(
            "View",
            options=["Month", "Quarter (Calendar)", "Year (FY)"],
            default="Month",
            key="institution_view",
        )

        institution_plot_df = institution_df[
            (institution_df["decision_issue_date"] >= institution_start_date)
            & (institution_df["decision_issue_date"] <= institution_end_date)
        ].copy()

        if institution_plot_df.empty:
            st.warning("No institution-rate records fall within the selected date range.")
            return

        institution_plot_df["institution_group"] = institution_plot_df["trial_outcome_category"].map(
            {
                "Institution Granted": "Referred",
                "Institution Denied": "Denied",
            }
        )
        if institution_view == "Month":
            period_column = "decision_month"
            period_label = "Month"
            period_title = "Institution Outcomes by Month"
            institution_plot_df[period_column] = pd.to_datetime(
                institution_plot_df["decision_issue_date"]
            ).dt.to_period("M").dt.to_timestamp()
        elif institution_view == "Quarter (Calendar)":
            period_column = "decision_quarter"
            period_label = "Quarter"
            period_title = "Institution Outcomes by Quarter"
            institution_plot_df[period_column] = pd.to_datetime(
                institution_plot_df["decision_issue_date"]
            ).dt.to_period("Q").astype(str)
        else:
            period_column = "decision_year"
            period_label = "Year"
            period_title = "Institution Outcomes by Year"
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
        if institution_view == "Year (FY)":
            fiscal_years = sorted(institution_status_counts[period_column].unique())
            institution_fig.update_xaxes(
                tickmode="array",
                tickvals=fiscal_years,
                ticktext=[f"FY{str(year)[-2:]}" for year in fiscal_years],
            )
        centered_chart(institution_fig)

        timing_view = st.segmented_control(
            "Timing View",
            options=["Month", "Quarter (Calendar)", "Year (FY)"],
            default="Month",
            key="timing_view",
        )
        final_written_df = final_written_timing_df.copy()
        if not final_written_df.empty:
            final_written_df["institution_decision_date"] = pd.to_datetime(
                final_written_df["institution_decision_date"]
            ).dt.date
            final_written_df["final_written_decision_date"] = pd.to_datetime(
                final_written_df["final_written_decision_date"]
            ).dt.date
        final_written_df = final_written_df[
            final_written_df["final_written_decision_date"] >= final_written_df["institution_decision_date"]
        ].copy()
        final_written_df = final_written_df[
            (final_written_df["final_written_decision_date"] >= institution_start_date)
            & (final_written_df["final_written_decision_date"] <= institution_end_date)
        ].copy()

        if not final_written_df.empty:
            final_written_df["time_from_institution_to_final_written_decision_months"] = (
                (
                    pd.to_datetime(final_written_df["final_written_decision_date"])
                    - pd.to_datetime(final_written_df["institution_decision_date"])
                ).dt.days / 30.44
            )
            if timing_view == "Month":
                timing_period_column = "decision_month"
                timing_period_label = "Month"
                timing_title = "Average Time from Institution Decision to Final Written Decision by Month"
                final_written_df[timing_period_column] = pd.to_datetime(
                    final_written_df["final_written_decision_date"]
                ).dt.to_period("M").dt.to_timestamp()
            elif timing_view == "Quarter (Calendar)":
                timing_period_column = "decision_quarter"
                timing_period_label = "Quarter"
                timing_title = "Average Time from Institution Decision to Final Written Decision by Quarter"
                final_written_df[timing_period_column] = pd.to_datetime(
                    final_written_df["final_written_decision_date"]
                ).dt.to_period("Q").astype(str)
            else:
                timing_period_column = "decision_fiscal_year"
                timing_period_label = "Year"
                timing_title = "Average Time from Institution Decision to Final Written Decision by Year"
                final_written_df[timing_period_column] = pd.to_datetime(
                    final_written_df["final_written_decision_date"]
                ).dt.year + (
                    pd.to_datetime(final_written_df["final_written_decision_date"]).dt.month >= 10
                ).astype(int)

            final_written_periods = (
                final_written_df.groupby(timing_period_column, as_index=False)["time_from_institution_to_final_written_decision_months"]
                .mean()
            )
            final_written_fig = px.bar(
                final_written_periods,
                x=timing_period_column,
                y="time_from_institution_to_final_written_decision_months",
                title=timing_title,
                labels={
                    timing_period_column: timing_period_label,
                    "time_from_institution_to_final_written_decision_months": "Average Months",
                },
            )
            final_written_fig.update_traces(marker_color="#33265f")
            final_written_fig.update_traces(hovertemplate="%{y:.1f}<extra></extra>")
            final_written_fig.update_layout(
                bargap=0.05,
                xaxis_title=timing_period_label,
                yaxis_title="Average Months",
            )
            if timing_view == "Year (FY)":
                fiscal_years = sorted(final_written_periods[timing_period_column].unique())
                final_written_fig.update_xaxes(
                    tickmode="array",
                    tickvals=fiscal_years,
                    ticktext=[f"FY{str(year)[-2:]}" for year in fiscal_years],
                )
            centered_chart(final_written_fig)

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

        all_ipr_age_df = filtered_df[
            filtered_df["patent_owner_grant_date"].notna()
        ].copy()
        all_ipr_age_df["patent_owner_grant_date"] = pd.to_datetime(
            all_ipr_age_df["patent_owner_grant_date"]
        ).dt.date
        all_ipr_age_df = all_ipr_age_df[
            all_ipr_age_df["decision_date"] >= all_ipr_age_df["patent_owner_grant_date"]
        ].copy()
        all_ipr_age_df["patent_age_days"] = (
            pd.to_datetime(all_ipr_age_df["decision_date"])
            - pd.to_datetime(all_ipr_age_df["patent_owner_grant_date"])
        ).dt.days
        all_ipr_age_df["patent_age_years"] = all_ipr_age_df["patent_age_days"] / 365.25
        all_ipr_age_df["patent_age_bucket_start"] = all_ipr_age_df["patent_age_years"].apply(
            get_patent_age_bucket_start
        )
        all_ipr_age_df["patent_age_bucket_label"] = all_ipr_age_df["patent_age_years"].apply(
            get_patent_age_bucket_label
        )

        discretionary_age_counts = (
            discretionary_df[discretionary_df["patent_age_bucket_start"].notna()]
            .groupby(["patent_age_bucket_start", "patent_age_bucket_label"], as_index=False)
            .size()
            .rename(columns={"size": "discretionary_denials"})
        )
        all_ipr_age_counts = (
            all_ipr_age_df[all_ipr_age_df["patent_age_bucket_start"].notna()]
            .groupby(["patent_age_bucket_start", "patent_age_bucket_label"], as_index=False)
            .size()
            .rename(columns={"size": "total_iprs"})
        )
        full_age_counts = pd.DataFrame(
            {
                "patent_age_bucket_start": AGE_BUCKET_STARTS,
                "patent_age_bucket_label": AGE_BUCKET_LABELS,
            }
        )
        age_counts = full_age_counts.merge(
            all_ipr_age_counts,
            on=["patent_age_bucket_start", "patent_age_bucket_label"],
            how="left",
        ).merge(
            discretionary_age_counts,
            on=["patent_age_bucket_start", "patent_age_bucket_label"],
            how="left",
        ).fillna({"total_iprs": 0, "discretionary_denials": 0})
        age_counts["total_iprs"] = age_counts["total_iprs"].astype(int)
        age_counts["discretionary_denials"] = age_counts["discretionary_denials"].astype(int)
        age_counts["denial_rate"] = age_counts.apply(
            lambda row: (
                row["discretionary_denials"] / row["total_iprs"]
                if row["total_iprs"] > 0
                else 0
            ),
            axis=1,
        )
        age_counts_long = age_counts.melt(
            id_vars=["patent_age_bucket_start", "patent_age_bucket_label"],
            value_vars=["total_iprs", "discretionary_denials"],
            var_name="series",
            value_name="count",
        )
        age_counts_long["series"] = age_counts_long["series"].map(
            {
                "total_iprs": "Total IPRs",
                "discretionary_denials": "Discretionary Denials",
            }
        )

        metric_col, series_col = st.columns([1, 1])
        with metric_col:
            age_metric = st.segmented_control(
                "Metric",
                options=["Counts", "Discretionary Denial Rate"],
                default="Counts",
                key="age_metric",
            )
        with series_col:
            if age_metric == "Counts":
                age_series = st.segmented_control(
                    "Series",
                    options=["Total IPRs", "Discretionary Denials", "Both"],
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
                        "Total IPRs": AGE_TOTAL_COLOR,
                        "Discretionary Denials": AGE_DD_COLOR,
                    },
                )
                age_fig.update_layout(barmode="overlay")
                age_fig.update_traces(opacity=0.8)
                age_fig.update_traces(hovertemplate="%{y}<extra></extra>")
            else:
                series_map = {
                    "Total IPRs": ("total_iprs", AGE_TOTAL_COLOR),
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
                y="denial_rate",
                title="Discretionary Denial Rate by Patent Age",
                labels={
                    "patent_age_bucket_start": "Patent Age",
                    "denial_rate": "Discretionary Denial Rate",
                },
            )
            age_fig.update_traces(marker_color=AGE_DD_COLOR)
            age_fig.update_traces(hovertemplate="%{y:.0%}<extra></extra>")
            age_fig.update_layout(
                bargap=0.05,
                xaxis_title="Patent Age",
                yaxis_title="Discretionary Denial Rate",
            )
        age_fig.update_xaxes(
            tickmode="array",
            tickvals=AGE_BUCKET_STARTS,
            ticktext=["0-3", "3-6", "6-9", "9-12", "12-15", "15-18", "18-20", "20+"],
            range=[-1.5, 21.5],
        )
        if age_metric == "Discretionary Denial Rate":
            age_fig.update_yaxes(tickformat=".0%")

        centered_chart(age_fig)

        monthly_ipr_counts = (
            filtered_df.assign(
                decision_month=pd.to_datetime(filtered_df["decision_date"]).dt.to_period("M").dt.to_timestamp()
            )
            .groupby("decision_month", as_index=False)
            .size()
            .rename(columns={"size": "total_iprs"})
        )
        monthly_dd_counts = (
            discretionary_df.assign(
                decision_month=pd.to_datetime(discretionary_df["decision_date"]).dt.to_period("M").dt.to_timestamp()
            )
            .groupby("decision_month", as_index=False)
            .size()
            .rename(columns={"size": "discretionary_denials"})
        )
        monthly_counts = monthly_ipr_counts.merge(
            monthly_dd_counts,
            on="decision_month",
            how="left",
        ).fillna({"discretionary_denials": 0})
        monthly_counts["discretionary_denials"] = monthly_counts["discretionary_denials"].astype(int)
        monthly_counts["denial_rate"] = monthly_counts.apply(
            lambda row: (
                row["discretionary_denials"] / row["total_iprs"]
                if row["total_iprs"] > 0
                else 0
            ),
            axis=1,
        )
        monthly_counts_long = monthly_counts.melt(
            id_vars=["decision_month"],
            value_vars=["total_iprs", "discretionary_denials"],
            var_name="series",
            value_name="count",
        )
        monthly_counts_long["series"] = monthly_counts_long["series"].map(
            {
                "total_iprs": "Total IPRs",
                "discretionary_denials": "Discretionary Denials",
            }
        )

        month_metric_col, month_series_col = st.columns([1, 1])
        with month_metric_col:
            month_metric = st.segmented_control(
                "Metric",
                options=["Counts", "Discretionary Denial Rate"],
                default="Counts",
                key="month_metric",
            )
        with month_series_col:
            if month_metric == "Counts":
                month_series = st.segmented_control(
                    "Series",
                    options=["Total IPRs", "Discretionary Denials", "Both"],
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
                        "Total IPRs": MONTH_TOTAL_COLOR,
                        "Discretionary Denials": MONTH_DD_COLOR,
                    },
                )
                month_fig.update_layout(barmode="overlay")
                month_fig.update_traces(opacity=0.8)
                month_fig.update_traces(hovertemplate="%{y}<extra></extra>")
            else:
                month_series_map = {
                    "Total IPRs": ("total_iprs", MONTH_TOTAL_COLOR),
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
                y="denial_rate",
                title="Discretionary Denial Rate by Month",
                labels={
                    "decision_month": "Month",
                    "denial_rate": "Discretionary Denial Rate",
                },
            )
            month_fig.update_traces(marker_color=MONTH_DD_COLOR)
            month_fig.update_traces(hovertemplate="%{y:.0%}<extra></extra>")
            month_fig.update_layout(
                bargap=0.05,
                xaxis_title="Month",
                yaxis_title="Discretionary Denial Rate",
            )
            month_fig.update_yaxes(tickformat=".0%")

        centered_chart(month_fig)

        if not issue_df.empty:
            issue_plot_df = issue_df.copy()
            issue_plot_df["decision_date"] = pd.to_datetime(issue_plot_df["decision_date"]).dt.date
            issue_plot_df = issue_plot_df[
                (issue_plot_df["decision_date"] >= start_date)
                & (issue_plot_df["decision_date"] <= end_date)
            ].copy()

            issue_labels = {
                "parallel_litigation_314a": "314(a) Parallel Litigation",
                "serial_petitions_314a": "314(a) Serial Petitions",
                "settled_expectations_314a": "314(a) Settled Expectations",
                "previous_art_or_arguments_325d": "325(d)",
                "estoppel_315e": "315(e)",
            }

            issue_counts = pd.DataFrame(
                {
                    "issue": list(issue_labels.values()),
                    "count": [
                        int(issue_plot_df["parallel_litigation_314a"].fillna(False).sum()),
                        int(issue_plot_df["serial_petitions_314a"].fillna(False).sum()),
                        int(issue_plot_df["settled_expectations_314a"].fillna(False).sum()),
                        int(issue_plot_df["previous_art_or_arguments_325d"].fillna(False).sum()),
                        int(issue_plot_df["estoppel_315e"].fillna(False).sum()),
                    ],
                }
            )
            issue_color_map = {
                "314(a) Parallel Litigation": "#8b1e3f",
                "314(a) Serial Petitions": "#c27c2c",
                "314(a) Settled Expectations": "#6c8a3a",
                "325(d)": "#2f6f8f",
                "315(e)": "#5b4b8a",
            }

            issue_fig = px.bar(
                issue_counts,
                x="count",
                y="issue",
                orientation="h",
                color="issue",
                title="Issue Breakdown in Discretionary Denial Briefs",
                labels={
                    "count": "Discretionary Denials",
                    "issue": "",
                },
                color_discrete_map=issue_color_map,
            )
            issue_fig.update_traces(hovertemplate="%{x}<extra></extra>")
            issue_fig.update_layout(
                xaxis_title="Discretionary Denials",
                yaxis_title="",
                showlegend=False,
                yaxis=dict(categoryorder="array", categoryarray=list(reversed(list(issue_labels.values())))),
            )
            centered_chart(issue_fig)

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
            total_ipr_tech_center_counts = (
                filtered_df[
                    filtered_df["patent_owner_technology_center_number"].notna()
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
                .rename(columns={"size": "total_iprs"})
            )
            tech_center_counts = tech_center_counts.set_index(
                "patent_owner_technology_center_number"
            ).reindex(TECH_CENTER_ORDER, fill_value=0).reset_index()
            tech_center_counts = tech_center_counts.merge(
                total_ipr_tech_center_counts,
                on="patent_owner_technology_center_number",
                how="left",
            ).fillna({"total_iprs": 0})
            tech_center_counts["total_iprs"] = tech_center_counts["total_iprs"].astype(int)
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
                    options=["Total IPRs", "Discretionary Denials", "Both"],
                    default="Both",
                    key="tech_center_series",
                )
                if tech_center_series == "Both":
                    tech_center_plot_df = tech_center_counts.melt(
                        id_vars=["technology_center_label"],
                        value_vars=["total_iprs", "discretionary_denials"],
                        var_name="series",
                        value_name="count",
                    )
                    tech_center_plot_df["series"] = tech_center_plot_df["series"].map(
                        {
                            "total_iprs": "Total IPRs",
                            "discretionary_denials": "Discretionary Denials",
                        }
                    )
                    tech_center_fig = px.bar(
                        tech_center_plot_df,
                        x="technology_center_label",
                        y="count",
                        color="series",
                        title="Proceedings by Technology Center Number",
                        labels={
                            "technology_center_label": "Technology Center Number",
                            "count": "Proceedings",
                            "series": "",
                        },
                        color_discrete_map={
                            "Total IPRs": TECH_TOTAL_COLOR,
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
                        ("total_iprs", TECH_TOTAL_COLOR)
                        if tech_center_series == "Total IPRs"
                        else ("discretionary_denials", TECH_DD_COLOR)
                    )
                    tech_center_fig = px.bar(
                        tech_center_counts,
                        x="technology_center_label",
                        y=value_column,
                        title="Proceedings by Technology Center Number",
                        labels={
                            "technology_center_label": "Technology Center Number",
                            value_column: "Proceedings",
                        },
                        category_orders={
                            "technology_center_label": [str(value) for value in TECH_CENTER_ORDER]
                        },
                    )
                    tech_center_fig.update_traces(marker_color=color)
                tech_center_fig.update_layout(
                    bargap=0.1,
                    xaxis_title="Technology Center Number",
                    yaxis_title="Proceedings",
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

        serial_petition_table = issue_plot_df[
            issue_plot_df["serial_petitions_314a"].fillna(False)
        ].copy()
        if not serial_petition_table.empty:
            serial_petition_table["petitioner_label"] = serial_petition_table[
                "regular_petitioner_real_party_in_interest_name"
            ].fillna("Unknown")
            serial_petition_table = (
                serial_petition_table[
                    serial_petition_table["patent_owner_patent_number"].notna()
                ]
                .groupby("patent_owner_patent_number", as_index=False)
                .agg(
                    challenge_count=("trial_number", "nunique"),
                    petitioners=(
                        "petitioner_label",
                        lambda values: ", ".join(sorted(set(values))),
                    ),
                    trial_numbers=(
                        "trial_number",
                        lambda values: ", ".join(sorted(set(values))),
                    ),
                    latest_decision_date=("decision_date", "max"),
                )
                .sort_values(
                    by=["challenge_count", "latest_decision_date", "patent_owner_patent_number"],
                    ascending=[False, False, True],
                )
            )
            st.subheader("Patents Flagged for 314(a) Serial Petitions")
            st.dataframe(
                serial_petition_table[
                    [
                        "patent_owner_patent_number",
                        "challenge_count",
                        "petitioners",
                        "trial_numbers",
                        "latest_decision_date",
                    ]
                ].rename(
                    columns={
                        "patent_owner_patent_number": "Patent Number",
                        "challenge_count": "Challenge Count",
                        "petitioners": "Petitioners",
                        "trial_numbers": "Trial Numbers",
                        "latest_decision_date": "Latest Decision Date",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

if __name__ == "__main__":
    main()
