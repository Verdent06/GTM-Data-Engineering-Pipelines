"""
MochaCare GTM Engine — Streamlit dashboard for qualified home-health leads (FL / TX / AZ).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


DATA_FILE = Path(__file__).resolve().parent / "healthcare_agencies_fl_tx_az_leads.csv"

# Brand-adjacent palette (works on light & dark Streamlit themes)
ACCENT = "#0d9488"  # teal
ACCENT_MUTED = "#5eead4"
SURFACE = "rgba(13, 148, 136, 0.08)"


def _inject_styles() -> None:
    st.markdown(
        f"""
        <style>
            @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&display=swap');
            html, body, [class*="css"] {{
                font-family: 'DM Sans', 'Segoe UI', system-ui, sans-serif;
            }}
            .block-container {{
                padding-top: 2rem;
                padding-bottom: 3rem;
                max-width: 1400px;
            }}
            .mc-hero {{
                background: linear-gradient(135deg, {SURFACE} 0%, transparent 55%);
                border-radius: 16px;
                padding: 1.5rem 1.75rem 1.25rem;
                margin-bottom: 1.5rem;
                border: 1px solid rgba(13, 148, 136, 0.2);
            }}
            .mc-hero h1 {{
                font-size: 1.85rem;
                font-weight: 700;
                letter-spacing: -0.02em;
                margin: 0 0 0.35rem 0;
                line-height: 1.2;
            }}
            .mc-hero p {{
                margin: 0;
                opacity: 0.88;
                font-size: 1rem;
                line-height: 1.5;
            }}
            .mc-badge {{
                display: inline-block;
                font-size: 0.7rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.08em;
                color: {ACCENT};
                margin-bottom: 0.5rem;
            }}
            div[data-testid="stMetric"] {{
                background: {SURFACE};
                border: 1px solid rgba(13, 148, 136, 0.15);
                border-radius: 12px;
                padding: 1rem 1.1rem;
            }}
            div[data-testid="stMetric"] label {{
                font-size: 0.8rem !important;
            }}
            section[data-testid="stSidebar"] {{
                border-right: 1px solid rgba(128,128,128,0.15);
            }}
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data
def load_leads(path_str: str) -> pd.DataFrame:
    p = Path(path_str)
    if not p.is_file():
        raise FileNotFoundError(f"Data file not found: {p}")
    df = pd.read_csv(p)
    df.columns = [c.strip() for c in df.columns]
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.strip().str.upper()
    return df


def _numeric_star(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace("-", "", regex=False), errors="coerce")


def _has_email(row: pd.Series) -> bool:
    v = row.get("owner_email")
    if pd.isna(v):
        return False
    s = str(v).strip()
    if not s or s.lower() in ("none", "nan", "null"):
        return False
    return "@" in s


def apply_filters(
    df: pd.DataFrame,
    states: list[str],
    min_score: int,
    emails_only: bool,
) -> pd.DataFrame:
    out = df.copy()
    if states and "state" in out.columns:
        out = out[out["state"].isin(states)]
    if "lead_score" in out.columns:
        out = out[out["lead_score"] >= min_score]
    if emails_only:
        mask = out.apply(_has_email, axis=1)
        out = out[mask]
    return out.reset_index(drop=True)


def hubspot_ready_df(df: pd.DataFrame) -> pd.DataFrame:
    """Drop internal IDs; keep founder-facing columns."""
    cols = [c for c in df.columns if c != "cms_ccn"]
    h = df[cols].copy()
    rename = {
        "agency_name": "Company Name",
        "owner_name": "Contact Full Name",
        "owner_title": "Job Title",
        "owner_email": "Email",
        "phone": "Phone",
        "address": "Street Address",
        "city": "City",
        "state": "State",
        "zip_code": "Postal Code",
        "website": "Website URL",
        "lead_score": "Lead Score",
        "star_rating": "CMS Star Rating",
        "ownership_type": "Ownership Type",
        "email_source": "Email Source",
    }
    return h.rename(columns={k: v for k, v in rename.items() if k in h.columns})


def main() -> None:
    st.set_page_config(
        page_title="MochaCare GTM Engine",
        page_icon="◆",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    _inject_styles()

    try:
        df_raw = load_leads(str(DATA_FILE))
    except FileNotFoundError as e:
        st.error(str(e))
        st.stop()

    all_states = sorted(df_raw["state"].dropna().unique().tolist()) if "state" in df_raw.columns else []
    default_states = [s for s in ("FL", "TX", "AZ") if s in all_states]
    if not default_states:
        default_states = all_states

    with st.sidebar:
        st.markdown("### Filters")
        st.caption("Refine the qualified agency universe for outreach.")
        selected_states = st.multiselect(
            "Filter by State",
            options=all_states if all_states else ["FL", "TX", "AZ"],
            default=default_states or all_states,
        )
        min_score = st.slider(
            "Minimum Lead Score",
            min_value=80,
            max_value=100,
            value=80,
            help="Pipeline scores 0–100; default matches VIP threshold.",
        )
        emails_only = st.checkbox(
            "Only show leads with extracted emails",
            value=False,
            help="Requires a non-empty owner email (Hunter / verified).",
        )
        st.divider()
        st.caption(f"**Source file:** `{DATA_FILE.name}`")
        st.caption(f"**Rows loaded:** {len(df_raw):,}")

    filtered = apply_filters(df_raw, selected_states, min_score, emails_only)
    display_df = filtered.drop(columns=["cms_ccn"], errors="ignore")

    # —— Hero ——
    st.markdown(
        """
        <div class="mc-hero">
            <div class="mc-badge">YC W26 · HealthTech</div>
            <h1>MochaCare GTM Engine</h1>
            <p>Qualified home health agency leads — FL, TX & AZ — scored and enriched for founder review.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # —— KPIs ——
    n_total = len(filtered)
    email_mask = filtered.apply(_has_email, axis=1) if len(filtered) else pd.Series(dtype=bool)
    n_emails = int(email_mask.sum()) if len(filtered) else 0
    stars = _numeric_star(filtered["star_rating"]) if "star_rating" in filtered.columns else pd.Series(dtype=float)
    avg_star = float(stars.mean()) if stars.notna().any() else float("nan")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric(
            "Total qualified agencies",
            f"{n_total:,}",
            help="Count after state, score, and email filters.",
        )
    with c2:
        st.metric(
            "Emails extracted",
            f"{n_emails:,}",
            delta=None if n_total == 0 else f"{100 * n_emails / n_total:.0f}% of view",
            help="Rows with a valid-looking email in owner_email.",
        )
    with c3:
        st.metric(
            "Avg. CMS star rating",
            f"{avg_star:.2f}" if pd.notna(avg_star) else "—",
            help="Mean of numeric star ratings in the current filter (excludes “-”).",
        )

    st.divider()

    # —— Chart + table (wide layout) ——
    left, right = st.columns([1, 1.35])

    with left:
        st.subheader("Agencies by state")
        if "state" in filtered.columns and len(filtered):
            counts = (
                filtered.groupby("state", as_index=False)
                .size()
                .rename(columns={"size": "agencies"})
                .sort_values("agencies", ascending=True)
            )
            fig = px.bar(
                counts,
                x="agencies",
                y="state",
                orientation="h",
                color="agencies",
                color_continuous_scale=[[0, "#ccfbf1"], [0.5, "#2dd4bf"], [1, "#0f766e"]],
                labels={"agencies": "Agencies", "state": "State"},
            )
            fig.update_layout(
                margin=dict(l=8, r=8, t=8, b=8),
                height=max(320, 48 + 36 * len(counts)),
                xaxis_title=None,
                yaxis_title=None,
                coloraxis_showscale=False,
                showlegend=False,
            )
            fig.update_traces(marker_line_width=0)
            try:
                st.plotly_chart(fig, use_container_width=True, theme="streamlit")
            except TypeError:
                st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No rows to chart — adjust filters.")

        st.markdown("<div style='height:1.25rem'></div>", unsafe_allow_html=True)
        st.subheader("Lead Score Distribution")
        if "lead_score" in filtered.columns and len(filtered):
            score_counts = (
                filtered.groupby("lead_score", as_index=False)
                .size()
                .rename(columns={"size": "agencies"})
                .sort_values("lead_score")
            )
            score_counts["lead_score_cat"] = score_counts["lead_score"].astype(str)
            fig_scores = px.bar(
                score_counts,
                x="lead_score_cat",
                y="agencies",
                labels={"lead_score_cat": "Lead score", "agencies": "Agencies"},
                color_discrete_sequence=[ACCENT],
            )
            fig_scores.update_layout(
                margin=dict(l=8, r=8, t=8, b=8),
                height=max(260, 52 + 28 * min(len(score_counts), 24)),
                xaxis_title=None,
                yaxis_title=None,
                showlegend=False,
                bargap=0.25,
            )
            fig_scores.update_traces(
                marker_line_width=0,
                marker_color=ACCENT,
                opacity=0.92,
            )
            fig_scores.update_xaxes(type="category")
            try:
                st.plotly_chart(fig_scores, use_container_width=True, theme="streamlit")
            except TypeError:
                st.plotly_chart(fig_scores, use_container_width=True)
        else:
            st.info("No lead scores to chart — adjust filters.")

    with right:
        st.subheader("Lead roster")
        st.caption("`cms_ccn` omitted from this view for clarity.")
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=min(560, 56 + 36 * min(len(display_df), 15)),
        )

    st.divider()

    # —— Export ——
    hubspot_df = hubspot_ready_df(filtered)
    csv_bytes = hubspot_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download HubSpot-ready CSV",
        data=csv_bytes,
        file_name="mochacare_hubspot_leads_filtered.csv",
        mime="text/csv",
        type="primary",
        use_container_width=False,
    )
    st.caption(
        "Exports the **currently filtered** table with founder-friendly column names. "
        "Re-import into HubSpot or your sequencing tool."
    )


if __name__ == "__main__":
    main()
