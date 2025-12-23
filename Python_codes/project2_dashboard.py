# ==================================================
# Project 2: Healthcare Data Analysis (Streamlit)
# ==================================================

import json
import boto3
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# =========================
# Page Config
# =========================
st.set_page_config(
    page_title="Healthcare Data Analysis",
    page_icon="🩺",
    layout="wide"
)
st.title("🩺 Project 2: Healthcare Data Analysis")

# =========================
# Constants
# =========================
AWS_REGION = "us-east-1"
LAMBDA_NAME = "project2-googledrive-s3-api"

S3_GOLD = "s3://project2-healthcare-gold-bucket/"
PATHS = {
    "bed": f"{S3_GOLD}dim_bed/",
    "nurse": f"{S3_GOLD}dim_nurse/",
    "staff": f"{S3_GOLD}fact_staff/"
}

COLORS = {
    "Beds": "#B0230A",
    "Patients": "#FB5F5F",
    "Medicare": "#FB5F5F",
    "Medicaid": "#5F5FFB"
}

# =========================
# Lambda Trigger
# =========================
lambda_client = boto3.client("lambda", region_name=AWS_REGION)

if st.button("▶ Run Google Drive → S3 Ingestion"):
    with st.spinner("Running Lambda..."):
        res = lambda_client.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps({"source": "streamlit"})
        )
        st.info(json.loads(res["Payload"].read()).get("body", "No response"))

# =========================
# Cached Loaders
# =========================
@st.cache_data(show_spinner=True)
def load_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

@st.cache_data(show_spinner=False)
def join_tables(bed: pd.DataFrame, staff: pd.DataFrame) -> pd.DataFrame:
    return bed.merge(
        staff,
        left_on="PROVIDER_NUM",
        right_on="PROVNUM",
        how="inner",
        suffixes=("_bed", "_staff")
    )

dim_bed   = load_parquet(PATHS["bed"])
dim_nurse = load_parquet(PATHS["nurse"])
fact_staff= load_parquet(PATHS["staff"])

provider_df = join_tables(dim_bed, fact_staff)
st.divider()

# =================================================
# Metric 1: Beds & Patients by State
# =================================================
st.header("Beds & Patients by State")

ptype = st.selectbox(
    "Select PROVIDER_TYPE:",
    dim_bed["PROVIDER_TYPE"].unique()
)

metric = st.radio(
    "Select Metric:",
    ["Both", "Beds", "Patients"],
    horizontal=True
)

agg = (
    dim_bed.query("PROVIDER_TYPE == @ptype")
    .groupby("STATE", as_index=False)
    .agg({
        "NUMBER_OF_CERTIFIED_BEDS": "sum",
        "AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_TOTAL": "sum"
    })
)

if metric == "Both":
    plot_df = agg.melt(
        "STATE",
        value_vars=[
            "NUMBER_OF_CERTIFIED_BEDS",
            "AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_TOTAL"
        ],
        var_name="Metric",
        value_name="Count"
    )
    fig = px.bar(
        plot_df,
        x="STATE",
        y="Count",
        color="Metric",
        barmode="group",
        title=f"Beds & Patients per State ({ptype})",
        color_discrete_map={
            "NUMBER_OF_CERTIFIED_BEDS": COLORS["Beds"],
            "AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_TOTAL": COLORS["Patients"]
        }
    )
else:
    col = "NUMBER_OF_CERTIFIED_BEDS" if metric == "Beds" else "AVERAGE_NUMBER_OF_RESIDENTS_PER_DAY_TOTAL"
    fig = px.bar(
        agg,
        x="STATE",
        y=col,
        title=f"{metric} per State ({ptype})",
        color_discrete_sequence=[COLORS[metric]]
    )

st.plotly_chart(fig, use_container_width=True)
st.divider()

# =================================================
# Metric 2: Bed Utilization Rate
# =================================================
st.header("Bed Utilization Rate by State")

ptype_util = st.selectbox(
    "Select PROVIDER_TYPE:",
    dim_bed["PROVIDER_TYPE"].unique(),
    key="util"
)

bed_util = (
    dim_bed.query("PROVIDER_TYPE == @ptype_util")
    .groupby("STATE", as_index=False)["BED_UTILIZATION_RATE"]
    .mean()
    .fillna(0)
    .sort_values("STATE")
)

fig = px.bar(
    bed_util,
    x="STATE",
    y="BED_UTILIZATION_RATE",
    title=f"Bed Utilization Rate ({ptype_util})",
    labels={"BED_UTILIZATION_RATE": "Utilization Rate (%)"},
    hover_data={"BED_UTILIZATION_RATE": ":.2f"},
    color_discrete_sequence=[COLORS.get(ptype_util, "#CA1CF6")]
)

st.plotly_chart(fig, use_container_width=True)
st.divider()

# =================================================
# Metric 3: Nurse to Patient Ratio
# =================================================
st.header("Nurse to Patient Ratio by State")

nurse_map = {
    "RN": "RN_TO_PATIENT_RATE",
    "LPN": "LPN_TO_PATIENT_RATE",
    "Nurse Aide": "NURSE_AIDE_TO_PATIENT_RATE",
    "All Nurse": "NURSE_TO_PATIENT_RATE"
}

provider = st.radio(
    "Select PROVIDER_TYPE:",
    dim_nurse["PROVIDER_TYPE"].unique(),
    horizontal=True
)

nurse_type = st.selectbox("Select Nurse Type:", nurse_map.keys())
rate_col = nurse_map[nurse_type]

nurse_rate = (
    dim_nurse.query("PROVIDER_TYPE == @provider")
    .groupby("STATE", as_index=False)[rate_col]
    .mean()
    .fillna(0)
    .sort_values("STATE")
)

st.metric(
    f"Average {nurse_type} to Patient Ratio ({provider})",
    f"{nurse_rate[rate_col].mean():.2f}"
)

fig = px.bar(
    nurse_rate,
    x="STATE",
    y=rate_col,
    title=f"{nurse_type} Ratio by State ({provider})",
    hover_data={rate_col: ":.2f"}
)

st.plotly_chart(fig, use_container_width=True)
st.divider()

# =================================================
# Metric 4: Provider Staffing & Census
# =================================================
st.header("Provider Staffing & Census")

search_by = st.radio(
    "Search Provider By:",
    ["Provider Number", "Provider Name"],
    horizontal=True
)

if search_by == "Provider Number":
    pid = st.selectbox(
        "Provider Number",
        sorted(provider_df["PROVIDER_NUM"].unique())
    )
    df = provider_df.query("PROVIDER_NUM == @pid")
else:
    pname = st.selectbox(
        "Provider Name",
        sorted(provider_df["PROVIDER_NAME"].unique())
    )
    df = provider_df.query("PROVIDER_NAME == @pname")

if df.empty:
    st.warning("No data found.")
    st.stop()

info = df.iloc[0]
df = df.sort_values("WORKDATE")

# Provider Info
c1, c2, c3 = st.columns(3)
c1.metric("Provider Number", info["PROVIDER_NUM"])
c2.metric("Provider Name", info["PROVIDER_NAME"])
c3.metric("Provider Type", info["PROVIDER_TYPE"])
c1.metric("State", info["STATE_bed"])
c2.metric("City", info["CITY"])
c3.metric("Certified Beds", int(info["NUMBER_OF_CERTIFIED_BEDS"]))

# Daily Census
daily_census = df.groupby("WORKDATE", as_index=False)["MDSCENSUS"].sum()
fig = px.line(
    daily_census,
    x="WORKDATE",
    y="MDSCENSUS",
    color_discrete_sequence=["#B0230A"],
    title=f"Daily MDS Census – {info['PROVIDER_NAME']}"
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# =================================================
# Metric 5: Average Staff Hours by State
# =================================================
st.header("Average Staff Hours by State")

ptype_state = st.radio(
    "Select Provider Type:",
    dim_bed["PROVIDER_TYPE"].unique(),
    horizontal=True
)

staff_cols = {
    "RNDON": ["HRS_RNDON_EMP", "HRS_RNDON_CTR"],
    "RNADMIN": ["HRS_RNADMIN_EMP", "HRS_RNADMIN_CTR"],
    "RN": ["HRS_RN_EMP", "HRS_RN_CTR"],
    "LPNADMIN": ["HRS_LPNADMIN_EMP", "HRS_LPNADMIN_CTR"],
    "LPN": ["HRS_LPN_EMP", "HRS_LPN_CTR"],
    "CNA": ["HRS_CNA_EMP", "HRS_CNA_CTR"],
    "NATRN": ["HRS_NATRN_EMP", "HRS_NATRN_CTR"],
    "MEDAIDE": ["HRS_MEDAIDE_EMP", "HRS_MEDAIDE_CTR"]
}

state_hours = (
    provider_df.query("PROVIDER_TYPE == @ptype_state")
    .groupby("STATE_bed", as_index=False)
    .apply(lambda x: pd.Series({
        k: x[v].sum().sum() for k, v in staff_cols.items()
    }))
    .reset_index(drop=True)
)

plot_df = state_hours.melt(
    id_vars="STATE_bed",
    var_name="Staff Type",
    value_name="Avg Hours"
)

HOT_COLORS = {
    "RNDON":   "#D7263D",
    "RNADMIN":"#F47D22",
    "RN":     "#F9C80E",
    "LPNADMIN":"#F307EF",
    "LPN":    "#3A3838",
    "CNA":    "#6A03FC",
    "NATRN":  "#03FD24",
    "MEDAIDE":"#43E6D0"
}

fig = px.bar(
    plot_df,
    x="STATE_bed",
    y="Avg Hours",
    color="Staff Type",
    color_discrete_map=HOT_COLORS,
    barmode="group",
    title=f"Average Staff Hours by State ({ptype_state})"
)

st.plotly_chart(fig, use_container_width=True)
