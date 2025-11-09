

import streamlit as st
import requests
import time
import pandas as pd
import os
import json


# --- Prompt history storage ---
PROMPT_HISTORY_FILE = "prompt_history.json"
def load_prompt_history():
    if os.path.exists(PROMPT_HISTORY_FILE):
        with open(PROMPT_HISTORY_FILE, "r") as f:
            return json.load(f)
    return []

def save_prompt_history(history):
    with open(PROMPT_HISTORY_FILE, "w") as f:
        json.dump(history[-20:], f)  # keep last 20

if "prompt_history" not in st.session_state:
    st.session_state.prompt_history = load_prompt_history()

st.title("DataSphere: ChatGPT for Pipelines")

# --- Layout: Main and Analytics side by side ---
main_col, analytics_col = st.columns([2.5, 1])


with main_col:
    st.write("Enter your pipeline request in natural language:")
    prompt = st.text_area("Pipeline Request", st.session_state.prompt_history[-1] if st.session_state.prompt_history else "Ingest, clean, validate, summarize, export to S3, and notify the team.")

    st.markdown("**Select Data Source:**")
    data_source = st.radio("Data Source", ["Upload File", "Database", "S3"], horizontal=True)
    uploaded_file = None
    db_conn = ""
    s3_path = ""
    if data_source == "Upload File":
        uploaded_file = st.file_uploader("Upload your data file (CSV, Excel, etc.)", type=["csv", "xlsx", "xls", "json"])
    elif data_source == "Database":
        db_conn = st.text_input("Database connection string (e.g., sqlite:///mydb.db)")
    elif data_source == "S3":
        s3_path = st.text_input("S3 path (e.g., s3://bucket/key.csv)")

    submit = st.button("Submit", key="submit_btn")
    if submit and prompt.strip():
        files = None
        data = {"prompt": prompt, "data_source": data_source}
        if data_source == "Upload File" and uploaded_file is not None:
            files = {"file": (uploaded_file.name, uploaded_file, uploaded_file.type)}
        elif data_source == "Database":
            data["db_conn"] = db_conn
        elif data_source == "S3":
            data["s3_path"] = s3_path
        with st.spinner("Processing..."):
            start = time.time()
            if files:
                response = requests.post(
                    "http://127.0.0.1:8000/chat_pipeline",
                    data=data,
                    files=files
                )
            else:
                response = requests.post(
                    "http://127.0.0.1:8000/chat_pipeline",
                    json=data
                )
            elapsed = time.time() - start
            if response.ok:
                data = response.json()
                st.success(f"Response time: {elapsed:.2f} seconds")
                st.subheader(":bookmark_tabs: Pipeline Plan")
                st.code(data.get("pipeline_plan", ""), language="markdown")
                st.subheader(":triangular_flag_on_post: Parsed Steps")
                steps = data.get("parsed_steps", [])
                if isinstance(steps, list) and steps and isinstance(steps[0], dict):
                    for i, step in enumerate(steps):
                        st.markdown(f"**Step {i+1}:** {step.get('step','')} — *{step.get('action','')}*  ")
                        st.json(step)
                else:
                    st.json(steps)
                st.subheader(":rocket: Actions")
                actions = data.get("actions", [])
                for action in actions:
                    st.info(action)
                # Update prompt history
                if prompt not in st.session_state.prompt_history:
                    st.session_state.prompt_history.append(prompt)
                    save_prompt_history(st.session_state.prompt_history)
            else:
                st.error(f"Error: {response.status_code}")

    st.markdown("---")
    st.subheader(":scroll: Prompt History")
    if st.session_state.prompt_history:
        for i, p in enumerate(reversed(st.session_state.prompt_history[-10:])):
            if st.button(f"Use", key=f"use_{i}"):
                st.session_state["prompt"] = p
                st.experimental_rerun()
            st.markdown(f"`{p}`")
    else:
        st.info("No prompt history yet.")

with analytics_col:
    st.subheader(":bar_chart: Analytics")
    # Show analytics only if last response exists
    if submit and prompt.strip() and response.ok:
        # Step type bar chart
        if isinstance(steps, list) and steps and isinstance(steps[0], dict):
            step_types = [step.get('step','unknown') for step in steps]
            step_counts = pd.Series(step_types).value_counts()
            st.write("**Step Type Distribution:**")
            st.bar_chart(step_counts)
        # Action type pie chart
        if 'actions' in locals() and actions:
            action_types = [a.split()[0].strip('[]') if a.startswith('[') else 'Other' for a in actions]
            action_counts = pd.Series(action_types).value_counts()
            st.write("**Action Type Distribution:**")
            st.pyplot(action_counts.plot.pie(autopct="%1.0f%%", ylabel="").get_figure())
    else:
        st.info("Analytics will appear after you submit a request.")

with st.sidebar:
    st.title("DataSphere")
    st.markdown("""
    **Autonomous Data Mesh Orchestrator**
    - Enter a pipeline request in natural language
    - See parsed steps and orchestration actions
    - Powered by FastAPI, LangChain, Airflow, Ray, DuckDB, Redis
    """)
    st.markdown("---")
    st.markdown("**Example Prompts:**")
    st.code("Ingest, clean, validate, summarize, export to S3, and notify the team.")
    st.code("Ingest sales data and run a daily summary.")
    st.code("Clean and validate customer data, then export results.")
