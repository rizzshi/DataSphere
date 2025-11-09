import streamlit as st
import requests

st.title("DataSphere: ChatGPT for Pipelines")

st.write("Enter your pipeline request in natural language:")
prompt = st.text_area("Pipeline Request", "Ingest, clean, validate, summarize, export to S3, and notify the team.")

if st.button("Submit"):
    with st.spinner("Processing..."):
        response = requests.post(
            "http://127.0.0.1:8000/chat_pipeline",
            json={"prompt": prompt}
        )
        if response.ok:
            data = response.json()
            st.subheader("Pipeline Plan")
            st.code(data.get("pipeline_plan", ""))
            st.subheader("Parsed Steps")
            st.json(data.get("parsed_steps", []))
            st.subheader("Actions")
            st.json(data.get("actions", []))
        else:
            st.error(f"Error: {response.status_code}")
