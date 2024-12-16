import streamlit as st
import os
from databricks.sdk import WorkspaceClient
from functions import deploy_time
import mlflow


# Ensure environment variable is set correctly
assert os.getenv('UPLOAD_VOLUME'), "UPLOAD_VOLUME must be set in app.yaml."
volume_uri = os.getenv("UPLOAD_VOLUME")
#import the workspace client for file uploads 
w = WorkspaceClient()

st.set_page_config(
    page_title="Query MlFlow Experiments",
    page_icon="🧪",
)

st.sidebar.info("Last Deployment: {}".format(deploy_time()), icon="ℹ️")

st.title("🧪 Query MlFlow Experiments")

