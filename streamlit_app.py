import streamlit as st
import os

st.title("Secrets test")

# Show whether secrets are available
has_secret = False
secret_from_st = None
try:
    secret_from_st = st.secrets.get("AERODATABOX_API_KEY")
    if secret_from_st:
        has_secret = True
except Exception:
    pass

env_secret = os.environ.get("AERODATABOX_API_KEY")

st.write("st.secrets AERODATABOX_API_KEY present:", bool(secret_from_st))
st.write("Environment AERODATABOX_API_KEY present:", bool(env_secret))
st.write("st.secrets keys available:", list(st.secrets.keys()) if hasattr(st, "secrets") else [])
st.code("Remember: I will not display the secret value to avoid exposing it.")
