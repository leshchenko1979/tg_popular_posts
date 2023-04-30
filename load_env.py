import toml
import os

import streamlit as st

"""Load environment variables from .streamlit/secrets.toml to os.environ"""
def load_secrets():
    if st.secrets:
        os.environ.update(st.secrets)
        return

    try:
        with open(".streamlit/secrets.toml") as f:
            secrets = toml.load(f)
        for k, v in secrets.items():
            os.environ[k] = v
    except FileNotFoundError:
        print("No secrets.toml file found")


load_secrets()
