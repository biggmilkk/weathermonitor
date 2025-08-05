# utils/clients.py
import httpx
import streamlit as st

@st.cache_resource
def get_async_client():
    """Singleton AsyncClient for all scrapers."""
    return httpx.AsyncClient(timeout=30.0)
