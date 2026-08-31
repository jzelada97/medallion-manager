"""Streamlit tab for the walled AI assistant.

Renders the chat interface and delegates all LLM + tool logic to the orchestrator.
Keeps conversation history in st.session_state across Streamlit reruns.
"""

import os
import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from assistant.orchestrator import chat, is_available

SYSTEM_MESSAGE = {
    "role": "assistant",
    "content": (
        "¡Hola! 👋 Soy el asistente técnico de **HR Insights ETL**.\n\n"
        "Puedo consultar datos reales del warehouse y explicar el proyecto. "
        "Prueba: *¿cuántas personas hay?*, *top ciudades*, "
        "*¿cómo funciona el matching?* o *busca personas en Madrid*."
    ),
}


def render_assistant_tab():
    st.title("🤖 Asistente Virtual HR Insights ETL")
    st.caption("Asistente amurallado con herramientas MCP y Groq")

    if is_available():
        st.success("Groq + MCP conectados — asistente con IA activo.", icon="✅")
    else:
        st.warning("Añade `GROQ_API_KEY` al `.env` para activar el asistente.", icon="⚠️")

    if "messages" not in st.session_state:
        st.session_state.messages = [SYSTEM_MESSAGE]

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("¿Qué deseas consultar sobre los datos o el proyecto?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Consultando herramientas..."):
                reply = chat(prompt, st.session_state.messages[:-1])
            st.markdown(reply)

        st.session_state.messages.append({"role": "assistant", "content": reply})

    if len(st.session_state.messages) > 1:
        if st.button("🗑️ Limpiar conversación"):
            st.session_state.messages = [SYSTEM_MESSAGE]
            st.rerun()
