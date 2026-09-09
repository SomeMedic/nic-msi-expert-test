"""Agent process entry point; no model weights are loaded in this service."""
from expert_clients.settings import load_settings
from expert_observability.logging import configure_logging

from .application import create_app

settings = load_settings("agent-runtime")
configure_logging(settings.service_name, settings.log_level)
app = create_app(settings)
