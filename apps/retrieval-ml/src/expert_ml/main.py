"""Retrieval-ml entrypoint: local CPU models with authenticated internal inference."""
from expert_clients.dependencies import Dependencies
from expert_clients.settings import load_settings
from expert_observability.logging import configure_logging

from .application import create_app

settings = load_settings("retrieval-ml")
configure_logging(settings.service_name, settings.log_level)
dependencies = Dependencies(settings)
app = create_app(settings, dependencies=dependencies)
