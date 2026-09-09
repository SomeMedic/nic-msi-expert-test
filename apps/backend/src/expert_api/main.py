"""Backend process entrypoint."""
from expert_clients.settings import load_settings
from expert_observability.logging import configure_logging

from .application import create_app

settings = load_settings("backend")
configure_logging(settings.service_name, settings.log_level)
app = create_app(settings)
