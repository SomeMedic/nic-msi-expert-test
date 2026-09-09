"""Production entry point for the verified parser and real indexing pipeline."""
from expert_clients.settings import load_settings
from expert_observability.logging import configure_logging

from expert_ingest.application import create_app

settings = load_settings("ingestion-worker")
configure_logging(settings.service_name, settings.log_level)
app = create_app(settings)
