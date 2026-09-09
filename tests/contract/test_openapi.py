import json
from pathlib import Path

from scripts.export_openapi import export


def test_openapi_is_deterministic_current_and_contains_only_public_roots():
    rendered = export()
    assert rendered == export()
    assert Path("contracts/openapi.json").read_text(encoding="utf-8") == rendered
    schema = json.loads(rendered)
    definitions = schema["components"]["schemas"]
    for private in ("DraftAnswer", "DraftClaim", "CriticInternalResult", "EvidencePack", "EvidenceUnit", "RouteDecision", "StartRunRequest", "RerankRequest"):
        assert private not in definitions
    for public in ("PublicRun", "FinalAnswer", "DocumentMetadata", "SystemStatus", "ErrorEnvelope"):
        assert public in definitions
    for obsolete in ("/ask", "/query", "/chat"):
        assert obsolete not in schema["paths"]
    assert schema["paths"]["/api/v1/system/status"]["get"]["operationId"] == "get_system_status"

    def check_references(value):
        if isinstance(value, dict):
            if "$ref" in value:
                prefix = "#/components/schemas/"
                assert value["$ref"].startswith(prefix)
                assert value["$ref"][len(prefix):] in definitions
            for child in value.values():
                check_references(child)
        elif isinstance(value, list):
            for child in value:
                check_references(child)

    check_references(schema)
