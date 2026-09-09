"""One deterministic public schema source for the frontend generator."""
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from pydantic.json_schema import models_json_schema

from expert_contracts import PUBLIC_SCHEMA_MODELS
from expert_contracts.errors import ErrorEnvelope


def _normalize_defaults(value):
    # FastAPI omits default:null annotations during its OpenAPI serialization.
    # Preserve null types/enum values; only this non-semantic annotation differs.
    if isinstance(value, dict):
        return {key: _normalize_defaults(item) for key, item in value.items() if not (key == "default" and item is None)}
    if isinstance(value, list):
        return [_normalize_defaults(item) for item in value]
    return value


def public_openapi(app: FastAPI) -> dict:
    schema = get_openapi(title=app.title, version=app.version, routes=app.routes)
    schema.setdefault("components", {})["securitySchemes"] = {
        "ApplicationKey": {"type": "http", "scheme": "bearer", "description": "Dedicated local application key"},
        "ApplicationSession": {"type": "apiKey", "in": "cookie", "name": "expert_session"},
    }
    _, definitions = models_json_schema(
        [(model, "validation") for model in PUBLIC_SCHEMA_MODELS],
        ref_template="#/components/schemas/{model}",
    )
    components = schema.setdefault("components", {}).setdefault("schemas", {})
    for name, definition in definitions.get("$defs", {}).items():
        definition = _normalize_defaults(definition)
        if name in components and components[name] != definition:
            raise RuntimeError(f"Conflicting public OpenAPI schema: {name}")
        components[name] = definition
    # FastAPI's default 422 describes raw validation internals; the HTTP boundary
    # emits the same safe envelope for malformed and cross-field-invalid input.
    for pathname, path in schema["paths"].items():
        for method, operation in path.items():
            if not isinstance(operation, dict) or "responses" not in operation:
                continue
            if pathname.startswith("/api/v1/") and pathname != "/api/v1/system/status" and not (pathname == "/api/v1/auth/session" and method == "post"):
                operation["security"] = [{"ApplicationKey": []}, {"ApplicationSession": []}]
            if operation.get("operationId") in {"upload_document", "upload_document_version", "reindex_version"}:
                operation.setdefault("parameters", []).append({"name": "Idempotency-Key", "in": "header", "required": True,
                    "schema": {"type": "string", "minLength": 1, "maxLength": 128}})
            for status in ("400", "401", "403", "404", "409", "413", "415", "422", "429", "500", "503", "504"):
                operation["responses"].setdefault(status, {
                    "description": "Safe public error",
                    "content": {"application/json": {"schema": {"$ref": f"#/components/schemas/{ErrorEnvelope.__name__}"}}},
                })
            if "422" in operation["responses"]:
                operation["responses"]["422"]["content"] = {"application/json": {"schema": {"$ref": "#/components/schemas/ErrorEnvelope"}}}
    components.pop("HTTPValidationError", None)
    components.pop("ValidationError", None)
    return schema
