"""Export public DTOs and implemented routes without starting runtime services.

No deployment secrets, network calls, DB startup or ML imports are needed.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from fastapi import FastAPI

from expert_api.routes import install_public_routes
from expert_api.openapi import public_openapi
from expert_observability.health import install_health

ROOT = Path(__file__).resolve().parents[1]


def export() -> str:
    application = FastAPI(title="Digital Expert backend", version="1.0.0")
    install_public_routes(application)
    install_health(application, "backend", None)
    return json.dumps(public_openapi(application), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    path = ROOT / "contracts/openapi.json"
    rendered = export()
    if args.check:
        if not path.is_file() or path.read_text(encoding="utf-8") != rendered:
            raise SystemExit("Public OpenAPI differs; run scripts/export_openapi.py")
        print("Public OpenAPI is current")
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
        print("Exported contracts/openapi.json")
