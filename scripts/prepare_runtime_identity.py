"""Prepare/check a deterministic runtime manifest; never contact inference APIs.

Default preparation remains provisional. --freeze is a release action following
the source/test gate; application loaders reject provisional manifests.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
for directory in ("apps/agent-runtime/src", "packages/contracts/src", "packages/service-clients/src"):
    sys.path.insert(0, str(ROOT / directory))

from expert_agent.llm.prompts import PromptCatalog  # noqa: E402
from expert_agent.llm.types import Role, ServingProfile  # noqa: E402
from expert_agent.retrieval.types import RetrievalConfig  # noqa: E402
from expert_clients.runtime_identity import (  # noqa: E402
    MANIFEST_PATH, PROFILE_PATH, PROMPTS_PATH, canonical_json, configuration_fingerprint,
    decode_json, inventory, load_runtime_identity, read_owned_file,
)

SEMANTIC_LIMITS = {
    "run_deadline_seconds": 300, "run_lease_seconds": 60, "run_heartbeat_seconds": 10,
    "run_max_recovery_attempts": 2, "run_global_concurrency": 1, "run_max_active_per_operator": 1,
}


def build_manifest(root: Path, *, frozen: bool = False) -> dict[str, Any]:
    profile = ServingProfile.model_validate(decode_json(read_owned_file(root, PROFILE_PATH, maximum=16384)))
    profile.validate_lock(root / "models.lock.json")
    catalog = PromptCatalog(root / PROMPTS_PATH)
    roles: tuple[Role, ...] = ("router", "drafter", "critic", "repair")
    prompts = {}
    for role in roles:
        prepared = catalog.prepare(role, {})
        prompts[role] = {"prompt_sha256": prepared.prompt_sha256,
            "schema_sha256": hashlib.sha256(prepared.schema_json.encode("utf-8")).hexdigest(),
            "max_output_tokens": prepared.max_output_tokens, "timeout_seconds": prepared.timeout_seconds}
    manifest = {"schema_version": "p08.runtime.v1", "release_state": "frozen" if frozen else "provisional",
        "graph_version": "p08.graph.v1", "context_token_budget": 5000,
        "semantic_limits": SEMANTIC_LIMITS, "retrieval_config": asdict(RetrievalConfig()),
        "serving_profile": profile.model_dump(mode="json"),
        "effective_prompts": {"version": catalog.version, "roles": prompts}, "files": inventory(root)}
    manifest["configuration_fingerprint"] = configuration_fingerprint(manifest)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Compare without writing; provisional is explicit.")
    parser.add_argument("--freeze", action="store_true", help="Generate a frozen release after the owner source gate.")
    args = parser.parse_args()
    if args.check and args.freeze:
        parser.error("--check and --freeze are mutually exclusive")
    profile = ServingProfile.model_validate(decode_json(read_owned_file(ROOT, PROFILE_PATH, maximum=16384)))
    profile.validate_lock(ROOT / "models.lock.json")
    target = ROOT / MANIFEST_PATH
    if args.check:
        current = decode_json(read_owned_file(ROOT, MANIFEST_PATH, maximum=256 * 1024))
        identity = load_runtime_identity(target, root=ROOT, require_frozen=False)
        prepared = build_manifest(ROOT, frozen=current["release_state"] == "frozen")
        if canonical_json(current) != canonical_json(prepared):
            raise ValueError("Manifest does not match current effective configuration")
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        prepared = build_manifest(ROOT, frozen=args.freeze)
        target.write_text(json.dumps(prepared, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8", newline="\n")
        identity = load_runtime_identity(target, root=ROOT, require_frozen=args.freeze)
    print(json.dumps({"status": "passed", "release_state": prepared["release_state"],
        "configuration_fingerprint": identity.configuration_fingerprint, "files": len(prepared["files"]),
        "graph_version": identity.graph_version, "context_token_budget": identity.context_token_budget}))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(json.dumps({"status": "failed", "error_code": "RUNTIME_IDENTITY_PREPARATION_FAILED"}))
        raise SystemExit(1) from None
