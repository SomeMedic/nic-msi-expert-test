"""Prepare only the six public, pinned FRIDA tokenizer files for CPU code tests.

Default is a plan. This utility never downloads weights or opens corpus files,
and never writes the full-model artifact_verification.json attestation.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

from expert_ingest.parsing.tokenizer import MODEL_ID, REVISION, TOKENIZER_FILES

ROOT = Path(__file__).resolve().parents[1]


def inputs(root: Path = ROOT) -> tuple[dict, list[tuple[dict, Path]]]:
    model = json.loads((root / "models.lock.json").read_text(encoding="utf-8"))["models"]["embedding"]
    if (model["model_id"] != MODEL_ID or model["revision"] != REVISION
            or model["tokenizer_revision"] != REVISION or model["local_path"] != "models/frida"):
        raise ValueError("TEST_TOKENIZER_IDENTITY_INVALID")
    base = root / "models/frida"
    if (root / "models").is_symlink() or base.is_symlink() or not base.resolve().is_relative_to(root.resolve() / "models"):
        raise ValueError("TEST_TOKENIZER_PATH_INVALID")
    specs = {spec["path"]: spec for spec in model["files"]}
    if len(specs) != len(model["files"]):
        raise ValueError("TEST_TOKENIZER_FILES_INVALID")
    selected = []
    for name in TOKENIZER_FILES:
        spec, target = specs[name], base / name
        if (target.is_symlink() or not target.resolve().is_relative_to(base.resolve())
                or type(spec["size_bytes"]) is not int or not 0 < spec["size_bytes"] <= 16 * 1024**2
                or re.fullmatch(r"[0-9a-f]{64}", spec["sha256"]) is None):
            raise ValueError("TEST_TOKENIZER_FILE_INVALID")
        partial = target.with_name(target.name + ".partial")
        if partial.is_symlink():
            raise ValueError("TEST_TOKENIZER_PARTIAL_INVALID")
        selected.append((spec, target))
    return model, selected


def main(argv: list[str] | None = None) -> int:
    from scripts.prepare_models import download_file, verify_file

    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--allow-network", action="store_true")
    mode.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    try:
        model, selected = inputs()
        report = {"scope": "public_tokenizer_only", "model_id": model["model_id"], "revision": model["revision"],
                  "files": [spec["path"] for spec, _ in selected],
                  "size_bytes": sum(spec["size_bytes"] for spec, _ in selected), "weights_verified": False}
        if args.allow_network or args.verify_only:
            for spec, target in selected:
                if args.allow_network:
                    download_file(model, spec, target)
                else:
                    verify_file(target, spec)
            report["status"] = "verified"
        else:
            report["status"] = "planned"
        print(json.dumps(report, ensure_ascii=False))
        return 0
    except (OSError, ValueError, KeyError, TypeError, RuntimeError):
        print(json.dumps({"status": "failed", "error_code": "TEST_TOKENIZER_PREPARATION_FAILED"}))
        return 1


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    raise SystemExit(main())
