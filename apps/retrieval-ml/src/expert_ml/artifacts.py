"""Verify trusted deployment manifests before loading any local model artifacts."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import platform
from pathlib import Path, PurePosixPath
from typing import Any, Final

from .errors import ModelError

FRIDA_MODEL: Final = "ai-forever/FRIDA"
FRIDA_REVISION: Final = "850455b605544a944739b25f81ddf812b6e3d0d5"
RERANKER_MODEL: Final = "Qwen/Qwen3-Reranker-0.6B"
RERANKER_REVISION: Final = "e61197ed45024b0ed8a2d74b80b4d909f1255473"
DOCUMENT_PREFIX = "search_document: "
QUERY_PREFIX = "search_query: "
TOKENIZER_FILES = (
    "config.json", "merges.txt", "special_tokens_map.json", "tokenizer.json",
    "tokenizer_config.json", "vocab.json",
)


def fingerprint(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(8 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def verify_snapshot(manifest: dict, key: str, directory: Path, model: str, revision: str) -> dict:
    spec = manifest["models"][key]
    if (spec["model_id"], spec["revision"], spec["tokenizer_revision"]) != (model, revision, revision):
        raise ModelError("MODEL_UNAVAILABLE")
    root = directory.resolve(strict=True)
    files: list[dict] = []
    names: set[str] = set()
    for entry in spec["files"]:
        name = entry["path"]
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts or "\\" in name or name in names:
            raise ModelError("MODEL_UNAVAILABLE")
        names.add(name)
        path = (root / name).resolve(strict=True)
        if (not path.is_relative_to(root) or not path.is_file()
                or path.stat().st_size != entry["size_bytes"] or sha256_file(path) != entry["sha256"]):
            raise ModelError("MODEL_UNAVAILABLE")
        files.append({field: entry[field] for field in ("path", "size_bytes", "sha256")})
    # Optional HF files must not override the verified tokenizer/configuration.
    actual = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    if actual - names - {"artifact_verification.json"}:
        raise ModelError("MODEL_UNAVAILABLE")
    if not {"model.safetensors", "config.json", "tokenizer.json", "tokenizer_config.json"} <= names:
        raise ModelError("MODEL_UNAVAILABLE")
    return {"spec": spec, "model_fingerprint": fingerprint({
        "model": model, "revision": revision, "files": sorted(files, key=lambda item: item["path"]),
    })}


def tokenizer_fingerprint(spec: dict) -> str:
    """Exactly the P04 FridaTokenizerBudget recipe, including its ordered runtime keys."""
    entries = {item["path"]: item for item in spec["files"]}
    verified = [(name, entries[name]["sha256"]) for name in TOKENIZER_FILES]
    runtime = {name: importlib.metadata.version(name) for name in ("transformers", "tokenizers")}
    return hashlib.sha256(json.dumps(
        [FRIDA_MODEL, FRIDA_REVISION, DOCUMENT_PREFIX, QUERY_PREFIX, 512, verified, runtime],
        ensure_ascii=False, separators=(",", ":"),
    ).encode()).hexdigest()


def runtime_identity(cpu_threads: int) -> dict:
    versions = {name: importlib.metadata.version(name)
                for name in ("torch", "transformers", "tokenizers", "safetensors")}
    if (platform.python_version_tuple()[:2] != ("3", "12") or versions["torch"] != "2.9.0+cpu"
            or versions["transformers"] != "4.57.3" or versions["tokenizers"] != "0.22.2"):
        raise ModelError("MODEL_UNAVAILABLE")
    import torch

    return {
        "schema_version": "p05.runtime.v1", "python": platform.python_version(),
        "platform": {"system": platform.system(), "machine": platform.machine()},
        # Build flags can contain vendor build paths: only their digest leaves the process.
        "torch_build_config_sha256": hashlib.sha256(torch.__config__.show().encode("utf-8")).hexdigest(),
        "packages": versions, "device": "cpu", "dtype": "float32", "cpu_threads": cpu_threads,
        "adapter_recipe": "p05.frida.v1",
        "implementation": {name: sha256_file(Path(__file__).with_name(name))
                           for name in ("adapters.py", "artifacts.py")},
    }
