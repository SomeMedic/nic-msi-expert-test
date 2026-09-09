"""Bounded deployment identity shared without importing graph, ML or agent code.

The manifest is a trusted, read-only deployment artifact, not a signature. Code
pins use UTF-8 with CRLF/CR normalized to LF; prompts and attestations retain
their exact bytes because their upstream runtime loaders verify raw SHA256.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
from typing import Any

MANIFEST_PATH = "config/runtime/manifest.json"
PROFILE_PATH = "config/runtime/serving-profile.json"
PROMPTS_PATH = "config/prompts/p07.v1"
MAX_FILE_BYTES = 8 * 1024**2
MAX_TOTAL_BYTES = 32 * 1024**2
MAX_FILES = 256
SOURCE_ROOTS = (
    "apps/agent-runtime/src/expert_agent",
    "packages/contracts/src/expert_contracts",
    "packages/service-clients/src/expert_clients",
)
FIXED_FILES = frozenset({
    "models.lock.json", "uv.lock", "pyproject.toml", "apps/agent-runtime/pyproject.toml",
    "packages/contracts/pyproject.toml", "packages/service-clients/pyproject.toml",
    "scripts/prepare_runtime_identity.py", PROFILE_PATH,
    f"{PROMPTS_PATH}/manifest.json", *(f"{PROMPTS_PATH}/{role}.txt" for role in ("router", "drafter", "critic", "repair")),
})
REQUIRED_CODE = frozenset({
    *(f"apps/agent-runtime/src/expert_agent/{name}.py" for name in
      ("graph", "run_manager", "checkpoint", "execution", "repository", "validation", "rendering")),
    "apps/agent-runtime/src/expert_agent/llm/prompts.py",
    "apps/agent-runtime/src/expert_agent/llm/client.py",
    "apps/agent-runtime/src/expert_agent/retrieval/types.py",
    "packages/contracts/src/expert_contracts/model.py",
    "packages/service-clients/src/expert_clients/runtime_identity.py",
})
SEMANTIC_BOUNDS = {
    "run_deadline_seconds": (1, 3600), "run_lease_seconds": (1, 300),
    "run_heartbeat_seconds": (1, 299), "run_max_recovery_attempts": (0, 10),
    "run_global_concurrency": (1, 1), "run_max_active_per_operator": (1, 1),
}
RETRIEVAL_BOUNDS = {
    **{name: (1, 256) for name in ("dense_top_k", "lexical_top_k", "fusion_top_k", "rerank_top_k",
       "descriptor_top_k", "max_context_candidates", "max_lexical_terms", "max_expansion_nodes")},
    "rrf_constant": (1, 1000), "max_ancestor_depth": (1, 64), "max_units": (1, 72),
    "max_excerpt_characters": (1, 30000), "statement_timeout_ms": (1, 30000),
}
_SHA = re.compile(r"[0-9a-f]{64}")
_PROFILE_LITERALS = {
    "schema_version": "p07.vllm.v1", "model": "Qwen/Qwen3-14B-AWQ",
    "revision": "31c69efc29464b6bb0aee1398b5a7b50a99340c3",
    "tokenizer_revision": "31c69efc29464b6bb0aee1398b5a7b50a99340c3",
    "image_digest": "sha256:f2309d913a07da49ea20b2a694703f4cfcb5ad8e7437ec0f26145479ac01e002",
    "vllm_version": "0.12.0", "structured_backend": "xgrammar", "disable_fallback": True,
    "enable_thinking": False, "max_model_len": 8192, "max_num_seqs": 1,
}
_PROFILE_HASHES = {"model_fingerprint", "tokenizer_fingerprint", "chat_template_sha256", "observed_config_sha256"}


class RuntimeIdentityError(ValueError):
    """A fixed error code; no source, private path or raw decoder error."""


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("Duplicate key")
        result[name] = value
    return result


def decode_json(data: bytes) -> Any:
    def invalid(_: str) -> None:
        raise ValueError("Non-finite JSON")
    return json.loads(data, object_pairs_hook=_unique, parse_constant=invalid)


def _closed(value: Any, keys: set[str] | frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("Invalid closed object")
    return value


def _bounds(value: Any, bounds: dict[str, tuple[int, int]]) -> dict[str, int]:
    _closed(value, set(bounds))
    if any(type(value[name]) is not int or not low <= value[name] <= high
           for name, (low, high) in bounds.items()):
        raise ValueError("Invalid integer bounds")
    return value


def _relative(path: str) -> PurePosixPath:
    if (not isinstance(path, str) or len(path) > 240 or not re.fullmatch(r"[A-Za-z0-9_./-]+", path)
            or path.startswith("/") or any(p in {"", ".", ".."} for p in path.split("/"))):
        raise ValueError("Invalid relative path")
    return PurePosixPath(path)


def read_owned_file(root: Path, name: str, *, maximum: int = MAX_FILE_BYTES) -> bytes:
    relative = _relative(name)
    path = root
    for component in relative.parts:
        path = path / component
        if path.is_symlink() or path.is_junction():
            raise ValueError("Linked runtime file")
    if not path.resolve(strict=True).is_relative_to(root):
        raise ValueError("Runtime file outside root")
    # NOFOLLOW closes the final-component link race on Linux; NONBLOCK avoids
    # opening a substituted FIFO indefinitely. Production roots are read-only.
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0))
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ValueError("Runtime file is not regular")
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise ValueError("Runtime file exceeds bound")
    return data


def _mode(name: str) -> str:
    return "utf8-lf" if name.endswith((".py", ".toml", ".lock")) else "raw"


def inventory(root: Path) -> list[dict[str, Any]]:
    """Fixed roots only; unknown/missing source files change the checked inventory."""
    root = root.resolve(strict=True)
    names = set(FIXED_FILES)
    entries = 0

    def scan_error(error: OSError) -> None:
        raise error

    for directory in SOURCE_ROOTS:
        base = root / directory
        if not base.is_dir() or base.is_symlink() or base.is_junction():
            raise ValueError("Invalid runtime source root")
        for current, directories, files in os.walk(base, followlinks=False, onerror=scan_error):
            folder = Path(current)
            entries += len(directories) + len(files)
            if entries > 2048 or len(folder.relative_to(base).parts) > 8:
                raise ValueError("Runtime directory scan exceeds bound")
            for name in directories:
                path = folder / name
                if path.is_symlink() or path.is_junction():
                    raise ValueError("Linked runtime directory")
            directories[:] = [name for name in directories if name != "__pycache__"]
            for name in files:
                if name.endswith(".py"):
                    names.add((folder / name).relative_to(root).as_posix())
                    if len(names) > MAX_FILES:
                        raise ValueError("Too many runtime files")
    if not REQUIRED_CODE <= names:
        raise ValueError("Missing required runtime source")
    total, rows = 0, []
    for name in sorted(names):
        data = read_owned_file(root, name)
        total += len(data)
        if total > MAX_TOTAL_BYTES:
            raise ValueError("Runtime inventory exceeds bound")
        mode = _mode(name)
        if mode == "utf8-lf":
            data = data.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")
        rows.append({"path": name, "hash_mode": mode, "size_bytes": len(data),
                     "sha256": hashlib.sha256(data).hexdigest()})
    return rows


def configuration_fingerprint(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json({key: value for key, value in manifest.items()
                                         if key != "configuration_fingerprint"}).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RuntimeIdentity:
    configuration_fingerprint: str
    graph_version: str
    context_token_budget: int
    _retrieval_json: str
    _profile_json: str
    _limits_json: str
    _prompts_json: str

    @property
    def retrieval_config(self) -> dict[str, int]:
        return json.loads(self._retrieval_json)

    @property
    def serving_profile(self) -> dict[str, Any]:
        return json.loads(self._profile_json)

    @property
    def semantic_limits(self) -> dict[str, int]:
        return json.loads(self._limits_json)

    @property
    def effective_prompts(self) -> dict[str, Any]:
        return json.loads(self._prompts_json)


def load_runtime_identity(manifest_path: Path, *, root: Path, require_frozen: bool = True) -> RuntimeIdentity:
    try:
        root = root.resolve(strict=True)
        manifest_path = manifest_path if manifest_path.is_absolute() else root / manifest_path
        if manifest_path.absolute() != root / MANIFEST_PATH:
            raise ValueError("Manifest must use fixed root path")
        manifest = _closed(decode_json(read_owned_file(root, MANIFEST_PATH, maximum=256 * 1024)), {
            "schema_version", "release_state", "graph_version", "context_token_budget", "semantic_limits",
            "retrieval_config", "serving_profile", "effective_prompts", "files", "configuration_fingerprint"})
        if (manifest["schema_version"] != "p08.runtime.v1" or manifest["graph_version"] != "p08.graph.v1"
                or manifest["release_state"] not in {"provisional", "frozen"}
                or require_frozen and manifest["release_state"] != "frozen"):
            raise ValueError("Runtime release is not ready")
        budget = manifest["context_token_budget"]
        if type(budget) is not int or not 512 <= budget <= 5000:
            raise ValueError("Invalid complete Drafter budget")
        limits = _bounds(manifest["semantic_limits"], SEMANTIC_BOUNDS)
        if limits["run_heartbeat_seconds"] >= limits["run_lease_seconds"]:
            raise ValueError("Heartbeat exceeds lease")
        _bounds(manifest["retrieval_config"], RETRIEVAL_BOUNDS)
        prompts = _closed(manifest["effective_prompts"], {"version", "roles"})
        if prompts["version"] != "p07.v1":
            raise ValueError("Invalid prompt version")
        _closed(prompts["roles"], {"router", "drafter", "critic", "repair"})
        for role, row in prompts["roles"].items():
            _closed(row, {"prompt_sha256", "schema_sha256", "max_output_tokens", "timeout_seconds"})
            if (any(not isinstance(row[key], str) or not _SHA.fullmatch(row[key])
                    for key in ("prompt_sha256", "schema_sha256"))
                    or type(row["max_output_tokens"]) is not int
                    or row["max_output_tokens"] != (512 if role == "router" else 1600)
                    or type(row["timeout_seconds"]) is not int
                    or row["timeout_seconds"] != (60 if role == "router" else 120)):
                raise ValueError("Invalid effective prompt metadata")
        profile = _closed(decode_json(read_owned_file(root, PROFILE_PATH, maximum=16384)),
                          set(_PROFILE_LITERALS) | _PROFILE_HASHES)
        if manifest["serving_profile"] != profile:
            raise ValueError("Serving profile drift")
        if (any(type(profile[key]) is not type(value) or profile[key] != value
                for key, value in _PROFILE_LITERALS.items())
                or any(not isinstance(profile[key], str) or not _SHA.fullmatch(profile[key]) for key in _PROFILE_HASHES)):
            raise ValueError("Serving policy mismatch")
        if not isinstance(manifest["files"], list) or not 1 <= len(manifest["files"]) <= MAX_FILES:
            raise ValueError("Invalid file inventory")
        for row in manifest["files"]:
            _closed(row, {"path", "hash_mode", "size_bytes", "sha256"})
            _relative(row["path"])
            if (type(row["size_bytes"]) is not int or not 0 <= row["size_bytes"] <= MAX_FILE_BYTES
                    or not isinstance(row["sha256"], str) or not _SHA.fullmatch(row["sha256"])
                    or row["hash_mode"] not in {"raw", "utf8-lf"}):
                raise ValueError("Invalid file pin")
        if manifest["files"] != inventory(root):
            raise ValueError("Runtime file inventory drift")
        fingerprint = configuration_fingerprint(manifest)
        if manifest["configuration_fingerprint"] != fingerprint:
            raise ValueError("Configuration digest mismatch")
        return RuntimeIdentity(fingerprint, manifest["graph_version"], budget,
            canonical_json(manifest["retrieval_config"]), canonical_json(profile),
            canonical_json(limits), canonical_json(prompts))
    except (OSError, ValueError, TypeError, KeyError, AttributeError, RuntimeError):
        raise RuntimeIdentityError("RUNTIME_IDENTITY_INVALID") from None
