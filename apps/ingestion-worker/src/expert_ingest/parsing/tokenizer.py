"""Local, hash-pinned FRIDA token counting; no weights and no implicit downloads."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
from pathlib import Path
from typing import Any, Protocol


MODEL_ID = "ai-forever/FRIDA"
REVISION = "850455b605544a944739b25f81ddf812b6e3d0d5"
DOCUMENT_PREFIX = "search_document: "
QUERY_PREFIX = "search_query: "
TOKEN_LIMIT = 512
TOKENIZER_FILES = (
    "config.json", "merges.txt", "special_tokens_map.json",
    "tokenizer.json", "tokenizer_config.json", "vocab.json",
)


class TokenizerBudget(Protocol):
    """Count the complete prefixed input, including special tokens, without truncation."""

    model_id: str
    revision: str
    fingerprint: str

    def document_tokens(self, text: str) -> int: ...


class TokenizerUnavailable(ValueError):
    """Safe preparation/configuration failure; never contains a local path or raw error."""


class FridaTokenizerBudget:
    model_id = MODEL_ID
    revision = REVISION

    def __init__(self, tokenizer: Any, fingerprint: str):
        self._tokenizer = tokenizer
        self.fingerprint = fingerprint

    @classmethod
    def from_local(cls, model_path: Path, lock_path: Path) -> FridaTokenizerBudget:
        """Verify the six tokenizer inputs before loading from an explicit local directory.

        The manifest is a trusted deployment input. Runtime assets must be mounted read-only.
        This intentionally does not import the preparation/download CLI or load model weights.
        """
        try:
            spec = json.loads(lock_path.read_text(encoding="utf-8"))["models"]["embedding"]
            if (spec["model_id"] != MODEL_ID or spec["revision"] != REVISION
                    or spec["tokenizer_revision"] != REVISION):
                raise ValueError("unexpected tokenizer revision")
            contract = spec["contract"]
            if (contract["document_prefix"] != DOCUMENT_PREFIX or contract["query_prefix"] != QUERY_PREFIX
                    or contract["max_input_tokens"] != TOKEN_LIMIT or contract["silent_truncation"] is not False):
                raise ValueError("unexpected tokenizer recipe")
            specs = {item["path"]: item for item in spec["files"]}
            verified = []
            root = model_path.resolve(strict=True)
            if not root.is_dir():
                raise ValueError("tokenizer directory missing")
            if (root / "added_tokens.json").exists():
                raise ValueError("unlocked optional tokenizer artifact")
            for name in TOKENIZER_FILES:
                entry = specs[name]
                path = (root / name).resolve(strict=True)
                if not path.is_relative_to(root) or not path.is_file():
                    raise ValueError("unsafe tokenizer artifact")
                data = path.read_bytes()
                digest = hashlib.sha256(data).hexdigest()
                if len(data) != entry["size_bytes"] or digest != entry["sha256"]:
                    raise ValueError("tokenizer artifact differs")
                verified.append((name, digest))
            runtime = {name: importlib.metadata.version(name) for name in ("transformers", "tokenizers")}
            fingerprint = hashlib.sha256(json.dumps(
                [MODEL_ID, REVISION, DOCUMENT_PREFIX, QUERY_PREFIX, TOKEN_LIMIT, verified, runtime],
                ensure_ascii=False, separators=(",", ":"),
            ).encode()).hexdigest()
            # A lazy import keeps the light worker/DTO import surface free of ML libraries.
            from transformers import AutoTokenizer

            tokenizer = AutoTokenizer.from_pretrained(
                str(root), local_files_only=True, trust_remote_code=False,
            )
            if not tokenizer.is_fast or tokenizer.num_special_tokens_to_add(pair=False) != 2:
                raise ValueError("unexpected tokenizer special tokens")
            return cls(tokenizer, fingerprint)
        except (OSError, ValueError, KeyError, TypeError, ImportError) as exc:
            raise TokenizerUnavailable("LOCAL_TOKENIZER_UNAVAILABLE") from exc

    def document_tokens(self, text: str) -> int:
        return len(self._tokenizer.encode(DOCUMENT_PREFIX + text, add_special_tokens=True, truncation=False))

    def query_tokens(self, text: str) -> int:
        return len(self._tokenizer.encode(QUERY_PREFIX + text, add_special_tokens=True, truncation=False))

    def document_ids(self, text: str) -> tuple[int, ...]:
        """Exact input parity probe for the ML adapter; never used to reconstruct source text."""
        return tuple(self._tokenizer.encode(DOCUMENT_PREFIX + text, add_special_tokens=True, truncation=False))

    def source_offsets(self, text: str) -> tuple[tuple[int, int], ...]:
        """Unprefixed Unicode offsets for diagnostics; several BPE tokens may share one character."""
        return tuple(self._tokenizer(text, add_special_tokens=False, truncation=False,
                                     return_offsets_mapping=True)["offset_mapping"])
