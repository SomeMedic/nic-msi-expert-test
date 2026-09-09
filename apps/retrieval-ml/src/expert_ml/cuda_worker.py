"""Private token-only CUDA scorer, run by the optional isolated CUDA interpreter."""
from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import sys

MAX_LINE = 262144


def serve() -> None:
    import torch
    from transformers import AutoModelForCausalLM
    from transformers.utils import logging

    logging.disable_progress_bar()
    logging.set_verbosity_error()
    if torch.__version__ != "2.9.0+cu128" or not torch.cuda.is_available():
        raise RuntimeError("CUDA runtime unavailable")
    if importlib.metadata.version("transformers") != "4.57.3":
        raise RuntimeError("CUDA runtime version mismatch")
    torch.set_num_threads(2)
    model = AutoModelForCausalLM.from_pretrained(
        sys.argv[1], local_files_only=True, trust_remote_code=False,
        use_safetensors=True, dtype=torch.float16).eval().to("cuda")
    max_tokens = int(sys.argv[2])
    pad_token_id = int(sys.argv[3])
    if not 2048 <= max_tokens <= 8192 or not 0 <= pad_token_id < model.config.vocab_size:
        raise ValueError("Invalid CUDA batch limit")
    identity = {
        "schema_version": "p05.reranker.cuda.v1", "python": platform.python_version(),
        "packages": {name: importlib.metadata.version(name)
                     for name in ("torch", "transformers", "tokenizers", "safetensors")},
        "device": "cuda", "dtype": "float16", "cuda_version": torch.version.cuda,
        "gpu_name": torch.cuda.get_device_name(0),
        "gpu_capability": list(torch.cuda.get_device_capability(0)),
        "max_batch_tokens": max_tokens,
        "pad_token_id": pad_token_id,
        "implementation_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    print(json.dumps({"ready": True, "identity": identity}), flush=True)
    while True:
        line = sys.stdin.buffer.readline(MAX_LINE + 1)
        if not line:
            return
        if len(line) > MAX_LINE or not line.endswith(b"\n"):
            raise ValueError("Invalid IPC frame")
        sequences = json.loads(line)["sequences"]
        if (not isinstance(sequences, list) or not 1 <= len(sequences) <= 16
                or any(not isinstance(ids, list) or not 1 <= len(ids) <= 2048 for ids in sequences)
                or sum(map(len, sequences)) > 8192
                or any(type(token) is not int or not 0 <= token < model.config.vocab_size
                       for ids in sequences for token in ids)):
            raise ValueError("Invalid token batch")
        scores = []
        offset = 0
        with torch.inference_mode():
            while offset < len(sequences):
                end = offset + 1
                while (end < len(sequences)
                       and (end + 1 - offset) * max(map(len, sequences[offset:end + 1])) <= max_tokens):
                    end += 1
                batch = sequences[offset:end]
                width = max(map(len, batch))
                token_ids = [[pad_token_id] * (width - len(ids)) + ids for ids in batch]
                masks = [[0] * (width - len(ids)) + [1] * len(ids) for ids in batch]
                inputs = {
                    "input_ids": torch.tensor(token_ids, dtype=torch.long, device="cuda"),
                    "attention_mask": torch.tensor(masks, dtype=torch.long, device="cuda"),
                }
                logits = model(**inputs, logits_to_keep=1, use_cache=False).logits[:, -1, :]
                # Subtract in float32, matching the CPU scoring formula without fp16 overflow.
                values = torch.sigmoid(logits[:, 9693].float() - logits[:, 2152].float())
                if not bool(torch.isfinite(values).all()):
                    raise ValueError("Invalid model scores")
                scores.extend(values.tolist())
                offset = end
        print(json.dumps({"scores": scores}), flush=True)


if __name__ == "__main__":
    for name in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_HUB_DISABLE_TELEMETRY",
                 "HF_HUB_DISABLE_IMPLICIT_TOKEN", "HF_DATASETS_OFFLINE"):
        os.environ[name] = "1"
    try:
        serve()
    except Exception:
        # No raw inputs, model paths, or exception content cross the IPC boundary.
        print(json.dumps({"error": "MODEL_UNAVAILABLE"}), flush=True)
        sys.exit(1)
