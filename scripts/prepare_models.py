"""Explicit, resumable download of the three pinned HF snapshots; stdlib only.

Default mode is a network-free plan. --allow-network performs downloads;
--verify-only checks all local bytes without opening a network connection.
No source documents or inference requests are sent by this preparation utility.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
BLOCK_SIZE = 8 * 1024 * 1024


def safe_path(base: Path, relative: str) -> Path:
    path = (base / relative).resolve()
    if not path.is_relative_to(base.resolve()) or path == base.resolve():
        raise ValueError(f"Path escapes model directory: {relative}")
    return path


def verify_file(path: Path, spec: dict) -> dict:
    size = path.stat().st_size
    if size != spec["size_bytes"]:
        raise ValueError(f"Size mismatch for {path.name}: {size} != {spec['size_bytes']}")
    sha256 = hashlib.sha256()
    git_hash = hashlib.sha1(usedforsecurity=False)
    git_hash.update(f"blob {size}\0".encode("ascii"))
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(BLOCK_SIZE), b""):
            sha256.update(block)
            git_hash.update(block)
    digest = sha256.hexdigest()
    if spec.get("sha256") and digest != spec["sha256"]:
        raise ValueError(f"SHA256 mismatch for {path.name}")
    if spec.get("git_blob_sha1") and git_hash.hexdigest() != spec["git_blob_sha1"]:
        raise ValueError(f"Git blob SHA1 mismatch for {path.name}")
    if not spec.get("sha256") and not spec.get("git_blob_sha1"):
        raise ValueError(f"Missing upstream checksum for {path.name}")
    return {"path": spec["path"], "size_bytes": size, "sha256": digest}


class HttpsRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if urllib.parse.urlsplit(newurl).scheme != "https":
            raise ValueError("Refusing non-HTTPS model redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def download_file(model: dict, spec: dict, target: Path, source_url: str | None = None) -> dict:
    if target.exists():
        result = verify_file(target, spec)
        print(f"verified {model['model_id']}/{spec['path']}", flush=True)
        return result
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(target.name + ".partial")
    url = source_url or (
        "https://huggingface.co/" + model["model_id"] + "/resolve/"
        + model["revision"] + "/" + urllib.parse.quote(spec["path"], safe="/")
    )
    if urllib.parse.urlsplit(url).scheme != "https":
        raise ValueError("Model downloads require HTTPS")
    opener = urllib.request.build_opener(HttpsRedirectHandler())
    for attempt in range(1, 4):
        offset = partial.stat().st_size if partial.exists() else 0
        if offset == spec["size_bytes"]:
            result = verify_file(partial, spec)
            partial.replace(target)
            return result
        if offset > spec["size_bytes"]:
            raise ValueError(f"Oversized partial file: {partial.name}")
        headers = {"User-Agent": "digital-expert-model-prepare/1", "Accept-Encoding": "identity"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
        request = urllib.request.Request(url, headers=headers)
        try:
            print(f"download {model['model_id']}/{spec['path']} ({offset}/{spec['size_bytes']} bytes)", flush=True)
            with opener.open(request, timeout=60) as response:
                if offset and response.status == 206:
                    content_range = response.headers.get("Content-Range", "")
                    expected_prefix = f"bytes {offset}-"
                    if not content_range.startswith(expected_prefix):
                        raise ValueError("Unexpected Content-Range for resumed model download")
                    mode = "ab"
                elif response.status == 200:
                    mode = "wb"
                    offset = 0
                else:
                    raise ValueError(f"Unexpected HTTP status: {response.status}")
                last_progress = time.monotonic()
                with partial.open(mode) as stream:
                    while block := response.read(BLOCK_SIZE):
                        stream.write(block)
                        offset += len(block)
                        if offset > spec["size_bytes"]:
                            raise ValueError("Server response exceeds locked artifact size")
                        if time.monotonic() - last_progress >= 20:
                            print(f"progress {model['model_id']}/{spec['path']}: {offset}/{spec['size_bytes']} bytes", flush=True)
                            last_progress = time.monotonic()
            result = verify_file(partial, spec)
            partial.replace(target)
            print(f"complete {model['model_id']}/{spec['path']}", flush=True)
            return result
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            if attempt == 3:
                raise RuntimeError(f"Download failed for {model['model_id']}/{spec['path']}: {type(exc).__name__}") from exc
            print(f"retry {attempt}/3 {model['model_id']}/{spec['path']}: {type(exc).__name__}", flush=True)
            time.sleep(attempt * 2)
    raise RuntimeError("Download attempts exhausted")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, default=PROJECT / "models.lock.json")
    parser.add_argument("--model", choices=("all", "llm", "embedding", "reranker"), default="all")
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    if args.allow_network and args.verify_only:
        parser.error("--allow-network and --verify-only are mutually exclusive")
    if not 1 <= args.workers <= 4:
        parser.error("--workers must be between 1 and 4")
    lock = json.loads(args.lock.read_text(encoding="utf-8-sig"))
    tasks = []
    selected = {
        role: model for role, model in lock["models"].items()
        if args.model in ("all", role)
    }
    for role, model in selected.items():
        revision = model["revision"]
        if len(revision) != 40 or any(c not in "0123456789abcdef" for c in revision):
            raise ValueError(f"Unpinned revision for {role}")
        base = safe_path(PROJECT, model["local_path"])
        if not base.is_relative_to(PROJECT / "models"):
            raise ValueError("Model output must remain under project models/")
        for spec in model["files"]:
            tasks.append((role, model, spec, safe_path(base, spec["path"])))
    total = sum(spec["size_bytes"] for _, _, spec, _ in tasks)
    print(f"Selected {len(selected)} model(s), {len(tasks)} files, {total:,} bytes", flush=True)
    if not args.allow_network and not args.verify_only:
        print("Plan only. Use --allow-network to prepare or --verify-only for offline verification.")
        return
    remaining = sum(spec["size_bytes"] for _, _, spec, path in tasks if not path.exists())
    if args.allow_network and shutil.disk_usage(PROJECT).free < remaining + 2 * 1024**3:
        raise RuntimeError("Insufficient free disk space for selected snapshots plus 2 GiB reserve")

    def process(task):
        role, model, spec, target = task
        result = verify_file(target, spec) if args.verify_only else download_file(model, spec, target)
        return role, result

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        results = list(pool.map(process, tasks))
    for role, model in selected.items():
        report = {
            "model_id": model["model_id"], "revision": model["revision"],
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "verification": "all_sizes_and_upstream_hashes_matched", "inference_smoke": "not_run",
            "files": [result for item_role, result in results if item_role == role],
        }
        report_path = safe_path(PROJECT, model["local_path"]) / "artifact_verification.json"
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("All selected model artifacts verified. Inference smoke remains separate.", flush=True)


if __name__ == "__main__":
    main()
