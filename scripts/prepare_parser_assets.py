"""Explicit offline assets for Docling 2.66.0 and EasyOCR ru/en; no corpus upload."""

from __future__ import annotations

import argparse
import json
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from prepare_models import PROJECT, download_file, safe_path, verify_file


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-network", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    if args.allow_network and args.verify_only:
        parser.error("--allow-network and --verify-only are mutually exclusive")
    lock = json.loads((PROJECT / "parser-assets.lock.json").read_text(encoding="utf-8"))
    root = safe_path(PROJECT, lock["root"])
    tasks = []
    for model in lock["hf_models"]:
        base = safe_path(PROJECT, model["local_path"])
        if not base.is_relative_to(root):
            raise ValueError("Parser model must be under parser asset root")
        for spec in model["files"]:
            tasks.append((model, spec, safe_path(base, spec["path"]), None))
    for model in lock["ocr_archives"]:
        tasks.append((model, model["archive"], safe_path(root / "downloads", model["archive"]["path"]), model["source_url"]))
    total = sum(spec["size_bytes"] for _, spec, _, _ in tasks)
    print(f"Parser assets: {len(tasks)} files, {total:,} download bytes", flush=True)
    if not args.allow_network and not args.verify_only:
        print("Plan only; use --allow-network or --verify-only.")
        return
    if args.allow_network and shutil.disk_usage(PROJECT).free < total + 1024**3:
        raise RuntimeError("Insufficient free disk space for parser assets plus 1 GiB reserve")

    def process(task):
        model, spec, path, url = task
        record = verify_file(path, spec) if args.verify_only else download_file(model, spec, path, url)
        return {"model_id": model["model_id"], **record}

    with ThreadPoolExecutor(max_workers=2) as pool:
        records = list(pool.map(process, tasks))
    for model in lock["ocr_archives"]:
        spec = model["member"]
        target = safe_path(root / "EasyOcr", spec["path"])
        if not target.exists() and not args.verify_only:
            target.parent.mkdir(parents=True, exist_ok=True)
            archive_path = safe_path(root / "downloads", model["archive"]["path"])
            partial = target.with_name(target.name + ".partial")
            # Read only the locked member; never extract archive-supplied paths.
            with zipfile.ZipFile(archive_path) as archive:
                member = archive.getinfo(spec["path"])
                if member.file_size != spec["size_bytes"]:
                    raise ValueError("Unexpected OCR archive member size")
                with archive.open(member) as source, partial.open("wb") as sink:
                    shutil.copyfileobj(source, sink, length=1024 * 1024)
            verify_file(partial, spec)
            partial.replace(target)
        records.append({"model_id": model["model_id"], **verify_file(target, spec)})
    report = {
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "verification": "all_locked_sizes_and_hashes_matched", "ocr_inference_smoke": "not_run",
        "docling_version": lock["docling_version"], "files": records,
    }
    (root / "artifact_verification.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Parser assets verified at {root}; offline OCR inference remains separate.", flush=True)


if __name__ == "__main__":
    main()
