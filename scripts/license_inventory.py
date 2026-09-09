"""Offline installed-package declarations, not a legal compatibility decision.

Workspace mode uses the current interpreter's metadata reader without importing
scanned packages. Explicit image modes use isolated, offline collectors; the native
metadata fallback disables Python site/.pth processing and application entrypoints.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import importlib
import importlib.metadata as metadata
import json
import os
import platform
import re
import shutil
import sys
import sysconfig
import subprocess
import tomllib
import time
from datetime import datetime, timezone
from collections import Counter
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import yaml
from packaging.licenses import InvalidLicenseExpression, canonicalize_license_expression

SCHEMA = "p12.installed-licenses.v2"
MAX_METADATA_BYTES = 2 * 1024 * 1024
DEFAULT_OUTPUT = "docs/implementation/evidence/p12-licenses/inventory.json"
LICENSE_NAME = re.compile(r"^(licen[sc]e|copying|notice|copyright)([._-].*)?$", re.I)
UNKNOWN = {"unknown", "none", "noassertion", "n/a", "unspecified"}
UNRESOLVED = {"missing", "unknown", "invalid"}
IMAGE_LABELS = ("org.opencontainers.image.licenses", "org.opencontainers.image.source",
                "org.opencontainers.image.revision", "org.opencontainers.image.version",
                "org.opencontainers.image.vendor", "license", "licenses")

# Sent as code to an explicitly selected immutable image, never its service entrypoint.
# -I -S is required by the caller: metadata discovery cannot execute package .pth files.
NATIVE_METADATA_PROBE = r'''
import csv, email.parser, email.policy, hashlib, io, json, re, sys, sysconfig
from pathlib import Path

MAX = 2 * 1024 * 1024
def sha(raw): return hashlib.sha256(raw).hexdigest()
def read(path, boundary, limit=MAX):
    resolved=path.resolve()
    if not resolved.is_relative_to(boundary.resolve()) or not resolved.is_file() or resolved.stat().st_size>limit:
        raise ValueError("METADATA_BOUNDARY")
    with resolved.open("rb") as handle: raw=handle.read(limit+1)
    if len(raw)>limit: raise ValueError("METADATA_LIMIT")
    return raw
def license_file(path, boundary):
    raw=read(path,boundary)
    return {"path_sha256":sha(str(path).encode()),"sha256":sha(raw),"size_bytes":len(raw)}

if not sys.flags.isolated or not sys.flags.no_site: raise ValueError("ISOLATED_METADATA_REQUIRED")
roots={Path(sysconfig.get_paths()["purelib"]).resolve()}
roots.update(p.resolve() for p in Path("/app/.venv/lib").glob("python*/site-packages") if p.is_dir())
roots=sorted(p for p in roots if p.is_dir())
records=[]; visited=set(); parser=email.parser.BytesParser(policy=email.policy.default)
for root in roots:
    candidates=list(root.glob("*.dist-info/METADATA"))+list(root.glob("*.egg-info/PKG-INFO"))
    for meta_path in candidates:
        if meta_path.resolve() in visited: continue
        visited.add(meta_path.resolve())
        record_path=meta_path.parent/"RECORD"
        record_raw=read(record_path,root) if record_path.is_file() else None
        installed_paths=[]
        record_files=[]
        if record_raw:
            for row in csv.reader(io.StringIO(record_raw.decode("utf-8"))):
                if not row: continue
                relative=Path(row[0])
                # Vendored dist-info RECORD entries are relative to their own
                # containing site directory, not the outer environment root.
                candidate=(meta_path.parent.parent/relative).resolve()
                if relative.is_absolute() or not candidate.is_relative_to(root): continue
                record_files.append(candidate)
                if candidate.name=="METADATA" and candidate.parent.name.endswith(".dist-info"):
                    candidates.append(candidate)
                if re.match(r"^(licen[sc]e|copying|notice|copyright)([._-].*)?$",candidate.name,re.I):
                    installed_paths.append(candidate)
        raw=read(meta_path,root); msg=parser.parsebytes(raw)
        files={str(p):license_file(p,root) for p in installed_paths if p.is_file()}
        missing_license_files=[]
        rejected_license_reference_hashes=[]
        for value in msg.get_all("License-File",[]):
            rel=Path(value)
            safe_source=not rel.is_absolute() and ".." not in rel.parts and "\\" not in value and ":" not in value and not rel.name.startswith(".")
            flat=[p for p in installed_paths if p.parent==meta_path.parent and p.name==rel.name] if safe_source else []
            if flat:
                choices=flat
            elif not safe_source or any(part.startswith(".") for part in rel.parts):
                rejected_license_reference_hashes.append(sha(value.encode()))
                continue
            else:
                choices=[meta_path.parent/rel,meta_path.parent/"licenses"/rel]
            chosen=next((p for p in choices if p.is_file()),None)
            if chosen is None:
                # Some older wheels keep declared LICENSES/... under their
                # package directory. Resolve only one exact suffix in this
                # distribution's RECORD; never search arbitrary image files.
                suffix=tuple(rel.parts)
                matches=sorted(set(p for p in record_files if suffix and tuple(p.parts[-len(suffix):])==suffix and p.is_file()))
                if len(matches)==1: chosen=matches[0]
            if chosen is None:
                missing_license_files.append(rel.as_posix())
                continue
            files[str(chosen)]=license_file(chosen,root)
        records.append({"ecosystem":"python","environment_sha256":sha(str(root).encode()),
            "name":msg.get("Name"),"version":msg.get("Version"),"metadata_sha256":sha(raw),
            "metadata_path_sha256":sha(str(meta_path.resolve()).encode()),
            "license_expression":msg.get("License-Expression"),"license_declaration":msg.get("License"),
            "classifiers":[v for v in msg.get_all("Classifier",[]) if v.startswith("License ::")],
            "license_files":sorted(files.values(),key=lambda x:x["path_sha256"]),
            "missing_license_files":sorted(set(missing_license_files)),
            "rejected_license_reference_hashes":sorted(set(rejected_license_reference_hashes)),
            "parent_record_sha256":sha(record_raw) if record_raw else None})

status_path=Path("/var/lib/dpkg/status")
status_raw=read(status_path,Path("/var/lib/dpkg"),32*1024*1024)
for paragraph in re.split(br"\n\s*\n",status_raw):
    if not paragraph.strip(): continue
    msg=parser.parsebytes(paragraph)
    if msg.get("Status")!="install ok installed": continue
    name=msg.get("Package","")
    if not re.fullmatch(r"[a-z0-9][a-z0-9+.-]*",name): raise ValueError("DPKG_PACKAGE_INVALID")
    copyright_path=Path("/usr/share/doc")/name/"copyright"
    files=[license_file(copyright_path,Path("/usr/share/doc"))] if copyright_path.is_file() else []
    # Debian copyright files can describe different licenses for different files.
    # Retain their hashes; do not invent one package-wide SPDX expression.
    records.append({"ecosystem":"debian","environment_sha256":sha(b"/var/lib/dpkg/status"),
        "name":name,"version":msg.get("Version"),"metadata_sha256":sha(paragraph),
        "license_expression":None,"license_declaration":None,"classifiers":[],
        "license_files":files,"missing_license_files":[],"rejected_license_reference_hashes":[],"parent_record_sha256":None})
if not records or len(records)>10000: raise ValueError("PACKAGE_COUNT_INVALID")
output={"schema_version":"p12.native-image-metadata.v1","python_version":sys.version.split()[0],
    "isolated":bool(sys.flags.isolated),"site_disabled":bool(sys.flags.no_site),
    "dpkg_status_sha256":sha(status_raw),"records":records,
    "scope":"Python METADATA/declared vendored METADATA and installed dpkg records; embedded unregistered binaries not enumerated"}
print(json.dumps(output,ensure_ascii=True,sort_keys=True,separators=(",",":")))
'''


class InventoryError(ValueError):
    """Input cannot be safely or completely inventoried."""


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def normalized_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def bounded_read(path: Path, allowed_root: Path, limit: int = MAX_METADATA_BYTES) -> bytes:
    resolved = path.resolve()
    if not resolved.is_relative_to(allowed_root.resolve()):
        raise InventoryError("INPUT_PATH_OUTSIDE_APPROVED_ROOT")
    if not resolved.is_file() or resolved.stat().st_size > limit:
        raise InventoryError("INPUT_MISSING_OR_OVERSIZED")
    with resolved.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        raise InventoryError("INPUT_OVERSIZED")
    return data


def relative_reference(value: str) -> PurePosixPath:
    # Metadata is untrusted: a license reference cannot authorize a filesystem read.
    if "\\" in value or ":" in value:
        raise InventoryError("UNSAFE_LICENSE_REFERENCE")
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(p in {".", ".."} for p in path.parts):
        raise InventoryError("UNSAFE_LICENSE_REFERENCE")
    if any(p.startswith(".") for p in path.parts):
        raise InventoryError("UNSAFE_LICENSE_REFERENCE")
    return path


def license_facts(expression: Any, declaration: Any, classifiers: list[str]) -> dict[str, Any]:
    """Preserve declarations; SPDX normalization never rewrites the original field."""
    result: dict[str, Any] = {
        "expression": expression, "declaration": declaration, "classifiers": sorted(set(classifiers)),
        "status": "missing", "normalized_expression": None, "file_reference": None,
    }
    value = expression if expression is not None else declaration
    if value is None or isinstance(value, str) and not value.strip():
        if classifiers:
            result["status"] = "classifiers"
        return result
    if not isinstance(value, str):
        # npm's historical object/list syntax is retained as a declaration, not
        # translated into AND/OR semantics which its metadata does not establish.
        legacy = expression is None and (
            isinstance(value, dict) and isinstance(value.get("type"), str)
            or isinstance(value, list) and bool(value)
            and all(isinstance(item, dict) and isinstance(item.get("type"), str) for item in value)
        )
        result["status"] = "declaration" if legacy else "invalid"
        return result
    if value.strip().lower() in UNKNOWN:
        result["status"] = "unknown"
        return result
    match = re.fullmatch(r"SEE LICEN[CS]E IN (.+)", value.strip(), re.I)
    if match and expression is None:
        result.update(status="see_license", file_reference=match.group(1))
        return result
    try:
        result["normalized_expression"] = str(canonicalize_license_expression(value))
        result["status"] = "expression"
    except InvalidLicenseExpression:
        result["status"] = "invalid" if expression is not None else "declaration"
    return result


def lock_binding(entries: list[dict[str, Any]], name: str, version: str, *, python: bool) -> dict[str, Any]:
    compare = normalized_name if python else str
    matches = [item for item in entries if compare(item["name"]) == compare(name) and item["version"] == version]
    return {"status": "matched" if matches else "unmatched", "entries": sorted(matches, key=canonical_bytes)}


def python_lock(data: bytes) -> list[dict[str, Any]]:
    lock = tomllib.loads(data.decode("utf-8"))
    return [
        {"name": p["name"], "version": p["version"], "source": p["source"],
         "artifact_hashes": sorted({a["hash"] for a in [p.get("sdist", {}), *p.get("wheels", [])] if "hash" in a})}
        for p in lock["package"]
    ]


def requirements_lock(data: bytes) -> list[dict[str, Any]]:
    """Read uv's pinned requirements output, without evaluating pip directives."""
    entries: list[dict[str, Any]] = []
    for line in data.decode("utf-8").splitlines():
        value = line.strip().removesuffix("\\").strip()
        if not value or value.startswith("#"):
            continue
        pin = re.fullmatch(r"([A-Za-z0-9_.-]+)==([A-Za-z0-9_.+!-]+)", value)
        checksum = re.fullmatch(r"--hash=(sha256:[0-9a-f]{64})", value)
        if pin:
            entries.append({"name": pin[1], "version": pin[2],
                            "source": {"requirements_pin": True}, "artifact_hashes": []})
        elif checksum and entries:
            entries[-1]["artifact_hashes"].append(checksum[1])
        else:
            raise InventoryError("UNSUPPORTED_REQUIREMENTS_LOCK_SYNTAX")
    names = [normalized_name(e["name"]) for e in entries]
    if not entries or len(names) != len(set(names)):
        raise InventoryError("REQUIREMENTS_LOCK_EMPTY_OR_DUPLICATE")
    for entry in entries:
        entry["artifact_hashes"] = sorted(set(entry["artifact_hashes"]))
        if not entry["artifact_hashes"]:
            raise InventoryError("REQUIREMENTS_LOCK_HASH_MISSING")
    return entries


def npm_lock(data: bytes) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    lock = yaml.safe_load(data)
    entries = []
    for key, item in lock["packages"].items():
        name, version = key.split("(", 1)[0].rsplit("@", 1)
        entries.append({"name": name, "version": version, "resolution": item.get("resolution", {})})
    groups: dict[str, list[str]] = {}
    for group, values in lock.get("importers", {}).get(".", {}).items():
        if group in {"dependencies", "devDependencies", "optionalDependencies"}:
            for name in values:
                groups.setdefault(name, []).append(group)
    return entries, groups


def importer_errors(raw: bytes, frontend: Path) -> list[dict[str, str]]:
    """A matching orphan in the store cannot hide a stale root dependency link."""
    importer = yaml.safe_load(raw).get("importers", {}).get(".", {})
    manifest = json.loads(bounded_read(frontend / "package.json", frontend))
    result = []
    for group in ("dependencies", "devDependencies", "optionalDependencies"):
        declared = manifest.get(group, {})
        locked = importer.get(group, {})
        for name in sorted(set(declared) | set(locked)):
            record = locked.get(name, {})
            code = None
            expected = str(record.get("version", "")).split("(", 1)[0]
            if name not in declared or name not in locked or declared[name] != record.get("specifier"):
                code = "NPM_MANIFEST_IMPORTER_MISMATCH"
            else:
                relative_reference(name)
                installed = frontend / "node_modules" / name / "package.json"
                if not installed.is_file():
                    if group == "optionalDependencies":
                        continue
                    code = "NPM_DIRECT_DEPENDENCY_MISSING"
                else:
                    actual = json.loads(bounded_read(installed, frontend / "node_modules"))
                    if actual.get("name") != name or actual.get("version") != expected:
                        code = "NPM_DIRECT_DEPENDENCY_VERSION_MISMATCH"
            if code:
                result.append({"ecosystem": "npm", "name": name, "version": expected, "code": code})
    return result


def file_fact(path: Path, base: Path) -> dict[str, Any]:
    data = bounded_read(path, base)
    return {"path": path.relative_to(base).as_posix(), "size_bytes": len(data), "sha256": digest(data)}


def python_record(dist: metadata.Distribution, site: Path, entries: list[dict[str, Any]]) -> dict[str, Any]:
    fields = dist.metadata
    name, version = fields["Name"], dist.version
    if not name or not version:
        raise InventoryError("PYTHON_IDENTITY_MISSING")
    classifiers = [s for s in fields.get_all("Classifier", []) if s.startswith("License ::")]
    facts = license_facts(fields.get("License-Expression"), fields.get("License"), classifiers)
    declared_files = sorted(set(fields.get_all("License-File", [])))
    files = [Path(str(dist.locate_file(item))) for item in dist.files or []]
    # A parent wheel's RECORD may also list vendored distributions. They are
    # independent component occurrences, not alternative parent metadata.
    metadatas = [p for p in files if p.name in {"METADATA", "PKG-INFO"} and p.parent.parent == site]
    if len(metadatas) != 1:
        raise InventoryError("PYTHON_METADATA_FILE_MISSING_OR_AMBIGUOUS")
    dist_root = metadatas[0].parent
    metadata_file = file_fact(metadatas[0], site)
    errors: list[str] = []
    candidates = {p for p in files if p.is_relative_to(dist_root) and LICENSE_NAME.match(p.name)}
    for reference in declared_files + ([facts["file_reference"]] if facts["file_reference"] else []):
        try:
            # Older wheels flatten source-tree license directories into dist-info.
            # A basename fallback is permitted only when the exact installed file
            # is listed by this distribution's RECORD; never follow that source path.
            source_ref = PurePosixPath(reference)
            safe_source = (not source_ref.is_absolute() and "\\" not in reference and ":" not in reference
                           and ".." not in source_ref.parts and not source_ref.name.startswith("."))
            flat = dist_root / source_ref.name
            flattened = safe_source and flat in files and flat.is_file()
            if flattened:
                matches = [flat]
            else:
                relative = relative_reference(reference)
                matches = [p for p in (dist_root / "licenses" / relative, dist_root / relative) if p.is_file()]
            if not matches:
                errors.append("DECLARED_LICENSE_FILE_MISSING")
            candidates.update(matches)
        except InventoryError as exc:
            errors.append(str(exc))
    license_files = []
    for path in sorted(candidates):
        try:
            license_files.append(file_fact(path, dist_root))
        except InventoryError as exc:
            errors.append(str(exc))
    urls = fields.get_all("Project-URL", []) + ([fields["Home-page"]] if fields.get("Home-page") else [])
    binding = lock_binding(entries, name, version, python=True)
    editable = any("editable" in item["source"] for item in binding["entries"])
    return {
        "ecosystem": "python", "name": name, "version": version,
        "license": facts, "declared_license_files": declared_files, "license_files": license_files,
        "metadata_files": [metadata_file], "source_urls": sorted(set(urls)), "lock": binding,
        "distribution_mode": "workspace_editable" if editable else "installed_distribution",
        "reviewed_at": None, "review_status": "not_reviewed", "errors": sorted(set(errors)),
    }


def vendored_records(dist: metadata.Distribution, site: Path, parent: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    files = [Path(str(dist.locate_file(item))) for item in dist.files or []]
    for path in sorted(set(files)):
        if path.name != "METADATA" or path.parent.parent == site:
            continue
        # Only nested distribution metadata declared in the parent's RECORD is
        # visited. No recursive scan of source modules or package imports.
        bounded_read(path, site)
        if not path.parent.name.endswith(".dist-info"):
            continue
        record = python_record(metadata.PathDistribution(path.parent), path.parent.parent, [])
        record["distribution_mode"] = "vendored_distribution"
        record["vendored_parent"] = {"name": parent["name"], "version": parent["version"],
                                     "metadata_sha256": parent["metadata_files"][0]["sha256"]}
        record["lock"] = {"status": "parent_distribution", "entries": parent["lock"]["entries"],
                          "parent_status": parent["lock"]["status"], "individual_version_locked": False}
        for fact in record["metadata_files"]:
            fact["path"] = (path.parent.parent.relative_to(site) / fact["path"]).as_posix()
        result.append(record)
    return result


def package_roots(node_modules: Path) -> list[Path]:
    store = node_modules / ".pnpm"
    if not store.is_dir():
        raise InventoryError("PNPM_STORE_MISSING")
    roots: set[Path] = set()
    # Inspect only package-root package.json files, not recursive package sources.
    # Also visit root links: a missing/stale top-level dependency must be visible.
    containers = [node_modules] + [p / "node_modules" for p in store.iterdir() if p.is_dir()]
    for container in containers:
        if not container.is_dir():
            continue
        for item in container.iterdir():
            if item.name.startswith("."):
                continue
            candidates = list(item.iterdir()) if item.name.startswith("@") and item.is_dir() else [item]
            for package in candidates:
                resolved = package.resolve()
                if not resolved.is_relative_to(node_modules.resolve()):
                    raise InventoryError("NPM_PACKAGE_OUTSIDE_NODE_MODULES")
                if (resolved / "package.json").is_file():
                    roots.add(resolved)
    if not roots:
        raise InventoryError("PNPM_PACKAGES_MISSING")
    return sorted(roots)


def resolve_dependency(package: Path, name: str, node_modules: Path) -> Path | None:
    relative = relative_reference(name)
    candidates = [package / "node_modules" / relative]
    candidates.extend(p / relative for p in package.parents
                      if p.name == "node_modules" and p.is_relative_to(node_modules))
    candidates.append(node_modules / relative)
    for candidate in candidates:
        if (candidate / "package.json").is_file():
            resolved = candidate.resolve()
            if not resolved.is_relative_to(node_modules):
                raise InventoryError("NPM_DEPENDENCY_OUTSIDE_NODE_MODULES")
            return resolved
    return None


def reachable_npm(frontend: Path) -> tuple[set[Path], list[dict[str, str]], list[dict[str, str]]]:
    """Walk installed links/manifest edges without loading JavaScript or a solver."""
    nodes = (frontend / "node_modules").resolve()
    pending = [frontend.resolve()]
    seen: set[Path] = set()
    edges: list[dict[str, str]] = []
    errors: list[dict[str, str]] = []
    while pending:
        package = pending.pop()
        if package in seen:
            continue
        seen.add(package)
        value = json.loads(bounded_read(package / "package.json", package))
        groups = ["dependencies", "optionalDependencies", "peerDependencies"]
        if package == frontend.resolve():
            groups.append("devDependencies")
        for group in groups:
            for name in sorted(value.get(group, {})):
                optional = (group == "optionalDependencies" or name in value.get("optionalDependencies", {})
                            or group == "peerDependencies"
                            and value.get("peerDependenciesMeta", {}).get(name, {}).get("optional", False))
                target = resolve_dependency(package, name, nodes)
                if target is None:
                    if not optional:
                        errors.append({"ecosystem": "npm", "name": value["name"], "version": value["version"],
                                       "code": "NPM_REQUIRED_DEPENDENCY_MISSING:" + name})
                    continue
                linked = json.loads(bounded_read(target / "package.json", nodes))
                edges.append({"from_name": value["name"], "from_version": value["version"], "group": group,
                              "dependency_name": name, "to_name": linked["name"], "to_version": linked["version"],
                              "installed_path": target.relative_to(nodes).as_posix()})
                pending.append(target)
    seen.remove(frontend.resolve())
    return seen, sorted(edges, key=canonical_bytes), sorted(errors, key=canonical_bytes)


def npm_record(package: Path, node_modules: Path, entries: list[dict[str, Any]],
               groups: dict[str, list[str]], *, workspace: bool = False) -> dict[str, Any]:
    raw = bounded_read(package / "package.json", package)
    value = json.loads(raw)
    name, version = value.get("name"), value.get("version")
    if not isinstance(name, str) or not name or not isinstance(version, str) or not version:
        raise InventoryError("NPM_IDENTITY_MISSING")
    facts = license_facts(None, value.get("license", value.get("licenses")), [])
    errors: list[str] = []
    candidates = {p for p in package.iterdir() if p.is_file() and LICENSE_NAME.match(p.name)}
    if facts["file_reference"]:
        try:
            path = package / relative_reference(facts["file_reference"])
            if not path.is_file():
                errors.append("DECLARED_LICENSE_FILE_MISSING")
            else:
                candidates.add(path)
        except InventoryError as exc:
            errors.append(str(exc))
    license_files = []
    for path in sorted(candidates):
        try:
            license_files.append(file_fact(path, package))
        except InventoryError as exc:
            errors.append(str(exc))
    source = value.get("repository")
    urls = [source] if isinstance(source, str) else [source["url"]] if isinstance(source, dict) and "url" in source else []
    if isinstance(value.get("homepage"), str):
        urls.append(value["homepage"])
    binding = {"status": "workspace_manifest", "entries": []} if workspace else lock_binding(entries, name, version, python=False)
    return {
        "ecosystem": "npm", "name": name, "version": version, "license": facts,
        "license_files": license_files, "metadata_sha256": digest(raw),
        "installed_paths": ["workspace:apps/frontend"] if workspace else [package.relative_to(node_modules).as_posix()],
        "source_urls": sorted(set(urls)), "lock": binding,
        "direct_dependency_groups": sorted(groups.get(name, [])),
        "distribution_mode": "private_workspace" if workspace and value.get("private") else "installed_package",
        "reviewed_at": None, "review_status": "not_reviewed", "errors": sorted(set(errors)),
    }


def merge_npm(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        key = record["name"], record["version"]
        if key not in grouped:
            grouped[key] = record
            continue
        previous = grouped[key]
        before = {k: v for k, v in previous.items() if k not in {"installed_paths", "distribution_mode"}}
        after = {k: v for k, v in record.items() if k not in {"installed_paths", "distribution_mode"}}
        if before != after:
            raise InventoryError("NPM_SAME_VERSION_METADATA_CONFLICT")
        previous["installed_paths"] = sorted(set(previous["installed_paths"] + record["installed_paths"]))
        if record["distribution_mode"] == "installed_package":
            previous["distribution_mode"] = "installed_package"
    return list(grouped.values())


def blockers(records: list[dict[str, Any]]) -> list[dict[str, str]]:
    result = []
    for record in records:
        if record["distribution_mode"] == "unreferenced_pnpm_store":
            continue
        reasons = list(record["errors"])
        if record["lock"]["status"] == "unmatched":
            reasons.append("INSTALLED_VERSION_NOT_LOCKED")
        first_party_missing = (record["distribution_mode"] in {"workspace_editable", "private_workspace"}
                               and record["license"]["status"] == "missing")
        if record["license"]["status"] in UNRESOLVED and not first_party_missing:
            reasons.append("LICENSE_" + record["license"]["status"].upper())
        for reason in sorted(set(reasons)):
            result.append({"ecosystem": record["ecosystem"], "name": record["name"],
                           "version": record["version"], "code": reason})
    return sorted(result, key=canonical_bytes)


def build_inventory(root: Path, site: Path, *, python_lock_paths: list[Path] | None = None,
                    include_frontend: bool = True) -> dict[str, Any]:
    root, site = root.resolve(), site.resolve()
    if not site.is_relative_to(root) or not site.relative_to(root).parts[0].startswith(".venv"):
        raise InventoryError("PYTHON_ENVIRONMENT_OUTSIDE_WORKSPACE_VENV")
    if site.name not in {"site-packages", "dist-packages"} or not site.is_dir():
        raise InventoryError("PYTHON_SITE_PACKAGES_MISSING")
    npm_lock_path = root / "apps/frontend/pnpm-lock.yaml"
    inputs: dict[str, str] = {}
    py_entries: list[dict[str, Any]] = []
    for lock_path in sorted({p.resolve() for p in python_lock_paths or [root / "uv.lock"]}):
        raw = bounded_read(lock_path, root, 16 * 1024 * 1024)
        reference = lock_path.relative_to(root).as_posix()
        inputs[reference] = digest(raw)
        entries = python_lock(raw) if lock_path.name == "uv.lock" else requirements_lock(raw)
        py_entries.extend(dict(entry, lock_file=reference) for entry in entries)
    environment_config: dict[str, str] = {}
    config_path = root / site.relative_to(root).parts[0] / "pyvenv.cfg"
    if config_path.is_file():
        config_raw = bounded_read(config_path, root, 16384)
        inputs[config_path.relative_to(root).as_posix()] = digest(config_raw)
        for line in config_raw.decode("utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() in {"implementation", "version_info", "uv", "include-system-site-packages"}:
                environment_config[key.strip()] = value.strip()
    distributions = list(metadata.distributions(path=[str(site)]))
    if not distributions:
        raise InventoryError("PYTHON_DISTRIBUTIONS_MISSING")
    records = [python_record(d, site, py_entries) for d in distributions]
    identities = [(normalized_name(r["name"]), r["version"]) for r in records]
    if len(set(identities)) != len(identities) or len({n for n, _ in identities}) != len(identities):
        raise InventoryError("DUPLICATE_PYTHON_DISTRIBUTION")
    vendor_records = [r for d, parent in zip(distributions, records, strict=True)
                      for r in vendored_records(d, site, parent)]
    records.extend(vendor_records)
    frontend = root / "apps/frontend"
    nodes = frontend / "node_modules"
    dependency_edges: list[dict[str, str]] = []
    dependency_errors: list[dict[str, str]] = []
    if include_frontend:
        npm_raw = bounded_read(npm_lock_path, root, 16 * 1024 * 1024)
        inputs[npm_lock_path.relative_to(root).as_posix()] = digest(npm_raw)
        npm_entries, groups = npm_lock(npm_raw)
        reachable, dependency_edges, dependency_errors = reachable_npm(frontend)
        npm_records = []
        for package in sorted(set(package_roots(nodes)) | reachable):
            record = npm_record(package, nodes, npm_entries, groups)
            if package not in reachable:
                record["distribution_mode"] = "unreferenced_pnpm_store"
            npm_records.append(record)
        records += merge_npm(npm_records)
        records.append(npm_record(frontend, nodes, npm_entries, groups, workspace=True))
        dependency_errors += importer_errors(npm_raw, frontend)
    records.sort(key=lambda r: (r["ecosystem"], normalized_name(r["name"]), r["version"]))
    issues = sorted(blockers(records) + dependency_errors, key=canonical_bytes)
    first_party_findings = [
        {"ecosystem": r["ecosystem"], "name": r["name"], "version": r["version"],
         "code": "FIRST_PARTY_LICENSE_NOT_DECLARED", "distribution_authorized": False}
        for r in records if r["distribution_mode"] in {"workspace_editable", "private_workspace"}
        and r["license"]["status"] == "missing"
    ]
    return {
        "schema_version": SCHEMA,
        "scope": {
            "python_site_packages": site.relative_to(root).as_posix(),
            "collector_python": platform.python_version(), "collector_platform": sys.platform,
            "scanned_environment_config": environment_config,
            "scanned_environment_is_collector": site == Path(sysconfig.get_paths()["purelib"]).resolve(),
            "frontend": "root dependency links plus dependencies/optional/peer edges; build tools included"
                        if include_frontend else "excluded; separate inventory, no duplicate frontend occurrences",
            "vendored_packages": "nested METADATA declared in parent RECORD; parent binding, not independent lock pins",
            "unreferenced_store": "separately inventoried cache artifacts; not reachable runtime/build components",
            "excluded": ["uninstalled lock alternatives", "container OS packages", "model weights", "private corpus"],
            "legal_compatibility_assessed": False,
        },
        "inputs": inputs,
        "collector": {"script_sha256": digest(Path(__file__).read_bytes()),
                      "packaging_version": metadata.version("packaging"), "pyyaml_version": metadata.version("PyYAML")},
        "summary": {"package_count": len(records),
                    "by_ecosystem": dict(sorted(Counter(r["ecosystem"] for r in records).items())),
                    "license_statuses": dict(sorted(Counter(r["license"]["status"] for r in records).items())),
                    "unreferenced_store_count": sum(r["distribution_mode"] == "unreferenced_pnpm_store" for r in records),
                    "vendored_distribution_count": len(vendor_records),
                    "blocker_count": len(issues), "first_party_missing_count": len(first_party_findings),
                    "reviewed_count": 0},
        "blockers": issues, "first_party_review_findings": first_party_findings,
        "npm_dependency_edges": dependency_edges, "packages": records,
    }


def output_path(root: Path, value: str) -> Path:
    candidate = Path(value)
    candidate = (root / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
    permitted = [root / ".cache", root / "ci-results", root / "docs/implementation/evidence/p12-licenses"]
    if candidate.suffix != ".json" or not any(candidate.is_relative_to(p.resolve()) for p in permitted):
        raise InventoryError("OUTPUT_OUTSIDE_APPROVED_EVIDENCE_DIRECTORY")
    return candidate


def check_inventory(path: Path, current: dict[str, Any]) -> list[str]:
    failures = []
    if not path.is_file():
        failures.append("INVENTORY_MISSING")
    else:
        try:
            saved = json.loads(path.read_bytes())
            if saved != current:
                failures.append("INVENTORY_STALE_OR_INVALID")
        except (ValueError, OSError):
            failures.append("INVENTORY_INVALID")
    if current["blockers"]:
        failures.append("UNRESOLVED_PACKAGE_DECLARATIONS_OR_LOCK_BINDINGS")
    return failures


def docker_metadata(arguments: list[str]) -> list[Any]:
    """Only fixed read-only inspect calls reach here; no arbitrary command input."""
    result = subprocess.run(["docker", *arguments], capture_output=True, timeout=30, check=False)
    if result.returncode != 0 or len(result.stdout) > 256 * 1024:
        raise InventoryError("LOCAL_IMAGE_METADATA_UNAVAILABLE")
    return [json.loads(line) for line in result.stdout.decode("utf-8").splitlines()]


def build_image_inventory(root: Path, baseline_path: Path) -> dict[str, Any]:
    """Image identities and supplied labels only; never a package-license PASS."""
    root = root.resolve()
    baseline_raw = bounded_read(baseline_path, root, 4 * 1024 * 1024)
    compose_raw = bounded_read(root / "compose.yaml", root)
    baseline = json.loads(baseline_raw)
    if baseline.get("schema_version") != "p12.image-matrix-baseline.v1":
        raise InventoryError("IMAGE_BASELINE_INVALID")
    endpoint = os.environ.get("DOCKER_HOST")
    if not endpoint:
        endpoints = docker_metadata(["context", "inspect", "--format", "{{json .Endpoints.docker.Host}}"])
        endpoint = endpoints[0] if len(endpoints) == 1 else None
    if not isinstance(endpoint, str) or not endpoint.startswith(("npipe://", "unix://")):
        raise InventoryError("REMOTE_DOCKER_ENDPOINT_FORBIDDEN")
    targets: dict[str, dict[str, Any]] = {}
    pinned = {r["tag"]: r["id"] for r in baseline["images"]}
    for service, definition in yaml.safe_load(compose_raw)["services"].items():
        reference = definition.get("image")
        if reference is None:
            raise InventoryError("COMPOSE_SERVICE_IMAGE_MISSING")
        if not isinstance(reference, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/:@-]{0,511}", reference) is None:
            raise InventoryError("IMAGE_REFERENCE_INVALID")
        if reference not in pinned and re.search(r"@sha256:[a-f0-9]{64}$", reference) is None:
            raise InventoryError("IMAGE_REFERENCE_NOT_PINNED")
        target = targets.setdefault(reference, {"expected_id": pinned.get(reference), "uses": []})
        target["uses"].append({"kind": "compose_service", "name": service})
    for container in baseline.get("containers", []):
        if not container["name"].startswith(("nic-msi-expert-", "nic-msi-p00-llm-")):
            continue  # Unrelated local projects never become collection targets.
        image_id = container["image_id"]
        if re.fullmatch(r"sha256:[a-f0-9]{64}", image_id) is None:
            raise InventoryError("IMAGE_BASELINE_ID_INVALID")
        target = targets.setdefault(image_id, {"expected_id": image_id, "uses": []})
        target["uses"].append({"kind": "container_in_scanner_baseline", "name": container["name"]})
    grouped: dict[str, dict[str, Any]] = {}
    projection = "\n".join(["{{json .Id}}", "{{json .RepoDigests}}", "{{json .Os}}", "{{json .Architecture}}",
                            "{{json .Size}}", *['{{if .Config.Labels}}{{json (index .Config.Labels "'
                            + key + '")}}{{else}}null{{end}}' for key in IMAGE_LABELS]])
    for reference, target in sorted(targets.items()):
        fields = docker_metadata(["image", "inspect", reference, "--format", projection])
        if len(fields) != 5 + len(IMAGE_LABELS) or re.fullmatch(r"sha256:[a-f0-9]{64}", fields[0]) is None:
            raise InventoryError("IMAGE_INSPECT_INVALID")
        image_id, digests, operating_system, arch, size = fields[:5]
        if target["expected_id"] is not None and target["expected_id"] != image_id:
            raise InventoryError("IMAGE_TAG_CHANGED_SINCE_SCANNER_BASELINE")
        labels = dict(zip(IMAGE_LABELS, fields[5:], strict=True))
        if any(v is not None and (not isinstance(v, str) or len(v) > 4096) for v in labels.values()):
            raise InventoryError("IMAGE_LABEL_INVALID_OR_OVERSIZED")
        declarations = {k: license_facts(None, labels[k], []) for k in IMAGE_LABELS if "license" in k}
        identity = {"image_id": image_id, "repo_digests": sorted(digests or []), "os": operating_system,
                    "architecture": arch, "size_bytes": size, "labels": labels, "license_declarations": declarations,
                    "component_licenses": None, "package_license_coverage": "unavailable",
                    "review_status": "not_reviewed", "reviewed_at": None}
        if image_id not in grouped:
            grouped[image_id] = dict(identity, references=[], uses=[])
        record = grouped[image_id]
        if {k: record[k] for k in identity} != identity:
            raise InventoryError("IMAGE_IDENTITY_METADATA_CONFLICT")
        record["references"].append(reference)
        record["uses"].extend(target["uses"])
    images = sorted(grouped.values(), key=lambda r: r["image_id"])
    issues = [{"image_id": r["image_id"], "code": "IMAGE_PACKAGE_LICENSE_INVENTORY_UNAVAILABLE"} for r in images]
    return {"schema_version": "p12.local-image-license-provenance.v1",
            "scope": {"read_only_docker_inspect": True, "labels_only": True, "model_weights_read": False,
                      "container_filesystems_read": False, "legal_compatibility_assessed": False,
                      "deployment_state": "container uses from hashed scanner baseline, not a live deployment assertion"},
            "inputs": {baseline_path.resolve().relative_to(root).as_posix(): digest(baseline_raw),
                       "compose.yaml": digest(compose_raw)},
            "collector": {"script_sha256": digest(Path(__file__).read_bytes())},
            "summary": {"image_count": len(images), "reference_count": len(targets), "blocker_count": len(issues),
            "package_license_coverage_count": 0, "reviewed_count": 0}, "images": images, "blockers": issues}


def image_component_projection(raw: bytes, expected_image: str) -> dict[str, Any]:
    """Retain package declarations and classifier findings, never source/config bodies."""
    report = json.loads(raw)
    if (report.get("SchemaVersion") != 2 or report.get("Metadata", {}).get("ImageID") != expected_image
            or not isinstance(report.get("Results"), list)):
        raise InventoryError("IMAGE_COMPONENT_REPORT_IDENTITY_INVALID")
    packages: dict[str, dict[str, Any]] = {}
    detected: list[dict[str, Any]] = []
    targets = []
    for result in report["Results"]:
        kind = result.get("Type")
        target = result.get("Target")
        # License-file results may omit Type. Retain that absence, rather than
        # inventing a package ecosystem from a classifier-only result.
        if not isinstance(target, str) or (kind is not None and not isinstance(kind, str)):
            raise InventoryError("IMAGE_COMPONENT_TARGET_INVALID")
        target_hash = digest(target.encode())
        targets.append({"target_sha256": target_hash, "type": kind, "class": result.get("Class")})
        for package in result.get("Packages", []):
            if not isinstance(package.get("Name"), str) or not package["Name"]:
                raise InventoryError("IMAGE_COMPONENT_NAME_MISSING")
            version = package.get("Version")
            if version is not None and not isinstance(version, str):
                raise InventoryError("IMAGE_COMPONENT_VERSION_INVALID")
            # Go binaries built from local modules can omit a version. The
            # image digest still pins the artifact; do not assign its tag as
            # an invented version of an embedded component.
            names = package.get("Licenses") or []
            if not isinstance(names, list) or any(not isinstance(name, str) or len(name) > 4096 for name in names):
                raise InventoryError("IMAGE_COMPONENT_LICENSE_INVALID")
            identity = {"target_sha256": target_hash, "type": kind, "package_id": package.get("ID"),
                        "name": package["Name"], "version": version,
                        "release": package.get("Release"), "epoch": package.get("Epoch"),
                        "architecture": package.get("Arch")}
            key = digest(canonical_bytes(identity))
            record = {**identity, "occurrence_id": key, "declared_licenses": sorted(set(names)),
                      "version_status": "reported" if version else "unknown",
                      "license_facts": [license_facts(None, name, []) for name in sorted(set(names))],
                      "license_status": "reported" if names else "unknown",
                      "source": "Trivy installed package metadata; not upstream legal review",
                      "review_status": "not_reviewed", "reviewed_at": None}
            if key in packages and packages[key] != record:
                raise InventoryError("IMAGE_COMPONENT_DECLARATION_CONFLICT")
            packages[key] = record
        for finding in result.get("Licenses", []):
            name = finding.get("Name")
            if not isinstance(name, str) or not name or len(name) > 4096:
                raise InventoryError("IMAGE_CLASSIFIED_LICENSE_INVALID")
            # Classifier output is a separate observation. It cannot silently
            # become a package's declared license or a compatibility decision.
            detected.append({"target_sha256": target_hash, "type": kind, "name": name,
                             "package_name": finding.get("PkgName"), "confidence": finding.get("Confidence"),
                             "file_path_sha256": digest(str(finding.get("FilePath", "")).encode()),
                             "source": "Trivy license classifier", "review_status": "not_reviewed"})
    if not packages:
        raise InventoryError("IMAGE_PACKAGE_INVENTORY_EMPTY")
    records = sorted(packages.values(), key=lambda r: (r["type"] or "", r["name"], r["version"] or "", r["occurrence_id"]))
    unknown = sum(p["license_status"] == "unknown" for p in records)
    return {"packages": records, "targets": sorted(targets, key=canonical_bytes),
            "classified_license_observations": sorted(detected, key=canonical_bytes),
            "summary": {"package_occurrences": len(records), "reported_license_occurrences": len(records) - unknown,
                        "unknown_license_occurrences": unknown, "classifier_observations": len(detected),
                        "unknown_version_occurrences": sum(p["version_status"] == "unknown" for p in records),
                        "reviewed_count": 0},
            "inventory_complete_for_detected_packages": True,
            "all_embedded_components_detected": None, "legal_compatibility_assessed": False}


def collect_image_components(root: Path, image_id: str, provenance: Path, output: Path, *,
                             license_full: bool = True, run=None) -> dict[str, Any]:
    """Export one exact local image; pinned offline license scan in owned scratch."""
    scanner = importlib.import_module("scripts.scan_project" if __package__ else "scan_project")
    if not isinstance(scanner.__file__, str):
        raise InventoryError("SCANNER_HELPER_SOURCE_UNAVAILABLE")
    run = run or scanner.command
    root = root.resolve()
    output = output_path(root, str(output))
    if output.exists():
        raise InventoryError("IMAGE_COMPONENT_OUTPUT_ALREADY_EXISTS")
    provenance_raw = bounded_read(provenance, root, 8 * 1024 * 1024)
    inventory = json.loads(provenance_raw)
    if inventory.get("schema_version") != "p12.local-image-license-provenance.v1":
        raise InventoryError("IMAGE_PROVENANCE_INVALID")
    matches = [row for row in inventory["images"] if row["image_id"] == image_id]
    if len(matches) != 1 or re.fullmatch(r"sha256:[a-f0-9]{64}", image_id) is None:
        raise InventoryError("IMAGE_NOT_IN_EXACT_PROVENANCE")
    owner = uuid4().hex
    workspace = root / ".cache/license-images" / owner
    workspace.mkdir(parents=True, exist_ok=False)
    for name in ("cache", "raw"):
        (workspace / name).mkdir()
    archive, raw_path = workspace / "image.tar", workspace / "raw/scan.json"
    report: dict[str, Any] = {"schema_version": "p12.image-component-licenses.v1", "state": "running",
        "image_id": image_id, "run_id": owner, "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "provenance_sha256": digest(provenance_raw), "script_sha256": digest(Path(__file__).read_bytes()),
        "scanner_helper_sha256": digest(Path(scanner.__file__).read_bytes()),
        "scanner_image": scanner.TRIVY_IMAGE, "scanner_version": scanner.TRIVY_VERSION,
        "network": "none", "main_services_modified": False, "weights_or_secrets_mounted": False,
        "license_full": license_full, "phase": "local_identity", "error_code": None}
    docker: list[str] = []
    name = "expert-license-" + owner[:12]
    started = time.monotonic()
    try:
        docker = scanner.local_docker(run)
        for reference, is_scanner in ((scanner.TRIVY_IMAGE, True), (image_id, False)):
            value = json.loads(run([*docker, "image", "inspect", "--format",
                '{"id":{{json .Id}},"size":{{json .Size}},"os":{{json .Os}},"arch":{{json .Architecture}},"digests":{{json .RepoDigests}}}', reference], timeout=30))
            if value["os"] != "linux" or value["arch"] != "amd64":
                raise InventoryError("IMAGE_COMPONENT_PLATFORM_MISMATCH")
            if is_scanner:
                if scanner.TRIVY_IMAGE not in value["digests"]:
                    raise InventoryError("LICENSE_SCANNER_IDENTITY_MISMATCH")
                report["scanner_image_id"] = value["id"]
            elif value["id"] != image_id or value["size"] != matches[0]["size_bytes"]:
                raise InventoryError("LICENSE_TARGET_IDENTITY_MISMATCH")
        needed = int(matches[0]["size_bytes"] * 1.5) + 2 * 1024**3
        if shutil.disk_usage(workspace).free < needed:
            raise InventoryError("IMAGE_COMPONENT_DISK_BUDGET")
        report["phase"] = "export"
        run([*docker, "image", "save", "--output", str(archive), image_id], timeout=600)
        if not archive.is_file() or not 0 < archive.stat().st_size <= 40 * 1024**3:
            raise InventoryError("IMAGE_ARCHIVE_MISSING_OR_OVERSIZED")
        report["archive_sha256"] = scanner.digest(archive)
        report["archive_size_bytes"] = archive.stat().st_size
        report["phase"] = "offline_license_scan"
        arguments = [*scanner.container_args(docker, workspace, name, owner, network="none", image_scan=True),
            *scanner.mount(archive, "/input/image.tar", True), *scanner.mount(workspace / "raw", "/results"),
            scanner.TRIVY_IMAGE, "image", "--input", "/input/image.tar", "--cache-dir", "/cache",
            "--skip-db-update", "--skip-java-db-update", "--skip-check-update", "--offline-scan",
            "--disable-telemetry", "--skip-version-check", "--no-progress", "--parallel", "2",
            "--scanners", "license", *(["--license-full"] if license_full else []),
            "--list-all-pkgs", "--pkg-types", "os,library",
            "--format", "json", "--output", "/results/scan.json", "--timeout", "12m"]
        run(arguments, timeout=780)
        report["phase"] = "projection"
        raw = bounded_read(raw_path, workspace, 128 * 1024 * 1024)
        report["raw_report_sha256"] = digest(raw)
        parsed = json.loads(raw)
        report["result_shape"] = [{"type_present": isinstance(row.get("Type"), str),
            "target_present": isinstance(row.get("Target"), str),
            "package_count": len(row.get("Packages") or []), "license_count": len(row.get("Licenses") or []),
            "missing_version_count": sum(not p.get("Version") for p in row.get("Packages") or []),
            "missing_name_count": sum(not p.get("Name") for p in row.get("Packages") or [])}
            for row in parsed.get("Results", [])]
        report.update(image_component_projection(raw, image_id), state="collected")
    except Exception as error:
        report.update(state="error", error_code=str(error) if isinstance(error, InventoryError)
                      else error.code if isinstance(error, scanner.ScanError) else "IMAGE_COMPONENT_COLLECTION_FAILED")
        if isinstance(error, scanner.ScanError) and getattr(error, "diagnostics", None):
            report["command_diagnostics"] = error.diagnostics
    finally:
        if docker:
            try:
                scanner.cleanup_container(docker, name, owner, run)
                report["owned_container_absent"] = True
            except Exception:
                report.update(state="error", cleanup_error="LICENSE_CONTAINER_CLEANUP_FAILED")
        try:
            for path in (archive, raw_path):
                if path.resolve().is_relative_to(workspace.resolve()) and workspace.parent.resolve() == (root / ".cache/license-images").resolve():
                    path.unlink(missing_ok=True)
                else:
                    raise InventoryError("LICENSE_SCRATCH_IDENTITY_CHANGED")
            report["private_raw_and_archive_removed"] = True
        except Exception:
            report.update(state="error", cleanup_error="LICENSE_PRIVATE_SCRATCH_CLEANUP_FAILED")
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as stream:
            stream.write(canonical_bytes(report))
    return report


def native_component_projection(raw: bytes, expected_image: str) -> dict[str, Any]:
    """Project metadata collected without invoking application or site code."""
    value = json.loads(raw)
    if value.get("schema_version") == "p12.native-image-error.v1":
        code = value.get("error_code")
        if isinstance(code, str) and re.fullmatch(r"NATIVE_[A-Z_]{1,80}", code):
            raise InventoryError(code)
        raise InventoryError("NATIVE_METADATA_EXECUTION_FAILED")
    if (value.get("schema_version") != "p12.native-image-metadata.v1"
            or value.get("isolated") is not True or value.get("site_disabled") is not True
            or not isinstance(value.get("records"), list) or not 0 < len(value["records"]) <= 10000):
        raise InventoryError("NATIVE_METADATA_REPORT_INVALID")
    records = []
    seen = set()
    seen_paths = set()
    for item in value["records"]:
        if (item.get("ecosystem") not in {"python", "debian"} or not isinstance(item.get("name"), str)
                or not item["name"] or not isinstance(item.get("version"), str) or not item["version"]):
            raise InventoryError("NATIVE_PACKAGE_IDENTITY_INVALID")
        for field in ("environment_sha256", "metadata_sha256"):
            if re.fullmatch(r"[a-f0-9]{64}", item.get(field, "")) is None:
                raise InventoryError("NATIVE_PACKAGE_HASH_INVALID")
        facts = license_facts(item.get("license_expression"), item.get("license_declaration"), item["classifiers"])
        declarations = [v for v in (item.get("license_expression"), item.get("license_declaration"))
                        if isinstance(v, str) and v.strip()]
        identity = {"image_id": expected_image, "target_sha256": item["environment_sha256"],
                    "type": item["ecosystem"], "name": item["name"], "version": item["version"],
                    "metadata_sha256": item["metadata_sha256"]}
        metadata_path = item.get("metadata_path_sha256")
        if metadata_path is not None:
            if not isinstance(metadata_path, str) or re.fullmatch(r"[a-f0-9]{64}", metadata_path) is None:
                raise InventoryError("NATIVE_PACKAGE_PATH_HASH_INVALID")
            path_key = (item["environment_sha256"], item["ecosystem"], metadata_path)
            if path_key in seen_paths:
                raise InventoryError("NATIVE_PACKAGE_DUPLICATE")
            seen_paths.add(path_key)
            # Identical vendored metadata can exist in two installed directories.
            # Bind the physical occurrence without disclosing its filesystem path.
            identity["metadata_path_sha256"] = metadata_path
        key = digest(canonical_bytes(identity))
        if key in seen:
            raise InventoryError("NATIVE_PACKAGE_DUPLICATE")
        seen.add(key)
        for file in item["license_files"]:
            if (set(file) != {"path_sha256", "sha256", "size_bytes"}
                    or any(re.fullmatch(r"[a-f0-9]{64}", file[k]) is None for k in ("path_sha256", "sha256"))
                    or type(file["size_bytes"]) is not int or not 0 <= file["size_bytes"] <= MAX_METADATA_BYTES):
                raise InventoryError("NATIVE_LICENSE_FILE_INVALID")
        records.append({**identity, "occurrence_id": key, "version_status": "reported",
            "declared_licenses": sorted(set(declarations)), "license_facts": [facts],
            "license_status": "reported" if declarations else "unknown", "license_files": item["license_files"],
            "parent_record_sha256": item.get("parent_record_sha256"),
            "missing_license_files": item.get("missing_license_files", []),
            "rejected_license_reference_hashes": item.get("rejected_license_reference_hashes", []),
            "source": "isolated stdlib metadata reader; not Trivy or upstream legal review",
            "review_status": "not_reviewed", "reviewed_at": None})
    records.sort(key=lambda r: (r["type"], r["name"], r["version"], r["occurrence_id"]))
    unknown = sum(not p["declared_licenses"] for p in records)
    return {"packages": records, "targets": [], "classified_license_observations": [],
        "summary": {"package_occurrences": len(records), "reported_license_occurrences": len(records)-unknown,
            "unknown_license_occurrences": unknown, "unknown_version_occurrences": 0,
            "classifier_observations": 0, "reviewed_count": 0},
        "missing_license_file_occurrences": sum(bool(p["missing_license_files"]) for p in records),
        "rejected_license_reference_occurrences": sum(bool(p["rejected_license_reference_hashes"]) for p in records),
        "native_metadata_provenance": {k: value[k] for k in ("python_version", "dpkg_status_sha256", "scope")},
        "inventory_complete_for_detected_packages": True, "all_embedded_components_detected": None,
        "legal_compatibility_assessed": False}


def collect_native_image_components(root: Path, image_id: str, provenance: Path, output: Path, *, run=None) -> dict[str, Any]:
    """Explicit Debian/Python fallback; no export, service entrypoint, network or mounts."""
    scanner = importlib.import_module("scripts.scan_project" if __package__ else "scan_project")
    if not isinstance(scanner.__file__, str):
        raise InventoryError("SCANNER_HELPER_SOURCE_UNAVAILABLE")
    run = run or scanner.command
    root = root.resolve()
    output = output_path(root, str(output))
    if output.exists():
        raise InventoryError("IMAGE_COMPONENT_OUTPUT_ALREADY_EXISTS")
    provenance_raw = bounded_read(provenance, root, 8 * 1024 * 1024)
    inventory = json.loads(provenance_raw)
    matches = [row for row in inventory["images"] if row["image_id"] == image_id]
    if (inventory.get("schema_version") != "p12.local-image-license-provenance.v1" or len(matches) != 1
            or re.fullmatch(r"sha256:[a-f0-9]{64}", image_id) is None):
        raise InventoryError("IMAGE_NOT_IN_EXACT_PROVENANCE")
    owner = uuid4().hex
    name = "expert-license-native-" + owner[:12]
    report: dict[str, Any] = {"schema_version": "p12.image-component-licenses.v1", "state": "running",
        "collection_method": "native_metadata", "image_id": image_id, "run_id": owner,
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "provenance_sha256": digest(provenance_raw),
        "script_sha256": digest(Path(__file__).read_bytes()), "scanner_helper_sha256": digest(Path(scanner.__file__).read_bytes()),
        "probe_sha256": digest(NATIVE_METADATA_PROBE.encode()), "network": "none", "main_services_modified": False,
        "weights_or_secrets_mounted": False, "service_entrypoint_executed": False, "site_processing_disabled": True,
        "mounts": [], "archive_created": False, "license_full": False, "error_code": None}
    docker: list[str] = []
    started = time.monotonic()
    try:
        docker = scanner.local_docker(run)
        identity = json.loads(run([*docker, "image", "inspect", "--format",
            '{"id":{{json .Id}},"size":{{json .Size}},"os":{{json .Os}},"arch":{{json .Architecture}}}', image_id], timeout=30))
        if (identity["id"] != image_id or identity["size"] != matches[0]["size_bytes"]
                or identity["os"] != "linux" or identity["arch"] != "amd64"):
            raise InventoryError("LICENSE_TARGET_IDENTITY_MISMATCH")
        report["phase"] = "isolated_metadata_read"
        # ASCII base64 transport avoids cross-platform Docker argv rewriting of
        # multiline Python quoting. Only closed error categories can leave it.
        bootstrap = ("import base64,json\ntry:\n exec(compile(base64.b64decode('"
            + base64.b64encode(NATIVE_METADATA_PROBE.encode()).decode() + "'),'<metadata-probe>','exec'))\n"
            "except Exception as error:\n"
            " allowed={'METADATA_BOUNDARY','METADATA_LIMIT','ISOLATED_METADATA_REQUIRED','LICENSE_REFERENCE_INVALID',"
            "'DECLARED_LICENSE_FILE_MISSING','DPKG_PACKAGE_INVALID','PACKAGE_COUNT_INVALID'}\n"
            " code='NATIVE_'+(str(error) if type(error) is ValueError and str(error) in allowed else type(error).__name__.upper())\n"
            " print(json.dumps({'schema_version':'p12.native-image-error.v1','error_code':code}))")
        raw = run([*docker, "run", "--rm", "--pull", "never", "--platform", "linux/amd64",
            "--name", name, "--label", "expert.scan.owner="+owner, "--network", "none", "--read-only",
            "--no-healthcheck", "--user", "65534:65534", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--pids-limit", "64", "--cpus", "1", "--memory", "512m", "--memory-swap", "512m",
            "--tmpfs", "/tmp:rw,nosuid,noexec,size=67108864", "--workdir", "/tmp",
            "--entrypoint", "/usr/local/bin/python", image_id, "-I", "-S", "-c", bootstrap], timeout=180)
        if len(raw) > 1024 * 1024:
            raise InventoryError("NATIVE_METADATA_OUTPUT_LIMIT")
        report["raw_report_sha256"] = digest(raw)
        report.update(native_component_projection(raw, image_id), state="collected", phase="projection")
    except Exception as error:
        report.update(state="error", error_code=str(error) if isinstance(error, InventoryError)
                      else error.code if isinstance(error, scanner.ScanError) else "NATIVE_METADATA_COLLECTION_FAILED")
        if isinstance(error, scanner.ScanError) and getattr(error, "diagnostics", None):
            report["command_diagnostics"] = error.diagnostics
    finally:
        if docker:
            try:
                scanner.cleanup_container(docker, name, owner, run)
                report["owned_container_absent"] = True
            except Exception:
                report.update(state="error", cleanup_error="LICENSE_CONTAINER_CLEANUP_FAILED")
        # Metadata bytes exist only in the bounded command transport, never an archive/raw file.
        report["private_raw_and_archive_removed"] = True
        report["elapsed_seconds"] = round(time.monotonic()-started, 3)
        report["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as stream:
            stream.write(canonical_bytes(report))
    return report


def build_image_component_matrix(root: Path, provenance: Path, reports: Path) -> dict[str, Any]:
    """Check exact image coverage from immutable reports; unknown legal status stays explicit."""
    root = root.resolve()
    provenance_raw = bounded_read(provenance, root, 8 * 1024 * 1024)
    inventory = json.loads(provenance_raw)
    if inventory.get("schema_version") != "p12.local-image-license-provenance.v1":
        raise InventoryError("IMAGE_PROVENANCE_INVALID")
    expected = {item["image_id"] for item in inventory["images"]}
    if len(expected) != len(inventory["images"]) or not expected:
        raise InventoryError("IMAGE_PROVENANCE_DUPLICATE_OR_EMPTY")
    reports = reports.resolve()
    if not reports.is_relative_to(root) or not reports.is_dir():
        raise InventoryError("IMAGE_COMPONENT_REPORT_DIRECTORY_INVALID")
    paths = sorted(reports.glob("*.json"))
    if len(paths) > 256:
        raise InventoryError("IMAGE_COMPONENT_REPORT_COUNT_LIMIT")
    inputs = {provenance.resolve().relative_to(root).as_posix(): digest(provenance_raw)}
    attempts: dict[str, list[dict[str, Any]]] = {key: [] for key in expected}
    successful: dict[str, dict[str, Any]] = {}
    for path in paths:
        raw = bounded_read(path, root, 128 * 1024 * 1024)
        record = json.loads(raw)
        image_id = record.get("image_id")
        if (record.get("schema_version") != "p12.image-component-licenses.v1" or image_id not in expected
                or record.get("provenance_sha256") != digest(provenance_raw)
                or record.get("state") not in {"collected", "error"}):
            raise InventoryError("IMAGE_COMPONENT_EVIDENCE_IDENTITY_INVALID")
        key = path.relative_to(root).as_posix()
        inputs[key] = digest(raw)
        attempts[image_id].append({"path": key, "state": record["state"], "error_code": record.get("error_code")})
        if record["state"] != "collected":
            continue
        if (record.get("network") != "none" or record.get("main_services_modified") is not False
                or record.get("weights_or_secrets_mounted") is not False
                or record.get("owned_container_absent") is not True
                or record.get("private_raw_and_archive_removed") is not True
                or record.get("inventory_complete_for_detected_packages") is not True
                or record.get("legal_compatibility_assessed") is not False
                or not record.get("packages")):
            raise InventoryError("IMAGE_COMPONENT_EVIDENCE_BOUNDARY_INVALID")
        if record.get("collection_method") not in {None, "native_metadata"}:
            raise InventoryError("IMAGE_COLLECTION_METHOD_INVALID")
        native = record.get("collection_method") == "native_metadata"
        if native and (record.get("archive_created") is not False or record.get("mounts") != []
                       or record.get("site_processing_disabled") is not True or record.get("service_entrypoint_executed") is not False):
            raise InventoryError("NATIVE_COMPONENT_EVIDENCE_BOUNDARY_INVALID")
        for field in ("script_sha256", "scanner_helper_sha256", "probe_sha256" if native else "archive_sha256", "raw_report_sha256"):
            if re.fullmatch(r"[a-f0-9]{64}", record.get(field, "")) is None:
                raise InventoryError("IMAGE_COMPONENT_EVIDENCE_HASH_INVALID")
        package_ids = [item["occurrence_id"] for item in record["packages"]]
        if len(set(package_ids)) != len(package_ids):
            raise InventoryError("IMAGE_COMPONENT_EVIDENCE_DUPLICATE_PACKAGE")
        actual_unknown = sum(not item["declared_licenses"] for item in record["packages"])
        if (record["summary"]["package_occurrences"] != len(package_ids)
                or record["summary"]["unknown_license_occurrences"] != actual_unknown):
            raise InventoryError("IMAGE_COMPONENT_EVIDENCE_COUNT_INVALID")
        if image_id in successful and successful[image_id]["packages"] != record["packages"]:
            raise InventoryError("IMAGE_COMPONENT_EVIDENCE_CONFLICT")
        successful[image_id] = record
    rows = [{"image_id": key, "state": "collected" if key in successful else "unavailable",
             "attempts": attempts[key], "summary": successful[key]["summary"] if key in successful else None}
            for key in sorted(expected)]
    blockers = [{"image_id": row["image_id"], "code": "IMAGE_PACKAGE_LICENSE_INVENTORY_UNAVAILABLE"}
                for row in rows if row["state"] != "collected"]
    return {"schema_version": "p12.image-license-matrix.v1", "inputs": inputs,
        "collector": {"script_sha256": digest(Path(__file__).read_bytes())}, "images": rows, "blockers": blockers,
        "summary": {"image_count": len(expected), "collected_image_count": len(successful),
            "blocker_count": len(blockers), "package_occurrences": sum(len(r["packages"]) for r in successful.values()),
            "unknown_license_occurrences": sum(r["summary"]["unknown_license_occurrences"] for r in successful.values()),
            "unknown_version_occurrences": sum(not p.get("version") for r in successful.values() for p in r["packages"]),
            "reviewed_count": 0},
        "gate": "exact image inventory coverage; unknown declarations retained, not legal approval",
        "legal_compatibility_assessed": False, "all_embedded_components_detected": None}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--python-site-packages", type=Path,
                        help="Optional existing workspace .venv* metadata directory; no interpreter is executed")
    parser.add_argument("--python-lock", type=Path, action="append",
                        help="Repeat explicit uv.lock/hash-pinned requirements sources; exact bindings retain their source")
    parser.add_argument("--python-only", action="store_true", help="Do not recount frontend in an additional environment")
    parser.add_argument("--image-baseline", type=Path,
                        help="Read-only local image identity/label inventory from scanner baseline plus Compose; package gaps fail --check")
    parser.add_argument("--image-components", help="Collect package-level SBOM/license facts for one exact sha256 image ID")
    parser.add_argument("--image-metadata-only", action="store_true",
                        help="Collect image package declarations without the additional source-header/license-file classifier")
    parser.add_argument("--image-native-metadata", action="store_true",
                        help="Explicit isolated Debian/Python metadata fallback without archive export or service execution")
    parser.add_argument("--image-provenance", type=Path, help="Existing exact local image inventory; required with --image-components")
    parser.add_argument("--image-reports", type=Path, help="Build/check exact-image coverage from component report directory")
    parser.add_argument("--check", action="store_true", help="Read-only freshness/completeness gate; unresolved declarations fail")
    args = parser.parse_args(argv)
    try:
        root = args.root.resolve()
        site = args.python_site_packages or Path(sysconfig.get_paths()["purelib"])
        if not site.is_absolute():
            site = root / site
        output = output_path(root, args.output)
        if args.image_components:
            if not args.image_provenance or args.check or args.image_baseline or args.image_reports or args.python_site_packages or args.python_lock or args.python_only:
                raise InventoryError("IMAGE_COMPONENT_ARGUMENT_CONFLICT")
            provenance = args.image_provenance if args.image_provenance.is_absolute() else root / args.image_provenance
            if args.image_native_metadata:
                if args.image_metadata_only:
                    raise InventoryError("IMAGE_NATIVE_AND_SCANNER_OPTIONS_CONFLICT")
                report = collect_native_image_components(root, args.image_components, provenance, output)
            else:
                report = collect_image_components(root, args.image_components, provenance, output,
                                                  license_full=not args.image_metadata_only)
            print(json.dumps({k: report.get(k) for k in ("state", "image_id", "phase", "error_code", "summary", "elapsed_seconds")}))
            return 0 if report["state"] == "collected" else 1
        if args.image_provenance and not args.image_reports:
            raise InventoryError("IMAGE_COMPONENT_ARGUMENT_CONFLICT")
        if args.image_metadata_only or args.image_native_metadata:
            raise InventoryError("IMAGE_METADATA_ONLY_REQUIRES_COMPONENT_COLLECTION")
        locks = [p if p.is_absolute() else root / p for p in args.python_lock] if args.python_lock else None
        if args.image_reports:
            if not args.image_provenance or args.image_baseline or args.python_site_packages or args.python_lock or args.python_only:
                raise InventoryError("IMAGE_COMPONENT_ARGUMENT_CONFLICT")
            provenance = args.image_provenance if args.image_provenance.is_absolute() else root / args.image_provenance
            reports = args.image_reports if args.image_reports.is_absolute() else root / args.image_reports
            current = build_image_component_matrix(root, provenance, reports)
        elif args.image_baseline:
            if args.python_site_packages or args.python_lock or args.python_only:
                raise InventoryError("IMAGE_AND_PYTHON_SCOPES_CANNOT_BE_MIXED")
            baseline = args.image_baseline if args.image_baseline.is_absolute() else root / args.image_baseline
            current = build_image_inventory(root, baseline)
        else:
            current = build_inventory(root, site, python_lock_paths=locks, include_frontend=not args.python_only)
        failures = check_inventory(output, current) if args.check else []
        if not args.check:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(canonical_bytes(current))
        print(json.dumps({"mode": "check" if args.check else "collect", "summary": current["summary"],
                          "failures": failures}, sort_keys=True))
        return 1 if failures else 0
    except (InventoryError, OSError, ValueError, KeyError, TypeError, yaml.YAMLError, subprocess.TimeoutExpired) as exc:
        # Do not echo arbitrary package metadata or file contents in CI logs.
        code = str(exc) if isinstance(exc, InventoryError) else "INVENTORY_INPUT_INVALID"
        print(json.dumps({"error": code}, sort_keys=True), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
