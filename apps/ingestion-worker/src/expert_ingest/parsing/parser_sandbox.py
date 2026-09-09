"""Irreversible Linux child restrictions, installed before PDF/vendor imports.

The parent must spawn with an environment allowlist and close_fds=True, retain a
wall deadline, and kill/join the child before releasing its workspace. Landlock
is needed even with empty credentials in the environment: the parent and child
share a UID and container, including the parent's mounted secret files.
"""
from __future__ import annotations

import ctypes
from dataclasses import dataclass
import errno
import os
from pathlib import Path
import platform
import sys


class SandboxUnavailable(RuntimeError):
    """Fail closed without exposing host paths or native exception messages."""

    def __init__(self) -> None:
        super().__init__("PARSER_SANDBOX_UNAVAILABLE")


@dataclass(frozen=True)
class SandboxLimits:
    source_path: Path
    output_dir: Path
    assets_path: Path | None = None
    # RLIMIT_AS bounds virtual mappings; the parser container separately limits
    # physical accounting to 6 GiB with no swap (see parser-profile.json).
    memory_bytes: int = 8 * 1024**3
    cpu_seconds: int = 960
    file_bytes: int = 256 * 1024**2
    open_files: int = 128
    processes: int = 64
    cpu_count: int = 2

    def __post_init__(self) -> None:
        for value, lower, upper in [
            (self.memory_bytes, 64 * 1024**2, 16 * 1024**3),
            (self.cpu_seconds, 1, 7200), (self.file_bytes, 1, 256 * 1024**2),
            (self.open_files, 16, 1024), (self.processes, 1, 256),
            (self.cpu_count, 1, 2),
        ]:
            if type(value) is not int or not lower <= value <= upper:
                raise ValueError("Invalid parser sandbox limit")


@dataclass(frozen=True)
class SandboxReport:
    enforced: bool
    platform: str
    no_new_privs: bool
    seccomp_network_denied: bool
    process_creation_denied: bool
    landlock_abi: int
    memory_bytes: int
    cpu_seconds: int
    file_bytes: int
    open_files: int
    processes: int
    cpu_count: int
    memory_max_bytes: int | None
    swap_max_bytes: int | None


class _Ruleset(ctypes.Structure):
    _fields_ = [("handled_access_fs", ctypes.c_uint64)]


class _PathRule(ctypes.Structure):
    # Linux UAPI explicitly packs this structure; sizeof must be 12, not 16.
    _pack_ = 1
    _fields_ = [("allowed_access", ctypes.c_uint64), ("parent_fd", ctypes.c_int32)]


class _ArgumentComparison(ctypes.Structure):
    _fields_ = [("arg", ctypes.c_uint), ("op", ctypes.c_int),
                ("datum_a", ctypes.c_uint64), ("datum_b", ctypes.c_uint64)]


def _cgroup_limits() -> tuple[int | None, int | None]:
    """Read effective visible cgroup-v2 ceilings before Landlock restricts sysfs.

    None means unavailable or unlimited, never an inferred request value. A
    tighter visible ancestor remains effective even if the leaf says ``max``.
    The deployed Docker profile exposes its own cgroup as the namespace root.
    """
    if sys.platform != "linux":
        return None, None
    try:
        membership = Path("/proc/self/cgroup").read_text(encoding="ascii")
        groups = [line[3:] for line in membership.splitlines() if line.startswith("0::")]
        if len(groups) != 1 or not groups[0].startswith("/"):
            return None, None
        root = Path("/sys/fs/cgroup").resolve(strict=True)
        leaf = (root / groups[0].lstrip("/")).resolve(strict=True)
        if not leaf.is_relative_to(root):
            return None, None
        directories = [leaf, *[parent for parent in leaf.parents if parent.is_relative_to(root)]]
        measured: list[int | None] = []
        for filename in ("memory.max", "memory.swap.max"):
            finite = []
            for directory in directories:
                value = (directory / filename).read_text(encoding="ascii").strip()
                if value == "max":
                    continue
                if not value.isascii() or not value.isdecimal():
                    return None, None
                finite.append(int(value))
            measured.append(min(finite) if finite else None)
        return measured[0], measured[1]
    except (OSError, UnicodeError, ValueError):
        return None, None


def _paths(limits: SandboxLimits) -> tuple[Path, Path, Path | None]:
    paths = [limits.source_path, limits.output_dir]
    if limits.assets_path is not None:
        paths.append(limits.assets_path)
    if any(not path.is_absolute() or path.is_symlink() or path.is_junction() for path in paths):
        raise SandboxUnavailable()
    source, output = limits.source_path.resolve(strict=True), limits.output_dir.resolve(strict=True)
    assets = limits.assets_path.resolve(strict=True) if limits.assets_path is not None else None
    if not source.is_file() or not output.is_dir() or (assets is not None and not assets.is_dir()):
        raise SandboxUnavailable()
    if source.is_relative_to(output) or (assets is not None and (
        assets.is_relative_to(output) or output.is_relative_to(assets)
    )):
        raise SandboxUnavailable()
    return source, output, assets


def _runtime_roots() -> list[Path]:
    # Specific runtime trees, never the workspace root or arbitrary PYTHONPATH.
    candidates = [Path(sys.prefix), Path(sys.base_prefix), Path(__file__).resolve().parents[1],
                  Path("/usr"), Path("/lib"), Path("/lib64"), Path("/etc/fonts"),
                  Path("/etc/ld.so.cache"), Path("/etc/localtime"), Path("/dev/null"),
                  Path("/dev/urandom"), Path("/proc/self"), Path("/proc/meminfo"),
                  Path("/proc/cpuinfo"), Path("/sys/devices/system/cpu")]
    roots = list(dict.fromkeys(path.resolve(strict=True) for path in candidates if path.exists()))
    if any(path in [Path("/"), Path("/app"), Path("/tmp"), Path("/run"), Path("/proc")]
           or path.is_relative_to("/run") or ".secrets" in path.parts for path in roots):
        raise SandboxUnavailable()
    return roots


def _landlock(libc, source: Path, output: Path, assets: Path | None, roots: list[Path]) -> int:
    # These syscall numbers are the Linux generic UAPI on the supported machines.
    libc.syscall.restype = ctypes.c_long
    abi = libc.syscall(444, 0, 0, 1)
    if abi < 3:
        raise SandboxUnavailable()  # ABI3 adds truncate protection.
    handled = (1 << 15) - 1
    ruleset = _Ruleset(handled)
    ruleset_fd = libc.syscall(444, ctypes.byref(ruleset), ctypes.sizeof(ruleset), 0)
    if ruleset_fd < 0:
        raise SandboxUnavailable()
    read = (1 << 0) | (1 << 2) | (1 << 3)  # execute, read_file, read_dir
    write = read & ~(1 << 0) | (1 << 1) | (1 << 4) | (1 << 5) | (1 << 7) | (1 << 8) | (1 << 13) | (1 << 14)
    traversal = (1 << 0) | (1 << 3)  # execute plus read_dir for path traversal only.
    entries = [(path, read) for path in roots] + [
        (source.parent.parent, traversal), (source.parent, read), (source, 1 << 2), (output, write)
    ]
    # dill, imported by torch, probes buffered file types with /dev/null r+b.
    # This exact character device stores no data and is not a general write root.
    entries.append((Path("/dev/null"), (1 << 1) | (1 << 2) | (1 << 14)))
    if assets is not None:
        entries.append((assets, read & ~(1 << 0)))
    try:
        for path, rights in entries:
            if not path.is_dir():
                rights &= (1 << 0) | (1 << 1) | (1 << 2) | (1 << 14)
            fd = os.open(path, getattr(os, "O_PATH") | getattr(os, "O_CLOEXEC"))
            try:
                rule = _PathRule(rights, fd)
                if libc.syscall(445, ruleset_fd, 1, ctypes.byref(rule), 0) < 0:
                    raise SandboxUnavailable()
            finally:
                os.close(fd)
        if libc.syscall(446, ruleset_fd, 0) < 0:
            raise SandboxUnavailable()
    finally:
        os.close(ruleset_fd)
    return abi


def _seccomp(seccomp) -> None:
    seccomp.seccomp_init.argtypes = [ctypes.c_uint32]
    seccomp.seccomp_init.restype = ctypes.c_void_p
    seccomp.seccomp_release.argtypes = [ctypes.c_void_p]
    seccomp.seccomp_load.argtypes = [ctypes.c_void_p]
    seccomp.seccomp_rule_add_array.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int,
                                             ctypes.c_uint, ctypes.POINTER(_ArgumentComparison)]
    seccomp.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    context = seccomp.seccomp_init(0x7FFF0000)
    if not context:
        raise SandboxUnavailable()
    deny = 0x00050000 | errno.EPERM
    try:
        # Blocking io_uring closes an alternate route to socket operations.
        names = ["socket", "socketpair", "connect", "bind", "listen", "accept", "accept4",
                 "sendto", "sendmsg", "sendmmsg", "recvfrom", "recvmsg", "recvmmsg",
                 "shutdown", "setsockopt", "getsockopt", "io_uring_setup", "io_uring_enter",
                 "io_uring_register", "ptrace", "process_vm_readv", "process_vm_writev",
                 "pidfd_open", "pidfd_getfd", "pidfd_send_signal", "bpf", "userfaultfd",
                 "open_by_handle_at", "mount", "umount2", "pivot_root", "setns", "unshare",
                 "fork", "vfork", "kill", "tkill"]
        for name in names:
            number = seccomp.seccomp_syscall_resolve_name(name.encode("ascii"))
            if number >= 0 and seccomp.seccomp_rule_add_array(context, deny, number, 0, None) < 0:
                raise SandboxUnavailable()
        # glibc pthread_create falls back from clone3 ENOSYS to clone. The latter
        # is permitted only for CLONE_THREAD, preserving bounded native ML threads.
        clone3 = seccomp.seccomp_syscall_resolve_name(b"clone3")
        if clone3 >= 0 and seccomp.seccomp_rule_add_array(
            context, 0x00050000 | errno.ENOSYS, clone3, 0, None
        ) < 0:
            raise SandboxUnavailable()
        clone = seccomp.seccomp_syscall_resolve_name(b"clone")
        comparison = _ArgumentComparison(0, 7, 0x00010000, 0)  # MASKED_EQ, CLONE_THREAD unset
        if clone < 0 or seccomp.seccomp_rule_add_array(context, deny, clone, 1, ctypes.byref(comparison)) < 0:
            raise SandboxUnavailable()
        tgkill = seccomp.seccomp_syscall_resolve_name(b"tgkill")
        own_threads = _ArgumentComparison(0, 1, os.getpid(), 0)  # NE, other thread group
        if tgkill >= 0 and seccomp.seccomp_rule_add_array(context, deny, tgkill, 1, ctypes.byref(own_threads)) < 0:
            raise SandboxUnavailable()
        if seccomp.seccomp_load(context) < 0:
            raise SandboxUnavailable()
    finally:
        seccomp.seccomp_release(context)


def configure_sandbox(limits: SandboxLimits, *, enforce: bool = True) -> SandboxReport:
    """Configure this fresh child, or raise; this function never weakens a limit.

    ``enforce=False`` is an explicit native functional-test mode. Its report is
    never suitable as evidence of a production sandbox, including on Linux.
    """
    source, output, assets = _paths(limits)
    memory_max_bytes, swap_max_bytes = _cgroup_limits()
    if not enforce:
        return SandboxReport(False, sys.platform, False, False, False, 0,
                             limits.memory_bytes, limits.cpu_seconds, limits.file_bytes,
                             limits.open_files, limits.processes, limits.cpu_count,
                             memory_max_bytes, swap_max_bytes)
    if sys.platform != "linux" or platform.machine() not in {"x86_64", "aarch64"}:
        raise SandboxUnavailable()
    try:
        import resource

        roots = _runtime_roots()
        # Load trusted native code before installing filesystem restrictions.
        libc = ctypes.CDLL(None, use_errno=True)
        seccomp = ctypes.CDLL("libseccomp.so.2", use_errno=True)
        if libc.prctl(38, 1, 0, 0, 0) != 0 or libc.prctl(4, 0, 0, 0, 0) != 0:
            raise SandboxUnavailable()  # no_new_privs; non-dumpable child
        applied = {}
        for name, kind, wanted in [
            ("memory", resource.RLIMIT_AS, limits.memory_bytes),
            ("cpu", resource.RLIMIT_CPU, limits.cpu_seconds),
            ("file", resource.RLIMIT_FSIZE, limits.file_bytes),
            ("fds", resource.RLIMIT_NOFILE, limits.open_files),
            ("processes", resource.RLIMIT_NPROC, limits.processes),
            ("core", resource.RLIMIT_CORE, 0),
        ]:
            _, old_hard = resource.getrlimit(kind)
            effective = wanted if old_hard == resource.RLIM_INFINITY else min(wanted, old_hard)
            resource.setrlimit(kind, (effective, effective))
            applied[name] = effective
        affinity = sorted(os.sched_getaffinity(0))[:limits.cpu_count]
        if not affinity:
            raise SandboxUnavailable()
        os.sched_setaffinity(0, affinity)
        abi = _landlock(libc, source, output, assets, roots)
        _seccomp(seccomp)
        return SandboxReport(True, sys.platform, True, True, True, abi,
                             applied["memory"], applied["cpu"], applied["file"],
                             applied["fds"], applied["processes"], len(affinity),
                             memory_max_bytes, swap_max_bytes)
    except SandboxUnavailable:
        raise
    except Exception:
        raise SandboxUnavailable() from None
