"""Optional CUDA ranking; the verified CPU embedding implementation stays unchanged."""
from __future__ import annotations

import gc
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
from threading import Timer
from typing import Any

from expert_clients.settings import Settings

from .adapters import LocalModels
from .artifacts import fingerprint
from .errors import ModelError


class CudaModels(LocalModels):
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._worker: subprocess.Popen[bytes] | None = None

    def load(self) -> None:
        # This exact CPU implementation verifies files, creates the original embedding
        # recipe and tests FRIDA. Its files and Python environment are unchanged.
        inherited_cuda = os.environ.get("CUDA_VISIBLE_DEVICES")
        super().load()
        self.reranker = None
        gc.collect()
        worker_path = Path(__file__).with_name("cuda_worker.py")
        environment = {
            key: value for key, value in os.environ.items()
            if key in {"PATH", "LD_LIBRARY_PATH", "SYSTEMROOT", "TEMP", "TMP", "TMPDIR",
                       "NVIDIA_VISIBLE_DEVICES", "NVIDIA_DRIVER_CAPABILITIES"}
        }
        environment.update(PYTHONUNBUFFERED="1", PYTHONDONTWRITEBYTECODE="1",
                           HF_HOME="/tmp/huggingface", TOKENIZERS_PARALLELISM="false")
        if inherited_cuda is not None:
            environment["CUDA_VISIBLE_DEVICES"] = inherited_cuda
        try:
            self._worker = subprocess.Popen(
                [str(self.settings.reranker_cuda_python), "-I", "-u", str(worker_path),
                 str(self.settings.reranker_model_path), str(self.settings.reranker_cuda_max_batch_tokens),
                 str(self.reranker_tokenizer.pad_token_id)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                env=environment, shell=False,
            )
            ready = self._exchange(None)
            identity = ready.get("identity")
            if (ready.get("ready") is not True or not isinstance(identity, dict)
                    or identity.get("device") != "cuda" or identity.get("dtype") != "float16"
                    or identity.get("implementation_sha256") != hashlib.sha256(worker_path.read_bytes()).hexdigest()):
                raise ModelError("MODEL_UNAVAILABLE")
            runtime_hash = fingerprint({
                "worker": identity,
                "controller_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                "cpu_tokenization_runtime": self.profile.reranker_recipe.runtime_fingerprint,
            })
            recipe = self.profile.reranker_recipe.model_copy(update={
                "runtime_fingerprint": runtime_hash, "device": "cuda", "dtype": "float16"})
            capability = self.profile.capabilities.reranker.model_copy(update={"device": "cuda"})
            self.profile = self.profile.model_copy(update={
                "reranker_recipe": recipe,
                "capabilities": self.profile.capabilities.model_copy(update={"reranker": capability}),
            })
            # Readiness requires a real GPU forward, not just CUDA device discovery.
            self.score("Когда проводят проверку?", ["Проверку проводят один раз в год."])
        except Exception:
            self.close()
            raise ModelError("MODEL_UNAVAILABLE") from None

    def _exchange(self, payload: dict | None) -> dict[str, Any]:
        worker = self._worker
        if worker is None or worker.stdin is None or worker.stdout is None or worker.poll() is not None:
            self._stop_worker()
            raise ModelError("MODEL_UNAVAILABLE")
        timer = Timer(self.settings.ml_request_timeout_seconds, worker.kill)
        timer.daemon = True
        timer.start()
        try:
            if payload is not None:
                worker.stdin.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
                worker.stdin.flush()
            line = worker.stdout.readline(262145)
            if not line.endswith(b"\n") or len(line) > 262144:
                raise ModelError("MODEL_UNAVAILABLE")
            response = json.loads(line)
            if not isinstance(response, dict) or response.get("error"):
                raise ModelError("MODEL_UNAVAILABLE")
            return response
        except (OSError, ValueError, ModelError):
            self._stop_worker()
            raise ModelError("MODEL_UNAVAILABLE") from None
        finally:
            timer.cancel()
            timer.join()

    def score(self, query: str, texts: list[str]) -> tuple[list[float], list[int]]:
        if self._worker is None and self.reranker is not None:
            # Only the original CPU startup probe takes this branch.
            return super().score(query, texts)
        sequences, lengths = self.reranker_ids(query, texts)
        values = self._exchange({"sequences": sequences}).get("scores")
        if (not isinstance(values, list) or len(values) != len(texts)
                or any(type(value) not in (float, int) or not math.isfinite(value) or not 0 <= value <= 1
                       for value in values)):
            self._stop_worker()
            raise ModelError("OUTPUT_SCHEMA_INVALID")
        return [float(value) for value in values], lengths

    def _stop_worker(self) -> None:
        worker, self._worker = self._worker, None
        if worker is not None:
            try:
                if worker.poll() is None:
                    worker.kill()
                worker.wait(timeout=10)
            except (OSError, subprocess.TimeoutExpired):
                # Do not mask the sanitized inference error during shutdown.
                pass
            finally:
                for pipe in (worker.stdin, worker.stdout):
                    if pipe is not None:
                        try:
                            pipe.close()
                        except OSError:
                            pass

    def close(self) -> None:
        self._stop_worker()
        super().close()


def configured_models(settings: Settings) -> LocalModels:
    return CudaModels(settings) if settings.reranker_device == "cuda" else LocalModels(settings)
