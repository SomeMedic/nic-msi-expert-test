"""Pinned local CPU inference. All loading, tokenization and forwards run on one thread."""
from __future__ import annotations

import json
import os
import time
from typing import Any

from expert_clients.settings import Settings
from expert_contracts.inference import (
    CapabilitiesResponse, DocumentEmbeddingRequest, DocumentEmbeddingResponse, DocumentEmbeddingResult,
    ModelCapability, QueryEmbeddingRequest, QueryEmbeddingResponse,
    RerankRequest, RerankResponse, RerankScore,
)

from .artifacts import (
    DOCUMENT_PREFIX, FRIDA_MODEL, FRIDA_REVISION, QUERY_PREFIX, RERANKER_MODEL, RERANKER_REVISION,
    fingerprint, runtime_identity, tokenizer_fingerprint, verify_snapshot,
)
from .errors import ModelError, token_limit
from .profile import EmbeddingRecipe, RerankerRecipe, RuntimeProfile

INSTRUCTION = "Given a web search query, retrieve relevant passages that answer the query"
RERANKER_PREFIX = (
    "<|im_start|>system\nJudge whether the Document meets the requirements based on the Query "
    'and the Instruct provided. Note that the answer can only be "yes" or "no".'
    "<|im_end|>\n<|im_start|>user\n"
)
RERANKER_SUFFIX = "<|im_end|>\n<|im_start|>assistant\n<think>\n\n</think>\n\n"


class LocalModels:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.frida: Any = None
        self.reranker: Any = None
        self.frida_tokenizer: Any = None
        self.reranker_tokenizer: Any = None
        self.profile: RuntimeProfile

    def load(self) -> None:
        for name in ("HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_HUB_DISABLE_TELEMETRY",
                     "HF_HUB_DISABLE_IMPLICIT_TOKEN", "HF_DATASETS_OFFLINE"):
            os.environ[name] = "1"
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        os.environ["CUDA_VISIBLE_DEVICES"] = ""
        settings = self.settings
        if (settings.embedding_revision != FRIDA_REVISION or settings.reranker_revision != RERANKER_REVISION
                or settings.embedding_model_path is None or settings.reranker_model_path is None):
            raise ModelError("MODEL_UNAVAILABLE")
        manifest = json.loads(settings.models_lock_path.read_text(encoding="utf-8"))
        embedding = verify_snapshot(manifest, "embedding", settings.embedding_model_path,
                                    FRIDA_MODEL, FRIDA_REVISION)
        rerank = verify_snapshot(manifest, "reranker", settings.reranker_model_path,
                                 RERANKER_MODEL, RERANKER_REVISION)
        contract = embedding["spec"]["contract"]
        expected = {"architecture": "T5EncoderModel", "pooling": "cls_first_token", "dimension": 1536,
                    "max_input_tokens": 512, "normalized": True, "query_prefix": QUERY_PREFIX,
                    "document_prefix": DOCUMENT_PREFIX, "device": "cpu", "dtype": "float32",
                    "silent_truncation": False}
        if contract != expected:
            raise ModelError("MODEL_UNAVAILABLE")
        rerank_contract = rerank["spec"]["contract"]
        if any(rerank_contract.get(key) != value for key, value in {
            "architecture": "Qwen3ForCausalLM", "adapter": "transformers_reference_yes_no",
            "true_token_id": 9693, "false_token_id": 2152, "padding_side": "left",
            "score_type": "sigmoid", "instruction": INSTRUCTION, "device": "cpu", "dtype": "float32",
            "silent_truncation": False, "max_pair_tokens_candidate": 2048,
        }.items()):
            raise ModelError("MODEL_UNAVAILABLE")
        pooling = json.loads((settings.embedding_model_path / "1_Pooling/config.json").read_text())
        if (pooling["word_embedding_dimension"] != 1536 or pooling["pooling_mode_cls_token"] is not True
                or any(value for key, value in pooling.items()
                       if key.startswith("pooling_mode_") and key != "pooling_mode_cls_token")):
            raise ModelError("MODEL_UNAVAILABLE")
        identity = runtime_identity(settings.ml_cpu_threads)
        runtime_hash = fingerprint(identity)
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer, RobertaTokenizerFast, T5EncoderModel
        from transformers.utils import logging as transformers_logging

        transformers_logging.disable_progress_bar()
        transformers_logging.set_verbosity_error()
        if torch.version.cuda is not None:
            raise ModelError("MODEL_UNAVAILABLE")
        torch.set_num_threads(settings.ml_cpu_threads)
        self.frida_tokenizer = RobertaTokenizerFast.from_pretrained(
            str(settings.embedding_model_path), local_files_only=True, trust_remote_code=False)
        if not self.frida_tokenizer.is_fast or self.frida_tokenizer.num_special_tokens_to_add(pair=False) != 2:
            raise ModelError("MODEL_UNAVAILABLE")
        self.frida = T5EncoderModel.from_pretrained(
            str(settings.embedding_model_path), local_files_only=True, trust_remote_code=False,
            use_safetensors=True, dtype=torch.float32).eval().to("cpu")
        self.reranker_tokenizer = AutoTokenizer.from_pretrained(
            str(settings.reranker_model_path), padding_side="left", local_files_only=True, trust_remote_code=False)
        if (self.reranker_tokenizer.encode("yes", add_special_tokens=False) != [9693]
                or self.reranker_tokenizer.encode("no", add_special_tokens=False) != [2152]):
            raise ModelError("MODEL_UNAVAILABLE")
        self.reranker = AutoModelForCausalLM.from_pretrained(
            str(settings.reranker_model_path), local_files_only=True, trust_remote_code=False,
            use_safetensors=True, dtype=torch.float32).eval().to("cpu")
        self.profile = RuntimeProfile(
            embedding_recipe=EmbeddingRecipe(
                revision=FRIDA_REVISION, tokenizer_revision=FRIDA_REVISION,
                tokenizer_fingerprint=tokenizer_fingerprint(embedding["spec"]),
                model_fingerprint=embedding["model_fingerprint"], runtime_fingerprint=runtime_hash),
            reranker_recipe=RerankerRecipe(
                revision=RERANKER_REVISION, model_fingerprint=rerank["model_fingerprint"],
                runtime_fingerprint=runtime_hash, max_input_tokens=settings.reranker_max_input_tokens,
                template_fingerprint=fingerprint({"instruction": INSTRUCTION, "prefix": RERANKER_PREFIX,
                                                  "suffix": RERANKER_SUFFIX, "yes": 9693, "no": 2152})),
            capabilities=CapabilitiesResponse(
                embedding=ModelCapability(model=FRIDA_MODEL, revision=FRIDA_REVISION, device="cpu",
                    max_input_tokens=512, max_batch_items=settings.embedding_max_batch_items,
                    max_batch_tokens=settings.embedding_max_batch_tokens, dimension=1536),
                reranker=ModelCapability(model=RERANKER_MODEL, revision=RERANKER_REVISION, device="cpu",
                    max_input_tokens=settings.reranker_max_input_tokens,
                    max_batch_items=settings.reranker_max_batch_items,
                    max_batch_tokens=settings.reranker_max_batch_tokens, score_type="sigmoid")),
            admission_slots=settings.ml_admission_slots, cpu_threads=settings.ml_cpu_threads)
        # Actual forwards are the readiness gate; two independent identical query calls must agree.
        first, _ = self.embed(["Проверка локальной модели."], QUERY_PREFIX)
        second, _ = self.embed(["Проверка локальной модели."], QUERY_PREFIX)
        if not torch.allclose(torch.tensor(first), torch.tensor(second), atol=1e-6, rtol=1e-5):
            raise ModelError("MODEL_UNAVAILABLE")
        self.score("Когда проводят проверку?", ["Проверку проводят один раз в год."])

    def close(self) -> None:
        # Called on the same executor only after the last native inference has completed.
        self.frida = self.reranker = self.frida_tokenizer = self.reranker_tokenizer = None

    def embedding_ids(self, texts: list[str], prefix: str) -> tuple[dict, list[int]]:
        if prefix not in {QUERY_PREFIX, DOCUMENT_PREFIX}:
            raise ModelError("VALIDATION_ERROR")
        if any(not text.strip() or text.lstrip().startswith(("search_query:", "search_document:"))
               for text in texts):
            raise ModelError("VALIDATION_ERROR", {"field": "text", "reason": "raw_nonempty_text_required"})
        if len(texts) > self.settings.embedding_max_batch_items:
            raise ModelError("VALIDATION_ERROR", {"field": "items", "limit": self.settings.embedding_max_batch_items})
        encoded = self.frida_tokenizer([prefix + text for text in texts], add_special_tokens=True,
                                        padding=False, truncation=False)
        lengths = [len(ids) for ids in encoded["input_ids"]]
        for length in lengths:
            if length > 512:
                raise token_limit(length, 512)
        if sum(lengths) > self.settings.embedding_max_batch_tokens:
            raise token_limit(sum(lengths), self.settings.embedding_max_batch_tokens)
        return encoded, lengths

    def embed(self, texts: list[str], prefix: str) -> tuple[list[list[float]], list[int]]:
        import torch
        import torch.nn.functional as functional

        encoded, lengths = self.embedding_ids(texts, prefix)
        inputs = self.frida_tokenizer.pad(encoded, padding=True, return_tensors="pt")
        with torch.inference_mode():
            vectors = functional.normalize(self.frida(**inputs).last_hidden_state[:, 0], p=2, dim=1)
        if (vectors.shape != (len(texts), 1536) or not bool(torch.isfinite(vectors).all())
                or not bool(torch.allclose(torch.linalg.vector_norm(vectors, dim=1),
                                           torch.ones(len(texts)), atol=1e-5))):
            raise ModelError("OUTPUT_SCHEMA_INVALID")
        return vectors.tolist(), lengths

    def reranker_ids(self, query: str, texts: list[str]) -> tuple[list[list[int]], list[int]]:
        if len(texts) > self.settings.reranker_max_batch_items:
            raise ModelError("VALIDATION_ERROR", {"field": "candidates", "limit": self.settings.reranker_max_batch_items})
        if any(not text.strip() for text in [query, *texts]):
            raise ModelError("VALIDATION_ERROR")
        # Source strings cannot inject serialized chat boundaries into the fixed template.
        if any(token in text for text in [query, *texts] for token in self.reranker_tokenizer.all_special_tokens):
            raise ModelError("VALIDATION_ERROR", {"field": "text", "reason": "reserved_model_token"})
        prefix = self.reranker_tokenizer.encode(RERANKER_PREFIX, add_special_tokens=False)
        suffix = self.reranker_tokenizer.encode(RERANKER_SUFFIX, add_special_tokens=False)
        sequences = [prefix + self.reranker_tokenizer.encode(
            f"<Instruct>: {INSTRUCTION}\n<Query>: {query}\n<Document>: {text}",
            add_special_tokens=False, truncation=False) + suffix for text in texts]
        lengths = [len(ids) for ids in sequences]
        for length in lengths:
            if length > self.settings.reranker_max_input_tokens:
                raise token_limit(length, self.settings.reranker_max_input_tokens)
        if sum(lengths) > self.settings.reranker_max_batch_tokens:
            raise token_limit(sum(lengths), self.settings.reranker_max_batch_tokens)
        return sequences, lengths

    def score(self, query: str, texts: list[str]) -> tuple[list[float], list[int]]:
        import torch

        sequences, lengths = self.reranker_ids(query, texts)
        scores: list[float] = []
        offset = 0
        while offset < len(sequences):
            end = offset + 1
            while end < len(sequences) and (end + 1 - offset) * max(lengths[offset:end + 1]) <= self.settings.reranker_max_batch_tokens:
                end += 1
            batch = sequences[offset:end]
            inputs = self.reranker_tokenizer.pad({"input_ids": batch,
                "attention_mask": [[1] * len(ids) for ids in batch]}, padding=True, return_tensors="pt")
            with torch.inference_mode():
                logits = self.reranker(**inputs, logits_to_keep=1, use_cache=False).logits[:, -1, :]
                probabilities = torch.sigmoid(logits[:, 9693] - logits[:, 2152])
            if not bool(torch.isfinite(probabilities).all()) or probabilities.shape != (len(batch),):
                raise ModelError("OUTPUT_SCHEMA_INVALID")
            scores.extend(probabilities.tolist())
            offset = end
        return scores, lengths

    def execute(self, operation: str, request: Any) -> tuple[Any, dict]:
        started = time.monotonic()
        if operation == "query":
            assert isinstance(request, QueryEmbeddingRequest)
            vectors, lengths = self.embed([request.text], QUERY_PREFIX)
            response: Any = QueryEmbeddingResponse(model=FRIDA_MODEL, revision=FRIDA_REVISION,
                vector=vectors[0], input_tokens=lengths[0], processing_ms=(time.monotonic() - started) * 1000)
        elif operation == "documents":
            assert isinstance(request, DocumentEmbeddingRequest)
            vectors, lengths = self.embed([item.text for item in request.items], DOCUMENT_PREFIX)
            response = DocumentEmbeddingResponse(model=FRIDA_MODEL, revision=FRIDA_REVISION,
                items=[DocumentEmbeddingResult(id=item.id, vector=vector, input_tokens=tokens)
                       for item, vector, tokens in zip(request.items, vectors, lengths, strict=True)],
                processing_ms=(time.monotonic() - started) * 1000)
            response.validate_binding(request)
        elif operation == "rerank":
            assert isinstance(request, RerankRequest)
            scores, lengths = self.score(request.query, [item.text for item in request.candidates])
            ordered = sorted(zip(request.candidates, scores, strict=True), key=lambda pair: (-pair[1], pair[0].id))
            response = RerankResponse(model=RERANKER_MODEL, revision=RERANKER_REVISION, score_type="sigmoid",
                scores=[RerankScore(id=item.id, score=score, rank=rank)
                        for rank, (item, score) in enumerate(ordered[:request.top_k], start=1)],
                processing_ms=(time.monotonic() - started) * 1000)
            response.validate_binding(request)
        else:
            raise ModelError("INVALID_REQUEST")
        return response, {"input_tokens": sum(lengths), "input_items": len(lengths),
                          "inference_ms": (time.monotonic() - started) * 1000}
