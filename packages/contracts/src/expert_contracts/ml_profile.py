"""Authenticated runtime identity; no local paths, credentials or input text."""
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from expert_contracts.common import StrictDTO
from expert_contracts.inference import CapabilitiesResponse

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class EmbeddingRecipe(StrictDTO):
    schema_version: Literal["p05.frida.v1"] = "p05.frida.v1"
    model: Literal["ai-forever/FRIDA"] = "ai-forever/FRIDA"
    revision: Literal["850455b605544a944739b25f81ddf812b6e3d0d5"]
    tokenizer_revision: Literal["850455b605544a944739b25f81ddf812b6e3d0d5"]
    dimension: Literal[1536] = 1536
    pooling: Literal["cls_first_token"] = "cls_first_token"
    normalization: Literal["l2"] = "l2"
    document_prefix: Literal["search_document: "] = "search_document: "
    query_prefix: Literal["search_query: "] = "search_query: "
    max_input_tokens: Literal[512] = 512
    silent_truncation: Literal[False] = False
    tokenizer_fingerprint: Digest
    model_fingerprint: Digest
    runtime_fingerprint: Digest
    device: Literal["cpu"] = "cpu"
    dtype: Literal["float32"] = "float32"


class RerankerRecipe(StrictDTO):
    schema_version: Literal["p05.reranker.v1"] = "p05.reranker.v1"
    model: Literal["Qwen/Qwen3-Reranker-0.6B"] = "Qwen/Qwen3-Reranker-0.6B"
    revision: Literal["e61197ed45024b0ed8a2d74b80b4d909f1255473"]
    model_fingerprint: Digest
    runtime_fingerprint: Digest
    template_fingerprint: Digest
    score_type: Literal["sigmoid"] = "sigmoid"
    true_token_id: Literal[9693] = 9693
    false_token_id: Literal[2152] = 2152
    padding_side: Literal["left"] = "left"
    max_input_tokens: int = Field(ge=1, le=2048)
    silent_truncation: Literal[False] = False
    device: Literal["cpu", "cuda"] = "cpu"
    dtype: Literal["float32", "float16"] = "float32"

    @model_validator(mode="after")
    def validate_device_dtype_pair(self) -> Self:
        if (self.device, self.dtype) not in {("cpu", "float32"), ("cuda", "float16")}:
            raise ValueError("reranker device/dtype must be cpu/float32 or cuda/float16")
        return self


class RuntimeProfile(StrictDTO):
    schema_version: Literal["p05.profile.v1"] = "p05.profile.v1"
    embedding_recipe: EmbeddingRecipe
    reranker_recipe: RerankerRecipe
    capabilities: CapabilitiesResponse
    admission_slots: int = Field(ge=1, le=2)
    cpu_threads: int = Field(ge=1, le=16)
