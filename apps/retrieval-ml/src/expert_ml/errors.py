"""Safe, typed inference failures; exception text never crosses HTTP boundaries."""
from expert_contracts.errors import Scalar


class ModelError(Exception):
    def __init__(self, code: str, details: dict[str, Scalar] | None = None):
        super().__init__(code)
        self.code = code
        self.details = details or {}


def token_limit(count: int, limit: int) -> ModelError:
    return ModelError("TOKEN_LIMIT_EXCEEDED", {"input_tokens": count, "max_tokens": limit})
