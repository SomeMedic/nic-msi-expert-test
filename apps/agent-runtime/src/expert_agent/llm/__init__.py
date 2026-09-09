"""Private local model-role gateway; importing this package loads no models."""
from .client import LlmGateway
from .types import CallContext, Descriptor, LlmError, RoleResult, ServingProfile

__all__ = ["LlmGateway", "CallContext", "Descriptor", "LlmError", "RoleResult", "ServingProfile"]
