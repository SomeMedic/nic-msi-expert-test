"""Shared contracts; public OpenAPI roots are explicitly allowlisted below.

Private evidence/model/inference DTOs are imported from their named modules.
"""
from .auth import ApplicationRole, SessionInfo, SessionRequest
from .common import (
    CursorPage, GenerationStatus, IngestionStage, JobStatus, LegalStatus, ListQuery,
    PublicationStatus, RunLinks, RunStage, RunStatus, StrictDTO, UTCDateTime,
)
from .documents import (
    ArchiveDocumentRequest, DeactivateRequest, DocumentDetail, DocumentList, DocumentMetadata,
    DocumentSummary, GenerationSummary, IngestionJob, JobCommandAccepted,
    JobProgress, LibraryProfile, PublicationInfo, PublishRequest, PurgeAccepted, PurgePlan,
    PurgeReferenceCounts, PurgeRequest, QualitySummary, ReindexRequest,
    UploadAccepted, UploadLinks, VersionDetail, VersionSummary, VersionUploadOptions,
)
from .debug import DebugCaptureItem, DebugCaptureList, DebugCapturePolicy, DebugCaptureSummary, DebugModelCall
from .errors import (
    ErrorCode, ErrorEnvelope, ErrorInfo, NO_RELEVANT_CONTEXT_MESSAGE,
    OUT_OF_SCOPE_MESSAGE, REFUSAL_MESSAGES, RefusalCode, VERIFICATION_FAILED_MESSAGE,
)
from .purge import PurgeFailureCode, PurgeStatus
from .events import (
    EVENT_ADAPTERS, INGESTION_EVENT_ADAPTER, PUBLIC_EVENT_MODELS, RUN_EVENT_ADAPTER,
    PublicIngestionEvent, PublicRunEvent,
)
from .runs import (
    CreateRunRequest, CriticPublicResult, DebugStep, FinalAnswer, PublicClaim,
    PublicRun, RefusalResult, RunAccepted, RunCancelAccepted, RunDebug, RunList,
    RunResult, RunSummary, SnapshotInfo, ValidationSummary,
)
from .sources import CitationDTO, PublicEvidence, SourceDescriptor, SourceSpan
from .structure import (
    CanonicalTreeNode, CanonicalTreePage, ParseQualityPage, QualityDiagnostic,
    StructuredTableCell, StructuredTableContext, StructuredTablePage,
    StructuredTablePageSegment, StructuredTableRow,
)
from .system import ComponentStatus, HealthStatus, IngestionCapabilities, SystemStatus

PUBLIC_SCHEMA_MODELS = (
    SessionRequest, SessionInfo,
    ErrorEnvelope, HealthStatus, SystemStatus, IngestionCapabilities, ListQuery, DocumentMetadata,
    VersionUploadOptions, UploadAccepted, ReindexRequest, PublishRequest,
    DeactivateRequest, ArchiveDocumentRequest, LibraryProfile, DocumentList, DocumentDetail, VersionDetail,
    IngestionJob, JobCommandAccepted, PurgePlan, PurgeRequest, PurgeAccepted, PurgeStatus,
    CreateRunRequest, RunAccepted, PublicRun, RunList, RunCancelAccepted,
    RunDebug, PublicEvidence, SourceDescriptor, CanonicalTreePage, ParseQualityPage,
    StructuredTablePage, DebugCapturePolicy, DebugCaptureList, *PUBLIC_EVENT_MODELS,
)

__all__ = [
    "ApplicationRole", "SessionInfo", "SessionRequest",
    "PUBLIC_SCHEMA_MODELS", "CursorPage", "GenerationStatus", "IngestionStage",
    "JobStatus", "LegalStatus", "ListQuery", "PublicationStatus", "RunLinks",
    "RunStage", "RunStatus", "StrictDTO", "UTCDateTime", "DeactivateRequest",
    "ArchiveDocumentRequest", "LibraryProfile",
    "DocumentDetail", "DocumentList", "DocumentMetadata", "DocumentSummary",
    "GenerationSummary", "IngestionJob", "JobCommandAccepted", "JobProgress",
    "PublicationInfo", "PublishRequest", "PurgeAccepted", "PurgePlan",
    "PurgeReferenceCounts", "PurgeRequest", "QualitySummary", "ReindexRequest",
    "PurgeStatus", "PurgeFailureCode",
    "UploadAccepted", "UploadLinks", "VersionDetail", "VersionSummary",
    "VersionUploadOptions", "ErrorCode", "ErrorEnvelope", "ErrorInfo",
    "NO_RELEVANT_CONTEXT_MESSAGE", "OUT_OF_SCOPE_MESSAGE", "REFUSAL_MESSAGES",
    "RefusalCode", "VERIFICATION_FAILED_MESSAGE", "EVENT_ADAPTERS",
    "INGESTION_EVENT_ADAPTER", "PUBLIC_EVENT_MODELS", "RUN_EVENT_ADAPTER",
    "PublicIngestionEvent", "PublicRunEvent", "CreateRunRequest", "CriticPublicResult",
    "DebugStep", "FinalAnswer", "PublicClaim", "PublicRun", "RefusalResult",
    "RunAccepted", "RunCancelAccepted", "RunDebug", "RunList", "RunResult",
    "RunSummary", "SnapshotInfo", "ValidationSummary", "CitationDTO",
    "PublicEvidence", "SourceDescriptor", "SourceSpan", "ComponentStatus",
    "HealthStatus", "SystemStatus", "IngestionCapabilities", "CanonicalTreeNode", "CanonicalTreePage",
    "ParseQualityPage", "QualityDiagnostic", "StructuredTableCell",
    "StructuredTableContext", "StructuredTablePage", "StructuredTablePageSegment",
    "StructuredTableRow",
    "DebugCaptureItem", "DebugCaptureList", "DebugCapturePolicy", "DebugCaptureSummary", "DebugModelCall",
]
