"""Fail-closed qualification helpers for small mathematical symbols.

The adapter is intentionally narrow.  It can qualify isolated raster glyphs for
``+``, ``−`` and ``±`` by topology, and text-layer ``3`` as superscript only
when bbox/baseline anchors prove it.  It never rewrites OCR text globally.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
import math
from statistics import median
from typing import Any, Iterable, Literal, Mapping, Sequence


BBox = tuple[float, float, float, float]
SourceKind = Literal["raster_topology", "typography_text_layer", "ocr_transcription"]
Status = Literal["resolved", "unresolved", "unsupported"]


@dataclass(frozen=True, slots=True)
class SymbolResolution:
    status: Status
    symbol: str | None
    source_kind: SourceKind
    supported_profile: str
    reason_codes: tuple[str, ...]
    features: Mapping[str, Any]
    may_insert_as_pdf_text_layer: bool = False


@dataclass(frozen=True, slots=True)
class RasterProfile:
    max_input_bytes: int = 2_000_000
    max_width: int = 2048
    max_height: int = 2048
    max_pixels: int = 1_000_000
    min_pixels: int = 8
    min_ink_fraction: float = 0.015
    max_ink_fraction: float = 0.70
    min_aspect: float = 0.45
    max_aspect: float = 2.60
    horizontal_coverage: float = 0.42
    vertical_coverage: float = 0.48
    center_window: tuple[float, float] = (0.32, 0.68)


DEFAULT_RASTER_PROFILE = RasterProfile()
SUPPORTED_RASTER_PROFILE = "isolated_dark_glyph_binary_topology_v1"
SUPPORTED_TYPOGRAPHY_PROFILE = "text_layer_bbox_baseline_superscript_v1"


def unresolved(
    source_kind: SourceKind,
    reason: str,
    *,
    features: Mapping[str, Any] | None = None,
    profile: str = "fail_closed_symbol_adapter_v1",
) -> SymbolResolution:
    return SymbolResolution(
        status="unresolved",
        symbol=None,
        source_kind=source_kind,
        supported_profile=profile,
        reason_codes=(reason,),
        features=dict(features or {}),
    )


def classify_binary_glyph(
    rows: Sequence[Sequence[bool | int]], profile: RasterProfile = DEFAULT_RASTER_PROFILE
) -> SymbolResolution:
    """Classify an isolated raster glyph using only local ink topology.

    ``True``/non-zero values are treated as foreground ink.  The classifier is
    deliberately conservative: ambiguous shapes return ``unresolved`` rather
    than repairing text from neighboring words, filenames, or expected values.
    """

    checked = _checked_matrix(rows, profile)
    if isinstance(checked, SymbolResolution):
        return checked
    matrix = _trim_matrix(checked)
    if not matrix:
        return unresolved("raster_topology", "empty_crop", profile=SUPPORTED_RASTER_PROFILE)

    height = len(matrix)
    width = len(matrix[0])
    ink = sum(sum(row) for row in matrix)
    ink_fraction = ink / (width * height)
    aspect = width / height
    row_counts = [sum(row) for row in matrix]
    col_counts = [sum(matrix[y][x] for y in range(height)) for x in range(width)]
    h_bands = _bands(row_counts, max(2, math.ceil(width * profile.horizontal_coverage)))
    v_bands = _bands(col_counts, max(2, math.ceil(height * profile.vertical_coverage)))
    h_features = [_band_feature(matrix, band, axis="row") for band in h_bands]
    v_features = [_band_feature(matrix, band, axis="column") for band in v_bands]
    components = _components(matrix)
    features: dict[str, Any] = {
        "width": width,
        "height": height,
        "ink_pixels": ink,
        "ink_fraction": round(ink_fraction, 4),
        "aspect": round(aspect, 4),
        "horizontal_bands": h_features,
        "vertical_bands": v_features,
        "component_count": len(components),
        "components": components,
    }

    if len(h_features) == 1 and height <= 4 and width >= profile.min_pixels and len(components) == 1:
        return SymbolResolution(
            status="resolved",
            symbol="−",
            source_kind="raster_topology",
            supported_profile=SUPPORTED_RASTER_PROFILE,
            reason_codes=("isolated_horizontal_stroke",),
            features=features,
        )
    if ink < profile.min_pixels:
        return unresolved(
            "raster_topology", "too_few_foreground_pixels", features=features, profile=SUPPORTED_RASTER_PROFILE
        )
    if not profile.min_ink_fraction <= ink_fraction <= profile.max_ink_fraction:
        return unresolved("raster_topology", "ink_fraction_outside_profile", features=features,
                          profile=SUPPORTED_RASTER_PROFILE)
    if not profile.min_aspect <= aspect <= profile.max_aspect:
        return unresolved("raster_topology", "aspect_outside_profile", features=features,
                          profile=SUPPORTED_RASTER_PROFILE)

    central_vertical = _central_vertical(v_features, profile.center_window, width=width)
    if len(h_features) == 2 and central_vertical is not None:
        upper, lower = h_features
        residual = _residual_ink(matrix, h_bands, (int(central_vertical["start"]), int(central_vertical["end"])))
        features["residual_ink_outside_symbol_strokes"] = residual
        if upper["thickness"] > height * 0.38 or lower["thickness"] > height * 0.38:
            return unresolved("raster_topology", "horizontal_band_too_thick_for_stroke_symbol",
                              features=features, profile=SUPPORTED_RASTER_PROFILE)
        separated = lower["center"] - upper["center"] >= height * 0.22
        lower_wide = lower["coverage"] >= 0.70
        stem_crosses_upper = central_vertical["orthogonal_start"] <= upper["center"] <= central_vertical["orthogonal_end"]
        stem_extension = _central_ink_outside_horizontal_bands(matrix, central_vertical, h_bands)
        features["central_stem_pixels_outside_horizontal_bands"] = stem_extension
        stem_below_lower = _central_ink_below_band(matrix, central_vertical, lower)
        features["central_stem_pixels_below_lower_band"] = stem_below_lower
        if len(components) <= 2 and residual <= max(1, int(ink * 0.05)) and separated and lower_wide \
                and stem_crosses_upper \
                and stem_extension >= max(2, int(height * 0.12)) and stem_below_lower == 0:
            return SymbolResolution(
                status="resolved",
                symbol="±",
                source_kind="raster_topology",
                supported_profile=SUPPORTED_RASTER_PROFILE,
                reason_codes=("two_horizontal_bands_with_center_stem",),
                features=features,
            )

    if len(h_features) == 1:
        horizontal = h_features[0]
        residual = _residual_ink(matrix, h_bands, None if central_vertical is None else (
            int(central_vertical["start"]), int(central_vertical["end"])
        ))
        features["residual_ink_outside_symbol_strokes"] = residual
        outside_horizontal = _ink_outside_rows(matrix, (int(horizontal["start"]), int(horizontal["end"])))
        features["ink_outside_horizontal_stroke"] = outside_horizontal
        if h_features[0]["thickness"] > height * 0.38:
            return unresolved("raster_topology", "horizontal_band_too_thick_for_stroke_symbol",
                              features=features, profile=SUPPORTED_RASTER_PROFILE)
        if central_vertical is None:
            if len(components) == 1 and outside_horizontal == 0:
                return SymbolResolution(
                    status="resolved",
                    symbol="−",
                    source_kind="raster_topology",
                    supported_profile=SUPPORTED_RASTER_PROFILE,
                    reason_codes=("single_validated_horizontal_stroke",),
                    features=features,
                )
            return unresolved("raster_topology", "residual_ink_outside_horizontal_stroke",
                              features=features, profile=SUPPORTED_RASTER_PROFILE)
        band = horizontal
        stem_extends_above = central_vertical["orthogonal_start"] < band["center"] - height * 0.12
        stem_extends_below = central_vertical["orthogonal_end"] > band["center"] + height * 0.12
        if len(components) == 1 and residual <= max(1, int(ink * 0.05)) and stem_extends_above and stem_extends_below:
            return SymbolResolution(
                status="resolved",
                symbol="+",
                source_kind="raster_topology",
                supported_profile=SUPPORTED_RASTER_PROFILE,
                reason_codes=("single_horizontal_band_with_center_stem",),
                features=features,
            )

    return unresolved("raster_topology", "topology_not_in_supported_symbol_profile", features=features,
                      profile=SUPPORTED_RASTER_PROFILE)


def classify_image_bytes(data: bytes, *, threshold: int | None = None) -> SymbolResolution:
    """Load an image with Pillow if available, then classify its dark glyph."""

    if len(data) > DEFAULT_RASTER_PROFILE.max_input_bytes:
        return unresolved(
            "raster_topology",
            "input_bytes_exceed_symbol_profile",
            features={"input_bytes": len(data), "max_input_bytes": DEFAULT_RASTER_PROFILE.max_input_bytes},
            profile=SUPPORTED_RASTER_PROFILE,
        )
    try:
        from PIL import Image
    except ImportError:
        return SymbolResolution(
            status="unsupported",
            symbol=None,
            source_kind="raster_topology",
            supported_profile=SUPPORTED_RASTER_PROFILE,
            reason_codes=("pillow_not_available",),
            features={},
        )
    with Image.open(BytesIO(data)) as image:
        grey = image.convert("L")
        if grey.size[0] > DEFAULT_RASTER_PROFILE.max_width or grey.size[1] > DEFAULT_RASTER_PROFILE.max_height:
            return unresolved(
                "raster_topology",
                "image_dimensions_exceed_symbol_profile",
                features={"width": grey.size[0], "height": grey.size[1]},
                profile=SUPPORTED_RASTER_PROFILE,
            )
        return classify_luma_pixels(list(grey.getdata()), grey.size[0], grey.size[1], threshold=threshold)


def classify_luma_pixels(
    pixels: Sequence[int],
    width: int,
    height: int,
    *,
    threshold: int | None = None,
) -> SymbolResolution:
    if width <= 0 or height <= 0 or len(pixels) != width * height:
        return unresolved(
            "raster_topology",
            "invalid_pixel_buffer",
            features={"width": width, "height": height, "pixel_count": len(pixels)},
            profile=SUPPORTED_RASTER_PROFILE,
        )
    if width > DEFAULT_RASTER_PROFILE.max_width or height > DEFAULT_RASTER_PROFILE.max_height \
            or width * height > DEFAULT_RASTER_PROFILE.max_pixels:
        return unresolved(
            "raster_topology",
            "pixel_buffer_exceeds_symbol_profile",
            features={"width": width, "height": height, "pixel_count": width * height},
            profile=SUPPORTED_RASTER_PROFILE,
        )
    cutoff = max(_otsu_threshold(pixels), 200) if threshold is None else threshold
    rows = [
        [pixels[y * width + x] <= cutoff for x in range(width)]
        for y in range(height)
    ]
    edge_touches = _foreground_touches_edge(rows)
    result = classify_binary_glyph(rows)
    if result.symbol == "−" and edge_touches:
        return unresolved(
            "raster_topology",
            "minus_candidate_touches_crop_edge",
            features={**dict(result.features), "threshold": cutoff, "foreground_touches_crop_edge": True},
            profile=SUPPORTED_RASTER_PROFILE,
        )
    features = {**dict(result.features), "threshold": cutoff, "foreground_touches_crop_edge": edge_touches}
    return SymbolResolution(
        status=result.status,
        symbol=result.symbol,
        source_kind=result.source_kind,
        supported_profile=result.supported_profile,
        reason_codes=result.reason_codes,
        features=features,
        may_insert_as_pdf_text_layer=result.may_insert_as_pdf_text_layer,
    )


def resolve_typographic_three(
    char: Mapping[str, Any],
    line_peers: Iterable[Mapping[str, Any]],
) -> SymbolResolution:
    """Resolve a text-layer 3 as superscript only with bbox/baseline evidence."""

    text = str(char.get("text", ""))
    if text in {"з", "З"}:
        return unresolved(
            "typography_text_layer",
            "cyrillic_lookalike_not_digit_three",
            features=_char_features(char),
            profile=SUPPORTED_TYPOGRAPHY_PROFILE,
        )
    if text == "³":
        return SymbolResolution(
            status="resolved",
            symbol="³",
            source_kind="typography_text_layer",
            supported_profile=SUPPORTED_TYPOGRAPHY_PROFILE,
            reason_codes=("source_codepoint_is_superscript_three",),
            features=_char_features(char),
            may_insert_as_pdf_text_layer=True,
        )
    if text != "3":
        return unresolved(
            "typography_text_layer", "not_digit_three", features=_char_features(char),
            profile=SUPPORTED_TYPOGRAPHY_PROFILE
        )
    bbox = _bbox(char)
    if bbox is None:
        return unresolved("typography_text_layer", "missing_candidate_bbox", features=_char_features(char),
                          profile=SUPPORTED_TYPOGRAPHY_PROFILE)
    peers = list(line_peers)
    peer_boxes = [
        peer_box for peer in peers
        if str(peer.get("text", "")).strip() and (peer_box := _bbox(peer)) is not None
    ]
    if not peer_boxes:
        return unresolved("typography_text_layer", "missing_baseline_anchor", features=_char_features(char),
                          profile=SUPPORTED_TYPOGRAPHY_PROFILE)

    peer_bottom = median(box[3] for box in peer_boxes)
    peer_height = median(box[3] - box[1] for box in peer_boxes)
    height = bbox[3] - bbox[1]
    bottom_lift = peer_bottom - bbox[3]
    size = _float(char.get("size"))
    peer_sizes: list[float] = []
    for peer in peers:
        peer_size = _float(peer.get("size"))
        if peer_size is not None:
            peer_sizes.append(peer_size)
    size_ratio = size / median(peer_sizes) if size is not None and peer_sizes else None
    features = {
        **_char_features(char),
        "peer_baseline_y": round(peer_bottom, 4),
        "peer_height": round(peer_height, 4),
        "candidate_height": round(height, 4),
        "bottom_lift": round(bottom_lift, 4),
        "size_ratio": round(size_ratio, 4) if size_ratio is not None else None,
    }
    lifted = bottom_lift >= max(peer_height * 0.22, 1.0)
    smaller = height <= peer_height * 0.88 or (size_ratio is not None and size_ratio <= 0.88)
    if lifted and smaller:
        return SymbolResolution(
            status="resolved",
            symbol="³",
            source_kind="typography_text_layer",
            supported_profile=SUPPORTED_TYPOGRAPHY_PROFILE,
            reason_codes=("bbox_lifted_above_baseline_with_smaller_height",),
            features=features,
            may_insert_as_pdf_text_layer=True,
        )
    return unresolved("typography_text_layer", "ordinary_digit_three_baseline", features=features,
                      profile=SUPPORTED_TYPOGRAPHY_PROFILE)


def resolve_ocr_transcription_symbol(text: str) -> SymbolResolution:
    """Keep OCR-only symbol transcription untrusted unless another source proves it."""

    return unresolved(
        "ocr_transcription",
        "ocr_text_alone_cannot_prove_math_symbol",
        features={"text": text},
        profile="ocr_transcription_requires_source_anchor_v1",
    )


def _trim_matrix(matrix: list[list[bool]]) -> list[list[bool]]:
    if not matrix or not matrix[0]:
        return []
    rows = [index for index, row in enumerate(matrix) if any(row)]
    cols = [index for index in range(len(matrix[0])) if any(row[index] for row in matrix)]
    if not rows or not cols:
        return []
    return [row[min(cols): max(cols) + 1] for row in matrix[min(rows): max(rows) + 1]]


def _checked_matrix(
    rows: Sequence[Sequence[bool | int]], profile: RasterProfile
) -> list[list[bool]] | SymbolResolution:
    height = len(rows)
    if height == 0:
        return []
    if height > profile.max_height:
        return unresolved(
            "raster_topology",
            "matrix_height_exceeds_symbol_profile",
            features={"height": height, "max_height": profile.max_height},
            profile=SUPPORTED_RASTER_PROFILE,
        )
    first_width = len(rows[0])
    if first_width == 0:
        return []
    if first_width > profile.max_width or first_width * height > profile.max_pixels:
        return unresolved(
            "raster_topology",
            "matrix_size_exceeds_symbol_profile",
            features={"width": first_width, "height": height, "pixel_count": first_width * height},
            profile=SUPPORTED_RASTER_PROFILE,
        )
    matrix: list[list[bool]] = []
    for index, row in enumerate(rows):
        if len(row) != first_width:
            return unresolved(
                "raster_topology",
                "matrix_rows_not_rectangular",
                features={"row": index, "width": len(row), "expected_width": first_width},
                profile=SUPPORTED_RASTER_PROFILE,
            )
        matrix.append([bool(value) for value in row])
    return matrix


def _bands(counts: Sequence[int], threshold: int, *, max_gap: int = 1) -> list[tuple[int, int]]:
    raw: list[tuple[int, int]] = []
    start: int | None = None
    for index, count in enumerate(counts):
        if count >= threshold and start is None:
            start = index
        elif count < threshold and start is not None:
            raw.append((start, index - 1))
            start = None
    if start is not None:
        raw.append((start, len(counts) - 1))

    merged: list[tuple[int, int]] = []
    for band in raw:
        if merged and band[0] - merged[-1][1] <= max_gap + 1:
            merged[-1] = (merged[-1][0], band[1])
        else:
            merged.append(band)
    return merged


def _band_feature(matrix: Sequence[Sequence[bool]], band: tuple[int, int], *, axis: Literal["row", "column"]) -> dict[str, Any]:
    height = len(matrix)
    width = len(matrix[0])
    if axis == "row":
        start, end = band
        xs = [x for y in range(start, end + 1) for x, value in enumerate(matrix[y]) if value]
        coverage = (max(xs) - min(xs) + 1) / width if xs else 0.0
        orthogonal_start = min(xs) if xs else None
        orthogonal_end = max(xs) if xs else None
    else:
        start, end = band
        ys = [y for x in range(start, end + 1) for y in range(height) if matrix[y][x]]
        coverage = (max(ys) - min(ys) + 1) / height if ys else 0.0
        orthogonal_start = min(ys) if ys else None
        orthogonal_end = max(ys) if ys else None
    return {
        "start": start,
        "end": end,
        "center": (start + end) / 2,
        "thickness": end - start + 1,
        "coverage": round(coverage, 4),
        "orthogonal_start": orthogonal_start,
        "orthogonal_end": orthogonal_end,
    }


def _components(matrix: Sequence[Sequence[bool]]) -> list[dict[str, int]]:
    height = len(matrix)
    width = len(matrix[0])
    seen = [[False for _ in range(width)] for _ in range(height)]
    components: list[dict[str, int]] = []
    for y in range(height):
        for x in range(width):
            if seen[y][x] or not matrix[y][x]:
                continue
            stack = [(x, y)]
            seen[y][x] = True
            count = 0
            x0 = x1 = x
            y0 = y1 = y
            while stack:
                cx, cy = stack.pop()
                count += 1
                x0 = min(x0, cx)
                x1 = max(x1, cx)
                y0 = min(y0, cy)
                y1 = max(y1, cy)
                for nx, ny in ((cx - 1, cy), (cx + 1, cy), (cx, cy - 1), (cx, cy + 1)):
                    if 0 <= nx < width and 0 <= ny < height and not seen[ny][nx] and matrix[ny][nx]:
                        seen[ny][nx] = True
                        stack.append((nx, ny))
            components.append({"x0": x0, "y0": y0, "x1": x1, "y1": y1, "pixels": count})
    components.sort(key=lambda item: item["pixels"], reverse=True)
    return components


def _residual_ink(
    matrix: Sequence[Sequence[bool]], horizontal_bands: Sequence[tuple[int, int]], vertical_band: tuple[int, int] | None
) -> int:
    residual = 0
    for y, row in enumerate(matrix):
        in_horizontal = any(start <= y <= end for start, end in horizontal_bands)
        for x, value in enumerate(row):
            if not value:
                continue
            in_vertical = vertical_band is not None and vertical_band[0] <= x <= vertical_band[1]
            if not in_horizontal and not in_vertical:
                residual += 1
    return residual


def _ink_outside_rows(matrix: Sequence[Sequence[bool]], row_band: tuple[int, int]) -> int:
    start, end = row_band
    return sum(1 for y, row in enumerate(matrix) if not start <= y <= end for value in row if value)


def _central_ink_below_band(
    matrix: Sequence[Sequence[bool]], vertical: Mapping[str, Any], lower: Mapping[str, Any]
) -> int:
    x0 = int(vertical["start"])
    x1 = int(vertical["end"])
    y0 = int(lower["end"]) + 1
    return sum(1 for y in range(y0, len(matrix)) for x in range(x0, x1 + 1) if matrix[y][x])


def _central_ink_outside_horizontal_bands(
    matrix: Sequence[Sequence[bool]], vertical: Mapping[str, Any], horizontal_bands: Sequence[tuple[int, int]]
) -> int:
    x0 = int(vertical["start"])
    x1 = int(vertical["end"])
    return sum(
        1
        for y, row in enumerate(matrix)
        if not any(start <= y <= end for start, end in horizontal_bands)
        for x in range(x0, x1 + 1)
        if row[x]
    )


def _foreground_touches_edge(matrix: Sequence[Sequence[bool]]) -> bool:
    if not matrix or not matrix[0]:
        return False
    last_y = len(matrix) - 1
    last_x = len(matrix[0]) - 1
    return (
        any(matrix[0])
        or any(matrix[last_y])
        or any(row[0] or row[last_x] for row in matrix)
    )


def _central_vertical(
    features: Sequence[Mapping[str, Any]], window: tuple[float, float], *, width: int
) -> Mapping[str, Any] | None:
    if not features:
        return None
    candidates = [
        feature for feature in features
        if window[0] <= float(feature["center"]) / width <= window[1]
    ]
    if len(candidates) != 1:
        return None
    return candidates[0]


def _otsu_threshold(pixels: Sequence[int]) -> int:
    histogram = [0] * 256
    for pixel in pixels:
        histogram[max(0, min(255, int(pixel)))] += 1
    total = len(pixels)
    weighted_sum = sum(index * count for index, count in enumerate(histogram))
    sum_background = 0.0
    weight_background = 0
    best_threshold = 127
    best_variance = -1.0
    for threshold, count in enumerate(histogram):
        weight_background += count
        if weight_background == 0:
            continue
        weight_foreground = total - weight_background
        if weight_foreground == 0:
            break
        sum_background += threshold * count
        mean_background = sum_background / weight_background
        mean_foreground = (weighted_sum - sum_background) / weight_foreground
        variance = weight_background * weight_foreground * (mean_background - mean_foreground) ** 2
        if variance > best_variance:
            best_variance = variance
            best_threshold = threshold
    return best_threshold


def _bbox(char: Mapping[str, Any]) -> BBox | None:
    value = char.get("bbox")
    if not isinstance(value, Sequence) or len(value) != 4:
        return None
    values = tuple(_float(item) for item in value)
    if any(item is None for item in values):
        return None
    x0, y0, x1, y1 = (float(item) for item in values if item is not None)
    if x1 <= x0 or y1 <= y0:
        return None
    return (x0, y0, x1, y1)


def _char_features(char: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "text": str(char.get("text", "")),
        "bbox": tuple(char.get("bbox", ())) if isinstance(char.get("bbox"), Sequence) else None,
        "font": char.get("font"),
        "size": char.get("size"),
        "flags": char.get("flags"),
    }


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
