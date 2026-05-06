from __future__ import annotations

import hashlib
import html
import json
import re
import time
from dataclasses import dataclass
from difflib import SequenceMatcher
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

WAYLAND_BASE_URL = "https://www.waylandgames.co.uk"
WAYLAND_SEARCH_PATHS: tuple[tuple[str, dict[str, str]], ...] = (
    ("/search", {"q": "{query}", "type": "product"}),
    ("/search", {"controller": "search", "s": "{query}"}),
    ("/search", {"s": "{query}"}),
)
WAYLAND_ALLOWED_PATH_PARTS = (
    "/games-workshop/",
    "/warhammer",
    "/black-library",
    "/board-card-games/",
    "/role-playing-games/",
    "/miniature-games/",
    "/card-games/",
    "/painting-modelling/",
)
WAYLAND_DISALLOWED_PATH_PARTS = (
    "/blog",
    "/faqs",
    "/faq",
    "/contact",
    "/search",
    "/manufacturers",
    "/module/",
    "/login",
    "/cart",
)
WAYLAND_USER_AGENT = "Mozilla/5.0 (compatible; FoxfableDescriptionBackfill/1.0; +https://foxfable.co.uk)"
WAYLAND_TIMEOUT_SECONDS = 20
WAYLAND_SEARCH_TIMEOUT_SECONDS = 10
WAYLAND_RETRY_DELAYS_SECONDS: tuple[float, ...] = (1.0, 2.0)
WAYLAND_MAX_SEARCH_RESULTS = 8
WAYLAND_MAX_CANDIDATE_PAGES = 5
WAYLAND_TITLE_SIMILARITY_THRESHOLD = 0.72
WAYLAND_TITLE_TOKEN_HIT_MIN = 2
SOURCE_TEXT_MIN_LENGTH = 80
SIMILARITY_SUBSTRING_WINDOW = 120
SIMILARITY_RATIO_THRESHOLD = 0.88
OPENROUTER_API_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_DEFAULT_MODEL = "openai/gpt-4.1-mini"
OPENROUTER_TIMEOUT_SECONDS = 45
OPENROUTER_RETRY_STATUS_CODES = {408, 409, 429, 500, 502, 503, 504}
OPENROUTER_RETRY_DELAYS_SECONDS: tuple[float, ...] = (1.0, 2.0)
DESCRIPTION_BACKFILL_MATCHER_POLICY_VERSION = 1
DESCRIPTION_BACKFILL_OUTPUT_SCHEMA_VERSION = 1
DESCRIPTION_BACKFILL_MANIFEST_VERSION = 1


@dataclass(frozen=True)
class DescriptionBackfillProduct:
    product_id: str
    title: str
    vendor: str = ""
    product_type: str = ""
    current_description_html: str = ""
    skus: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceResolutionCandidate:
    url: str
    title: str = ""
    excerpt: str = ""


@dataclass(frozen=True)
class WaylandSourceCandidate:
    url: str
    title: str
    description: str
    sku_hits: tuple[str, ...]
    title_similarity: float
    title_token_hits: int
    score: int
    source_kind: str = "wayland"
    notes: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceResolutionResult:
    status: str
    source_site: str
    selected_sku: str
    reason: str
    candidate: WaylandSourceCandidate | None = None
    candidates_considered: tuple[WaylandSourceCandidate, ...] = ()
    search_urls: tuple[str, ...] = ()
    search_result_count: int = 0


@dataclass(frozen=True)
class OpenRouterRuntimeConfig:
    api_key: str
    model: str = OPENROUTER_DEFAULT_MODEL
    timeout_seconds: int = OPENROUTER_TIMEOUT_SECONDS
    retry_delays_seconds: tuple[float, ...] = OPENROUTER_RETRY_DELAYS_SECONDS
    app_name: str = "shopify-description-backfill"
    referer: str = "https://foxfable.co.uk"


@dataclass(frozen=True)
class RewriteResult:
    status: str
    text: str
    html: str
    used_repair_pass: bool
    similarity_ratio: float
    reason: str
    raw_response_text: str = ""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    model: str = ""


@dataclass(frozen=True)
class SimilarityDecision:
    accepted: bool
    ratio: float
    reason: str
    normalized_source: str
    normalized_rewrite: str


@dataclass(frozen=True)
class HtmlShapeResult:
    accepted: bool
    html: str
    paragraphs: tuple[str, ...]
    reason: str


class _WaylandHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title = ""
        self.h1 = ""
        self.description_meta = ""
        self.text_chunks: list[str] = []
        self.anchors: list[tuple[str, str]] = []
        self.product_skus: list[str] = []
        self.ld_json_blobs: list[str] = []
        self._in_title = False
        self._in_h1 = False
        self._in_script_ld_json = False
        self._title_parts: list[str] = []
        self._h1_parts: list[str] = []
        self._anchor_href = ""
        self._anchor_text_parts: list[str] = []
        self._script_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = {key.lower(): (value or "") for key, value in attrs}
        tag_name = tag.lower()
        if tag_name == "title":
            self._in_title = True
            self._title_parts = []
            return
        if tag_name == "h1":
            self._in_h1 = True
            self._h1_parts = []
            return
        if tag_name == "a":
            self._anchor_href = normalized.get("href", "").strip()
            self._anchor_text_parts = []
            return
        if tag_name == "meta":
            meta_name = (normalized.get("name") or normalized.get("property") or "").strip().lower()
            content = normalize_text(normalized.get("content", ""))
            if meta_name in {"description", "og:description", "twitter:description"} and content:
                if len(content) > len(self.description_meta):
                    self.description_meta = content
            return
        if tag_name == "script" and (normalized.get("type") or "").strip().lower() == "application/ld+json":
            self._in_script_ld_json = True
            self._script_parts = []

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.lower()
        if tag_name == "title":
            self._in_title = False
            self.title = normalize_text(" ".join(self._title_parts))
            return
        if tag_name == "h1":
            self._in_h1 = False
            self.h1 = normalize_text(" ".join(self._h1_parts))
            return
        if tag_name == "a":
            if self._anchor_href:
                self.anchors.append((self._anchor_href, normalize_text(" ".join(self._anchor_text_parts))))
            self._anchor_href = ""
            self._anchor_text_parts = []
            return
        if tag_name == "script" and self._in_script_ld_json:
            self._in_script_ld_json = False
            blob = "".join(self._script_parts).strip()
            if blob:
                self.ld_json_blobs.append(blob)
            self._script_parts = []

    def handle_data(self, data: str) -> None:
        text = normalize_text(data)
        if self._in_script_ld_json:
            self._script_parts.append(data)
        if not text:
            return
        if self._in_title:
            self._title_parts.append(text)
        if self._in_h1:
            self._h1_parts.append(text)
        if self._anchor_href:
            self._anchor_text_parts.append(text)
        self.text_chunks.append(text)
        for sku in re.findall(r"\b[A-Z0-9][A-Z0-9._/-]{4,}\b", text):
            self.product_skus.append(sku)


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_search_text(value: str) -> str:
    compact = re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()
    return f" {compact} " if compact else " "


def normalize_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").lower())
    return slug.strip("-")


def safe_spreadsheet_cell(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\r", " ").replace("\n", " ")
    if text.startswith(("=", "+", "-", "@")):
        return "'" + text
    return text


def current_description_backfill_policy_version(
    *,
    preview_columns: list[str] | tuple[str, ...] = (),
    review_columns: list[str] | tuple[str, ...] = (),
    similarity_ratio_threshold: float = SIMILARITY_RATIO_THRESHOLD,
) -> str:
    payload = {
        "manifest_version": DESCRIPTION_BACKFILL_MANIFEST_VERSION,
        "matcher_policy_version": DESCRIPTION_BACKFILL_MATCHER_POLICY_VERSION,
        "output_schema_version": DESCRIPTION_BACKFILL_OUTPUT_SCHEMA_VERSION,
        "source_site": "waylandgames.co.uk",
        "source_rules": {
            "single_confident_candidate": True,
            "title_similarity_threshold": WAYLAND_TITLE_SIMILARITY_THRESHOLD,
            "title_token_hit_min": WAYLAND_TITLE_TOKEN_HIT_MIN,
            "sku_must_match": True,
            "min_source_text_length": SOURCE_TEXT_MIN_LENGTH,
        },
        "rewrite_rules": {
            "sku_required": True,
            "repair_passes": 1,
            "paragraph_only_html": True,
            "similarity_ratio_threshold": similarity_ratio_threshold,
            "substring_window": SIMILARITY_SUBSTRING_WINDOW,
        },
        "preview_columns": list(preview_columns),
        "review_columns": list(review_columns),
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    return f"dbv1-{digest}"


def build_scope_reason(sku_filters: list[str] | tuple[str, ...], limit: int | None) -> str:
    if sku_filters:
        if limit is not None:
            return f"sku_filter+limit:{len(sku_filters)}:{limit}"
        return f"sku_filter:{len(sku_filters)}"
    if limit is not None:
        return f"catalog_limit:{limit}"
    return "full_catalog"


def normalize_skus(values: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    cleaned: list[str] = []
    for value in values:
        sku = normalize_text(value).upper()
        if not sku or sku in cleaned:
            continue
        cleaned.append(sku)
    return tuple(cleaned)


def select_primary_sku(values: list[str] | tuple[str, ...]) -> tuple[str, str]:
    normalized = normalize_skus(values)
    if not normalized:
        return "", "missing_sku"
    if len(normalized) > 1:
        return "", "ambiguous_multi_variant_sku"
    return normalized[0], ""


def load_openrouter_runtime(env: dict[str, str]) -> OpenRouterRuntimeConfig:
    api_key = (env.get("OPENROUTER_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is required for description backfill.")
    model = (env.get("OPENROUTER_MODEL") or OPENROUTER_DEFAULT_MODEL).strip() or OPENROUTER_DEFAULT_MODEL
    timeout_raw = (env.get("OPENROUTER_TIMEOUT_SECONDS") or "").strip()
    retries_raw = (env.get("OPENROUTER_RETRY_DELAYS_SECONDS") or "").strip()
    app_name = (env.get("OPENROUTER_APP_NAME") or "shopify-description-backfill").strip() or "shopify-description-backfill"
    referer = (env.get("OPENROUTER_HTTP_REFERER") or "https://foxfable.co.uk").strip() or "https://foxfable.co.uk"
    timeout_seconds = OPENROUTER_TIMEOUT_SECONDS
    if timeout_raw:
        try:
            timeout_seconds = max(5, int(timeout_raw))
        except ValueError as exc:
            raise RuntimeError(f"OPENROUTER_TIMEOUT_SECONDS must be an integer, got {timeout_raw!r}.") from exc
    retry_delays = OPENROUTER_RETRY_DELAYS_SECONDS
    if retries_raw:
        values: list[float] = []
        for chunk in retries_raw.split(","):
            token = chunk.strip()
            if not token:
                continue
            try:
                values.append(max(0.0, float(token)))
            except ValueError as exc:
                raise RuntimeError(
                    f"OPENROUTER_RETRY_DELAYS_SECONDS must be comma-separated numbers, got {retries_raw!r}."
                ) from exc
        retry_delays = tuple(values)
    return OpenRouterRuntimeConfig(
        api_key=api_key,
        model=model,
        timeout_seconds=timeout_seconds,
        retry_delays_seconds=retry_delays,
        app_name=app_name,
        referer=referer,
    )


def build_openrouter_messages(
    product: DescriptionBackfillProduct,
    *,
    selected_sku: str,
    source_description: str,
    prior_attempt: str = "",
    repair_pass: bool = False,
) -> list[dict[str, str]]:
    system = (
        "You rewrite tabletop retail product descriptions for Shopify. "
        "Return only customer-facing copy, avoid copying source phrasing, and preserve factual meaning. "
        "Output plain text only, no markdown or headings."
    )
    parts = [
        f"Product title: {product.title}",
        f"Vendor: {product.vendor or 'Unknown'}",
        f"Product type: {product.product_type or 'Unknown'}",
        f"Required SKU: {selected_sku}",
        "Requirements:",
        "- Write one concise paragraph suitable for Shopify product description HTML.",
        "- Include the exact SKU once in the paragraph.",
        "- Keep the wording materially different from the source text.",
        "- Do not invent rules, contents, or release details not present in the source.",
        "- Do not mention that this was rewritten or AI-generated.",
        "Source description:",
        source_description,
    ]
    if repair_pass:
        parts.extend([
            "Previous attempt failed validation.",
            f"Previous attempt: {prior_attempt}",
            "Repair the copy so it still reads naturally and includes the exact SKU.",
        ])
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": "\n".join(parts)},
    ]


def rewrite_description_with_openrouter(
    session: requests.Session,
    runtime: OpenRouterRuntimeConfig,
    product: DescriptionBackfillProduct,
    *,
    selected_sku: str,
    source_description: str,
    similarity_ratio_threshold: float = SIMILARITY_RATIO_THRESHOLD,
) -> RewriteResult:
    first_attempt = _run_openrouter_completion(
        session,
        runtime,
        build_openrouter_messages(
            product,
            selected_sku=selected_sku,
            source_description=source_description,
            repair_pass=False,
        ),
    )
    first_text = normalize_text(first_attempt["text"])
    if selected_sku.lower() not in first_text.lower():
        repaired = _run_openrouter_completion(
            session,
            runtime,
            build_openrouter_messages(
                product,
                selected_sku=selected_sku,
                source_description=source_description,
                prior_attempt=first_text,
                repair_pass=True,
            ),
        )
        return _finalize_rewrite_result(
            source_description,
            repaired,
            selected_sku=selected_sku,
            used_repair_pass=True,
            similarity_ratio_threshold=similarity_ratio_threshold,
        )
    return _finalize_rewrite_result(
        source_description,
        first_attempt,
        selected_sku=selected_sku,
        used_repair_pass=False,
        similarity_ratio_threshold=similarity_ratio_threshold,
    )


def evaluate_similarity_gate(
    source_text: str,
    rewritten_text: str,
    *,
    required_sku: str,
    ratio_threshold: float = SIMILARITY_RATIO_THRESHOLD,
    substring_window: int = SIMILARITY_SUBSTRING_WINDOW,
) -> SimilarityDecision:
    normalized_source = normalize_search_text(source_text).strip()
    normalized_rewrite = normalize_search_text(rewritten_text).strip()
    if not normalized_rewrite:
        return SimilarityDecision(False, 1.0, "empty_rewrite", normalized_source, normalized_rewrite)
    if required_sku and required_sku.lower() not in rewritten_text.lower():
        return SimilarityDecision(False, 1.0, "missing_sku", normalized_source, normalized_rewrite)
    if normalized_source == normalized_rewrite:
        return SimilarityDecision(False, 1.0, "exact_match", normalized_source, normalized_rewrite)
    if _contains_long_normalized_substring(normalized_source, normalized_rewrite, substring_window):
        return SimilarityDecision(False, 1.0, "source_contains_rewrite", normalized_source, normalized_rewrite)
    if _contains_long_normalized_substring(normalized_rewrite, normalized_source, substring_window):
        return SimilarityDecision(False, 1.0, "rewrite_contains_source", normalized_source, normalized_rewrite)
    ratio = SequenceMatcher(None, normalized_source, normalized_rewrite).ratio()
    if ratio >= ratio_threshold:
        return SimilarityDecision(False, ratio, "similarity_threshold", normalized_source, normalized_rewrite)
    return SimilarityDecision(True, ratio, "", normalized_source, normalized_rewrite)


def shape_description_html(text: str) -> HtmlShapeResult:
    plain = strip_html_to_text(text)
    if not plain:
        return HtmlShapeResult(False, "", (), "empty_text")
    paragraphs = tuple(
        normalize_text(part)
        for part in re.split(r"(?:\n\s*\n|(?<=[.!?])\s{2,})", plain)
        if normalize_text(part)
    )
    if not paragraphs:
        return HtmlShapeResult(False, "", (), "empty_text")
    html_fragment = "".join(f"<p>{html.escape(paragraph)}</p>" for paragraph in paragraphs)
    return HtmlShapeResult(True, html_fragment, paragraphs, "")


def strip_html_to_text(value: str) -> str:
    if "<" not in (value or ""):
        return normalize_text(value)
    parser = _TextOnlyHTMLParser()
    parser.feed(value)
    return normalize_text(" ".join(parser.text_chunks))


def build_manifest_entry(
    *,
    product: DescriptionBackfillProduct,
    selected_sku: str,
    policy_version: str,
    state: str,
    scope_reason: str,
    source_result: SourceResolutionResult | None = None,
    rewrite_result: RewriteResult | None = None,
    review_reason: str = "",
    failure: str = "",
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "manifest_version": DESCRIPTION_BACKFILL_MANIFEST_VERSION,
        "policy_version": policy_version,
        "product_id": product.product_id,
        "title": product.title,
        "selected_sku": selected_sku,
        "state": state,
        "scope_reason": scope_reason,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    if source_result is not None:
        entry["source"] = {
            "site": source_result.source_site,
            "status": source_result.status,
            "reason": source_result.reason,
            "search_result_count": source_result.search_result_count,
            "candidate_url": source_result.candidate.url if source_result.candidate is not None else "",
            "candidate_title": source_result.candidate.title if source_result.candidate is not None else "",
        }
    if rewrite_result is not None:
        entry["rewrite"] = {
            "status": rewrite_result.status,
            "reason": rewrite_result.reason,
            "used_repair_pass": rewrite_result.used_repair_pass,
            "similarity_ratio": rewrite_result.similarity_ratio,
            "html": rewrite_result.html,
            "text": rewrite_result.text,
            "model": rewrite_result.model,
        }
    if review_reason:
        entry["review_reason"] = review_reason
    if failure:
        entry["failure"] = failure
    return entry


def manifest_entry_matches_policy(entry: dict[str, Any], policy_version: str) -> bool:
    return (
        isinstance(entry, dict)
        and int(entry.get("manifest_version") or 0) == DESCRIPTION_BACKFILL_MANIFEST_VERSION
        and (entry.get("policy_version") or "") == policy_version
    )


def resolve_wayland_source(
    session: requests.Session,
    product: DescriptionBackfillProduct,
    *,
    selected_sku: str,
    search_limit: int = WAYLAND_MAX_SEARCH_RESULTS,
    page_limit: int = WAYLAND_MAX_CANDIDATE_PAGES,
) -> SourceResolutionResult:
    search_query = build_wayland_search_query(product, selected_sku)
    search_results, attempted_urls = fetch_wayland_search_results(
        session,
        search_query,
        limit=search_limit,
    )
    if not search_results:
        return SourceResolutionResult(
            status="missing",
            source_site="waylandgames.co.uk",
            selected_sku=selected_sku,
            reason="no_search_results",
            candidates_considered=(),
            search_urls=tuple(attempted_urls),
            search_result_count=0,
        )
    candidates: list[WaylandSourceCandidate] = []
    for result in search_results[:page_limit]:
        try:
            page = fetch_wayland_product_page(session, result.url)
        except RuntimeError:
            continue
        candidate = score_wayland_source_candidate(product, selected_sku=selected_sku, page=page)
        if candidate is not None:
            candidates.append(candidate)
    candidates.sort(key=lambda item: (-item.score, item.url))
    if not candidates:
        return SourceResolutionResult(
            status="missing",
            source_site="waylandgames.co.uk",
            selected_sku=selected_sku,
            reason="no_confident_candidate",
            candidates_considered=(),
            search_urls=tuple(attempted_urls),
            search_result_count=len(search_results),
        )
    accepted = [candidate for candidate in candidates if candidate.score >= 100]
    if len(accepted) != 1:
        reason = "ambiguous_candidate_set" if len(accepted) > 1 else "weak_title_or_sku_match"
        return SourceResolutionResult(
            status="review",
            source_site="waylandgames.co.uk",
            selected_sku=selected_sku,
            reason=reason,
            candidates_considered=tuple(candidates),
            search_urls=tuple(attempted_urls),
            search_result_count=len(search_results),
        )
    return SourceResolutionResult(
        status="accepted",
        source_site="waylandgames.co.uk",
        selected_sku=selected_sku,
        reason="",
        candidate=accepted[0],
        candidates_considered=tuple(candidates),
        search_urls=tuple(attempted_urls),
        search_result_count=len(search_results),
    )


def build_wayland_search_query(product: DescriptionBackfillProduct, selected_sku: str) -> str:
    parts = [selected_sku, product.title, product.vendor]
    return normalize_text(" ".join(part for part in parts if part))


def fetch_wayland_search_results(
    session: requests.Session,
    query: str,
    *,
    limit: int = WAYLAND_MAX_SEARCH_RESULTS,
) -> tuple[list[SourceResolutionCandidate], list[str]]:
    attempted_urls: list[str] = []
    seen: set[str] = set()
    for path, template_params in WAYLAND_SEARCH_PATHS:
        params = {key: value.format(query=query) for key, value in template_params.items()}
        attempted_urls.append(f"{WAYLAND_BASE_URL}{path}")
        response = _request_with_retries(
            session,
            "GET",
            f"{WAYLAND_BASE_URL}{path}",
            timeout=WAYLAND_SEARCH_TIMEOUT_SECONDS,
            params=params,
        )
        parser = parse_wayland_html(response.text)
        results: list[SourceResolutionCandidate] = []
        for href, text in parser.anchors:
            absolute = urljoin(response.url, href)
            if not is_wayland_product_url(absolute):
                continue
            if absolute in seen:
                continue
            seen.add(absolute)
            results.append(SourceResolutionCandidate(url=absolute, title=text))
            if len(results) >= limit:
                return results, attempted_urls
        if results:
            return results, attempted_urls
    return [], attempted_urls


def fetch_wayland_product_page(session: requests.Session, url: str) -> dict[str, Any]:
    response = _request_with_retries(
        session,
        "GET",
        url,
        timeout=WAYLAND_TIMEOUT_SECONDS,
    )
    parser = parse_wayland_html(response.text)
    ld_json = parse_wayland_ld_json(parser.ld_json_blobs)
    page_title = parser.h1 or parser.title
    description = extract_wayland_description(ld_json, parser)
    sku_values = extract_wayland_skus(ld_json, parser)
    return {
        "url": response.url,
        "title": normalize_text(page_title),
        "description": description,
        "sku_values": sku_values,
        "page_text": normalize_text(" ".join(parser.text_chunks[:400])),
        "structured_data": ld_json,
    }


def score_wayland_source_candidate(
    product: DescriptionBackfillProduct,
    *,
    selected_sku: str,
    page: dict[str, Any],
) -> WaylandSourceCandidate | None:
    description = normalize_text(page.get("description") or "")
    if len(description) < SOURCE_TEXT_MIN_LENGTH:
        return None
    page_title = normalize_text(page.get("title") or "")
    similarity = title_similarity(product.title, page_title)
    token_hits = title_token_hits(product.title, page_title)
    page_text = normalize_text(" ".join([
        page.get("url") or "",
        page_title,
        description,
        page.get("page_text") or "",
        " ".join(page.get("sku_values") or []),
    ]))
    normalized_page_text = normalize_search_text(page_text)
    normalized_sku = normalize_text(selected_sku).upper()
    sku_hits = tuple(
        sku for sku in normalize_skus(tuple(page.get("sku_values") or ()))
        if sku == normalized_sku
    )
    if not sku_hits and normalized_sku:
        if re.search(rf"(?<![A-Z0-9]){re.escape(normalized_sku)}(?![A-Z0-9])", page_text.upper()):
            sku_hits = (normalized_sku,)
    notes: list[str] = []
    score = 0
    if similarity >= WAYLAND_TITLE_SIMILARITY_THRESHOLD:
        score += 60
        notes.append("title_similarity")
    if token_hits >= WAYLAND_TITLE_TOKEN_HIT_MIN:
        score += 25
        notes.append("title_tokens")
    if sku_hits:
        score += 40
        notes.append("sku")
    if normalize_slug(product.title) and normalize_slug(product.title) in normalize_slug(page.get("url") or ""):
        score += 5
        notes.append("url_slug")
    if " out of stock " in normalized_page_text or " add to basket " in normalized_page_text:
        score += 5
        notes.append("product_page")
    if not sku_hits:
        return None
    if similarity < WAYLAND_TITLE_SIMILARITY_THRESHOLD or token_hits < WAYLAND_TITLE_TOKEN_HIT_MIN:
        score = min(score, 99)
    else:
        score = max(score, 100)
    return WaylandSourceCandidate(
        url=page["url"],
        title=page_title,
        description=description,
        sku_hits=sku_hits,
        title_similarity=similarity,
        title_token_hits=token_hits,
        score=score,
        notes=tuple(notes),
    )


def title_similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, normalize_slug(left), normalize_slug(right)).ratio()


def title_token_hits(left: str, right: str) -> int:
    left_tokens = [token for token in re.findall(r"[a-z0-9]+", (left or "").lower()) if len(token) >= 3]
    right_search = normalize_search_text(right)
    return sum(1 for token in dict.fromkeys(left_tokens) if f" {token} " in right_search)


def is_wayland_product_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if host not in {"waylandgames.co.uk", "www.waylandgames.co.uk"}:
        return False
    path = (parsed.path or "").lower()
    if any(part in path for part in WAYLAND_DISALLOWED_PATH_PARTS):
        return False
    return any(part in path for part in WAYLAND_ALLOWED_PATH_PARTS)


def parse_wayland_html(html_text: str) -> _WaylandHTMLParser:
    parser = _WaylandHTMLParser()
    parser.feed(html_text)
    return parser


def parse_wayland_ld_json(blobs: list[str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for blob in blobs:
        try:
            payload = json.loads(blob)
        except ValueError:
            continue
        items.extend(_flatten_ld_json(payload))
    return items


def extract_wayland_description(ld_json_items: list[dict[str, Any]], parser: _WaylandHTMLParser) -> str:
    candidates: list[str] = []
    for item in ld_json_items:
        for key in ("description",):
            value = item.get(key)
            if isinstance(value, str):
                candidates.append(strip_html_to_text(value))
    if parser.description_meta:
        candidates.append(strip_html_to_text(parser.description_meta))
    page_text = normalize_text(" ".join(parser.text_chunks))
    match = re.search(
        r"(?:description|product description|details)\s*[:\-]?\s+(.{80,1200}?)(?:\s+(?:specifications|reviews|delivery|manufacturer)\b|$)",
        page_text,
        flags=re.IGNORECASE,
    )
    if match:
        candidates.append(normalize_text(match.group(1)))
    filtered = [candidate for candidate in candidates if len(candidate) >= SOURCE_TEXT_MIN_LENGTH]
    if not filtered:
        return ""
    filtered.sort(key=len, reverse=True)
    return filtered[0]


def extract_wayland_skus(ld_json_items: list[dict[str, Any]], parser: _WaylandHTMLParser) -> tuple[str, ...]:
    values: list[str] = []
    for item in ld_json_items:
        for key in ("sku", "mpn", "productID", "gtin13", "gtin12"):
            raw = item.get(key)
            if isinstance(raw, str):
                values.append(raw)
        offers = item.get("offers")
        if isinstance(offers, dict):
            sku = offers.get("sku")
            if isinstance(sku, str):
                values.append(sku)
    values.extend(parser.product_skus)
    return normalize_skus(tuple(values))


def _request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: int,
    params: dict[str, str] | None = None,
    json_body: dict[str, Any] | None = None,
    retry_delays: tuple[float, ...] = WAYLAND_RETRY_DELAYS_SECONDS,
    headers: dict[str, str] | None = None,
) -> requests.Response:
    attempts = len(retry_delays) + 1
    last_error: Exception | None = None
    merged_headers = {"User-Agent": WAYLAND_USER_AGENT, **(headers or {})}
    for attempt in range(attempts):
        try:
            response = session.request(
                method,
                url,
                timeout=timeout,
                params=params,
                json=json_body,
                headers=merged_headers,
            )
            if response.status_code in OPENROUTER_RETRY_STATUS_CODES and attempt < attempts - 1:
                time.sleep(retry_delays[attempt])
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code} while requesting {url}")
            return response
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            time.sleep(retry_delays[attempt])
    if last_error is not None:
        raise RuntimeError(f"Network error while requesting {url}: {last_error}") from last_error
    raise RuntimeError(f"Failed to request {url}")


def _run_openrouter_completion(
    session: requests.Session,
    runtime: OpenRouterRuntimeConfig,
    messages: list[dict[str, str]],
) -> dict[str, Any]:
    response = _request_with_retries(
        session,
        "POST",
        OPENROUTER_API_URL,
        timeout=runtime.timeout_seconds,
        json_body={
            "model": runtime.model,
            "messages": messages,
        },
        retry_delays=runtime.retry_delays_seconds,
        headers={
            "Authorization": f"Bearer {runtime.api_key}",
            "HTTP-Referer": runtime.referer,
            "X-Title": runtime.app_name,
            "Content-Type": "application/json",
        },
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"OpenRouter returned invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("OpenRouter returned a non-object JSON payload.")
    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError("OpenRouter returned no choices.")
    message = (choices[0] or {}).get("message") or {}
    content = message.get("content")
    if isinstance(content, list):
        text = normalize_text(" ".join(str(part.get("text") or "") for part in content if isinstance(part, dict)))
    else:
        text = normalize_text(str(content or ""))
    if not text:
        raise RuntimeError("OpenRouter returned empty content.")
    usage = payload.get("usage") or {}
    return {
        "text": text,
        "model": str(payload.get("model") or runtime.model),
        "prompt_tokens": int(usage.get("prompt_tokens") or 0),
        "completion_tokens": int(usage.get("completion_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
        "raw_response_text": json.dumps(payload, sort_keys=True)[:4000],
    }


def _finalize_rewrite_result(
    source_description: str,
    completion: dict[str, Any],
    *,
    selected_sku: str,
    used_repair_pass: bool,
    similarity_ratio_threshold: float,
) -> RewriteResult:
    text = normalize_text(completion["text"])
    similarity = evaluate_similarity_gate(
        source_description,
        text,
        required_sku=selected_sku,
        ratio_threshold=similarity_ratio_threshold,
    )
    if not similarity.accepted:
        return RewriteResult(
            status="review",
            text=text,
            html="",
            used_repair_pass=used_repair_pass,
            similarity_ratio=similarity.ratio,
            reason=similarity.reason,
            raw_response_text=completion.get("raw_response_text", ""),
            prompt_tokens=int(completion.get("prompt_tokens") or 0),
            completion_tokens=int(completion.get("completion_tokens") or 0),
            total_tokens=int(completion.get("total_tokens") or 0),
            model=str(completion.get("model") or ""),
        )
    html_shape = shape_description_html(text)
    if not html_shape.accepted:
        return RewriteResult(
            status="review",
            text=text,
            html="",
            used_repair_pass=used_repair_pass,
            similarity_ratio=similarity.ratio,
            reason=html_shape.reason,
            raw_response_text=completion.get("raw_response_text", ""),
            prompt_tokens=int(completion.get("prompt_tokens") or 0),
            completion_tokens=int(completion.get("completion_tokens") or 0),
            total_tokens=int(completion.get("total_tokens") or 0),
            model=str(completion.get("model") or ""),
        )
    return RewriteResult(
        status="accepted",
        text=text,
        html=html_shape.html,
        used_repair_pass=used_repair_pass,
        similarity_ratio=similarity.ratio,
        reason="",
        raw_response_text=completion.get("raw_response_text", ""),
        prompt_tokens=int(completion.get("prompt_tokens") or 0),
        completion_tokens=int(completion.get("completion_tokens") or 0),
        total_tokens=int(completion.get("total_tokens") or 0),
        model=str(completion.get("model") or ""),
    )


def _contains_long_normalized_substring(haystack: str, needle: str, window: int) -> bool:
    compact_needle = normalize_text(needle)
    compact_haystack = normalize_text(haystack)
    if not compact_needle or not compact_haystack:
        return False
    if len(compact_needle) < window:
        return compact_needle in compact_haystack
    for start in range(0, max(1, len(compact_needle) - window + 1), max(1, window // 3)):
        chunk = compact_needle[start:start + window]
        if chunk and chunk in compact_haystack:
            return True
    return False


def _flatten_ld_json(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        items: list[dict[str, Any]] = []
        if "@graph" in payload and isinstance(payload["@graph"], list):
            for item in payload["@graph"]:
                items.extend(_flatten_ld_json(item))
        else:
            items.append(payload)
        return items
    if isinstance(payload, list):
        items: list[dict[str, Any]] = []
        for item in payload:
            items.extend(_flatten_ld_json(item))
        return items
    return []


class _TextOnlyHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.text_chunks: list[str] = []

    def handle_data(self, data: str) -> None:
        text = normalize_text(data)
        if text:
            self.text_chunks.append(text)
