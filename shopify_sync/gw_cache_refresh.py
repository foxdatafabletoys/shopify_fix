from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import tempfile
import time
import zipfile
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin, urlparse

import requests


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}
ARCHIVE_SUFFIXES = {".zip", ".7z", ".rar", ".tar", ".gz"}
EXTRACTABLE_ARCHIVE_SUFFIXES = {".zip"}
FETCH_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0)
GENERIC_LABELS = {
    "download",
    "download jpg",
    "download jpeg",
    "download png",
    "jpg",
    "jpeg",
    "png",
    "image",
    "view",
    "open",
}

GW_TRADE_FEED_BASE = "https://trade.games-workshop.com/wp-json/gw/v2/media"
# Groups 46/47 are the documented "Product Images" buckets, but GW's trade feed
# segments assets across many additional groups (Black Library covers, accessories,
# wholesale single-pack paints whose multi-pack variants we sell, etc.). The
# multi-pack base11 matcher in shopify_sync only works if we cache those single-pack
# images, so we scan a wide candidate range. Empty/non-existent groups cost one
# request each (~250 ms with the default rate-limit) and are skipped with a single
# log line — see _safe_fetch_first_page below.
#
# Override at runtime via the GW_TRADE_FEED_GROUPS env var, e.g.
#   GW_TRADE_FEED_GROUPS=46,47        # restore previous narrow scan
#   GW_TRADE_FEED_GROUPS=1-120        # explicit range (inclusive)
def _parse_groups_env(value: str | None) -> tuple[int, ...] | None:
    if not value:
        return None
    parts: list[int] = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            lo, hi = chunk.split("-", 1)
            parts.extend(range(int(lo.strip()), int(hi.strip()) + 1))
        else:
            parts.append(int(chunk))
    return tuple(dict.fromkeys(parts)) or None  # dedup, preserve order


import os as _os

GW_TRADE_FEED_IMAGE_GROUPS: tuple[int, ...] = (
    _parse_groups_env(_os.environ.get("GW_TRADE_FEED_GROUPS"))
    or tuple(range(1, 100))
)
GW_TRADE_FEED_COUNTRY = 220
GW_TRADE_FEED_LANG = "en"
GW_TRADE_FEED_PAGE_SIZE = 24
GW_TRADE_FEED_USER_AGENT = (
    "Mozilla/5.0 (compatible; FoxAndFableShopifySync/1.0; "
    "+https://github.com/foxdatafabletoys/shopify_fix)"
)
GW_TRADE_FEED_REFERER = "https://trade.games-workshop.com/resources/"
GW_TRADE_FEED_REQUEST_DELAY_SECONDS = 0.25
GW_TRADE_FEED_SOURCE_LABEL = "trade-feed"
GW_PACK_SOURCE_MARKER_FILENAME = ".gw-source"


@dataclass
class AnchorRecord:
    href: str
    text: str
    attrs: dict[str, str]
    pre_texts: list[str]


@dataclass
class ImageTarget:
    url: str
    filename: str


@dataclass
class ResourcePack:
    label: str
    images: list[ImageTarget]
    archives: list[str]
    source_label: str = ""


class AnchorParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[AnchorRecord] = []
        self._anchor_attrs: dict[str, str] | None = None
        self._anchor_text_parts: list[str] = []
        self._recent_texts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            self._anchor_attrs = {k.lower(): (v or "") for k, v in attrs}
            self._anchor_text_parts = []

    def handle_data(self, data: str) -> None:
        text = normalize_text(data)
        if not text:
            return
        if self._anchor_attrs is not None:
            self._anchor_text_parts.append(text)
        else:
            self._recent_texts.append(text)
            if len(self._recent_texts) > 8:
                self._recent_texts.pop(0)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._anchor_attrs is None:
            return
        href = self._anchor_attrs.get("href", "").strip()
        if href:
            self.anchors.append(
                AnchorRecord(
                    href=href,
                    text=normalize_text(" ".join(self._anchor_text_parts)),
                    attrs=self._anchor_attrs,
                    pre_texts=list(self._recent_texts),
                )
            )
        self._anchor_attrs = None
        self._anchor_text_parts = []


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip())


def normalize_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value or "")
    return slug.strip("-")


def is_meaningful_label(text: str) -> bool:
    candidate = normalize_text(text).strip("- ").lower()
    if not candidate or candidate in GENERIC_LABELS:
        return False
    if re.fullmatch(r"\d{2}/\d{2}/\d{4} \d{1,2}:\d{2} ?(?:am|pm)?", candidate):
        return False
    return True


def choose_anchor_label(anchor: AnchorRecord, fallback_url: str) -> str:
    candidates = [
        anchor.attrs.get("data-label", ""),
        anchor.attrs.get("title", ""),
        anchor.text,
        *reversed(anchor.pre_texts),
        Path(urlparse(fallback_url).path).stem,
    ]
    for candidate in candidates:
        if is_meaningful_label(candidate):
            return normalize_text(candidate)
    return Path(urlparse(fallback_url).path).stem or "gw-pack"


def is_supported_image_url(url: str) -> bool:
    suffix = Path(urlparse(url).path).suffix.lower()
    return suffix in IMAGE_SUFFIXES


def is_archive_url(url: str) -> bool:
    suffix = Path(urlparse(url).path).suffix.lower()
    return suffix in ARCHIVE_SUFFIXES


def is_html_like_url(url: str) -> bool:
    path = urlparse(url).path
    suffix = Path(path).suffix.lower()
    if suffix in IMAGE_SUFFIXES or suffix in ARCHIVE_SUFFIXES:
        return False
    return suffix in {"", ".html", ".htm", ".php", ".asp", ".aspx"}


def same_host(url_a: str, url_b: str) -> bool:
    return urlparse(url_a).netloc.lower() == urlparse(url_b).netloc.lower()


def parse_anchors(html: str) -> list[AnchorRecord]:
    parser = AnchorParser()
    parser.feed(html)
    return parser.anchors


def _get_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    action: str,
):
    last_error: Exception | None = None
    attempts = len(FETCH_RETRY_DELAYS_SECONDS) + 1
    for attempt in range(attempts):
        try:
            return session.get(url, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            time.sleep(FETCH_RETRY_DELAYS_SECONDS[attempt])
    if last_error is not None:
        raise RuntimeError(f"Network error while {action} {url}: {last_error}") from last_error
    raise RuntimeError(f"Network error while {action} {url}")


def fetch_text(session: requests.Session, url: str) -> str:
    response = _get_with_retries(session, url, timeout=60, action="fetching")
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code} while fetching {url}")
    return getattr(response, "text", "")


def fetch_binary(session: requests.Session, url: str) -> tuple[bytes, str]:
    response = _get_with_retries(session, url, timeout=120, action="downloading")
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code} while downloading {url}")
    final_url = getattr(response, "url", url)
    content = getattr(response, "content", b"")
    if not isinstance(content, (bytes, bytearray)):
        raise RuntimeError(f"Binary response missing content for {url}")
    return bytes(content), final_url


def unique_pack_dirname(label: str, used_names: dict[str, int]) -> str:
    normalized = normalize_slug(label) or "gw-pack"
    if normalized not in used_names:
        used_names[normalized] = 1
        return normalized

    product_code_matches = re.findall(r"(?<!\d)(\d{8,14})(?!\d)", label or "")
    if product_code_matches:
        candidate = f"{normalized}-{max(product_code_matches, key=len)}"
        if candidate not in used_names:
            used_names[candidate] = 1
            return candidate

    index = used_names[normalized]
    while True:
        candidate = f"{normalized}-{index}"
        if candidate not in used_names:
            used_names[normalized] = index + 1
            used_names[candidate] = 1
            return candidate
        index += 1


def unique_filename(name: str, used_names: set[str]) -> str:
    candidate = normalize_slug(Path(name).stem) or "image"
    suffix = Path(name).suffix.lower()
    assembled = f"{candidate}{suffix}"
    if assembled not in used_names:
        used_names.add(assembled)
        return assembled

    index = 1
    while True:
        assembled = f"{candidate}-{index}{suffix}"
        if assembled not in used_names:
            used_names.add(assembled)
            return assembled
        index += 1


def build_flattened_filename(url: str, final_url: str, used_names: set[str]) -> str:
    parsed = urlparse(final_url or url)
    path = Path(parsed.path)
    suffix = path.suffix.lower()
    parts = [normalize_slug(part) for part in path.parts[:-1] if normalize_slug(part)]
    stem = normalize_slug(path.stem) or "image"
    candidate = stem
    if parts:
        candidate = f"{parts[-1]}-{stem}"
    return unique_filename(f"{candidate}{suffix}", used_names)


def build_flattened_archive_member_name(member_name: str, used_names: set[str]) -> str:
    path = Path(member_name)
    suffix = path.suffix.lower()
    parts = [normalize_slug(part) for part in path.parts[:-1] if normalize_slug(part)]
    stem = normalize_slug(path.stem) or "image"
    candidate = stem
    if parts:
        candidate = f"{parts[-1]}-{stem}"
    return unique_filename(f"{candidate}{suffix}", used_names)


def derive_archive_asset_group_label(member_name: str, fallback_label: str) -> str:
    stem = Path(member_name).stem
    stem = re.sub(r"^(?:__?MACOSX[-_/]*)+", "", stem, flags=re.IGNORECASE)
    stem = stem.lstrip("-_ ")
    if not stem:
        return fallback_label
    code_match = re.search(r"(?<!\d)(\d{8,14})(?!\d)", stem)
    if not code_match:
        return fallback_label
    product_code = code_match.group(1)
    remainder = stem[code_match.end():].lstrip("-_ ")
    remainder = re.sub(r"[-_ ]?\d{1,3}$", "", remainder)
    remainder_slug = normalize_slug(remainder)
    if remainder_slug:
        return f"{product_code}-{remainder_slug}"
    return product_code


def is_ignored_archive_member(member_name: str) -> bool:
    path = Path(member_name)
    for part in path.parts:
        normalized = part.strip()
        if normalized == "__MACOSX" or normalized.startswith("._"):
            return True
    return False


def compute_tree_fingerprint(root: Path) -> str:
    digest = hashlib.sha1()
    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(root).as_posix()
        data = path.read_bytes()
        digest.update(rel.encode("utf-8"))
        digest.update(str(len(data)).encode("utf-8"))
        digest.update(hashlib.sha1(data).digest())
    return digest.hexdigest()


def load_status(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_status(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def discover_resource_packs(
    resources_url: str,
    session: requests.Session,
) -> tuple[list[ResourcePack], str]:
    top_html = fetch_text(session, resources_url)
    anchors = parse_anchors(top_html)
    packs: list[ResourcePack] = []
    seen_urls: set[str] = set()
    found_archive = False
    found_extractable_archive = False

    for anchor in anchors:
        resolved = urljoin(resources_url, anchor.href)
        if is_archive_url(resolved):
            found_archive = True
            if resolved in seen_urls:
                continue
            seen_urls.add(resolved)
            if Path(urlparse(resolved).path).suffix.lower() in EXTRACTABLE_ARCHIVE_SUFFIXES:
                found_extractable_archive = True
            packs.append(
                ResourcePack(
                    label=choose_anchor_label(anchor, resolved),
                    images=[],
                    archives=[resolved],
                )
            )
            continue
        if is_supported_image_url(resolved):
            label = choose_anchor_label(anchor, resolved)
            packs.append(
                ResourcePack(
                    label=label,
                    images=[
                        ImageTarget(
                            url=resolved,
                            filename=Path(urlparse(resolved).path).name,
                        )
                    ],
                    archives=[],
                )
            )
            continue
        if not is_html_like_url(resolved):
            continue
        if not same_host(resources_url, resolved):
            continue

        try:
            inner_html = fetch_text(session, resolved)
        except RuntimeError:
            continue
        inner_anchors = parse_anchors(inner_html)
        image_targets: list[ImageTarget] = []
        archive_targets: list[str] = []
        for inner_anchor in inner_anchors:
            inner_resolved = urljoin(resolved, inner_anchor.href)
            if is_archive_url(inner_resolved):
                found_archive = True
                if inner_resolved in seen_urls:
                    continue
                seen_urls.add(inner_resolved)
                archive_targets.append(inner_resolved)
                if Path(urlparse(inner_resolved).path).suffix.lower() in EXTRACTABLE_ARCHIVE_SUFFIXES:
                    found_extractable_archive = True
                continue
            if not is_supported_image_url(inner_resolved):
                continue
            if inner_resolved in seen_urls:
                continue
            seen_urls.add(inner_resolved)
            image_targets.append(
                ImageTarget(
                    url=inner_resolved,
                    filename=Path(urlparse(inner_resolved).path).name,
                )
            )
        if image_targets or archive_targets:
            packs.append(
                ResourcePack(
                    label=choose_anchor_label(anchor, resolved),
                    images=image_targets,
                    archives=archive_targets,
                )
            )

    if not packs and found_extractable_archive:
        raise RuntimeError("GW Product Images area exposed extractable archives, but no usable archive packs were discovered.")
    if not packs and found_archive:
        raise RuntimeError("GW Product Images area exposed archives only, but none were ZIP files supported by v1.")
    if not packs:
        raise RuntimeError("No usable GW Product Images resources were discovered.")
    return packs, "Product Images"


def _ensure_trade_feed_session_headers(session: requests.Session) -> None:
    headers = session.headers
    if headers.get("User-Agent", "") != GW_TRADE_FEED_USER_AGENT:
        headers["User-Agent"] = GW_TRADE_FEED_USER_AGENT
    if headers.get("Referer", "") != GW_TRADE_FEED_REFERER:
        headers["Referer"] = GW_TRADE_FEED_REFERER
    headers.setdefault("Accept", "application/json, text/javascript, */*; q=0.01")
    headers.setdefault("X-Requested-With", "XMLHttpRequest")


def _build_trade_feed_url(
    *,
    group: int,
    page: int,
    page_size: int,
    lang: str,
    country: int,
) -> str:
    return (
        f"{GW_TRADE_FEED_BASE}"
        f"?fe=1&group={group}"
        f"&per_page={page_size}"
        f"&page={page}"
        f"&lang={lang}"
        f"&country={country}"
    )


def fetch_trade_feed_page(
    session: requests.Session,
    *,
    group: int,
    page: int,
    page_size: int = GW_TRADE_FEED_PAGE_SIZE,
    lang: str = GW_TRADE_FEED_LANG,
    country: int = GW_TRADE_FEED_COUNTRY,
) -> dict:
    _ensure_trade_feed_session_headers(session)
    url = _build_trade_feed_url(
        group=group, page=page, page_size=page_size, lang=lang, country=country
    )
    response = _get_with_retries(session, url, timeout=60, action="fetching trade feed")
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code} while fetching trade feed page {page} (group {group})")
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"Trade feed returned non-JSON response for group {group} page {page}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Trade feed page payload is not an object: type={type(payload).__name__}")
    return payload


def discover_trade_feed_packs(
    session: requests.Session,
    *,
    groups: tuple[int, ...] = GW_TRADE_FEED_IMAGE_GROUPS,
    country: int = GW_TRADE_FEED_COUNTRY,
    lang: str = GW_TRADE_FEED_LANG,
    page_size: int = GW_TRADE_FEED_PAGE_SIZE,
    max_pages: int | None = None,
    request_delay_seconds: float = GW_TRADE_FEED_REQUEST_DELAY_SECONDS,
    logger: Callable[[str], None] | None = None,
    on_progress: Callable[[list[ResourcePack], dict[str, object]], None] | None = None,
) -> tuple[list[ResourcePack], str, dict[str, object]]:
    """Discover GW trade-feed image assets via the wp-json/gw/v2/media REST endpoint.

    Returns (packs, source_marker, stats). One pack is emitted per asset; each pack has
    label=asset.file_name and a single ImageTarget bound to asset.file_url. Only assets
    with mime_type starting with 'image/' are returned. Assets are deduped by their
    integer 'id' field.

    The session is mutated to include the browser User-Agent and Referer headers required
    by the upstream Cloudflare layer; this is idempotent.
    """
    _ensure_trade_feed_session_headers(session)
    log = logger or (lambda _msg: None)
    progress = on_progress or (lambda _packs, _stats: None)
    packs: list[ResourcePack] = []
    seen_ids: set[int] = set()
    page_count_by_group: dict[str, int] = {}
    request_count = 0
    image_count = 0

    def _snapshot_stats() -> dict[str, object]:
        return {
            "url": GW_TRADE_FEED_BASE,
            "groups": list(groups),
            "country": country,
            "lang": lang,
            "page_size": page_size,
            "page_count_by_group": dict(page_count_by_group),
            "request_count": request_count,
            "image_count": image_count,
        }

    for group in groups:
        page = 1
        # When scanning a wide candidate range, individual groups may not exist
        # (HTTP 4xx/5xx) or simply hold no image-mime-type assets. We tolerate
        # both: log and continue rather than aborting the whole refresh.
        try:
            first = fetch_trade_feed_page(
                session, group=group, page=page, page_size=page_size, lang=lang, country=country
            )
        except RuntimeError as exc:
            log(f"GW trade feed group {group} skipped: {exc}")
            page_count_by_group[str(group)] = 0
            request_count += 1
            continue
        request_count += 1
        total_page_count = int(first.get("page_count") or 0)
        if max_pages is not None:
            total_page_count = min(total_page_count, max_pages)
        page_count_by_group[str(group)] = total_page_count
        if total_page_count == 0:
            # Empty group — don't bother logging, would spam the log over a 99-group sweep.
            continue
        log(f"GW trade feed group {group}: page_count={total_page_count}")

        def _ingest(payload: dict) -> int:
            ingested = 0
            for asset in payload.get("assets", []) or []:
                if not isinstance(asset, dict):
                    continue
                asset_id = asset.get("id")
                if isinstance(asset_id, int):
                    if asset_id in seen_ids:
                        continue
                    seen_ids.add(asset_id)
                mime = (asset.get("mime_type") or "").lower()
                if not mime.startswith("image/"):
                    continue
                file_url = asset.get("file_url") or ""
                file_name = asset.get("file_name") or ""
                if not file_url or not file_name:
                    continue
                packs.append(
                    ResourcePack(
                        label=file_name,
                        images=[ImageTarget(url=file_url, filename=file_name)],
                        archives=[],
                        source_label=GW_TRADE_FEED_SOURCE_LABEL,
                    )
                )
                ingested += 1
            return ingested

        image_count += _ingest(first)
        progress(packs, _snapshot_stats())

        for page in range(2, total_page_count + 1):
            if request_delay_seconds > 0:
                time.sleep(request_delay_seconds)
            try:
                payload = fetch_trade_feed_page(
                    session, group=group, page=page, page_size=page_size, lang=lang, country=country
                )
            except RuntimeError as exc:
                # A wide group sweep may hit transient 5xx on individual pages.
                # Surface and skip the rest of this group rather than aborting
                # the entire refresh — partial coverage of a group is still
                # useful and consistent with the rest-of-refresh "best effort"
                # contract for trade-feed discovery.
                log(f"GW trade feed group {group} page {page} failed, skipping rest of group: {exc}")
                break
            request_count += 1
            ingested = _ingest(payload)
            image_count += ingested
            progress(packs, _snapshot_stats())
            if (page % 50) == 0:
                log(f"GW trade feed group {group}: ingested {image_count} images so far (page {page}/{total_page_count})")

    stats = _snapshot_stats()
    return packs, "GW Trade Feed", stats


def extract_images_from_zip(
    archive_bytes: bytes,
    *,
    archive_label: str,
    staging_root: Path,
    used_pack_names: dict[str, int],
    pack_dirs_by_label: dict[str, Path],
    used_filenames_by_dir: dict[Path, set[str]],
) -> int:
    extracted = 0
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            if is_ignored_archive_member(member.filename):
                continue
            suffix = Path(member.filename).suffix.lower()
            if suffix not in IMAGE_SUFFIXES:
                continue
            group_label = derive_archive_asset_group_label(member.filename, archive_label)
            pack_dir = pack_dirs_by_label.get(group_label)
            if pack_dir is None:
                pack_dir = staging_root / unique_pack_dirname(group_label, used_pack_names)
                pack_dir.mkdir(parents=True, exist_ok=True)
                pack_dirs_by_label[group_label] = pack_dir
            used_filenames = used_filenames_by_dir.setdefault(pack_dir, set())
            filename = build_flattened_archive_member_name(member.filename, used_filenames)
            with zf.open(member) as source:
                (pack_dir / filename).write_bytes(source.read())
            extracted += 1
    if extracted == 0:
        raise RuntimeError(f"ZIP pack '{archive_label}' did not contain any JPG/JPEG/PNG files.")
    return extracted


def list_image_members_from_zip(archive_bytes: bytes) -> list[str]:
    members: list[str] = []
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            if is_ignored_archive_member(member.filename):
                continue
            suffix = Path(member.filename).suffix.lower()
            if suffix not in IMAGE_SUFFIXES:
                continue
            members.append(member.filename)
    return members


def extract_selected_images_from_zip(
    archive_bytes: bytes,
    *,
    archive_label: str,
    member_names: list[str],
    pack_dir: Path,
    used_filenames: set[str],
) -> int:
    selected = set(member_names)
    extracted = 0
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue
            if member.filename not in selected:
                continue
            if is_ignored_archive_member(member.filename):
                continue
            suffix = Path(member.filename).suffix.lower()
            if suffix not in IMAGE_SUFFIXES:
                continue
            filename = build_flattened_archive_member_name(member.filename, used_filenames)
            with zf.open(member) as source:
                (pack_dir / filename).write_bytes(source.read())
            extracted += 1
    if extracted == 0:
        raise RuntimeError(f"ZIP pack '{archive_label}' did not contain the selected JPG/JPEG/PNG files.")
    return extracted


def publish_staging_cache(staging_root: Path, current_root: Path) -> None:
    backup_dir: Path | None = None
    if current_root.exists():
        backup_dir = current_root.with_name("_previous")
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        current_root.rename(backup_dir)
    try:
        staging_root.rename(current_root)
    except Exception:
        if backup_dir and backup_dir.exists() and not current_root.exists():
            backup_dir.rename(current_root)
        raise
    else:
        if backup_dir and backup_dir.exists():
            shutil.rmtree(backup_dir)


def refresh_gw_cache(
    *,
    resources_url: str,
    cache_root: Path,
    status_path: Path,
    dry: bool,
    logger: Callable[[str], None],
    session: requests.Session | None = None,
    enable_trade_feed: bool = True,
    trade_feed_max_pages: int | None = None,
) -> dict[str, object]:
    session = session or requests.Session()
    current_root = cache_root / "current"
    staging_root = cache_root / "_staging"

    packs, source_marker = discover_resource_packs(resources_url, session)
    legacy_pack_count = len(packs)
    legacy_image_count = sum(len(pack.images) for pack in packs)
    archive_target_count = sum(len(pack.archives) for pack in packs)

    trade_feed_started_at = timestamp_now() if enable_trade_feed else None
    trade_feed_packs: list[ResourcePack] = []
    trade_feed_stats: dict[str, object] = {
        "url": GW_TRADE_FEED_BASE,
        "groups": list(GW_TRADE_FEED_IMAGE_GROUPS),
        "country": GW_TRADE_FEED_COUNTRY,
        "lang": GW_TRADE_FEED_LANG,
        "page_size": GW_TRADE_FEED_PAGE_SIZE,
        "page_count_by_group": {},
        "request_count": 0,
        "image_count": 0,
        "started_at": trade_feed_started_at,
        "finished_at": None,
        "last_success_at": None,
        "failure_reason": "",
    }
    if enable_trade_feed:
        try:
            trade_feed_packs, _, discovered_stats = discover_trade_feed_packs(
                session,
                max_pages=trade_feed_max_pages,
                logger=logger,
            )
        except Exception as exc:  # graceful degradation
            trade_feed_stats["failure_reason"] = str(exc)
            trade_feed_stats["finished_at"] = timestamp_now()
            logger(f"GW trade feed discovery failed (legacy /resources/ packs will still publish): {exc}")
        else:
            trade_feed_stats.update(discovered_stats)
            trade_feed_stats["finished_at"] = timestamp_now()
            trade_feed_stats["last_success_at"] = trade_feed_stats["finished_at"]
            packs.extend(trade_feed_packs)

    trade_feed_image_count = int(trade_feed_stats.get("image_count") or 0)
    pack_count = len(packs)
    image_target_count = legacy_image_count + trade_feed_image_count

    if dry:
        logger(
            "GW cache refresh dry-run: discovered "
            f"{legacy_pack_count} legacy packs / {legacy_image_count} direct images / {archive_target_count} archives "
            f"from {resources_url} + {trade_feed_image_count} trade-feed images "
            f"({trade_feed_stats.get('failure_reason') or 'ok'})"
        )
        return {
            "status": "dry_run",
            "pack_count": pack_count,
            "image_count": image_target_count,
            "archive_count": archive_target_count,
            "source_url": resources_url,
            "source_marker": source_marker,
            "trade_feed": trade_feed_stats,
        }

    cache_root.mkdir(parents=True, exist_ok=True)

    previous = load_status(status_path)
    now = timestamp_now()
    status = {
        **previous,
        "status": "refreshing",
        "started_at": now,
        "finished_at": None,
        "source_url": resources_url,
        "source_marker": source_marker,
        "pack_count": pack_count,
        "image_count": image_target_count,
        "archive_count": archive_target_count,
        "published_cache_path": str(current_root),
        "staging_cache_path": str(staging_root),
        "trade_feed": trade_feed_stats,
    }
    save_status(status_path, status)

    if staging_root.exists():
        shutil.rmtree(staging_root)
    staging_root.mkdir(parents=True, exist_ok=True)

    try:
        used_pack_names: dict[str, int] = {}
        pack_dirs_by_label: dict[str, Path] = {}
        used_filenames_by_dir: dict[Path, set[str]] = {}
        for pack in packs:
            pack_dir = pack_dirs_by_label.get(pack.label)
            used_filenames: set[str] | None = None
            for image in pack.images:
                if pack_dir is None:
                    pack_dir = staging_root / unique_pack_dirname(pack.label, used_pack_names)
                    pack_dir.mkdir(parents=True, exist_ok=True)
                    pack_dirs_by_label[pack.label] = pack_dir
                    if pack.source_label:
                        (pack_dir / GW_PACK_SOURCE_MARKER_FILENAME).write_text(
                            pack.source_label, encoding="utf-8"
                        )
                if used_filenames is None:
                    used_filenames = used_filenames_by_dir.setdefault(pack_dir, set())
                content, final_url = fetch_binary(session, image.url)
                filename = build_flattened_filename(image.url, final_url, used_filenames)
                (pack_dir / filename).write_bytes(content)
            for archive_url in pack.archives:
                archive_suffix = Path(urlparse(archive_url).path).suffix.lower()
                if archive_suffix not in EXTRACTABLE_ARCHIVE_SUFFIXES:
                    raise RuntimeError(f"Archive type '{archive_suffix}' is not supported for {archive_url}")
                archive_bytes, final_url = fetch_binary(session, archive_url)
                final_suffix = Path(urlparse(final_url or archive_url).path).suffix.lower()
                if final_suffix not in EXTRACTABLE_ARCHIVE_SUFFIXES:
                    raise RuntimeError(f"Archive type '{archive_suffix}' is not supported for {archive_url}")
                extract_images_from_zip(
                    archive_bytes,
                    archive_label=pack.label,
                    staging_root=staging_root,
                    used_pack_names=used_pack_names,
                    pack_dirs_by_label=pack_dirs_by_label,
                    used_filenames_by_dir=used_filenames_by_dir,
                )

        publish_staging_cache(staging_root, current_root)
        finished_at = timestamp_now()
        published_image_count = sum(
            1
            for path in current_root.rglob("*")
            if path.is_file() and path.name != GW_PACK_SOURCE_MARKER_FILENAME
        )
        status.update(
            {
                "status": "published",
                "finished_at": finished_at,
                "last_success_at": finished_at,
                "failure_reason": "",
                "image_count": published_image_count,
                "published_fingerprint": compute_tree_fingerprint(current_root),
            }
        )
        save_status(status_path, status)
        logger(
            "GW cache refresh published: "
            f"{pack_count} packs / {published_image_count} extracted images "
            f"from {image_target_count} direct files and {archive_target_count} archives"
        )
        return status
    except Exception as exc:
        finished_at = timestamp_now()
        status.update(
            {
                "status": "failed",
                "finished_at": finished_at,
                "last_failure_at": finished_at,
                "failure_reason": str(exc),
            }
        )
        save_status(status_path, status)
        logger(f"GW cache refresh failed: {exc}")
        raise
    finally:
        if staging_root.exists():
            shutil.rmtree(staging_root)


def timestamp_now() -> str:
    from time import gmtime, strftime

    return strftime("%Y-%m-%dT%H:%M:%SZ", gmtime())
