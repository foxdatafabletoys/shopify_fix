from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin, urlparse

import requests


IMAGE_SUFFIXES = {".jpg", ".jpeg", ".png"}
ARCHIVE_SUFFIXES = {".zip", ".7z", ".rar", ".tar", ".gz"}
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


def parse_anchors(html: str) -> list[AnchorRecord]:
    parser = AnchorParser()
    parser.feed(html)
    return parser.anchors


def fetch_text(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=60)
    status_code = getattr(response, "status_code", 200)
    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code} while fetching {url}")
    return getattr(response, "text", "")


def fetch_binary(session: requests.Session, url: str) -> tuple[bytes, str]:
    response = session.get(url, timeout=120)
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

    for anchor in anchors:
        resolved = urljoin(resources_url, anchor.href)
        if is_archive_url(resolved):
            found_archive = True
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
                )
            )
            continue
        if not is_html_like_url(resolved):
            continue

        inner_html = fetch_text(session, resolved)
        inner_anchors = parse_anchors(inner_html)
        image_targets: list[ImageTarget] = []
        for inner_anchor in inner_anchors:
            inner_resolved = urljoin(resolved, inner_anchor.href)
            if is_archive_url(inner_resolved):
                found_archive = True
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
        if image_targets:
            packs.append(ResourcePack(label=choose_anchor_label(anchor, resolved), images=image_targets))

    if not packs and found_archive:
        raise RuntimeError("GW Product Images area exposed archive downloads only; direct-image support is required in v1.")
    if not packs:
        raise RuntimeError("No direct JPG/JPEG/PNG resources were discovered from the GW Product Images area.")
    return packs, "Product Images"


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
) -> dict[str, object]:
    session = session or requests.Session()
    current_root = cache_root / "current"
    staging_root = cache_root / "_staging"
    cache_root.mkdir(parents=True, exist_ok=True)

    packs, source_marker = discover_resource_packs(resources_url, session)
    pack_count = len(packs)
    image_count = sum(len(pack.images) for pack in packs)

    if dry:
        logger(f"GW cache refresh dry-run: discovered {pack_count} packs / {image_count} images from {resources_url}")
        return {
            "status": "dry_run",
            "pack_count": pack_count,
            "image_count": image_count,
            "source_url": resources_url,
            "source_marker": source_marker,
        }

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
        "image_count": image_count,
        "published_cache_path": str(current_root),
        "staging_cache_path": str(staging_root),
    }
    save_status(status_path, status)

    if staging_root.exists():
        shutil.rmtree(staging_root)
    staging_root.mkdir(parents=True, exist_ok=True)

    try:
        used_pack_names: dict[str, int] = {}
        for pack in packs:
            pack_dir = staging_root / unique_pack_dirname(pack.label, used_pack_names)
            pack_dir.mkdir(parents=True, exist_ok=True)
            used_filenames: set[str] = set()
            for image in pack.images:
                content, final_url = fetch_binary(session, image.url)
                filename = build_flattened_filename(image.url, final_url, used_filenames)
                (pack_dir / filename).write_bytes(content)

        publish_staging_cache(staging_root, current_root)
        finished_at = timestamp_now()
        status.update(
            {
                "status": "published",
                "finished_at": finished_at,
                "last_success_at": finished_at,
                "failure_reason": "",
                "published_fingerprint": compute_tree_fingerprint(current_root),
            }
        )
        save_status(status_path, status)
        logger(f"GW cache refresh published: {pack_count} packs / {image_count} images")
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
