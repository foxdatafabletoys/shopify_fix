from __future__ import annotations

import hashlib
import html
import json
import re
import sys
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Sequence
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import requests

OPENROUTER_API_KEY_ENV = "OPENROUTER_API_KEY"
OPENROUTER_MODEL_ENV = "OPENROUTER_MODEL"
OPENROUTER_DEFAULT_MODEL = "openrouter/auto"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_TIMEOUT_SECONDS = 30
OPENROUTER_RETRY_DELAYS_SECONDS: tuple[float, ...] = (1.0, 2.0)
DESCRIPTION_BACKFILL_SIMILARITY_THRESHOLD = 0.85
DESCRIPTION_BACKFILL_SUBSTRING_MIN_LENGTH = 120
WAYLAND_SEARCH_URL = "https://www.waylandgames.co.uk/search"
WAYLAND_TIMEOUT_SECONDS = 20
WAYLAND_BROWSER_TIMEOUT_MS = WAYLAND_TIMEOUT_SECONDS * 1000
WAYLAND_CHALLENGE_WAIT_TIMEOUT_MS = 45000
WAYLAND_RETRY_DELAYS_SECONDS: tuple[float, ...] = (1.0, 2.0)
WAYLAND_POST_LOAD_SETTLE_MS = 750
WAYLAND_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)
WAYLAND_EXTRA_HTTP_HEADERS: dict[str, str] = {
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Sec-CH-UA": '"Chromium";v="121", "Not A(Brand";v="99", "Google Chrome";v="121"',
    "Sec-CH-UA-Mobile": "?0",
    "Sec-CH-UA-Platform": '"macOS"',
    "Upgrade-Insecure-Requests": "1",
}
WAYLAND_PLAYWRIGHT_SESSION_ATTR = "_description_backfill_wayland_scraper"
GAMES_WORKSHOP_SEARCH_URL = "https://html.duckduckgo.com/html/"
GAMES_WORKSHOP_TIMEOUT_SECONDS = 20
GAMES_WORKSHOP_MAX_CANDIDATE_PAGES = 5
GAMES_WORKSHOP_ALLOWED_HOSTS = frozenset({
    "warhammer.com",
    "www.warhammer.com",
    "games-workshop.com",
    "www.games-workshop.com",
})
GAMES_WORKSHOP_DISALLOWED_PATH_MARKERS: tuple[str, ...] = (
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".gif",
    ".svg",
    ".pdf",
    "/blogs/",
    "/news/",
    "/community/",
    "/downloads/",
    "/articles/",
    "/events/",
    "/contact",
    "/privacy",
    "/terms",
)


@dataclass(frozen=True)
class WaylandCandidate:
    page_url: str
    title: str
    description_text: str
    sku_text: str
    title_score: float
    source_site: str = "wayland_games"


@dataclass(frozen=True)
class SourceResolution:
    status: str
    reason: str
    search_url: str
    source_site: str = "wayland_games"
    candidate: WaylandCandidate | None = None


@dataclass(frozen=True)
class RewriteResult:
    status: str
    reason: str
    source_text: str
    rewritten_text: str
    similarity: float
    repaired_for_sku: bool = False


class _AnchorCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.anchors: list[dict[str, str]] = []
        self._href = ""
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        self._href = dict(attrs).get("href") or ""
        self._chunks = []

    def handle_data(self, data: str) -> None:
        if self._href:
            self._chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._href:
            return
        text = _collapse_ws(" ".join(self._chunks))
        self.anchors.append({"href": self._href, "text": text})
        self._href = ""
        self._chunks = []


class _TextCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture = True
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"script", "style"}:
            self._capture = False
        elif tag.lower() in {"p", "br", "li", "div", "section"}:
            self._chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"}:
            self._capture = True
        elif tag.lower() in {"p", "br", "li", "div", "section"}:
            self._chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if self._capture:
            self._chunks.append(data)

    def text(self) -> str:
        lines = [_collapse_ws(line) for line in "".join(self._chunks).splitlines()]
        return "\n".join(line for line in lines if line)


def _collapse_ws(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", (value or "").lower())
    return slug.strip("-")


def normalize_text(value: str) -> str:
    raw = html.unescape(value or "")
    raw = re.sub(r"<[^>]+>", " ", raw)
    raw = raw.lower()
    raw = re.sub(r"[^a-z0-9]+", " ", raw)
    return _collapse_ws(raw)


def _token_set(value: str) -> set[str]:
    return {token for token in normalize_text(value).split() if token}


def _title_similarity(left: str, right: str) -> float:
    """F1-style token overlap. Symmetric and tolerant of one title carrying
    extra branding tokens (e.g. Wayland's `<title>` adds " | Games Workshop |
    Wayland Games"), while still rejecting near-misses across distinct
    products. Returns 0.0..1.0.
    """
    left_tokens = _token_set(left)
    right_tokens = _token_set(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    if overlap == 0:
        return 0.0
    precision = overlap / len(left_tokens)
    recall = overlap / len(right_tokens)
    return (2 * precision * recall) / (precision + recall)


def _sku_present_in_text(text: str, sku: str) -> bool:
    parts = re.findall(r"[a-z0-9]+", (sku or "").lower())
    if not parts:
        return False
    haystack = (text or "").lower()
    pattern = re.compile(
        rf"(?<![a-z0-9]){r'[^a-z0-9]*'.join(re.escape(part) for part in parts)}(?![a-z0-9])"
    )
    return bool(pattern.search(haystack))


def text_similarity(left: str, right: str) -> float:
    left_tokens = normalize_text(left).split()
    right_tokens = normalize_text(right).split()
    if not left_tokens or not right_tokens:
        return 0.0
    left_counts: dict[str, int] = {}
    right_counts: dict[str, int] = {}
    for token in left_tokens:
        left_counts[token] = left_counts.get(token, 0) + 1
    for token in right_tokens:
        right_counts[token] = right_counts.get(token, 0) + 1
    common = 0
    for token, count in left_counts.items():
        common += min(count, right_counts.get(token, 0))
    return common / max(len(left_tokens), len(right_tokens))


def contains_long_substring(left: str, right: str, *, min_length: int = DESCRIPTION_BACKFILL_SUBSTRING_MIN_LENGTH) -> bool:
    left_norm = normalize_text(left)
    right_norm = normalize_text(right)
    if not left_norm or not right_norm:
        return False
    shorter, longer = (left_norm, right_norm) if len(left_norm) <= len(right_norm) else (right_norm, left_norm)
    return len(shorter) >= min_length and shorter in longer


def require_openrouter_config(env: dict[str, str]) -> dict[str, str]:
    api_key = (env.get(OPENROUTER_API_KEY_ENV) or "").strip()
    if not api_key:
        raise RuntimeError(f"{OPENROUTER_API_KEY_ENV} must be set for --backfill-descriptions.")
    model = (env.get(OPENROUTER_MODEL_ENV) or "").strip() or OPENROUTER_DEFAULT_MODEL
    return {"api_key": api_key, "model": model}


def current_description_backfill_policy_version(
    *,
    preview_columns: Sequence[str],
    review_columns: Sequence[str],
) -> str:
    payload = {
        "wayland_search_strategy": "playwright",
        "wayland_search_url": WAYLAND_SEARCH_URL,
        "wayland_timeout_seconds": WAYLAND_TIMEOUT_SECONDS,
        "games_workshop_search_url": GAMES_WORKSHOP_SEARCH_URL,
        "openrouter_model_default": OPENROUTER_DEFAULT_MODEL,
        "openrouter_timeout_seconds": OPENROUTER_TIMEOUT_SECONDS,
        "similarity_threshold": DESCRIPTION_BACKFILL_SIMILARITY_THRESHOLD,
        "substring_min_length": DESCRIPTION_BACKFILL_SUBSTRING_MIN_LENGTH,
        "preview_columns": list(preview_columns),
        "review_columns": list(review_columns),
        "outcomes": ["updated", "review", "failed", "resume_completed", "policy_invalidated"],
        "required_match_signals": ["unique_candidate", "title_agreement", "sku_present"],
        "required_rewrite_gates": ["non_empty", "sku_present", "non_copy_like", "html_shape"],
    }
    digest = hashlib.sha1(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:12]
    return f"dbv1-{digest}"


def load_manifest(path: Any) -> dict[str, dict[str, Any]]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    if not raw.strip():
        return {}
    try:
        payload = json.loads(raw)
    except ValueError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {str(key): dict(value) for key, value in payload.items() if isinstance(value, dict)}


def save_manifest(path: Any, manifest: dict[str, dict[str, Any]]) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


_WAYLAND_QUERY_STOPWORDS: frozenset[str] = frozenset({
    "the", "a", "an", "of", "and", "with", "to", "for",
    "pb", "hb", "eng", "english",
})


def _normalise_wayland_query(*parts: str) -> str:
    """Wayland's search is case-sensitive and treats punctuation literally, so
    `STORMCAST ETERNALS: DRACOTHIAN GUARD` returns zero hits while
    `stormcast eternals dracothian guard` returns the real product.
    Drop punctuation, lowercase, and prune common stopwords.
    """
    cleaned: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part:
            continue
        for token in re.findall(r"[a-z0-9]+", part.lower()):
            if token in _WAYLAND_QUERY_STOPWORDS:
                continue
            if token in seen:
                continue
            seen.add(token)
            cleaned.append(token)
    return " ".join(cleaned)


def build_search_url(title: str, sku: str) -> str:
    """Build the SKU+title search URL.

    Wayland's Next.js search reads `?query=...` (the older `?s=` is silently
    ignored, yielding an empty results page). The query is treated literally:
    case-sensitive and not punctuation-tolerant, so we normalise to lowercase
    alphanumeric tokens before sending.
    """
    query = _normalise_wayland_query(sku, title)
    return f"{WAYLAND_SEARCH_URL}?query={quote_plus(query)}"


def build_title_only_search_url(title: str) -> str:
    """Same normalisation as build_search_url, without the SKU. Used as a
    fallback when the SKU-prefixed query returns zero hits because the F&F
    SKU doesn't exist in Wayland's index (GW catalogue-code drift)."""
    query = _normalise_wayland_query(title)
    return f"{WAYLAND_SEARCH_URL}?query={quote_plus(query)}"


def build_wayland_product_guess_url(title: str, sku: str) -> str:
    parts = [normalize_slug(title), normalize_slug(sku)]
    slug = "-".join(part for part in parts if part)
    return f"https://www.waylandgames.co.uk/{slug}"


def require_playwright():
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        python_exe = (sys.executable or "python").strip() or "python"
        raise RuntimeError(
            "Playwright is required for Wayland description scraping. "
            f"Install it with `{python_exe} -m pip install playwright` and "
            f"`{python_exe} -m playwright install chromium`."
        ) from exc
    return sync_playwright, PlaywrightError, PlaywrightTimeoutError


class WaylandPlaywrightScraper:
    def __init__(self) -> None:
        sync_playwright, playwright_error, playwright_timeout = require_playwright()
        self._playwright_error = playwright_error
        self._playwright_timeout = playwright_timeout
        self._playwright_manager = sync_playwright().start()
        self._browser = self._playwright_manager.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            user_agent=WAYLAND_USER_AGENT,
            locale="en-GB",
            viewport={"width": 1365, "height": 900},
            extra_http_headers=WAYLAND_EXTRA_HTTP_HEADERS,
        )
        # Hide a few common headless tells before any page script runs.
        self._context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
            "Object.defineProperty(navigator, 'languages', {get: () => ['en-GB', 'en']});"
            "Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});"
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(WAYLAND_BROWSER_TIMEOUT_MS)

    def fetch_html(self, url: str) -> str:
        last_error: Exception | None = None
        attempts = len(WAYLAND_RETRY_DELAYS_SECONDS) + 1
        for attempt in range(attempts):
            try:
                self._page.goto(url, wait_until="domcontentloaded", timeout=WAYLAND_BROWSER_TIMEOUT_MS)
                self._wait_for_challenge_clear()
                self._dismiss_cookie_banner()
                self._brief_settle()
                self._wait_for_challenge_clear()
                html_text = self._page.content()
                if self._looks_like_challenge_page(html_text):
                    raise RuntimeError(f"Cloudflare challenge did not clear for {url}")
                return html_text
            except (self._playwright_timeout, self._playwright_error) as exc:
                last_error = exc
            except RuntimeError as exc:
                last_error = exc
                if attempt >= attempts - 1:
                    break
                time.sleep(WAYLAND_RETRY_DELAYS_SECONDS[attempt])
        detail = str(last_error) if last_error is not None else "unknown browser failure"
        raise RuntimeError(f"Playwright failed while fetching {url}: {detail}")

    def close(self) -> None:
        close_map = (
            ("_page", "close"),
            ("_context", "close"),
            ("_browser", "close"),
            ("_playwright_manager", "stop"),
        )
        for attr_name, method_name in close_map:
            resource = getattr(self, attr_name, None)
            if resource is None:
                continue
            try:
                getattr(resource, method_name)()
            except KeyboardInterrupt:
                pass
            except Exception:
                pass
            setattr(self, attr_name, None)

    def _brief_settle(self) -> None:
        try:
            self._page.wait_for_timeout(WAYLAND_POST_LOAD_SETTLE_MS)
        except Exception:
            pass

    def _dismiss_cookie_banner(self) -> None:
        selectors = (
            "#onetrust-accept-btn-handler",
            "button:has-text('Accept')",
            "button:has-text('Allow all')",
            "button:has-text('I agree')",
        )
        for selector in selectors:
            try:
                button = self._page.locator(selector).first
                if button.count():
                    button.click(timeout=1000)
                    return
            except Exception:
                continue

    def _wait_for_challenge_clear(self) -> None:
        if not self._looks_like_challenge_page(self._page.content()):
            return
        try:
            self._page.wait_for_function(
                """
                () => {
                    const title = (document.title || "").toLowerCase();
                    const bodyText = (document.body?.innerText || "").toLowerCase();
                    return !title.includes("just a moment")
                        && !bodyText.includes("enable javascript and cookies to continue")
                        && !bodyText.includes("checking your browser before accessing");
                }
                """,
                timeout=WAYLAND_CHALLENGE_WAIT_TIMEOUT_MS,
            )
            self._brief_settle()
        except self._playwright_timeout:
            pass

    @staticmethod
    def _looks_like_challenge_page(html_text: str) -> bool:
        lowered = (html_text or "").lower()
        return (
            # Cloudflare
            "just a moment" in lowered
            or "enable javascript and cookies to continue" in lowered
            or "challenge-platform" in lowered
            or "_cf_chl_opt" in lowered
            # PerimeterX/HUMAN actual block pages (not the standard sensor script
            # which loads on every page; detect interstitials only)
            or "press &amp; hold" in lowered
            or "press & hold" in lowered
            or "px-captcha" in lowered
            or "access to this page has been denied" in lowered
            or "please verify you are a human" in lowered
        )


def _get_wayland_scraper(session: requests.Session) -> WaylandPlaywrightScraper:
    scraper = getattr(session, WAYLAND_PLAYWRIGHT_SESSION_ATTR, None)
    if scraper is None:
        scraper = WaylandPlaywrightScraper()
        setattr(session, WAYLAND_PLAYWRIGHT_SESSION_ATTR, scraper)
    return scraper


def close_wayland_scraper(session: requests.Session) -> None:
    scraper = getattr(session, WAYLAND_PLAYWRIGHT_SESSION_ATTR, None)
    if scraper is None:
        return
    try:
        scraper.close()
    finally:
        try:
            delattr(session, WAYLAND_PLAYWRIGHT_SESSION_ATTR)
        except AttributeError:
            pass


def fetch_url_with_retries(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    retry_delays: Sequence[float],
) -> requests.Response:
    last_error: Exception | None = None
    attempts = len(retry_delays) + 1
    for attempt in range(attempts):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                time.sleep(retry_delays[attempt])
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code} while fetching {url}")
            _ = response.text
            return response
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            time.sleep(retry_delays[attempt])
    if last_error is not None:
        raise RuntimeError(f"Network error while fetching {url}: {last_error}") from last_error
    raise RuntimeError(f"Failed to fetch {url}")


_WAYLAND_PRODUCT_PATH_RE = re.compile(r"^/[a-z0-9][a-z0-9-]*-\d{4,}/?$")
_WAYLAND_NON_PRODUCT_PATH_PREFIXES: tuple[str, ...] = (
    "/account",
    "/auth/",
    "/cart",
    "/checkout",
    "/wishlist",
    "/search",
    "/news",
    "/blog",
    "/pre-orders",
    "/help",
    "/about",
    "/contact",
    "/faq",
    "/terms",
    "/privacy",
    "/cookie",
    "/shipping",
    "/returns",
    "/affiliate",
    "/competitions",
    "/wayland-games-centre",
    "/how-to",
    "/_next",
    "/favicon",
    "/artwork-prints",
    "/eu-shipping",
    "/accessibility",
)


def _is_wayland_product_path(path: str) -> bool:
    """Wayland product URLs follow `/<slug-with-hyphens>-<numeric-code>` and
    sit at the site root (no category prefix). Filter out nav, account, blog,
    static-asset, and category landing-page URLs that share the same domain
    but aren't products.
    """
    if not path or path == "/" or path.startswith("#"):
        return False
    lowered = path.lower()
    for prefix in _WAYLAND_NON_PRODUCT_PATH_PREFIXES:
        if lowered.startswith(prefix):
            return False
    return bool(_WAYLAND_PRODUCT_PATH_RE.match(lowered))


def _parse_search_candidates(html_text: str, search_url: str) -> list[tuple[str, str]]:
    parser = _AnchorCollector()
    parser.feed(html_text)
    seen: set[str] = set()
    matches: list[tuple[str, str]] = []
    base = "{uri.scheme}://{uri.netloc}".format(uri=urlparse(search_url))
    for anchor in parser.anchors:
        href = anchor["href"]
        if not href:
            continue
        absolute = urljoin(base, href)
        parsed = urlparse(absolute)
        if "waylandgames.co.uk" not in parsed.netloc.lower():
            continue
        if not _is_wayland_product_path(parsed.path):
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        title = anchor["text"]
        if title:
            matches.append((absolute, title))
    return matches


def _normalize_search_redirect(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        encoded = parse_qs(parsed.query).get("uddg", [""])[0]
        if encoded:
            return encoded
    return url


def _build_games_workshop_search_url(title: str, sku: str) -> str:
    query_parts = [
        f"site:warhammer.com/en-GB/shop \"{title}\"",
        f"\"{sku}\"" if sku else "",
    ]
    query = " ".join(part for part in query_parts if part).strip()
    return f"{GAMES_WORKSHOP_SEARCH_URL}?q={quote_plus(query)}"


def _is_games_workshop_product_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.netloc or "").lower()
    if host not in GAMES_WORKSHOP_ALLOWED_HOSTS:
        return False
    path = (parsed.path or "").lower()
    if "/shop/" not in path:
        return False
    return not any(marker in path for marker in GAMES_WORKSHOP_DISALLOWED_PATH_MARKERS)


def _search_games_workshop_candidates(
    session: requests.Session,
    *,
    title: str,
    sku: str,
) -> tuple[list[tuple[str, str]], str]:
    search_url = _build_games_workshop_search_url(title, sku)
    response = fetch_url_with_retries(
        session,
        search_url,
        timeout=GAMES_WORKSHOP_TIMEOUT_SECONDS,
        retry_delays=WAYLAND_RETRY_DELAYS_SECONDS,
    )
    parser = _AnchorCollector()
    parser.feed(response.text)
    seen: set[str] = set()
    candidates: list[tuple[str, str]] = []
    for anchor in parser.anchors:
        resolved = _normalize_search_redirect(anchor["href"])
        if not _is_games_workshop_product_url(resolved):
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        candidates.append((resolved, anchor["text"]))
        if len(candidates) >= GAMES_WORKSHOP_MAX_CANDIDATE_PAGES:
            break
    return candidates, search_url


def resolve_games_workshop_source(
    session: requests.Session,
    *,
    title: str,
    sku: str,
) -> SourceResolution:
    candidate_links, search_url = _search_games_workshop_candidates(session, title=title, sku=sku)
    if not candidate_links:
        return SourceResolution(
            status="review",
            reason="no_games_workshop_candidate",
            search_url=search_url,
            source_site="games_workshop",
        )

    sku_matches: list[WaylandCandidate] = []
    title_only_matches: list[WaylandCandidate] = []
    for page_url, link_title in candidate_links:
        response = fetch_url_with_retries(
            session,
            page_url,
            timeout=GAMES_WORKSHOP_TIMEOUT_SECONDS,
            retry_delays=WAYLAND_RETRY_DELAYS_SECONDS,
        )
        page_html = response.text
        page_title = _extract_page_title(page_html) or link_title
        description_text = _extract_description_text(page_html)
        sku_present = _sku_present_in_text(page_html, sku)
        title_score = _title_similarity(title, page_title)
        if len(description_text) < 40:
            continue
        candidate = WaylandCandidate(
            page_url=page_url,
            title=page_title,
            description_text=description_text,
            sku_text=sku,
            title_score=title_score,
            source_site="games_workshop",
        )
        if sku_present and title_score >= 0.5:
            sku_matches.append(candidate)
        elif title_score >= 0.8:
            title_only_matches.append(candidate)

    pool = sku_matches or title_only_matches
    if not pool:
        return SourceResolution(
            status="review",
            reason="no_confident_games_workshop_match",
            search_url=search_url,
            source_site="games_workshop",
        )
    pool.sort(key=lambda c: c.title_score, reverse=True)
    best = pool[0]
    if len(pool) > 1 and pool[1].title_score >= best.title_score - 0.05:
        return SourceResolution(
            status="review",
            reason="multiple_confident_games_workshop_matches",
            search_url=search_url,
            source_site="games_workshop",
        )
    reason = "games_workshop_title_sku_match" if sku_matches else "games_workshop_title_only_match"
    return SourceResolution(
        status="accepted",
        reason=reason,
        search_url=search_url,
        source_site="games_workshop",
        candidate=best,
    )


def _search_wayland_candidates(
    session: requests.Session,
    *,
    title: str,
    sku: str,
) -> tuple[list[tuple[str, str]], str]:
    """Build the candidate list: a slug-guess URL plus search results from up
    to two queries.

    Wayland's search engine treats the query as a literal phrase. When we
    prepend the F&F variant SKU and that SKU doesn't exist on Wayland (because
    of GW catalogue-code drift), the search returns ZERO hits and the page
    falls back to a site-wide "featured products" carousel — so the candidate
    list ends up full of unrelated products. Fix: also run a title-only query
    and merge candidates. Title-only is more permissive (more results to score
    against) and recovers SKU-mismatched products.
    """
    primary_url = build_search_url(title, sku)
    scraper = _get_wayland_scraper(session)
    guessed_url = build_wayland_product_guess_url(title, sku)
    candidates: list[tuple[str, str]] = [(guessed_url, title)]
    seen = {guessed_url}
    queries = [primary_url]
    if sku:
        title_only_url = build_title_only_search_url(title)
        if title_only_url != primary_url:
            queries.append(title_only_url)
    for query_url in queries:
        try:
            search_html = scraper.fetch_html(query_url)
        except Exception:
            continue
        for candidate_url, candidate_title in _parse_search_candidates(search_html, query_url):
            if candidate_url in seen:
                continue
            seen.add(candidate_url)
            candidates.append((candidate_url, candidate_title))
    return candidates, primary_url


def _extract_description_text(page_html: str) -> str:
    meta_match = re.search(
        r'<meta[^>]+(?:name|property)=["\'](?:description|og:description)["\'][^>]+content=["\'](.*?)["\']',
        page_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if meta_match:
        return _collapse_ws(html.unescape(meta_match.group(1)))
    parser = _TextCollector()
    parser.feed(page_html)
    text = parser.text()
    paragraphs = [line for line in text.splitlines() if len(line) >= 40]
    return paragraphs[0] if paragraphs else ""


def _extract_page_title(page_html: str) -> str:
    match = re.search(r"<title>(.*?)</title>", page_html, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return _collapse_ws(html.unescape(match.group(1)))
    og = re.search(
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\'](.*?)["\']',
        page_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if og:
        return _collapse_ws(html.unescape(og.group(1)))
    return ""


def resolve_wayland_source(
    session: requests.Session,
    *,
    title: str,
    sku: str,
) -> SourceResolution:
    candidate_links, search_url = _search_wayland_candidates(session, title=title, sku=sku)
    if not candidate_links:
        return SourceResolution(status="review", reason="no_wayland_candidate", search_url=search_url)

    scraper = _get_wayland_scraper(session)
    # Two acceptance lanes:
    #   (a) "title + SKU" — title >= 0.6 AND SKU literally present on the page.
    #   (b) "title-only"  — title >= 0.85, no SKU required. This handles cases
    #       where Wayland's URL/SKU is a different GW code generation for the
    #       same product (e.g. F&F has 99120218074 but Wayland has 99120218009
    #       for Dracothian Guard — same model, different GW catalogue codes).
    sku_matches: list[WaylandCandidate] = []
    title_only_matches: list[WaylandCandidate] = []
    # Score up to 8 candidates — we merge two search queries (SKU+title and
    # title-only) plus a slug-guess URL, so candidate counts can exceed 5.
    for page_url, link_title in candidate_links[:8]:
        page_html = scraper.fetch_html(page_url)
        page_title = _extract_page_title(page_html) or link_title
        description_text = _extract_description_text(page_html)
        sku_present = _sku_present_in_text(page_html, sku)
        title_score = _title_similarity(title, page_title)
        if not description_text:
            continue
        candidate = WaylandCandidate(
            page_url=page_url,
            title=page_title,
            description_text=description_text,
            sku_text=sku,
            title_score=title_score,
        )
        # SKU-confirmed lane: F1 >= 0.5 (lenient — the SKU agrees, so a
        # moderate title overlap is enough confidence).
        # Title-only lane:    F1 >= 0.8 (strict — without SKU confirmation we
        # need the titles to be near-identical to avoid cross-product mixups
        # like accepting "Krondys Son of Dracothian" for "Dracothian Guard").
        if sku_present and title_score >= 0.5:
            sku_matches.append(candidate)
        elif not sku_present and title_score >= 0.8:
            title_only_matches.append(candidate)
    # Prefer SKU-confirmed matches when present; otherwise fall back to the
    # strict title-only lane. Within each lane, pick the highest title score.
    pool = sku_matches or title_only_matches
    if not pool:
        return SourceResolution(status="review", reason="no_confident_wayland_match", search_url=search_url)
    pool.sort(key=lambda c: c.title_score, reverse=True)
    best = pool[0]
    # Disambiguate ties: if the top two have similar scores, that's still a
    # review case — we shouldn't guess between two equally-good matches.
    if len(pool) > 1 and pool[1].title_score >= best.title_score - 0.05:
        return SourceResolution(status="review", reason="multiple_confident_wayland_matches", search_url=search_url)
    reason = "unique_title_sku_match" if sku_matches else "unique_title_only_match"
    return SourceResolution(status="accepted", reason=reason, search_url=search_url, candidate=best)


def resolve_preferred_source(
    session: requests.Session,
    *,
    title: str,
    sku: str,
) -> SourceResolution:
    wayland_result: SourceResolution | None = None
    wayland_error: Exception | None = None
    try:
        wayland_result = resolve_wayland_source(session, title=title, sku=sku)
    except Exception as exc:
        wayland_error = exc
    if wayland_result is not None and wayland_result.status == "accepted" and wayland_result.candidate is not None:
        return wayland_result

    games_workshop_result: SourceResolution | None = None
    games_workshop_error: Exception | None = None
    try:
        games_workshop_result = resolve_games_workshop_source(session, title=title, sku=sku)
    except Exception as exc:
        games_workshop_error = exc
    if (
        games_workshop_result is not None
        and games_workshop_result.status == "accepted"
        and games_workshop_result.candidate is not None
    ):
        return games_workshop_result

    if wayland_result is not None and games_workshop_result is not None:
        return SourceResolution(
            status="review",
            reason="no_confident_wayland_or_games_workshop_match",
            search_url=games_workshop_result.search_url,
            source_site=games_workshop_result.source_site,
        )
    if games_workshop_result is not None:
        return SourceResolution(
            status="review",
            reason="wayland_lookup_failed_no_confident_games_workshop_match" if wayland_error else games_workshop_result.reason,
            search_url=games_workshop_result.search_url,
            source_site=games_workshop_result.source_site,
        )
    if wayland_result is not None:
        return SourceResolution(
            status="review",
            reason="games_workshop_lookup_failed_no_confident_wayland_match" if games_workshop_error else wayland_result.reason,
            search_url=wayland_result.search_url,
            source_site=wayland_result.source_site,
        )
    details = []
    if wayland_error is not None:
        details.append(f"Wayland lookup failed: {wayland_error}")
    if games_workshop_error is not None:
        details.append(f"Games Workshop lookup failed: {games_workshop_error}")
    raise RuntimeError("; ".join(details) if details else "No description source could be resolved.")


def _build_rewrite_messages(source_text: str, sku: str) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "Rewrite supplier product descriptions into original ecommerce copy. "
                "Return one concise paragraph only. Preserve factual meaning, avoid direct copying, "
                "do not invent details, and include the SKU exactly once."
            ),
        },
        {
            "role": "user",
            "content": f"SKU: {sku}\nSource description:\n{source_text}",
        },
    ]


def rewrite_with_openrouter(
    session: requests.Session,
    env: dict[str, str],
    *,
    source_text: str,
    sku: str,
) -> str:
    config = require_openrouter_config(env)
    body = {
        "model": config["model"],
        "messages": _build_rewrite_messages(source_text, sku),
    }
    headers = {
        "Authorization": f"Bearer {config['api_key']}",
        "Content-Type": "application/json",
    }
    last_error: Exception | None = None
    attempts = len(OPENROUTER_RETRY_DELAYS_SECONDS) + 1
    for attempt in range(attempts):
        try:
            response = session.post(
                OPENROUTER_URL,
                headers=headers,
                json=body,
                timeout=OPENROUTER_TIMEOUT_SECONDS,
            )
            if response.status_code in {429, 500, 502, 503, 504} and attempt < attempts - 1:
                time.sleep(OPENROUTER_RETRY_DELAYS_SECONDS[attempt])
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"OpenRouter HTTP {response.status_code}: {response.text[:500]}")
            payload = response.json()
            content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
            if isinstance(content, list):
                content = "".join(
                    part.get("text", "") for part in content if isinstance(part, dict)
                )
            rewritten = _collapse_ws(str(content))
            if not rewritten:
                raise RuntimeError("OpenRouter returned an empty completion.")
            return rewritten
        except (requests.exceptions.RequestException, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt >= attempts - 1:
                break
            time.sleep(OPENROUTER_RETRY_DELAYS_SECONDS[attempt])
    if last_error is not None:
        raise RuntimeError(str(last_error)) from last_error
    raise RuntimeError("OpenRouter rewrite failed")


def enforce_sku(text: str, sku: str) -> tuple[str, bool]:
    cleaned = _collapse_ws(text)
    if sku in cleaned:
        return cleaned, False
    if not cleaned:
        return cleaned, False
    return f"{cleaned} SKU: {sku}.", True


def sanitize_description_html(text: str) -> str:
    cleaned = _collapse_ws(text)
    if not cleaned:
        return ""
    return f"<p>{html.escape(cleaned, quote=False)}</p>"


def evaluate_rewrite(source_text: str, rewritten_text: str, sku: str) -> RewriteResult:
    repaired_text, repaired = enforce_sku(rewritten_text, sku)
    if not repaired_text:
        return RewriteResult(
            status="review",
            reason="empty_rewrite",
            source_text=source_text,
            rewritten_text=repaired_text,
            similarity=0.0,
            repaired_for_sku=repaired,
        )
    if sku not in repaired_text:
        return RewriteResult(
            status="review",
            reason="missing_sku_after_repair",
            source_text=source_text,
            rewritten_text=repaired_text,
            similarity=0.0,
            repaired_for_sku=repaired,
        )
    similarity = text_similarity(source_text, repaired_text)
    if normalize_text(source_text) == normalize_text(repaired_text):
        return RewriteResult(
            status="review",
            reason="rewrite_equals_source",
            source_text=source_text,
            rewritten_text=repaired_text,
            similarity=1.0,
            repaired_for_sku=repaired,
        )
    if contains_long_substring(source_text, repaired_text):
        return RewriteResult(
            status="review",
            reason="rewrite_contains_long_source_substring",
            source_text=source_text,
            rewritten_text=repaired_text,
            similarity=similarity,
            repaired_for_sku=repaired,
        )
    if similarity > DESCRIPTION_BACKFILL_SIMILARITY_THRESHOLD:
        return RewriteResult(
            status="review",
            reason="rewrite_similarity_too_high",
            source_text=source_text,
            rewritten_text=repaired_text,
            similarity=similarity,
            repaired_for_sku=repaired,
        )
    return RewriteResult(
        status="accepted",
        reason="rewrite_passed",
        source_text=source_text,
        rewritten_text=repaired_text,
        similarity=similarity,
        repaired_for_sku=repaired,
    )
