"""Search providers for `monitor_tematico`.

The default provider intentionally uses only the Python standard library. It is
small and replaceable; tests should inject a fake provider instead of using the
network.

Provider implementations return normalized `SearchResult` objects. The agent
does the classification and prioritization later, so providers stay focused on
retrieval only.
"""

from __future__ import annotations

from datetime import date
from html import unescape
from html.parser import HTMLParser
import hashlib
import json
from pathlib import Path
from typing import Protocol
from urllib.parse import quote_plus, urlparse, parse_qs, unquote
from urllib.request import Request, urlopen

from src.agents.monitor_tematico._types import SearchResult


class SearchProvider(Protocol):
    """Protocol implemented by search providers used by the agent.

    Keeping this as a protocol avoids hard-coding DuckDuckGo, Tavily, Exa, or
    any future source into the agent itself.
    """

    @property
    def name(self) -> str:
        """Stable provider identifier."""

    def search(
        self,
        query: str,
        *,
        start_date: date,
        end_date: date,
        max_results: int,
    ) -> tuple[SearchResult, ...]:
        """Search external context for one query."""


class SearchProviderError(RuntimeError):
    """Raised when a search provider cannot complete a request."""


class NullSearchProvider:
    """Provider that deliberately returns no results.

    Useful when running the agent without internet or when validating that the
    agent can return `partial` instead of failing.
    """

    @property
    def name(self) -> str:
        return "null"

    def search(
        self,
        query: str,
        *,
        start_date: date,
        end_date: date,
        max_results: int,
    ) -> tuple[SearchResult, ...]:
        return ()


class StaticSearchProvider:
    """Deterministic provider useful for tests and manual fixtures.

    This gives unit tests realistic search results without depending on a live
    search engine, rate limits, or changing web pages.
    """

    def __init__(self, results_by_query: dict[str, tuple[SearchResult, ...]] | None = None) -> None:
        self._results_by_query = results_by_query or {}

    @property
    def name(self) -> str:
        return "static"

    def search(
        self,
        query: str,
        *,
        start_date: date,
        end_date: date,
        max_results: int,
    ) -> tuple[SearchResult, ...]:
        direct = self._results_by_query.get(query)
        if direct is not None:
            return direct[:max_results]

        matching: list[SearchResult] = []
        normalized_query = query.lower()
        for key, results in self._results_by_query.items():
            if key.lower() in normalized_query or normalized_query in key.lower():
                matching.extend(results)
        return tuple(matching[:max_results])


class DuckDuckGoHtmlSearchProvider:
    """Small DuckDuckGo HTML search provider without paid APIs.

    This is a pragmatic v1 provider. It is intentionally isolated here because
    HTML search pages can change; if that happens, only this provider should need
    replacement.
    """

    def __init__(self, *, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = timeout_seconds

    @property
    def name(self) -> str:
        return "duckduckgo_html"

    def search(
        self,
        query: str,
        *,
        start_date: date,
        end_date: date,
        max_results: int,
    ) -> tuple[SearchResult, ...]:
        dated_query = f"{query} after:{start_date.isoformat()} before:{end_date.isoformat()}"
        url = f"https://duckduckgo.com/html/?q={quote_plus(dated_query)}"
        request = Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (compatible; ML_finance/1.0; "
                    "+https://github.com/hugosanz28/ML_finance)"
                )
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw_html = response.read().decode("utf-8", errors="replace")
        except OSError as exc:
            raise SearchProviderError(f"{self.name} failed for query {query!r}: {exc}") from exc

        parser = _DuckDuckGoHtmlParser(query=query)
        parser.feed(raw_html)
        return parser.results[:max_results]


class CachedSearchProvider:
    """Wrap another search provider and persist results on disk.

    Cache entries are keyed by provider name, query, date window, and result
    limit. This keeps repeated local runs cheap and makes the thematic monitor
    more reproducible.
    """

    def __init__(self, provider: SearchProvider, *, cache_dir: str | Path) -> None:
        self.provider = provider
        self.cache_dir = Path(cache_dir).expanduser().resolve()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    @property
    def name(self) -> str:
        return f"cached:{self.provider.name}"

    def search(
        self,
        query: str,
        *,
        start_date: date,
        end_date: date,
        max_results: int,
    ) -> tuple[SearchResult, ...]:
        cache_path = self._cache_path(
            query=query,
            start_date=start_date,
            end_date=end_date,
            max_results=max_results,
        )
        if cache_path.is_file():
            return _load_cached_results(cache_path)

        results = self.provider.search(
            query,
            start_date=start_date,
            end_date=end_date,
            max_results=max_results,
        )
        _store_cached_results(cache_path, results)
        return results

    def _cache_path(
        self,
        *,
        query: str,
        start_date: date,
        end_date: date,
        max_results: int,
    ) -> Path:
        key = json.dumps(
            {
                "provider": self.provider.name,
                "query": query,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "max_results": max_results,
            },
            sort_keys=True,
        )
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.cache_dir / f"{digest}.json"


class _DuckDuckGoHtmlParser(HTMLParser):
    """Best-effort parser for DuckDuckGo's simple HTML results page.

    The parser only extracts title, URL, and snippet. It does not try to scrape
    full articles, which keeps the provider light and reduces fragility.
    """

    def __init__(self, *, query: str) -> None:
        super().__init__()
        self.query = query
        self.results: list[SearchResult] = []
        self._in_result_link = False
        self._in_snippet = False
        self._current_href: str | None = None
        self._current_title: list[str] = []
        self._pending_result_index: int | None = None
        self._snippet_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_map = {key: value or "" for key, value in attrs}
        class_attr = attrs_map.get("class", "")
        if tag == "a" and "result__a" in class_attr:
            self._in_result_link = True
            self._current_href = attrs_map.get("href")
            self._current_title = []
        elif tag in {"a", "div"} and "result__snippet" in class_attr:
            self._in_snippet = True
            self._snippet_parts = []
            self._pending_result_index = len(self.results) - 1 if self.results else None

    def handle_data(self, data: str) -> None:
        if self._in_result_link:
            self._current_title.append(data)
        elif self._in_snippet:
            self._snippet_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._in_result_link:
            title = _clean_text(" ".join(self._current_title))
            url = _normalize_duckduckgo_url(self._current_href or "")
            if title and url:
                self.results.append(SearchResult(title=title, url=url, query=self.query))
            self._in_result_link = False
            self._current_href = None
            self._current_title = []
        elif tag in {"a", "div"} and self._in_snippet:
            snippet = _clean_text(" ".join(self._snippet_parts))
            if snippet and self._pending_result_index is not None:
                existing = self.results[self._pending_result_index]
                self.results[self._pending_result_index] = SearchResult(
                    title=existing.title,
                    url=existing.url,
                    snippet=snippet,
                    query=existing.query,
                    published_date=existing.published_date,
                    metadata=existing.metadata,
                )
            self._in_snippet = False
            self._pending_result_index = None
            self._snippet_parts = []


def _clean_text(value: str) -> str:
    return " ".join(unescape(value).split())


def _normalize_duckduckgo_url(value: str) -> str:
    if not value:
        return ""
    parsed = urlparse(value)
    if "duckduckgo.com" in parsed.netloc and parsed.path == "/l/":
        target = parse_qs(parsed.query).get("uddg", [""])[0]
        return unquote(target)
    return value


def _store_cached_results(path: Path, results: tuple[SearchResult, ...]) -> None:
    payload = {
        "results": [
            {
                "title": result.title,
                "url": result.url,
                "snippet": result.snippet,
                "query": result.query,
                "published_date": result.published_date.isoformat() if result.published_date else None,
                "metadata": dict(result.metadata),
            }
            for result in results
        ]
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8")


def _load_cached_results(path: Path) -> tuple[SearchResult, ...]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return tuple(
        SearchResult(
            title=str(item["title"]),
            url=str(item["url"]),
            snippet=str(item.get("snippet") or ""),
            query=str(item.get("query") or ""),
            published_date=(
                date.fromisoformat(item["published_date"]) if item.get("published_date") else None
            ),
            metadata=item.get("metadata") or {},
        )
        for item in payload.get("results", [])
    )
