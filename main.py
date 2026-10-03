"""
Crossref Research Intelligence — exploratory CLI.

Implements the foundation of ROADMAP.md in a single file so the potential of the
Crossref REST API can be explored end-to-end:

    Phase 1   Crossref client (timeout, retry/backoff, rate-limit aware, cache, logging)
    Phase 2   DOI intelligence (normalize, validate, extract, normalized record)
    Phase 3   Citation engine (APA / IEEE / BibTeX / RIS locally, any CSL style remotely)
    Phase 4   Scholarly search (query fields, filters, sorting, cursor pagination, export)
    Phase 5   Metadata quality scoring & deduplication
    Phase 7   Reference graph (references, bibliographic coupling between two works)
    Phase 8   OpenAlex enrichment: forward citations, related works, open-access status (snowball)
    Phase 12+ Discovery previews: timeline, landscape, author, journal, funder

Usage:
    uv run main.py tour                       # guided showcase of everything
    uv run main.py doi 10.1016/j.eswa.2016.04.008
    uv run main.py search "multimodal rag" --from 2022-01-01 --type journal-article
    uv run main.py --help

Optional environment:
    CROSSREF_MAILTO=you@example.org   joins Crossref's "polite" pool (no account needed, higher rate limit)
    OPENALEX_API_KEY=...              free key from openalex.org settings, 10x the keyless daily budget
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import logging
import os
import random
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import httpx
from dotenv import load_dotenv
from rich import box
from rich.console import Console
from rich.markup import escape
from rich.panel import Panel
from rich.rule import Rule
from rich.table import Table
from rich.tree import Tree

load_dotenv(Path(__file__).with_name(".env"))

PROCESSOR_VERSION = "0.1.0"
API_BASE = "https://api.crossref.org"
DOI_RESOLVER = "https://doi.org"
CACHE_DIR = Path(os.getenv("CROSSREF_CACHE_DIR", ".cache/crossref"))
CACHE_TTL = int(os.getenv("CROSSREF_CACHE_TTL", str(7 * 24 * 3600)))
MAILTO = os.getenv("CROSSREF_MAILTO", "")
OPENALEX_BASE = "https://api.openalex.org"
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY", "")

DEMO_DOI = "10.1016/j.eswa.2016.04.008"
DEMO_TOPIC = "retrieval augmented generation"

console = Console()
log = logging.getLogger("crossref")


# ---------------------------------------------------------------------------
# Structured logging
# ---------------------------------------------------------------------------

class KeyValueFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, "fields", {})
        extra = " ".join(f"{k}={v}" for k, v in fields.items())
        return f"{self.formatTime(record, '%H:%M:%S')} {record.levelname.lower()} {record.getMessage()} {extra}".rstrip()


def setup_logging(verbosity: int) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(KeyValueFormatter())
    log.addHandler(handler)
    log.setLevel(logging.WARNING - 10 * min(verbosity, 2))


def kv(msg: str, level: int = logging.DEBUG, **fields: Any) -> None:
    log.log(level, msg, extra={"fields": fields})


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class CrossrefError(Exception):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.status = status


class NotFound(CrossrefError):
    pass


class BudgetExhausted(CrossrefError):
    pass


class InvalidDOI(CrossrefError):
    pass


# ---------------------------------------------------------------------------
# DOI utilities (Phase 2)
# ---------------------------------------------------------------------------

DOI_PATTERN = re.compile(r"10\.\d{4,9}/[^\s\"'<>]+", re.IGNORECASE)
DOI_PREFIXES = re.compile(r"^(https?://(dx\.)?doi\.org/|doi:\s*)", re.IGNORECASE)


def normalize_doi(value: str) -> str:
    """Turn any DOI spelling (URL, doi: prefix, mixed case) into the bare lowercase DOI."""
    doi = DOI_PREFIXES.sub("", value.strip())
    doi = doi.rstrip(".,;)]}")
    if not DOI_PATTERN.fullmatch(doi):
        raise InvalidDOI(f"Not a valid DOI: {value!r}")
    return doi.lower()


def extract_dois(text: str) -> list[str]:
    """Find every DOI mentioned in free text (reference lists, PDFs, notes), in order, deduplicated."""
    seen: dict[str, None] = {}
    for match in DOI_PATTERN.findall(text):
        try:
            seen.setdefault(normalize_doi(match), None)
        except InvalidDOI:
            continue
    return list(seen)


# ---------------------------------------------------------------------------
# Crossref client (Phase 1)
# ---------------------------------------------------------------------------

class DiskCache:
    def __init__(self, directory: Path, ttl: int, enabled: bool = True):
        self.directory = directory
        self.ttl = ttl
        self.enabled = enabled
        self.hits = 0
        self.misses = 0

    def _path(self, key: str) -> Path:
        return self.directory / f"{hashlib.sha256(key.encode()).hexdigest()}.json"

    def get(self, key: str) -> str | None:
        if not self.enabled:
            return None
        path = self._path(key)
        if path.exists() and time.time() - path.stat().st_mtime < self.ttl:
            self.hits += 1
            return path.read_text(encoding="utf-8")
        self.misses += 1
        return None

    def set(self, key: str, body: str) -> None:
        if not self.enabled:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        self._path(key).write_text(body, encoding="utf-8")


class CrossrefClient:
    """Thin, polite Crossref REST client with retry, backoff, rate-limit awareness and caching."""

    def __init__(
        self,
        mailto: str = MAILTO,
        timeout: float = 30.0,
        max_retries: int = 4,
        cache: DiskCache | None = None,
    ):
        agent = f"crossref-research-intelligence/{PROCESSOR_VERSION}"
        if mailto:
            agent += f" (mailto:{mailto})"
        self.mailto = mailto
        self.max_retries = max_retries
        self.cache = cache or DiskCache(CACHE_DIR, CACHE_TTL)
        self.http = httpx.Client(timeout=timeout, follow_redirects=True, headers={"User-Agent": agent})
        self.min_interval = 0.2
        self.last_request = 0.0
        self.requests = 0

    # -- transport ---------------------------------------------------------

    def _throttle(self) -> None:
        wait = self.last_request + self.min_interval - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self.last_request = time.monotonic()

    def _learn_rate_limit(self, response: httpx.Response) -> None:
        limit = response.headers.get("x-rate-limit-limit")
        interval = response.headers.get("x-rate-limit-interval", "1s").rstrip("s")
        if limit and interval:
            try:
                self.min_interval = float(interval) / max(int(limit), 1)
            except ValueError:
                pass

    def _request(self, url: str, params: dict | None = None, accept: str = "application/json") -> str:
        params = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        if self.mailto and url.startswith(API_BASE):
            params.setdefault("mailto", self.mailto)
        key = f"{accept} {url}?{json.dumps(params, sort_keys=True)}"
        cached = self.cache.get(key)
        if cached is not None:
            kv("cache hit", url=url)
            return cached

        host = httpx.URL(url).host
        for attempt in range(self.max_retries + 1):
            self._throttle()
            started = time.monotonic()
            try:
                response = self.http.get(url, params=params, headers={"Accept": accept})
            except httpx.TransportError as exc:
                if attempt == self.max_retries:
                    raise CrossrefError(f"Network error talking to {host}: {exc}") from exc
                self._backoff(attempt, None, reason=type(exc).__name__)
                continue

            self.requests += 1
            self._learn_rate_limit(response)
            kv(
                "request",
                url=url,
                status=response.status_code,
                ms=int((time.monotonic() - started) * 1000),
                pool=response.headers.get("x-api-pool", "-"),
            )

            if response.status_code == 200:
                self.cache.set(key, response.text)
                return response.text
            if response.status_code == 404:
                raise NotFound(f"Not found: {url}", 404)
            retry_after = response.headers.get("retry-after", "")
            if response.status_code == 429 and retry_after.isdigit() and int(retry_after) > 60:
                raise BudgetExhausted(
                    f"{host} daily budget exhausted; resets in {int(retry_after) // 3600}h{int(retry_after) % 3600 // 60:02d}m", 429
                )
            if response.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                self._backoff(attempt, response.headers.get("retry-after"), reason=str(response.status_code))
                continue
            raise CrossrefError(f"{host} returned HTTP {response.status_code}: {self._error_message(response)}", response.status_code)
        raise CrossrefError("Exhausted retries")

    def _backoff(self, attempt: int, retry_after: str | None, reason: str) -> None:
        delay = float(retry_after) if retry_after and retry_after.isdigit() else 2**attempt + random.random()
        kv("retrying", logging.WARNING, reason=reason, attempt=attempt + 1, sleep=f"{delay:.1f}s")
        time.sleep(delay)

    @staticmethod
    def _error_message(response: httpx.Response) -> str:
        try:
            message = response.json().get("message")
            if isinstance(message, list):
                return "; ".join(m.get("message", str(m)) for m in message)
            return str(message)
        except ValueError:
            return response.text[:300]

    def _json(self, path: str, params: dict | None = None) -> dict:
        return json.loads(self._request(f"{API_BASE}{path}", params))["message"]

    # -- endpoints ---------------------------------------------------------

    def work(self, doi: str) -> dict:
        return self._json(f"/works/{normalize_doi(doi)}")

    def works(
        self,
        query: str | None = None,
        filters: dict[str, str | list[str]] | None = None,
        rows: int = 20,
        offset: int | None = None,
        sort: str | None = None,
        order: str | None = None,
        select: list[str] | None = None,
        facets: list[str] | None = None,
        cursor: str | None = None,
        path: str = "/works",
        **query_fields: str | None,
    ) -> dict:
        """Search /works. query_fields maps e.g. author="..." to query.author=... ."""
        params: dict[str, Any] = {
            "query": query,
            "rows": rows,
            "offset": offset,
            "sort": sort,
            "order": order,
            "cursor": cursor,
            "filter": build_filter(filters),
            "select": ",".join(select) if select else None,
            "facet": ",".join(facets) if facets else None,
        }
        for field, value in query_fields.items():
            params[f"query.{field.replace('_', '-')}"] = value
        return self._json(path, params)

    def iter_works(self, max_results: int, page_size: int = 100, **kwargs: Any) -> Iterator[dict]:
        """Deep pagination with Crossref cursors (offset is capped at 10k by Crossref)."""
        if kwargs.get("query") and not kwargs.get("sort"):
            # cursor paging does not rank by relevance unless asked to
            kwargs["sort"] = "relevance"
        cursor, yielded = "*", 0
        while yielded < max_results:
            page = self.works(rows=min(page_size, max_results - yielded), cursor=cursor, **kwargs)
            items = page.get("items", [])
            if not items:
                return
            for item in items:
                yield item
                yielded += 1
            cursor = page.get("next-cursor")
            if not cursor:
                return

    def journal(self, issn: str) -> dict:
        return self._json(f"/journals/{issn}")

    def journals(self, query: str, rows: int = 10) -> dict:
        return self._json("/journals", {"query": query, "rows": rows})

    def funders(self, query: str, rows: int = 10) -> dict:
        return self._json("/funders", {"query": query, "rows": rows})

    def funder(self, funder_id: str) -> dict:
        return self._json(f"/funders/{funder_id}")

    def members(self, query: str, rows: int = 10) -> dict:
        return self._json("/members", {"query": query, "rows": rows})

    def prefix(self, prefix: str) -> dict:
        return self._json(f"/prefixes/{prefix}")

    def types(self) -> dict:
        return self._json("/types")

    def formatted(self, doi: str, fmt: str, style: str = "apa", locale: str = "en-US") -> str:
        """DOI content negotiation: BibTeX, RIS, CSL-JSON or any of ~10k CSL styles."""
        accept = {
            "bibtex": "application/x-bibtex",
            "ris": "application/x-research-info-systems",
            "csl": "application/vnd.citationstyles.csl+json",
        }.get(fmt, f"text/x-bibliography; style={style}; locale={locale}")
        return self._request(f"{DOI_RESOLVER}/{normalize_doi(doi)}", accept=accept).strip()


def build_filter(filters: dict[str, str | list[str]] | None) -> str | None:
    if not filters:
        return None
    parts = []
    for name, value in filters.items():
        for v in value if isinstance(value, list) else [value]:
            if v not in (None, "", False):
                parts.append(f"{name}:{str(v).lower() if isinstance(v, bool) else v}")
    return ",".join(parts) or None


SEARCH_SELECT = [
    "DOI", "title", "subtitle", "author", "container-title", "publisher", "type", "issued",
    "published-print", "published-online", "volume", "issue", "page", "ISSN", "ISBN", "URL",
    "abstract", "subject", "license", "funder", "is-referenced-by-count", "references-count",
    "score", "link", "published",
]


# ---------------------------------------------------------------------------
# Normalization (Phase 2)
# ---------------------------------------------------------------------------

def date_from_parts(node: dict | None) -> str | None:
    if not node:
        return None
    parts = (node.get("date-parts") or [[None]])[0]
    if not parts or parts[0] is None:
        return None
    return "-".join([f"{parts[0]:04d}", *[f"{p:02d}" for p in parts[1:3]]])


def clean_abstract(raw: str | None) -> str | None:
    if not raw:
        return None
    text = html.unescape(re.sub(r"<[^>]+>", " ", raw))
    text = re.sub(r"\s+", " ", text).strip()
    return re.sub(r"^(abstract|summary)\s*[:.]?\s*", "", text, flags=re.IGNORECASE) or None


def clean_orcid(value: str | None) -> str | None:
    return value.rsplit("/", 1)[-1] if value else None


def normalize_person(p: dict) -> dict:
    name = p.get("name") or " ".join(x for x in (p.get("given"), p.get("family")) if x)
    return {
        "given": p.get("given"),
        "family": p.get("family") or p.get("name"),
        "name": name,
        "orcid": clean_orcid(p.get("ORCID")),
        "affiliations": [a["name"] for a in p.get("affiliation", []) if a.get("name")],
        "sequence": p.get("sequence"),
    }


def first(values: list | None) -> str | None:
    return values[0] if values else None


def normalize_work(msg: dict) -> dict:
    """Crossref message -> canonical record (ROADMAP Phase 2 schema, plus provenance)."""
    published = (
        date_from_parts(msg.get("published"))
        or date_from_parts(msg.get("issued"))
        or date_from_parts(msg.get("published-print"))
        or date_from_parts(msg.get("published-online"))
    )
    return {
        "doi": msg.get("DOI", "").lower(),
        "title": clean_title(first(msg.get("title"))),
        "subtitle": first(msg.get("subtitle")),
        "authors": [normalize_person(a) for a in msg.get("author", [])],
        "editors": [normalize_person(e) for e in msg.get("editor", [])],
        "publisher": msg.get("publisher"),
        "journal": first(msg.get("container-title")),
        "journal_short": first(msg.get("short-container-title")),
        "issn": msg.get("ISSN", []),
        "isbn": msg.get("ISBN", []),
        "type": msg.get("type"),
        "publication_date": published,
        "year": int(published[:4]) if published else None,
        "online_date": date_from_parts(msg.get("published-online")),
        "print_date": date_from_parts(msg.get("published-print")),
        "volume": msg.get("volume"),
        "issue": msg.get("issue"),
        "pages": msg.get("page"),
        "url": msg.get("URL"),
        "abstract": clean_abstract(msg.get("abstract")),
        "language": msg.get("language"),
        "subjects": msg.get("subject", []),
        "references": [
            {
                "doi": r.get("DOI", "").lower() or None,
                "title": r.get("article-title") or r.get("volume-title"),
                "author": r.get("author"),
                "year": r.get("year"),
                "journal": r.get("journal-title"),
                "unstructured": r.get("unstructured"),
            }
            for r in msg.get("reference", [])
        ],
        "reference_count": msg.get("references-count", msg.get("reference-count", 0)),
        "cited_by_count": msg.get("is-referenced-by-count", 0),
        "license": [
            {"url": lic.get("URL"), "start": date_from_parts(lic.get("start")), "content_version": lic.get("content-version")}
            for lic in msg.get("license", [])
        ],
        "funding": [
            {"name": f.get("name"), "doi": f.get("DOI"), "awards": f.get("award", [])} for f in msg.get("funder", [])
        ],
        "full_text_links": [
            {"url": link.get("URL"), "content_type": link.get("content-type"), "intended": link.get("intended-application")}
            for link in msg.get("link", [])
        ],
        "updates": [
            {"type": u.get("type"), "doi": u.get("DOI"), "date": date_from_parts(u.get("updated"))}
            for u in msg.get("update-to", []) + msg.get("updated-by", [])
        ],
        "relation": msg.get("relation", {}),
        "score": msg.get("score"),
        "provenance": {
            "source": "crossref",
            "retrieved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "processor_version": PROCESSOR_VERSION,
        },
    }


def clean_title(title: str | None) -> str | None:
    if not title:
        return None
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", title))).strip()


# ---------------------------------------------------------------------------
# Metadata quality & deduplication (Phase 5)
# ---------------------------------------------------------------------------

QUALITY_CHECKS: dict[str, dict[str, Any]] = {
    "identifier": {
        "doi": lambda w: bool(w["doi"]),
        "issn_or_isbn": lambda w: bool(w["issn"] or w["isbn"]),
        "orcid": lambda w: any(a["orcid"] for a in w["authors"]),
    },
    "author": {
        "authors": lambda w: bool(w["authors"]),
        "family_names": lambda w: bool(w["authors"]) and all(a["family"] for a in w["authors"]),
        "affiliations": lambda w: any(a["affiliations"] for a in w["authors"]),
    },
    "publication": {
        "title": lambda w: bool(w["title"]),
        "journal": lambda w: bool(w["journal"]),
        "publisher": lambda w: bool(w["publisher"]),
        "publication_date": lambda w: bool(w["publication_date"]) and len(w["publication_date"]) >= 7,
        "volume": lambda w: bool(w["volume"]),
        "pages": lambda w: bool(w["pages"]),
    },
    "metadata": {
        "abstract": lambda w: bool(w["abstract"]),
        "references": lambda w: bool(w["references"]),
        "license": lambda w: bool(w["license"]),
        "funding": lambda w: bool(w["funding"]),
    },
}


def metadata_quality(work: dict) -> dict:
    """Completeness of the metadata record. NOT a measure of scientific quality."""
    groups, missing = {}, []
    for group, checks in QUALITY_CHECKS.items():
        passed = 0
        for name, check in checks.items():
            if check(work):
                passed += 1
            else:
                missing.append(name)
        groups[f"{group}_completeness"] = round(passed / len(checks), 2)
    return {"metadata_quality": round(sum(groups.values()) / len(groups), 2), **groups, "missing": missing}


def dedup_key(work: dict) -> str:
    if work.get("doi"):
        return f"doi:{work['doi']}"
    title = re.sub(r"[^a-z0-9]+", "", (work.get("title") or "").lower())
    author = (work["authors"][0]["family"] or "").lower() if work.get("authors") else ""
    return f"tay:{title}|{author}|{work.get('year')}"


def deduplicate(works: list[dict]) -> list[dict]:
    seen: dict[str, dict] = {}
    for w in works:
        seen.setdefault(dedup_key(w), w)
    return list(seen.values())


# ---------------------------------------------------------------------------
# Citation engine (Phase 3)
# ---------------------------------------------------------------------------

LOCAL_STYLES = ("apa", "ieee", "bibtex", "ris")
REMOTE_STYLES = {
    "vancouver": "elsevier-vancouver",
    "chicago": "chicago-author-date",
    "mla": "modern-language-association",
    "harvard": "harvard-cite-them-right",
}


def initials(given: str | None) -> str:
    if not given:
        return ""
    parts = re.split(r"[\s.]+", given.strip())
    return " ".join("-".join(f"{s[0]}." for s in part.split("-") if s) for part in parts if part)


def cite_apa(w: dict) -> str:
    names = [f"{a['family']}, {initials(a['given'])}".rstrip(", ") for a in w["authors"]]
    if len(names) > 20:
        authors = ", ".join(names[:19]) + ", ... " + names[-1]
    elif len(names) > 1:
        authors = ", ".join(names[:-1]) + ", & " + names[-1]
    else:
        authors = names[0] if names else (w["journal"] or "Anonymous")
    out = f"{authors} ({w['year'] or 'n.d.'}). {w['title']}."
    if w["journal"]:
        out += f" {w['journal']}"
        if w["volume"]:
            out += f", {w['volume']}"
            if w["issue"]:
                out += f"({w['issue']})"
        if w["pages"]:
            out += f", {w['pages'].replace('-', '–')}"
        out += "."
    return out + f" https://doi.org/{w['doi']}"


def cite_ieee(w: dict) -> str:
    names = [f"{initials(a['given'])} {a['family']}".strip() for a in w["authors"]]
    if len(names) > 6:
        authors = f"{names[0]} et al."
    elif len(names) > 2:
        authors = ", ".join(names[:-1]) + ", and " + names[-1]
    else:
        authors = " and ".join(names)
    parts = [f'{authors}, "{w["title"]},"' if authors else f'"{w["title"]},"']
    if w["journal"]:
        parts.append(f"{w['journal']},")
    if w["volume"]:
        parts.append(f"vol. {w['volume']},")
    if w["issue"]:
        parts.append(f"no. {w['issue']},")
    if w["pages"]:
        parts.append(f"pp. {w['pages'].replace('-', '–')},")
    parts.append(f"{w['year'] or 'n.d.'}, doi: {w['doi']}.")
    return " ".join(parts)


def bibtex_key(w: dict) -> str:
    family = re.sub(r"[^a-z]", "", (w["authors"][0]["family"] or "").lower()) if w["authors"] else "anon"
    word = next((t for t in re.findall(r"[a-z]+", (w["title"] or "").lower()) if len(t) > 3), "work")
    return f"{family}{w['year'] or ''}{word}"


def cite_bibtex(w: dict) -> str:
    entry = {"journal-article": "article", "proceedings-article": "inproceedings", "book-chapter": "incollection", "book": "book"}.get(w["type"], "misc")
    fields = {
        "title": w["title"],
        "author": " and ".join(f"{a['family']}, {a['given']}" if a["given"] else a["family"] for a in w["authors"]),
        "journal" if entry == "article" else "booktitle": w["journal"],
        "publisher": w["publisher"],
        "year": w["year"],
        "volume": w["volume"],
        "number": w["issue"],
        "pages": w["pages"].replace("-", "--") if w["pages"] else None,
        "doi": w["doi"],
        "url": w["url"],
    }
    body = ",\n".join(f"  {k} = {{{v}}}" for k, v in fields.items() if v)
    return f"@{entry}{{{bibtex_key(w)},\n{body}\n}}"


def cite_ris(w: dict) -> str:
    ty = {"journal-article": "JOUR", "proceedings-article": "CPAPER", "book-chapter": "CHAP", "book": "BOOK"}.get(w["type"], "GEN")
    lines = [f"TY  - {ty}", f"TI  - {w['title']}"]
    lines += [f"AU  - {a['family']}, {a['given']}" if a["given"] else f"AU  - {a['family']}" for a in w["authors"]]
    start, _, end = (w["pages"] or "").partition("-")
    for tag, value in (("T2", w["journal"]), ("PB", w["publisher"]), ("PY", w["year"]), ("VL", w["volume"]),
                       ("IS", w["issue"]), ("SP", start), ("EP", end), ("DO", w["doi"]), ("UR", w["url"]),
                       ("AB", w["abstract"])):
        if value:
            lines.append(f"{tag}  - {value}")
    for issn in w["issn"]:
        lines.append(f"SN  - {issn}")
    return "\n".join(lines + ["ER  - "])


def cite(client: CrossrefClient, work: dict, style: str) -> str:
    style = style.lower()
    local = {"apa": cite_apa, "ieee": cite_ieee, "bibtex": cite_bibtex, "ris": cite_ris}
    if style in local:
        return local[style](work)
    if style in ("csl", "csl-json"):
        return client.formatted(work["doi"], "csl")
    text = client.formatted(work["doi"], "text", style=REMOTE_STYLES.get(style, style))
    return re.sub(r"^(\[\d+\]|\d+\.)\s*", "", text)


# ---------------------------------------------------------------------------
# Analyses (Phase 7, 12–15 previews)
# ---------------------------------------------------------------------------

def facet_counts(client: CrossrefClient, facet: str, limit: int, query: str | None = None,
                 filters: dict | None = None, **query_fields: str | None) -> tuple[int, dict[str, int]]:
    msg = client.works(query=query, filters=filters, rows=0, facets=[f"{facet}:{limit}"], **query_fields)
    values = msg.get("facets", {}).get(facet, {}).get("values", {})
    return msg.get("total-results", 0), values


def topic_timeline(client: CrossrefClient, topic: str, start: int, end: int, filters: dict | None = None) -> tuple[int, dict[int, int]]:
    total, values = facet_counts(client, "published", 1000, query=topic, filters=filters)
    years = {int(y): c for y, c in values.items() if y.isdigit() and start <= int(y) <= end}
    return total, dict(sorted(years.items()))


def tokens(text: str | None) -> list[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower())


def contains_phrase(text: str | None, phrase: str) -> bool:
    hay, needle = tokens(text), tokens(phrase)
    return bool(needle) and any(hay[i : i + len(needle)] == needle for i in range(len(hay) - len(needle) + 1))


def phrase_sample(client: CrossrefClient, topic: str, scan: int, filters: dict | None = None) -> tuple[int, list[dict]]:
    """Crossref has no phrase search: scan the top-N relevance hits and keep titles containing the exact phrase."""
    scanned, matched = 0, []
    select = ["DOI", "title", "subtitle", "author", "container-title", "publisher", "type", "issued", "published",
              "funder", "license", "is-referenced-by-count", "references-count"]
    for item in client.iter_works(scan, query=topic, filters=filters, select=select, sort="relevance"):
        scanned += 1
        w = normalize_work(item)
        if contains_phrase(f"{w['title']} {w['subtitle'] or ''}", topic):
            matched.append(w)
    return scanned, deduplicate(matched)


def top_work_for_year(client: CrossrefClient, topic: str, year: int, filters: dict | None = None) -> dict | None:
    f = {**(filters or {}), "from-pub-date": f"{year}-01-01", "until-pub-date": f"{year}-12-31"}
    items = client.works(query=topic, filters=f, rows=1, select=SEARCH_SELECT).get("items", [])
    return normalize_work(items[0]) if items else None


def author_matches(author: dict, target: str) -> bool:
    tokens = [t for t in re.findall(r"[a-z]+", target.lower()) if t]
    name = f"{author.get('given') or ''} {author.get('family') or ''}".lower()
    family = (author.get("family") or "").lower()
    return bool(tokens) and tokens[-1] in family and all(t[0] in name or t in name for t in tokens[:-1])


def author_profile(client: CrossrefClient, name: str, max_works: int = 200, orcid: str | None = None) -> dict:
    if orcid:
        raw = list(client.iter_works(max_works, filters={"orcid": orcid}, select=SEARCH_SELECT))
    else:
        raw = list(client.iter_works(max_works, author=name, select=SEARCH_SELECT, sort="relevance"))
    works, seen = [], set()
    for item in raw:
        w = normalize_work(item)
        me = next((a for a in w["authors"] if (orcid and a["orcid"] == orcid) or (not orcid and author_matches(a, name))), None)
        if me and dedup_key(w) not in seen:
            seen.add(dedup_key(w))
            works.append((w, me))
    coauthors, journals, years, orcids, affiliations = Counter(), Counter(), Counter(), Counter(), Counter()
    for w, me in works:
        coauthors.update(a["name"] for a in w["authors"] if a is not me and a["name"])
        if w["journal"]:
            journals[w["journal"]] += 1
        if w["year"]:
            years[w["year"]] += 1
        if me["orcid"]:
            orcids[me["orcid"]] += 1
        affiliations.update(me["affiliations"])
    return {
        "query": name,
        "orcid_filter": orcid,
        "scanned": len(raw),
        "matched": len(works),
        "works": [w for w, _ in works],
        "coauthors": coauthors.most_common(10),
        "journals": journals.most_common(8),
        "years": dict(sorted(years.items())),
        "orcids": orcids.most_common(3),
        "affiliations": affiliations.most_common(5),
        "total_citations": sum(w["cited_by_count"] for w, _ in works),
    }


def resolve_references(client: CrossrefClient, work: dict, limit: int) -> list[dict]:
    resolved = []
    for ref in [r for r in work["references"] if r["doi"]][:limit]:
        try:
            resolved.append(normalize_work(client.work(ref["doi"])))
        except CrossrefError as exc:
            kv("reference lookup failed", logging.INFO, doi=ref["doi"], error=exc)
    return resolved


# ---------------------------------------------------------------------------
# OpenAlex enrichment (Phase 8): forward citations, related works, OA status
# ---------------------------------------------------------------------------

class OpenAlexClient:
    """OpenAlex works API. Keyless works; a free key (openalex.org settings) gives 10x the daily budget.

    Single-record lookups are free; list/filter calls cost one credit each, so batch ids with `|`.
    """

    def __init__(self, transport: CrossrefClient, api_key: str | None = None, mailto: str | None = None):
        self.transport = transport
        self.api_key = OPENALEX_API_KEY if api_key is None else api_key
        self.mailto = MAILTO if mailto is None else mailto

    @property
    def mode(self) -> str:
        return "api-key" if self.api_key else "keyless (free)"

    def _auth(self) -> dict:
        params = {"mailto": self.mailto} if self.mailto else {}
        return {**params, "api_key": self.api_key} if self.api_key else params

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{OPENALEX_BASE}{path}"
        try:
            return json.loads(self.transport._request(url, {**(params or {}), **self._auth()}))
        except CrossrefError as exc:
            if self.api_key and exc.status in (401, 403):
                kv("openalex api key rejected, falling back to keyless", logging.WARNING, status=exc.status)
                self.api_key = ""
                return json.loads(self.transport._request(url, {**(params or {}), **self._auth()}))
            raise

    def budget(self) -> dict:
        """Remaining daily budget, read from the headers of a free single-record lookup (never cached)."""
        response = self.transport.http.get(f"{OPENALEX_BASE}/works/doi:{DEMO_DOI}", params={**self._auth(), "select": "id"})
        if self.api_key and response.status_code in (401, 403):
            self.api_key = ""
            return {"status": response.status_code, "error": "api key rejected — using keyless mode"}
        h = response.headers
        return {
            "status": response.status_code,
            "limit_usd": h.get("x-ratelimit-limit-usd"),
            "remaining_usd": h.get("x-ratelimit-remaining-usd"),
            "remaining_credits": h.get("x-ratelimit-remaining"),
            "resets_in_s": h.get("x-ratelimit-reset"),
        }

    def work_by_doi(self, doi: str) -> dict:
        return self._get(f"/works/doi:{normalize_doi(doi)}")

    def list_works(self, filter_: str, per_page: int = 25, sort: str | None = None) -> dict:
        return self._get("/works", {"filter": filter_, "per_page": per_page, "sort": sort})

    def works_by_ids(self, ids: list[str], per_page: int = 100) -> list[dict]:
        out = []
        for i in range(0, len(ids), 100):
            chunk = [x.rsplit("/", 1)[-1] for x in ids[i : i + 100]]
            out += self.list_works("openalex:" + "|".join(chunk), per_page=min(per_page, 100)).get("results", [])
        return out

    def citing(self, openalex_id: str, rows: int = 25) -> dict:
        return self.list_works(f"cites:{openalex_id.rsplit('/', 1)[-1]}", per_page=rows, sort="cited_by_count:desc")


def inverted_index_to_text(index: dict[str, list[int]] | None) -> str | None:
    if not index:
        return None
    positions = sorted((pos, word) for word, poss in index.items() for pos in poss)
    return " ".join(word for _, word in positions)


def normalize_openalex(item: dict) -> dict:
    """OpenAlex work -> the same canonical shape as normalize_work, with provenance=openalex."""
    work = normalize_work({})
    source = ((item.get("primary_location") or {}).get("source") or {})
    biblio = item.get("biblio") or {}
    pages = "-".join(p for p in (biblio.get("first_page"), biblio.get("last_page")) if p) or None
    oa = item.get("open_access") or {}
    work.update({
        "doi": (item.get("doi") or "").replace("https://doi.org/", "").lower(),
        "openalex_id": item.get("id"),
        "title": clean_title(item.get("title")),
        "authors": [
            {
                "given": None,
                "family": (a.get("author", {}).get("display_name") or "?").split()[-1],
                "name": a.get("author", {}).get("display_name"),
                "orcid": clean_orcid(a.get("author", {}).get("orcid")),
                "affiliations": [i.get("display_name") for i in a.get("institutions", []) if i.get("display_name")],
                "sequence": a.get("author_position"),
            }
            for a in item.get("authorships", [])
        ],
        "journal": source.get("display_name"),
        "publisher": source.get("host_organization_name"),
        "issn": source.get("issn") or [],
        "type": item.get("type"),
        "publication_date": item.get("publication_date"),
        "year": item.get("publication_year"),
        "volume": biblio.get("volume"),
        "issue": biblio.get("issue"),
        "pages": pages,
        "url": item.get("doi") or item.get("id"),
        "abstract": inverted_index_to_text(item.get("abstract_inverted_index")),
        "subjects": [c.get("display_name") for c in item.get("topics", [])[:3]],
        "reference_count": len(item.get("referenced_works", [])),
        "cited_by_count": item.get("cited_by_count", 0),
        "open_access": {"is_oa": oa.get("is_oa"), "status": oa.get("oa_status"), "url": oa.get("oa_url")},
        "provenance": {**work["provenance"], "source": "openalex"},
    })
    return work


def snowball(client: CrossrefClient, openalex: OpenAlexClient, doi: str, rows: int) -> dict:
    """Backward (references), forward (citing works) and related works around one seed DOI."""
    seed = normalize_work(client.work(doi))
    oa_seed = openalex.work_by_doi(doi)
    backward_ids = oa_seed.get("referenced_works", [])
    backward = [normalize_openalex(w) for w in openalex.works_by_ids(backward_ids)] if backward_ids else []
    # Crossref sometimes lists references OpenAlex could not match; keep both, DOI-deduplicated.
    known = {w["doi"] for w in backward}
    crossref_only = [r["doi"] for r in seed["references"] if r["doi"] and r["doi"] not in known]
    citing_page = openalex.citing(oa_seed["id"], rows)
    forward = [normalize_openalex(w) for w in citing_page.get("results", [])]
    related_ids = oa_seed.get("related_works", [])
    related = [normalize_openalex(w) for w in openalex.works_by_ids(related_ids)] if related_ids else []

    sort = lambda ws: sorted(ws, key=lambda w: -w["cited_by_count"])
    merged: dict[str, dict] = {}
    for via, works in (("backward", backward), ("forward", forward), ("related", related)):
        for w in works:
            merged.setdefault(dedup_key(w), {**w, "found_via": []})["found_via"].append(via)
    return {
        "seed": {**seed, "open_access": normalize_openalex(oa_seed)["open_access"], "openalex_id": oa_seed.get("id"),
                 "abstract": seed["abstract"] or inverted_index_to_text(oa_seed.get("abstract_inverted_index"))},
        "backward": sort(backward),
        "crossref_only_references": crossref_only,
        "forward": forward,
        "forward_total": citing_page.get("meta", {}).get("count", 0),
        "related": sort(related),
        "merged": sort(merged.values()),
    }


def bibliographic_coupling(a: dict, b: dict) -> dict:
    refs_a = {r["doi"] for r in a["references"] if r["doi"]}
    refs_b = {r["doi"] for r in b["references"] if r["doi"]}
    shared = refs_a & refs_b
    union = refs_a | refs_b
    return {
        "shared": sorted(shared),
        "jaccard": round(len(shared) / len(union), 3) if union else 0.0,
        "a_cites_b": b["doi"] in refs_a,
        "b_cites_a": a["doi"] in refs_b,
        "refs_with_doi": (len(refs_a), len(refs_b)),
    }


def related_works(client: CrossrefClient, work: dict, rows: int = 10) -> list[dict]:
    """Bibliographic similarity via Crossref relevance on title + subjects (no citation index needed)."""
    query = " ".join(filter(None, [work["title"], work["subtitle"], *work["subjects"][:3]]))
    items = client.works(bibliographic=query, rows=rows + 5, select=SEARCH_SELECT).get("items", [])
    out = [normalize_work(i) for i in items]
    return [w for w in deduplicate(out) if w["doi"] != work["doi"] and w["title"] != work["title"]][:rows]


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def short_authors(w: dict, n: int = 3) -> str:
    names = [a["family"] or a["name"] or "?" for a in w["authors"]]
    return ", ".join(names[:n]) + (" et al." if len(names) > n else "") if names else "—"


def trunc(text: str | None, n: int) -> str:
    text = text or "—"
    return text if len(text) <= n else text[: n - 1] + "…"


def bar(value: int, maximum: int, width: int = 40) -> str:
    filled = int(round(width * value / maximum)) if maximum else 0
    return "█" * filled + "░" * (width - filled)


def show_work(w: dict, quality: bool = True) -> None:
    t = Table.grid(padding=(0, 2))
    t.add_column(style="bold cyan", no_wrap=True)
    t.add_column()
    authors = "; ".join(
        f"{a['name']}" + (f" [green](ORCID {a['orcid']})[/]" if a["orcid"] else "") for a in w["authors"][:8]
    )
    if len(w["authors"]) > 8:
        authors += f"; … (+{len(w['authors']) - 8})"
    affiliations = sorted({aff for a in w["authors"] for aff in a["affiliations"]})
    rows = [
        ("DOI", escape(w["doi"])),
        ("Type", w["type"]),
        ("Authors", authors or "—"),
        ("Affiliations", escape("; ".join(affiliations[:4])) if affiliations else None),
        ("Journal", escape(w["journal"] or "—") + (f"  [dim]vol {w['volume']}[/]" if w["volume"] else "")
         + (f"[dim] ({w['issue']})[/]" if w["issue"] else "") + (f"[dim] pp {w['pages']}[/]" if w["pages"] else "")),
        ("Publisher", escape(w["publisher"] or "—")),
        ("ISSN / ISBN", ", ".join(w["issn"] + w["isbn"]) or None),
        ("Published", w["publication_date"]),
        ("Cited by", f"{w['cited_by_count']:,} works (Crossref-registered citations)"),
        ("References", f"{w['reference_count']} ({sum(1 for r in w['references'] if r['doi'])} with DOI, deposited: {len(w['references'])})"),
        ("Subjects", ", ".join(w["subjects"]) or None),
        ("License", ", ".join(sorted({l['url'] for l in w["license"] if l["url"]})) or None),
        ("Funding", escape("; ".join(f["name"] + (f" ({', '.join(f['awards'][:2])})" if f["awards"] else "") for f in w["funding"])) or None),
        ("Full-text links", str(len(w["full_text_links"])) + " link(s): " + ", ".join(sorted({l["content_type"] or "?" for l in w["full_text_links"]})) if w["full_text_links"] else None),
        ("Updates", ", ".join(f"{u['type']} → {u['doi']}" for u in w["updates"]) or None),
        ("Abstract", escape(trunc(w["abstract"], 600)) if w["abstract"] else None),
    ]
    for label, value in rows:
        if value:
            t.add_row(label, value)
    if quality:
        q = metadata_quality(w)
        t.add_row(
            "Metadata quality",
            f"[bold]{q['metadata_quality']:.0%}[/]  "
            + "  ".join(f"{k.split('_')[0]} {v:.0%}" for k, v in q.items() if k.endswith("_completeness"))
            + (f"\n[dim]missing: {', '.join(q['missing'])}[/]" if q["missing"] else ""),
        )
    console.print(Panel(t, title=f"[bold]{escape(w['title'] or 'Untitled')}[/]", title_align="left", border_style="cyan"))


def works_table(works: list[dict], title: str = "", show_score: bool = False) -> Table:
    t = Table(title=title, box=box.SIMPLE_HEAVY, title_justify="left", expand=True)
    t.add_column("#", justify="right", style="dim", width=3)
    t.add_column("Year", width=4)
    t.add_column("Title", ratio=5)
    t.add_column("Authors", ratio=2)
    t.add_column("Venue", ratio=2, style="dim")
    t.add_column("Cited", justify="right", width=6)
    t.add_column("DOI", ratio=2, style="cyan", overflow="fold")
    if show_score:
        t.add_column("Score", justify="right", width=6, style="dim")
    for i, w in enumerate(works, 1):
        row = [str(i), str(w["year"] or "—"), escape(trunc(w["title"], 110)), escape(short_authors(w)),
               escape(trunc(w["journal"] or w["publisher"], 40)), f"{w['cited_by_count']:,}", w["doi"]]
        if show_score:
            row.append(f"{w['score']:.1f}" if w.get("score") else "")
        t.add_row(*row)
    return t


def counts_table(title: str, counts: list[tuple[str, int]] | dict, label: str = "Value", width: int = 30) -> Table:
    items = list(counts.items()) if isinstance(counts, dict) else counts
    t = Table(title=title, box=box.SIMPLE, title_justify="left", show_header=True)
    t.add_column(label)
    t.add_column("Count", justify="right")
    t.add_column("", style="magenta")
    peak = max((c for _, c in items), default=0)
    for name, count in items:
        t.add_row(escape(trunc(str(name), 60)), f"{count:,}", bar(count, peak, width))
    return t


def emit_json(data: Any) -> None:
    console.print_json(json.dumps(data, ensure_ascii=False, default=str))


def export(works: list[dict], path: Path) -> None:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        path.write_text("\n".join(json.dumps(w, ensure_ascii=False) for w in works) + "\n", encoding="utf-8")
    elif suffix == ".json":
        path.write_text(json.dumps(works, ensure_ascii=False, indent=2), encoding="utf-8")
    elif suffix == ".bib":
        path.write_text("\n\n".join(cite_bibtex(w) for w in works) + "\n", encoding="utf-8")
    elif suffix == ".ris":
        path.write_text("\n\n".join(cite_ris(w) for w in works) + "\n", encoding="utf-8")
    elif suffix == ".csv":
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["doi", "title", "authors", "year", "journal", "publisher", "type", "cited_by", "references", "metadata_quality", "url"])
            for w in works:
                writer.writerow([w["doi"], w["title"], "; ".join(a["name"] for a in w["authors"]), w["year"], w["journal"],
                                 w["publisher"], w["type"], w["cited_by_count"], w["reference_count"],
                                 metadata_quality(w)["metadata_quality"], w["url"]])
    else:
        raise SystemExit(f"Unsupported export format {suffix!r} (use .csv .json .jsonl .bib .ris)")
    console.print(f"[green]Exported {len(works)} works → {path}[/]")


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

@dataclass
class Ctx:
    client: CrossrefClient
    as_json: bool


def cmd_doi(ctx: Ctx, args: argparse.Namespace) -> None:
    work = normalize_work(ctx.client.work(args.doi))
    if ctx.as_json:
        emit_json({**work, "quality": metadata_quality(work)})
    else:
        show_work(work)


def search_filters(args: argparse.Namespace) -> dict:
    return {
        "from-pub-date": args.from_date,
        "until-pub-date": args.until,
        "type": args.type,
        "issn": args.issn,
        "has-abstract": args.has_abstract,
        "has-orcid": args.has_orcid,
        "has-references": args.has_references,
        "has-license": args.has_license,
        "has-funder": args.has_funder,
        "has-full-text": args.has_full_text,
        "orcid": args.orcid,
    }


def cmd_search(ctx: Ctx, args: argparse.Namespace) -> None:
    kwargs = dict(
        query=" ".join(args.query) or None,
        filters=search_filters(args),
        sort=args.sort,
        order=args.order,
        select=SEARCH_SELECT,
        title=args.title,
        author=args.author,
        bibliographic=args.bibliographic,
        container_title=args.journal,
        publisher_name=args.publisher,
        affiliation=args.affiliation,
    )
    if args.rows > 1000 or args.out:
        items = list(ctx.client.iter_works(args.rows, **kwargs))
        total = None
    else:
        page = ctx.client.works(rows=args.rows, **kwargs)
        items, total = page.get("items", []), page.get("total-results")
    works = deduplicate([normalize_work(i) for i in items])
    if args.min_quality:
        works = [w for w in works if metadata_quality(w)["metadata_quality"] >= args.min_quality]
    if args.out:
        export(works, Path(args.out))
    if ctx.as_json:
        emit_json({"total_results": total, "items": works})
        return
    head = f"{total:,} matches in Crossref — showing {len(works)}" if total is not None else f"{len(works)} works"
    console.print(works_table(works, title=head, show_score=args.sort in (None, "relevance")))


def cmd_cite(ctx: Ctx, args: argparse.Namespace) -> None:
    styles = args.style or ["apa", "ieee", "bibtex"]
    for raw in args.dois:
        work = normalize_work(ctx.client.work(raw))
        for style in styles:
            text = cite(ctx.client, work, style)
            if ctx.as_json or len(args.dois) > 1 or len(styles) == 1:
                console.print(text, markup=False, highlight=False)
                console.print()
            else:
                console.print(Panel(escape(text), title=style.upper(), title_align="left", border_style="green"))


def cmd_bulk(ctx: Ctx, args: argparse.Namespace) -> None:
    text = sys.stdin.read() if args.file == "-" else Path(args.file).read_text(encoding="utf-8", errors="ignore")
    dois = extract_dois(text)
    console.print(f"[dim]Detected {len(dois)} unique DOI(s) in {args.file}[/]")
    works, failed = [], []
    with console.status("Resolving DOIs…") as status:
        for i, doi in enumerate(dois, 1):
            status.update(f"Resolving {i}/{len(dois)} {doi}")
            try:
                works.append(normalize_work(ctx.client.work(doi)))
            except CrossrefError as exc:
                failed.append((doi, str(exc)))
    if args.style:
        for w in works:
            console.print(cite(ctx.client, w, args.style), markup=False, highlight=False)
            console.print()
    elif ctx.as_json:
        emit_json({"works": works, "failed": failed})
    else:
        console.print(works_table(works, title=f"Resolved {len(works)}/{len(dois)}"))
    for doi, err in failed:
        console.print(f"[red]✗ {doi}[/] {escape(err)}")
    if args.out:
        export(works, Path(args.out))


def cmd_refs(ctx: Ctx, args: argparse.Namespace) -> None:
    work = normalize_work(ctx.client.work(args.doi))
    resolved = resolve_references(ctx.client, work, args.resolve) if args.resolve else []
    if ctx.as_json:
        emit_json({"work": work["doi"], "references": work["references"], "resolved": resolved})
        return
    tree = Tree(f"[bold]{escape(trunc(work['title'], 100))}[/] [dim]({work['year']}, cited by {work['cited_by_count']:,})[/]")
    by_doi = {w["doi"]: w for w in resolved}
    for ref in work["references"][: args.limit]:
        if ref["doi"] in by_doi:
            r = by_doi[ref["doi"]]
            tree.add(f"[cyan]cites →[/] {escape(trunc(r['title'], 90))} [dim]{short_authors(r, 1)} {r['year']} · cited by {r['cited_by_count']:,}[/]")
        else:
            label = ref["title"] or ref["unstructured"] or ref["doi"] or "(unstructured reference)"
            meta = " ".join(str(x) for x in (ref["author"], ref["year"]) if x)
            tree.add(f"[cyan]cites →[/] {escape(trunc(label, 90))} [dim]{escape(meta)} {ref['doi'] or ''}[/]")
    if len(work["references"]) > args.limit:
        tree.add(f"[dim]… {len(work['references']) - args.limit} more[/]")
    if not work["references"]:
        tree.add("[yellow]Publisher did not deposit references for this work.[/]")
    console.print(tree)
    if resolved:
        console.print(works_table(sorted(resolved, key=lambda w: -w["cited_by_count"]), title="Resolved references — most cited first"))
        years = Counter(w["year"] for w in resolved if w["year"])
        console.print(counts_table("Age profile of resolved references", dict(sorted(years.items())), "Year", 30))


def cmd_snowball(ctx: Ctx, args: argparse.Namespace) -> None:
    result = snowball(ctx.client, OpenAlexClient(ctx.client), args.doi, args.rows)
    if args.out:
        export(result["merged"], Path(args.out))
    if ctx.as_json:
        emit_json(result)
        return
    seed, oa = result["seed"], result["seed"]["open_access"]
    access = f"[green]open access ({oa['status']})[/] {oa['url']}" if oa["is_oa"] else f"[yellow]{oa['status'] or 'unknown'}[/] — no free copy known"
    console.print(Panel(
        f"[bold]{escape(seed['title'])}[/]\n{escape(short_authors(seed, 4))} · {seed['year']} · {escape(seed['journal'] or '')}\n"
        f"Access: {access}" + (f"\n[dim]Abstract: {escape(trunc(seed['abstract'], 300))}[/]" if seed["abstract"] else ""),
        title="Seed", title_align="left", border_style="cyan"))
    console.print(works_table(result["backward"][: args.rows],
                              title=f"⬅ Backward — {len(result['backward'])} references (older work it builds on), most cited first"))
    if result["crossref_only_references"]:
        console.print(f"[dim]+ {len(result['crossref_only_references'])} DOI references known to Crossref but not matched by OpenAlex[/]")
    console.print(works_table(result["forward"],
                              title=f"➡ Forward — {result['forward_total']:,} works cite this (newer work building on it), most cited first"))
    console.print(works_table(result["related"][: args.rows], title="↔ Related — OpenAlex topic/concept neighbours"))
    multi = [w for w in result["merged"] if len(w["found_via"]) > 1]
    console.print(f"[bold]{len(result['merged'])}[/] unique works collected"
                  + (f", {len(multi)} found by more than one path (strong candidates)" if multi else "")
                  + (". Export with --out snowball.csv / .bib / .ris" if not args.out else "."))


def cmd_compare(ctx: Ctx, args: argparse.Namespace) -> None:
    a, b = (normalize_work(ctx.client.work(d)) for d in (args.doi_a, args.doi_b))
    coupling = bibliographic_coupling(a, b)
    if ctx.as_json:
        emit_json({"a": a["doi"], "b": b["doi"], **coupling})
        return
    t = Table(box=box.SIMPLE_HEAVY, expand=True)
    t.add_column("", style="bold cyan")
    t.add_column("A", ratio=1)
    t.add_column("B", ratio=1)
    for label, key in (("Title", "title"), ("Year", "year"), ("Journal", "journal"), ("Type", "type"),
                       ("Cited by", "cited_by_count"), ("References", "reference_count")):
        t.add_row(label, escape(str(a[key])), escape(str(b[key])))
    t.add_row("Authors", escape(short_authors(a, 4)), escape(short_authors(b, 4)))
    t.add_row("Metadata quality", f"{metadata_quality(a)['metadata_quality']:.0%}", f"{metadata_quality(b)['metadata_quality']:.0%}")
    console.print(t)
    console.print(
        f"Bibliographic coupling: [bold]{len(coupling['shared'])}[/] shared references "
        f"(Jaccard {coupling['jaccard']}, refs with DOI A={coupling['refs_with_doi'][0]} B={coupling['refs_with_doi'][1]})"
    )
    if coupling["a_cites_b"] or coupling["b_cites_a"]:
        console.print(f"[green]Direct citation:[/] {'A → B' if coupling['a_cites_b'] else ''} {'B → A' if coupling['b_cites_a'] else ''}")
    for doi in coupling["shared"][:15]:
        console.print(f"  • {doi}")


def cmd_related(ctx: Ctx, args: argparse.Namespace) -> None:
    work = normalize_work(ctx.client.work(args.doi))
    related = related_works(ctx.client, work, args.rows)
    if ctx.as_json:
        emit_json(related)
        return
    console.print(f"Seed: [bold]{escape(work['title'])}[/]")
    console.print(works_table(related, title="Bibliographically similar works (Crossref relevance)", show_score=True))


def growth_note(years: dict[int, int]) -> str | None:
    items = list(years.items())
    if len(items) < 3:
        return None
    (y0, c0), (y1, c1) = items[-3], items[-2]
    return f"Growth {y0}→{y1}: {(c1 - c0) / c0:+.0%} (current year is incomplete)" if c0 else None


def cmd_timeline(ctx: Ctx, args: argparse.Namespace) -> None:
    topic = " ".join(args.topic)
    filters = {"type": args.type, "from-pub-date": str(args.start), "until-pub-date": f"{args.end}-12-31"}
    if args.fuzzy:
        total, years = topic_timeline(ctx.client, topic, args.start, args.end, {"type": args.type})
        head = f"“{topic}” — {total:,} fuzzy matches (any query word), by year"
        matched: list[dict] = []
    else:
        scanned, matched = phrase_sample(ctx.client, topic, args.scan, filters)
        years = dict(sorted(Counter(w["year"] for w in matched if w["year"]).items()))
        head = f"“{topic}” — {len(matched)} exact-phrase titles among the top {scanned:,} relevance hits, by year"
    samples = {}
    for year in list(years)[-args.samples:] if args.samples else []:
        in_year = sorted((w for w in matched if w["year"] == year), key=lambda w: -w["cited_by_count"])
        samples[year] = in_year[0] if in_year else top_work_for_year(ctx.client, topic, year, {"type": args.type})
    if ctx.as_json:
        emit_json({"topic": topic, "mode": "fuzzy" if args.fuzzy else "phrase", "years": years, "samples": samples})
        return
    console.print(counts_table(head, years, "Year", 45))
    if note := growth_note(years):
        console.print(f"[dim]{note}[/]")
    if samples:
        st = Table(title="Most cited work per year", box=box.SIMPLE, title_justify="left", expand=True)
        st.add_column("Year", width=4)
        st.add_column("Title", ratio=4)
        st.add_column("Cited", justify="right", width=6)
        st.add_column("DOI", ratio=2, style="cyan")
        for year, w in samples.items():
            if w:
                st.add_row(str(year), escape(trunc(w["title"], 100)), f"{w['cited_by_count']:,}", w["doi"])
        console.print(st)


LANDSCAPE_FACETS = [
    ("container-title", "Top venues"),
    ("publisher-name", "Top publishers"),
    ("type-name", "Work types"),
    ("funder-name", "Top funders"),
    ("license", "Licenses"),
]


def landscape_from_works(works: list[dict], top: int) -> dict[str, list[tuple[str, int]]]:
    return {
        "container-title": Counter(w["journal"] for w in works if w["journal"]).most_common(top),
        "publisher-name": Counter(w["publisher"] for w in works if w["publisher"]).most_common(top),
        "type-name": Counter(w["type"] for w in works if w["type"]).most_common(top),
        "funder-name": Counter(f["name"] for w in works for f in w["funding"] if f["name"]).most_common(top),
        "license": Counter(l["url"] for w in works for l in {x["url"]: x for x in w["license"]}.values() if l["url"]).most_common(top),
    }


def cmd_landscape(ctx: Ctx, args: argparse.Namespace) -> None:
    topic = " ".join(args.topic)
    filters = {"from-pub-date": args.from_date, "until-pub-date": args.until}
    if args.fuzzy:
        msg = ctx.client.works(query=topic, filters=filters, rows=0, facets=[f"{f}:{args.top}" for f, _ in LANDSCAPE_FACETS])
        facets = {f: list(msg.get("facets", {}).get(f, {}).get("values", {}).items()) for f, _ in LANDSCAPE_FACETS}
        head = f"Landscape of “{topic}” — {msg.get('total-results', 0):,} fuzzy matches"
    else:
        scanned, works = phrase_sample(ctx.client, topic, args.scan, filters)
        facets = landscape_from_works(works, args.top)
        head = f"Landscape of “{topic}” — {len(works)} exact-phrase titles among top {scanned:,} hits"
    if ctx.as_json:
        emit_json({"topic": topic, "mode": "fuzzy" if args.fuzzy else "phrase", "facets": facets})
        return
    console.print(Rule(head))
    for facet, title in LANDSCAPE_FACETS:
        if facets[facet]:
            console.print(counts_table(title, facets[facet], facet, 30))
    console.print("[dim]Descriptive metadata counts only — not a ranking of venue or funder quality.[/]")


def cmd_author(ctx: Ctx, args: argparse.Namespace) -> None:
    name = " ".join(args.name)
    profile = author_profile(ctx.client, name, args.max, args.orcid)
    if ctx.as_json:
        emit_json(profile)
        return
    console.print(Rule(f"Author profile: {name}" + (f" (ORCID {args.orcid})" if args.orcid else "")))
    console.print(f"Scanned {profile['scanned']} works, {profile['matched']} attributed to this name. "
                  f"Citations received (Crossref): {profile['total_citations']:,}")
    if profile["orcids"]:
        console.print("ORCID iDs seen: " + ", ".join(f"[green]{o}[/] ×{c}" for o, c in profile["orcids"])
                      + ("  [yellow]→ several iDs suggest name homonyms; rerun with --orcid[/]" if len(profile["orcids"]) > 1 else ""))
    if profile["affiliations"]:
        console.print(counts_table("Affiliations", profile["affiliations"], "Affiliation", 20))
    console.print(counts_table("Publications per year", profile["years"], "Year", 30))
    console.print(counts_table("Frequent co-authors", profile["coauthors"], "Co-author", 20))
    console.print(counts_table("Venues", profile["journals"], "Venue", 20))
    top = sorted(profile["works"], key=lambda w: -w["cited_by_count"])[:10]
    console.print(works_table(top, title="Most cited works"))
    console.print("[dim]Name matching is fuzzy; counts describe Crossref metadata, not research impact.[/]")


def cmd_journal(ctx: Ctx, args: argparse.Namespace) -> None:
    target = args.issn_or_query
    if not re.fullmatch(r"\d{4}-\d{3}[\dXx]", target):
        found = ctx.client.journals(target, rows=8).get("items", [])
        if ctx.as_json:
            emit_json(found)
            return
        t = Table(title=f"Journals matching “{target}”", box=box.SIMPLE, title_justify="left")
        for col in ("Title", "Publisher", "ISSN", "DOIs"):
            t.add_column(col)
        for j in found:
            t.add_row(escape(j.get("title", "")), escape(j.get("publisher", "")), ", ".join(j.get("ISSN", [])),
                      f"{j.get('counts', {}).get('total-dois', 0):,}")
        console.print(t)
        console.print("[dim]Rerun with an ISSN for the full journal profile.[/]")
        return
    j = ctx.client.journal(target)
    latest = ctx.client.works(path=f"/journals/{target}/works", rows=args.rows, sort="published", order="desc", select=SEARCH_SELECT)
    if ctx.as_json:
        emit_json({"journal": j, "latest": [normalize_work(i) for i in latest.get("items", [])]})
        return
    counts, coverage = j.get("counts", {}), j.get("coverage", {})
    console.print(Panel(
        f"[bold]{escape(j.get('title', ''))}[/]\nPublisher: {escape(j.get('publisher', ''))}\nISSN: {', '.join(j.get('ISSN', []))}\n"
        f"Subjects: {', '.join(s.get('name', '') for s in j.get('subjects', [])) or '—'}\n"
        f"DOIs: {counts.get('total-dois', 0):,} total · {counts.get('current-dois', 0):,} current · {counts.get('backfile-dois', 0):,} backfile",
        border_style="cyan"))
    cov = [(k.replace("-current", ""), round(v * 100)) for k, v in sorted(coverage.items()) if k.endswith("-current")]
    if cov:
        console.print(counts_table("Metadata coverage, current content (%)", cov, "Field", 30))
    per_year = {y: c for y, c in (j.get("breakdowns", {}).get("dois-by-issued-year") or [])}
    if per_year:
        console.print(counts_table("DOIs by issued year (last 15)", dict(sorted(per_year.items())[-15:]), "Year", 30))
    console.print(works_table([normalize_work(i) for i in latest.get("items", [])], title="Latest works"))


def cmd_funder(ctx: Ctx, args: argparse.Namespace) -> None:
    query = " ".join(args.query)
    found = ctx.client.funders(query, rows=5).get("items", [])
    if not found:
        raise SystemExit(f"No funder matches {query!r}")
    f = found[0]
    msg =ctx.client.works(path=f"/funders/{f['id']}/works", rows=args.rows, sort="published", order="desc",
                           select=SEARCH_SELECT, facets=["published:30", "container-title:8"])
    total = msg.get("total-results", 0)
    years = {int(y): c for y, c in msg.get("facets", {}).get("published", {}).get("values", {}).items() if y.isdigit()}
    venues = msg.get("facets", {}).get("container-title", {}).get("values", {})
    works = [normalize_work(i) for i in msg.get("items", [])]
    if ctx.as_json:
        emit_json({"funder": f, "total_works": total, "years": years, "venues": venues, "latest": works})
        return
    alt = ", ".join(f.get("alt-names", [])[:4])
    console.print(Panel(f"[bold]{escape(f['name'])}[/]  ({escape(f.get('location', ''))})\nFunder ID: {f['id']}\n"
                        f"Also known as: {escape(alt) or '—'}\nFunded works registered in Crossref: [bold]{total:,}[/]",
                        border_style="cyan"))
    if len(found) > 1:
        console.print("[dim]Other matches: " + "; ".join(escape(x["name"]) for x in found[1:]) + "[/]")
    recent = dict(sorted(years.items())[-12:])
    console.print(counts_table("Funded works per year (last 12)", recent, "Year", 30))
    console.print(counts_table("Top venues", venues, "Venue", 20))
    console.print(works_table(works, title="Latest funded works"))


def cmd_config(ctx: Ctx, args: argparse.Namespace) -> None:
    head = ctx.client.http.get(f"{API_BASE}/works", params={"rows": 0, **({"mailto": ctx.client.mailto} if ctx.client.mailto else {})})
    crossref = {
        "mailto": ctx.client.mailto or None,
        "pool": head.headers.get("x-api-pool"),
        "rate_limit": f"{head.headers.get('x-rate-limit-limit')}/{head.headers.get('x-rate-limit-interval')}",
    }
    openalex = OpenAlexClient(ctx.client)
    budget = openalex.budget()
    status = {"crossref": crossref, "openalex": {"mode": openalex.mode, **budget},
              "cache": {"dir": str(CACHE_DIR), "ttl_s": CACHE_TTL, "enabled": ctx.client.cache.enabled}}
    if ctx.as_json:
        emit_json(status)
        return
    t = Table.grid(padding=(0, 2))
    t.add_column(style="bold cyan")
    t.add_column()
    t.add_row("Crossref", f"pool [bold]{crossref['pool']}[/] · {crossref['rate_limit']} · mailto {crossref['mailto'] or '[yellow]not set[/] (set CROSSREF_MAILTO)'}")
    if "error" in budget:
        oa_line = f"[yellow]{budget['error']}[/]"
    else:
        oa_line = (f"[bold]{openalex.mode}[/] · ${budget['remaining_usd']} of ${budget['limit_usd']} left today "
                   f"(resets in {int(budget['resets_in_s'] or 0) // 3600}h)")
    t.add_row("OpenAlex", oa_line + ("" if openalex.api_key else "\n[dim]add OPENALEX_API_KEY to .env for 10x budget[/]"))
    t.add_row("Cache", f"{CACHE_DIR} · TTL {CACHE_TTL // 3600}h · {'on' if ctx.client.cache.enabled else 'off'}")
    console.print(Panel(t, title="Configuration", title_align="left", border_style="cyan"))


def cmd_types(ctx: Ctx, args: argparse.Namespace) -> None:
    items = ctx.client.types().get("items", [])
    if ctx.as_json:
        emit_json(items)
        return
    t = Table(title="Crossref work types", box=box.SIMPLE, title_justify="left")
    t.add_column("id", style="cyan")
    t.add_column("label")
    for it in items:
        t.add_row(it["id"], it["label"])
    console.print(t)


def cmd_prefix(ctx: Ctx, args: argparse.Namespace) -> None:
    prefix = args.prefix.split("/")[0]
    p = ctx.client.prefix(prefix)
    if ctx.as_json:
        emit_json(p)
        return
    console.print(f"Prefix [cyan]{prefix}[/] belongs to [bold]{escape(p.get('name', ''))}[/] ({p.get('member')})")


# -- tour -------------------------------------------------------------------

POTENTIAL = [
    ("DOI → canonical record", "doi", "Phase 2", "Auto-fill metadata on paper upload (ARGUS one-click ingestion)"),
    ("Metadata quality score", "doi", "Phase 5", "Gate garbage before it enters the KB; flag records to enrich elsewhere"),
    ("Citation engine", "cite / bulk", "Phase 3", "Reference manager, thesis bibliography generator, Word/Docs plugin"),
    ("Reference parsing", "bulk", "Phase 3/17", "Paste a reference list or PDF text → clean BibTeX/RIS in seconds"),
    ("Scholarly search + export", "search --out", "Phase 4", "SLR screening sheet (CSV) with filters & cursor pagination"),
    ("Reference graph", "refs / compare", "Phase 7", "Citation neighborhood, bibliographic coupling, literature maps"),
    ("Related works", "related", "Phase 12", "'More like this' recommendations without a vector DB"),
    ("Topic timeline", "timeline", "Phase 13", "Trend analysis for proposals, thesis background, emerging topics"),
    ("Topic landscape", "landscape", "Phase 15", "Where to publish? who funds this? which publishers dominate?"),
    ("Author intelligence", "author", "Phase 14", "Co-author network, ORCID disambiguation, collaborator discovery"),
    ("Journal intelligence", "journal", "Phase 15", "Journal profile, metadata coverage, publication volume"),
    ("Funder intelligence", "funder", "Phase 15", "Grant output tracking for research offices"),
    ("Snowballing via OpenAlex", "snowball", "Phase 8", "Backward + forward citations + related → SLR candidate pool"),
    ("Agent tools", "--json", "Phase 19", "Every command emits JSON → wrap 1:1 as MCP tools"),
]


def cmd_tour(ctx: Ctx, args: argparse.Namespace) -> None:
    client, topic = ctx.client, args.topic
    step = lambda n, title, cmd: console.print(Rule(f"[bold]{n}. {title}[/]  [dim]$ uv run main.py {cmd}[/]", align="left"))

    step(1, "DOI normalization (any spelling → canonical DOI)", "doi <doi>")
    for raw in (f"https://doi.org/{args.doi.upper()}", f"doi:{args.doi}", f"https://dx.doi.org/{args.doi}"):
        console.print(f"  {raw:<55} → [cyan]{normalize_doi(raw)}[/]")

    step(2, "DOI → normalized record + metadata quality", f"doi {args.doi}")
    work = normalize_work(client.work(args.doi))
    show_work(work)

    step(3, "Citation engine", f"cite {args.doi} --style apa ieee bibtex vancouver")
    for style in ("apa", "ieee", "vancouver", "bibtex"):
        try:
            console.print(Panel(escape(cite(client, work, style)), title=style.upper(), title_align="left", border_style="green"))
        except CrossrefError as exc:
            console.print(f"[yellow]{style}: {exc}[/]")

    step(4, "Reference graph", f"refs {args.doi} --resolve 5")
    refs = [r for r in work["references"] if r["doi"]]
    console.print(f"{len(work['references'])} deposited references, {len(refs)} with DOI. Resolving the first 5…")
    resolved = resolve_references(client, work, 5)
    if resolved:
        console.print(works_table(sorted(resolved, key=lambda w: -w["cited_by_count"]), title="Cited by this paper"))

    step(5, "Related works (bibliographic similarity)", f"related {args.doi}")
    console.print(works_table(related_works(client, work, 5), show_score=True))

    step(6, "Scholarly search with filters", f'search "{topic}" --from 2023-01-01 --type journal-article --has-abstract --rows 8')
    page = client.works(query=topic, rows=8, select=SEARCH_SELECT,
                        filters={"from-pub-date": "2023-01-01", "type": "journal-article", "has-abstract": True})
    works = deduplicate([normalize_work(i) for i in page.get("items", [])])
    console.print(works_table(works, title=f"{page.get('total-results', 0):,} matches", show_score=True))

    step(7, "Topic timeline (exact phrase in title)", f'timeline "{topic}" --scan 1000')
    fuzzy_total = client.works(query=topic, rows=0).get("total-results", 0)
    scanned, matched = phrase_sample(client, topic, 1000)
    years = dict(sorted(Counter(w["year"] for w in matched if w["year"]).items()))
    console.print(f"[yellow]Crossref has no phrase search:[/] the plain query matches {fuzzy_total:,} works (any word). "
                  f"Scanning the top {scanned:,} relevance hits leaves {len(matched)} titles with the exact phrase.")
    console.print(counts_table(f"“{topic}” per year", years, "Year", 45))

    step(8, "Topic landscape", f'landscape "{topic}"')
    facets = landscape_from_works(matched, 6)
    for facet, title in (("container-title", "Top venues"), ("publisher-name", "Top publishers"), ("funder-name", "Top funders")):
        if facets[facet]:
            console.print(counts_table(title, facets[facet], facet, 25))

    if work["authors"]:
        lead = work["authors"][0]
        step(9, "Author intelligence", f'author "{lead["name"]}"' + (f" --orcid {lead['orcid']}" if lead["orcid"] else ""))
        profile = author_profile(client, lead["name"], 100, lead["orcid"])
        console.print(f"{profile['matched']} works attributed · {profile['total_citations']:,} Crossref citations · years "
                      f"{min(profile['years'], default='—')}–{max(profile['years'], default='—')}")
        console.print(counts_table("Frequent co-authors", profile["coauthors"][:6], "Co-author", 20))

    step(10, "What this unlocks", "")
    t = Table(box=box.SIMPLE_HEAVY, expand=True)
    t.add_column("Capability", style="bold")
    t.add_column("Command", style="cyan")
    t.add_column("Roadmap", style="dim")
    t.add_column("Product potential")
    for row in POTENTIAL:
        t.add_row(*row)
    console.print(t)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="crossref", description="Crossref Research Intelligence — explore the Crossref REST API.")
    p.add_argument("--json", action="store_true", help="emit normalized JSON instead of tables")
    p.add_argument("--no-cache", action="store_true", help="bypass the on-disk response cache")
    p.add_argument("--mailto", default=MAILTO, help="contact email for Crossref's polite pool (or CROSSREF_MAILTO)")
    p.add_argument("-v", "--verbose", action="count", default=0, help="-v session summary, -vv every request")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("tour", help="guided showcase of everything this can do")
    s.add_argument("--doi", default=DEMO_DOI)
    s.add_argument("--topic", default=DEMO_TOPIC)
    s.set_defaults(func=cmd_tour)

    s = sub.add_parser("doi", help="resolve a DOI into a normalized record + metadata quality")
    s.add_argument("doi")
    s.set_defaults(func=cmd_doi)

    s = sub.add_parser("search", help="search works with query fields, filters, sorting and export")
    s.add_argument("query", nargs="*")
    s.add_argument("--title")
    s.add_argument("--author")
    s.add_argument("--bibliographic", help="match a free-form citation string")
    s.add_argument("--journal", help="container title")
    s.add_argument("--publisher")
    s.add_argument("--affiliation")
    s.add_argument("--from", dest="from_date", metavar="YYYY[-MM-DD]")
    s.add_argument("--until", metavar="YYYY[-MM-DD]")
    s.add_argument("--type", help="e.g. journal-article, proceedings-article, book-chapter (see `types`)")
    s.add_argument("--issn")
    s.add_argument("--orcid")
    for flag in ("abstract", "orcid", "references", "license", "funder", "full-text"):
        s.add_argument(f"--has-{flag}", action="store_true", dest=f"has_{flag.replace('-', '_')}")
    s.add_argument("--sort", choices=["relevance", "published", "issued", "is-referenced-by-count", "references-count", "updated", "created"])
    s.add_argument("--order", choices=["asc", "desc"])
    s.add_argument("--rows", type=int, default=15, help="results to fetch (cursor pagination when >1000 or exporting)")
    s.add_argument("--min-quality", type=float, help="drop records below this metadata completeness (0–1)")
    s.add_argument("--out", help="export to .csv .json .jsonl .bib .ris")
    s.set_defaults(func=cmd_search)

    s = sub.add_parser("cite", help="generate citations (apa ieee bibtex ris local; vancouver chicago mla harvard csl or any CSL style remote)")
    s.add_argument("dois", nargs="+")
    s.add_argument("--style", nargs="+")
    s.set_defaults(func=cmd_cite)

    s = sub.add_parser("bulk", help="extract every DOI from a text file (or - for stdin) and resolve them")
    s.add_argument("file")
    s.add_argument("--style", help="print citations in this style instead of a table")
    s.add_argument("--out", help="export to .csv .json .jsonl .bib .ris")
    s.set_defaults(func=cmd_bulk)

    s = sub.add_parser("refs", help="reference list / citation neighborhood of a DOI")
    s.add_argument("doi")
    s.add_argument("--limit", type=int, default=25)
    s.add_argument("--resolve", type=int, default=0, help="resolve the first N DOI references for richer metadata")
    s.set_defaults(func=cmd_refs)

    s = sub.add_parser("snowball", help="references + citing works + related works around one DOI (Crossref + OpenAlex)")
    s.add_argument("doi")
    s.add_argument("--rows", type=int, default=10, help="rows per section")
    s.add_argument("--out", help="export all collected works to .csv .json .jsonl .bib .ris")
    s.set_defaults(func=cmd_snowball)

    s = sub.add_parser("compare", help="compare two works: metadata + bibliographic coupling")
    s.add_argument("doi_a")
    s.add_argument("doi_b")
    s.set_defaults(func=cmd_compare)

    s = sub.add_parser("related", help="bibliographically similar works")
    s.add_argument("doi")
    s.add_argument("--rows", type=int, default=10)
    s.set_defaults(func=cmd_related)

    this_year = datetime.now().year
    s = sub.add_parser("timeline", help="publication volume of a topic per year")
    s.add_argument("topic", nargs="+")
    s.add_argument("--start", type=int, default=this_year - 15)
    s.add_argument("--end", type=int, default=this_year)
    s.add_argument("--type")
    s.add_argument("--samples", type=int, default=0, help="also show the most cited work for the last N years")
    s.add_argument("--scan", type=int, default=1000, help="relevance hits to scan for exact-phrase titles")
    s.add_argument("--fuzzy", action="store_true", help="count every fuzzy match via facets (fast, but noisy)")
    s.set_defaults(func=cmd_timeline)

    s = sub.add_parser("landscape", help="top venues, publishers, types, funders and licenses for a topic")
    s.add_argument("topic", nargs="+")
    s.add_argument("--from", dest="from_date")
    s.add_argument("--until")
    s.add_argument("--top", type=int, default=10)
    s.add_argument("--scan", type=int, default=1000, help="relevance hits to scan for exact-phrase titles")
    s.add_argument("--fuzzy", action="store_true", help="facet counts over every fuzzy match (fast, but noisy)")
    s.set_defaults(func=cmd_landscape)

    s = sub.add_parser("author", help="author profile: output, co-authors, venues, ORCID")
    s.add_argument("name", nargs="+")
    s.add_argument("--orcid", help="restrict to works carrying this ORCID iD")
    s.add_argument("--max", type=int, default=200, help="works to scan")
    s.set_defaults(func=cmd_author)

    s = sub.add_parser("journal", help="journal profile by ISSN, or search journals by name")
    s.add_argument("issn_or_query")
    s.add_argument("--rows", type=int, default=8)
    s.set_defaults(func=cmd_journal)

    s = sub.add_parser("funder", help="funder profile and funded works")
    s.add_argument("query", nargs="+")
    s.add_argument("--rows", type=int, default=8)
    s.set_defaults(func=cmd_funder)

    s = sub.add_parser("prefix", help="who owns a DOI prefix")
    s.add_argument("prefix")
    s.set_defaults(func=cmd_prefix)

    s = sub.add_parser("config", help="show Crossref pool, OpenAlex auth mode and remaining budget")
    s.set_defaults(func=cmd_config)

    s = sub.add_parser("types", help="list Crossref work types")
    s.set_defaults(func=cmd_types)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    setup_logging(args.verbose)
    client = CrossrefClient(mailto=args.mailto, cache=DiskCache(CACHE_DIR, CACHE_TTL, enabled=not args.no_cache))
    try:
        args.func(Ctx(client, args.json), args)
    except (CrossrefError, InvalidDOI) as exc:
        console.print(f"[red]Error:[/] {escape(str(exc))}")
        return 1
    except KeyboardInterrupt:
        return 130
    finally:
        kv("session", logging.INFO, requests=client.requests, cache_hits=client.cache.hits, cache_misses=client.cache.misses)
    return 0


if __name__ == "__main__":
    sys.exit(main())
