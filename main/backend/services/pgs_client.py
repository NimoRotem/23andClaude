"""PGS Catalog API client with Redis caching and file download support."""

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
import redis.asyncio as aioredis

from backend.config import (
    PGS_CACHE_DIR,
    PGS_CATALOG_API,
    PGS_SEARCH_CACHE_TTL,
    REDIS_URL,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Patterns for query-type detection
# ---------------------------------------------------------------------------
_PGS_ID_RE = re.compile(r"^PGS\d{6,}$", re.IGNORECASE)
_PGP_ID_RE = re.compile(r"^PGP\d{6,}$", re.IGNORECASE)
_EFO_RE = re.compile(r"^EFO_\d+$", re.IGNORECASE)

# Module-level Redis singleton (lazy-initialised)
_redis: aioredis.Redis | None = None


async def _get_redis() -> aioredis.Redis:
    """Return (and lazily create) a module-level async Redis connection."""
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


def _cache_key(namespace: str, value: str) -> str:
    return f"pgs:{namespace}:{value}"


def _normalise_score(raw: dict[str, Any]) -> dict[str, Any]:
    """Extract the fields we care about from a PGS Catalog score object."""
    pub = raw.get("publication") or {}
    ftp = raw.get("ftp_harmonized_scoring_files") or {}

    # Builds available
    builds: list[str] = []
    if ftp.get("GRCh37"):
        builds.append("GRCh37")
    if ftp.get("GRCh38"):
        builds.append("GRCh38")

    # Ancestry
    ancestry_dist = raw.get("ancestry_distribution") or {}
    gwas_ancestry = ancestry_dist.get("gwas") or {}
    eval_ancestry = ancestry_dist.get("eval") or {}

    # Trait EFO list
    trait_efo: list[dict[str, str]] = []
    for t in raw.get("trait_efo", []) or []:
        trait_efo.append({
            "id": t.get("id", ""),
            "label": t.get("label", ""),
        })

    return {
        "id": raw.get("id", ""),
        "name": raw.get("name", ""),
        "trait_reported": raw.get("trait_reported", ""),
        "trait_efo": trait_efo,
        "variants_number": raw.get("variants_number", 0),
        "weight_type": raw.get("weight_type", ""),
        "publication": {
            "firstauthor": pub.get("firstauthor", ""),
            "date_publication": pub.get("date_publication", ""),
            "journal": pub.get("journal", ""),
            "doi": pub.get("doi", ""),
            "PMID": pub.get("PMID", ""),
            "pgp_id": pub.get("id", ""),
        },
        "ancestry_distribution": ancestry_dist,
        "ancestry_gwas": gwas_ancestry,
        "ancestry_eval": eval_ancestry,
        "samples_variants": raw.get("samples_variants") or [],
        "method_name": raw.get("method_name", ""),
        "date_release": raw.get("date_release", ""),
        "ftp_harmonized_scoring_files": ftp,
        "builds_available": builds,
    }


class PGSCatalogClient:
    """Async client for the PGS Catalog REST API with Redis caching."""

    def __init__(
        self,
        base_url: str = PGS_CATALOG_API,
        timeout: float = 15.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._http: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=self.timeout,
                headers={"Accept": "application/json"},
                follow_redirects=True,
            )
        return self._http

    async def close(self) -> None:
        if self._http and not self._http.is_closed:
            await self._http.aclose()
            self._http = None

    async def _get(self, path: str, params: dict | None = None) -> dict[str, Any]:
        """Issue a GET and return parsed JSON; raises on HTTP errors."""
        client = await self._client()
        resp = await client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def _get_all_pages(
        self, path: str, params: dict | None = None, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Follow PGS Catalog pagination, collecting results up to *limit*."""
        params = dict(params or {})
        collected: list[dict[str, Any]] = []
        url: str | None = path

        while url is not None:
            data = await self._get(url, params if url == path else None)

            # The API wraps paginated responses in {count, next, previous, results}
            if "results" in data:
                results = data["results"]
            else:
                # Single-object response (e.g. /score/{id})
                return [data]

            collected.extend(results)
            if limit and len(collected) >= limit:
                collected = collected[:limit]
                break

            next_url = data.get("next")
            if next_url:
                # next_url is absolute; make it relative so httpx base_url works
                if next_url.startswith("http"):
                    # Strip the base to get the path + query
                    next_url = next_url.split("/rest", 1)[-1]
                    url = "/rest" + next_url if not next_url.startswith("/rest") else next_url
                    # Actually, easier to just use the raw url with a fresh get
                    # But our _get uses base_url. Let's strip base properly.
                    url = next_url  # e.g. /score?limit=...&offset=...
                else:
                    url = next_url
                params = None  # params are embedded in the next URL
            else:
                url = None

        return collected

    # ------------------------------------------------------------------
    # Redis cache helpers
    # ------------------------------------------------------------------

    async def _cache_get(self, key: str) -> Any | None:
        try:
            r = await _get_redis()
            raw = await r.get(key)
            if raw is not None:
                return json.loads(raw)
        except Exception:
            logger.debug("Redis cache miss/error for %s", key, exc_info=True)
        return None

    async def _cache_set(self, key: str, value: Any, ttl: int = PGS_SEARCH_CACHE_TTL) -> None:
        try:
            r = await _get_redis()
            await r.set(key, json.dumps(value, default=str), ex=ttl)
        except Exception:
            logger.debug("Redis cache set error for %s", key, exc_info=True)

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def search(self, query: str, limit: int = 20) -> list[dict[str, Any]]:
        """Unified search that auto-detects query type.

        Returns a list of normalised score dicts.
        """
        query = query.strip()
        if not query:
            return []

        cache_key = _cache_key("search", f"{query}:{limit}")
        cached = await self._cache_get(cache_key)
        if cached is not None:
            return cached

        try:
            results = await self._dispatch_search(query, limit)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                results = []
            else:
                raise
        except httpx.TimeoutException:
            logger.warning("PGS Catalog API timeout for query=%s", query)
            raise

        await self._cache_set(cache_key, results)
        return results

    async def _dispatch_search(self, query: str, limit: int) -> list[dict[str, Any]]:
        """Route query to the right API endpoint(s)."""
        q_upper = query.upper()

        # --- Direct PGS ID lookup ---
        if _PGS_ID_RE.match(q_upper):
            try:
                raw = await self._get(f"/score/{q_upper}")
                return [_normalise_score(raw)]
            except httpx.HTTPStatusError:
                return []

        # --- PGP (publication) lookup ---
        if _PGP_ID_RE.match(q_upper):
            try:
                pub_data = await self._get(f"/publication/{q_upper}")
            except httpx.HTTPStatusError:
                return []
            # associated_pgs_ids is a list of PGS IDs
            pgs_ids = pub_data.get("associated_pgs_ids") or []
            if not pgs_ids:
                return []
            scores = await self._fetch_scores_by_ids(pgs_ids[:limit])
            return scores

        # --- EFO trait lookup ---
        if _EFO_RE.match(q_upper):
            try:
                trait_data = await self._get(f"/trait/{q_upper}")
            except httpx.HTTPStatusError:
                return []
            pgs_ids = trait_data.get("associated_pgs_ids") or []
            if not pgs_ids:
                return []
            scores = await self._fetch_scores_by_ids(pgs_ids[:limit])
            return scores

        # --- Free-text search (parallel score + trait search) ---
        score_results, trait_results = await asyncio.gather(
            self._search_scores_text(query, limit),
            self._search_traits_text(query, limit),
            return_exceptions=True,
        )

        merged: dict[str, dict] = {}
        for batch in (score_results, trait_results):
            if isinstance(batch, BaseException):
                logger.warning("Partial search failure: %s", batch)
                continue
            for item in batch:
                pgs_id = item.get("id", "")
                if pgs_id and pgs_id not in merged:
                    merged[pgs_id] = item

        results = list(merged.values())[:limit]
        return results

    async def _search_scores_text(self, term: str, limit: int) -> list[dict[str, Any]]:
        """Free-text search on /score/search."""
        raw_list = await self._get_all_pages(
            "/score/search", params={"term": term, "limit": min(limit, 100)}, limit=limit
        )
        return [_normalise_score(r) for r in raw_list]

    async def _search_traits_text(self, term: str, limit: int) -> list[dict[str, Any]]:
        """Free-text search on /trait/search — returns associated scores."""
        trait_list = await self._get_all_pages(
            "/trait/search", params={"term": term, "limit": 10}, limit=10
        )
        # Gather associated PGS IDs from all matching traits
        pgs_ids: list[str] = []
        for trait in trait_list:
            pgs_ids.extend(trait.get("associated_pgs_ids") or [])

        # Deduplicate, limit
        seen: set[str] = set()
        unique_ids: list[str] = []
        for pid in pgs_ids:
            if pid not in seen:
                seen.add(pid)
                unique_ids.append(pid)
            if len(unique_ids) >= limit:
                break

        if not unique_ids:
            return []
        return await self._fetch_scores_by_ids(unique_ids)

    async def _fetch_scores_by_ids(self, pgs_ids: list[str], max_fetch: int = 10) -> list[dict[str, Any]]:
        """Fetch full score metadata for a list of PGS IDs concurrently.

        Limited to max_fetch to avoid hammering the API with 100+ requests.
        """
        pgs_ids = pgs_ids[:max_fetch]

        async def _fetch_one(pid: str) -> dict[str, Any] | None:
            try:
                raw = await self._get(f"/score/{pid}")
                return _normalise_score(raw)
            except Exception:
                return None

        tasks = [_fetch_one(pid) for pid in pgs_ids]
        results = await asyncio.gather(*tasks)
        return [r for r in results if r is not None]

    # ------------------------------------------------------------------
    # Autocomplete
    # ------------------------------------------------------------------

    async def autocomplete(self, prefix: str, limit: int = 8) -> dict[str, list[dict]]:
        """Fast prefix-based search returning grouped suggestions."""
        prefix = prefix.strip()
        if not prefix:
            return {"traits": [], "scores": []}

        cache_key = _cache_key("autocomplete", f"{prefix}:{limit}")
        cached = await self._cache_get(cache_key)
        if cached is not None:
            return cached

        trait_task = self._autocomplete_traits(prefix, limit)
        score_task = self._autocomplete_scores(prefix, limit)
        trait_results, score_results = await asyncio.gather(
            trait_task, score_task, return_exceptions=True
        )

        traits = trait_results if not isinstance(trait_results, BaseException) else []
        scores = score_results if not isinstance(score_results, BaseException) else []

        result = {"traits": traits[:limit], "scores": scores[:limit]}
        # Shorter TTL for autocomplete (10 min)
        await self._cache_set(cache_key, result, ttl=600)
        return result

    async def _autocomplete_traits(self, prefix: str, limit: int) -> list[dict]:
        try:
            traits = await self._get_all_pages(
                "/trait/search", params={"term": prefix, "limit": min(limit, 20)}, limit=limit
            )
        except Exception:
            return []
        suggestions: list[dict] = []
        for t in traits:
            suggestions.append({
                "id": t.get("id", ""),
                "label": t.get("label", ""),
                "description": t.get("description", ""),
                "associated_pgs_count": len(t.get("associated_pgs_ids") or []),
            })
        return suggestions[:limit]

    async def _autocomplete_scores(self, prefix: str, limit: int) -> list[dict]:
        try:
            scores = await self._get_all_pages(
                "/score/search", params={"term": prefix, "limit": min(limit, 20)}, limit=limit
            )
        except Exception:
            return []
        suggestions: list[dict] = []
        for s in scores:
            suggestions.append({
                "id": s.get("id", ""),
                "name": s.get("name", ""),
                "trait_reported": s.get("trait_reported", ""),
                "variants_number": s.get("variants_number", 0),
            })
        return suggestions[:limit]

    # ------------------------------------------------------------------
    # Single score metadata
    # ------------------------------------------------------------------

    async def get_score(self, pgs_id: str) -> dict[str, Any]:
        """Full metadata for one PGS score."""
        pgs_id = pgs_id.upper().strip()

        cache_key = _cache_key("score", pgs_id)
        cached = await self._cache_get(cache_key)
        if cached is not None:
            return cached

        raw = await self._get(f"/score/{pgs_id}")
        normalised = _normalise_score(raw)

        await self._cache_set(cache_key, normalised)
        return normalised

    # ------------------------------------------------------------------
    # Download scoring file
    # ------------------------------------------------------------------

    async def download_scoring_file(
        self, pgs_id: str, build: str = "GRCh38"
    ) -> dict[str, Any]:
        """Download harmonised scoring file from the PGS Catalog FTP.

        Returns dict with:
          - pgs_id, build, file_path, metadata_json_path, file_size_bytes,
            downloaded_at, metadata (the full normalised score dict)
        """
        pgs_id = pgs_id.upper().strip()
        build = build.strip()

        # Fetch metadata (uses cache if available)
        metadata = await self.get_score(pgs_id)

        ftp_files = metadata.get("ftp_harmonized_scoring_files") or {}
        build_info = ftp_files.get(build)
        if not build_info:
            available = list(ftp_files.keys())
            raise ValueError(
                f"Build {build} not available for {pgs_id}. Available: {available}"
            )

        # The positions URL points to the harmonised scoring file
        download_url = build_info.get("positions")
        if not download_url:
            raise ValueError(f"No positions URL for {pgs_id} build {build}")

        # Convert ftp:// to https:// (PGS Catalog supports HTTPS mirrors)
        if download_url.startswith("ftp://"):
            download_url = download_url.replace(
                "ftp://ftp.ebi.ac.uk/pub/databases/spot/pgs",
                "https://ftp.ebi.ac.uk/pub/databases/spot/pgs",
            )

        # Prepare local directory
        cache_dir = PGS_CACHE_DIR / pgs_id
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Determine filename from URL
        filename = download_url.rsplit("/", 1)[-1]
        file_path = cache_dir / filename

        # Download using httpx with streaming
        async with httpx.AsyncClient(
            timeout=300.0, follow_redirects=True
        ) as dl_client:
            async with dl_client.stream("GET", download_url) as resp:
                resp.raise_for_status()
                with open(file_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        f.write(chunk)

        file_size = file_path.stat().st_size
        now = datetime.now(timezone.utc)

        # Write metadata sidecar JSON
        sidecar = {
            **metadata,
            "downloaded_at": now.isoformat(),
            "download_build": build,
            "local_file_path": str(file_path),
            "file_size_bytes": file_size,
        }
        metadata_path = cache_dir / "metadata.json"
        metadata_path.write_text(json.dumps(sidecar, indent=2, default=str))

        return {
            "pgs_id": pgs_id,
            "build": build,
            "file_path": str(file_path),
            "metadata_json_path": str(metadata_path),
            "file_size_bytes": file_size,
            "downloaded_at": now.isoformat(),
            "metadata": metadata,
        }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
pgs_client = PGSCatalogClient()
