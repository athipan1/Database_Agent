"""Utilities for deterministic FastAPI route migration."""

from __future__ import annotations

from collections.abc import Iterable
from typing import FrozenSet, Tuple

from fastapi import FastAPI


RouteSignature = Tuple[str, FrozenSet[str]]


def route_signature(route) -> RouteSignature:
    return (
        str(getattr(route, "path", "")),
        frozenset(getattr(route, "methods", None) or set()),
    )


def is_http_signature(signature: RouteSignature) -> bool:
    path, methods = signature
    return bool(path and methods)


def method_signatures(signatures: Iterable[tuple[str, str]]) -> set[RouteSignature]:
    return {(path, frozenset({method})) for path, method in signatures}


def copy_missing_routes(
    source: FastAPI,
    target: FastAPI,
    *,
    excluded: set[RouteSignature] | None = None,
    excluded_paths: set[str] | None = None,
) -> None:
    """Copy source routes without duplicating HTTP operations.

    Mount and WebSocket routes do not expose HTTP method sets, so they are
    retained independently instead of being collapsed into an empty signature.
    """

    excluded_signatures = excluded or set()
    paths_to_skip = excluded_paths or set()
    existing = {
        signature
        for route in target.router.routes
        if is_http_signature(signature := route_signature(route))
    }

    for route in source.router.routes:
        signature = route_signature(route)
        path = signature[0]
        if path in paths_to_skip or signature in excluded_signatures:
            continue
        if is_http_signature(signature):
            if signature in existing:
                continue
            existing.add(signature)
        target.router.routes.append(route)


def assert_unique_routes(app: FastAPI) -> None:
    """Raise a readable error when HTTP path/method operations are duplicated."""

    seen: set[RouteSignature] = set()
    duplicates: list[RouteSignature] = []
    for route in app.router.routes:
        signature = route_signature(route)
        if not is_http_signature(signature):
            continue
        if signature in seen:
            duplicates.append(signature)
        seen.add(signature)

    if duplicates:
        rendered = ", ".join(
            f"{path} [{','.join(sorted(methods))}]"
            for path, methods in duplicates
        )
        raise RuntimeError(f"Duplicate API route signatures: {rendered}")
