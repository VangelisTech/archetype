# Copyright 2026 Vangelis Technologies Inc.
# SPDX-License-Identifier: Apache-2.0

"""
Chronicle Loaders
=================

Source-specific readers that produce ``(Artifact, <Kind>)`` component
pairs ready for ``world.create_entity()``.  Each loader follows the
``experiments/loaders.py`` pattern: a pure function that reads an
external source and returns component instances, plus an async wrapper
that spawns entities into a world.

Loaders skip gracefully when their source is unavailable (missing path,
uninstalled optional dep, macOS permission denied) so ``ingest_all``
can be called unconditionally.
"""

from __future__ import annotations

import json
import logging
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from archetype.chronicle.components import (
    Artifact,
    ChatMessage,
    Photo,
    Video,
)

if TYPE_CHECKING:
    from archetype.core.aio import AsyncWorld

logger = logging.getLogger("archetype.chronicle")

# ============================================================================
# Helpers
# ============================================================================


def _iso_to_ms(iso_str: str | None) -> int:
    """Parse an ISO-8601 timestamp to Unix milliseconds.  Returns 0 on failure."""
    if not iso_str:
        return 0
    try:
        dt = datetime.fromisoformat(iso_str)
        return int(dt.timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def _extract_text_from_content(content: Any) -> str:
    """Extract human-readable text from a Claude message content field.

    Handles both plain strings (user messages) and Anthropic-style
    content block lists (assistant messages with text/tool_use/thinking).
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if not isinstance(block, dict):
                continue
            btype = block.get("type", "")
            if btype == "text":
                parts.append(block.get("text", ""))
            elif btype == "tool_use":
                parts.append(f"[tool: {block.get('name', '?')}]")
            elif btype == "tool_result":
                # Tool results can contain nested content blocks
                nested = block.get("content", "")
                if isinstance(nested, str):
                    parts.append(nested)
                elif isinstance(nested, list):
                    for nb in nested:
                        if isinstance(nb, dict) and nb.get("type") == "text":
                            parts.append(nb.get("text", ""))
            # Skip thinking blocks — they bloat the archive
        return "\n".join(parts)
    return str(content) if content else ""


# ============================================================================
# Claude Code JSONL
# ============================================================================


def _default_claude_code_dir() -> Path:
    return Path.home() / ".claude" / "projects"


def load_claude_code_sessions(
    projects_dir: str | Path | None = None,
) -> list[list]:
    """Read Claude Code JSONL session files and return component pairs.

    Each ``.jsonl`` file under ``projects_dir`` is one session.
    Returns ``[[Artifact, ChatMessage], ...]`` — one pair per session
    that contains at least one user or assistant turn.

    Args:
        projects_dir: Root of Claude Code project dirs.  Defaults to
            ``~/.claude/projects``.

    Returns:
        List of ``[Artifact, ChatMessage]`` pairs.

    Raises:
        FileNotFoundError: If ``projects_dir`` does not exist.
    """
    root = Path(projects_dir) if projects_dir else _default_claude_code_dir()
    if not root.exists():
        raise FileNotFoundError(f"Claude Code projects dir not found: {root}")

    results: list[list] = []
    for jsonl_path in sorted(root.rglob("*.jsonl")):
        pair = _parse_claude_code_session(jsonl_path)
        if pair is not None:
            results.append(pair)

    return results


def _parse_claude_code_session(jsonl_path: Path) -> list | None:
    """Parse a single Claude Code JSONL file into an (Artifact, ChatMessage) pair.

    Returns None if the file has no user/assistant turns.
    """
    turns: list[dict[str, Any]] = []
    session_id = jsonl_path.stem
    first_ts: int = 0
    last_model: str = ""
    cwd: str = ""
    git_branch: str = ""

    try:
        text = jsonl_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        logger.warning("Skipping unreadable session %s: %s", jsonl_path, exc)
        return None

    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue

        rtype = rec.get("type")
        if rtype not in ("user", "assistant"):
            continue

        ts = _iso_to_ms(rec.get("timestamp"))
        if first_ts == 0 and ts > 0:
            first_ts = ts
        if not cwd:
            cwd = rec.get("cwd", "")
        if not git_branch:
            git_branch = rec.get("gitBranch", "")

        msg = rec.get("message", {})
        if not isinstance(msg, dict):
            continue

        role = msg.get("role", rtype)
        content_raw = msg.get("content", "")
        content_text = _extract_text_from_content(content_raw)

        usage = msg.get("usage", {})
        tokens_in = usage.get("input_tokens", 0) or 0
        tokens_out = usage.get("output_tokens", 0) or 0

        model = msg.get("model", "")
        if model:
            last_model = model

        turns.append(
            {
                "role": role,
                "text": content_text,
                "uuid": rec.get("uuid", ""),
                "parent_uuid": rec.get("parentUuid") or "",
                "timestamp_ms": ts,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "model": model,
            }
        )

    if not turns:
        return None

    # Title: first user message, truncated
    title = ""
    for t in turns:
        if t["role"] == "user" and t["text"]:
            title = t["text"][:120]
            break

    artifact = Artifact.make(
        kind="chat_message",
        source="claude_code",
        source_id=session_id,
        created_at_ms=first_ts,
        uri=str(jsonl_path),
    )
    chat = ChatMessage.make(
        conversation_id=session_id,
        turns=turns,
        title=title,
        model=last_model,
        cwd=cwd,
        git_branch=git_branch,
        metadata={"project_dir": jsonl_path.parent.name},
    )
    return [artifact, chat]


# ============================================================================
# Claude.ai Export
# ============================================================================


def load_claude_ai_export(
    export_path: str | Path,
) -> list[list]:
    """Read a claude.ai data export and return component pairs.

    Accepts either a directory (unzipped export) or a path to
    ``conversations.json`` directly.

    Returns ``[[Artifact, ChatMessage], ...]``.
    """
    path = Path(export_path)
    if path.is_dir():
        conv_file = path / "conversations.json"
    else:
        conv_file = path

    if not conv_file.exists():
        raise FileNotFoundError(f"conversations.json not found at {conv_file}")

    data = json.loads(conv_file.read_text(encoding="utf-8"))
    results: list[list] = []

    for conv in data:
        pair = _parse_claude_ai_conversation(conv)
        if pair is not None:
            results.append(pair)

    return results


def _parse_claude_ai_conversation(conv: dict) -> list | None:
    """Parse a single claude.ai conversation dict."""
    conv_id = conv.get("uuid", conv.get("id", ""))
    title = conv.get("name", conv.get("title", ""))
    created = conv.get("created_at", conv.get("create_time", ""))
    created_ms = (
        _iso_to_ms(created) if isinstance(created, str) else int(created * 1000) if created else 0
    )

    chat_messages = conv.get("chat_messages", [])
    if not chat_messages:
        return None

    turns: list[dict[str, Any]] = []
    last_model = ""

    for msg in chat_messages:
        role = msg.get("sender", msg.get("role", ""))
        # claude.ai uses "human" / "assistant"
        if role == "human":
            role = "user"

        content_raw = msg.get("text", msg.get("content", ""))
        content_text = _extract_text_from_content(content_raw)
        if not content_text:
            continue

        ts = msg.get("created_at", msg.get("create_time", ""))
        ts_ms = _iso_to_ms(ts) if isinstance(ts, str) else int(ts * 1000) if ts else 0

        model = msg.get("model", msg.get("model_slug", ""))
        if model:
            last_model = model

        turns.append(
            {
                "role": role,
                "text": content_text,
                "timestamp_ms": ts_ms,
                "tokens_in": 0,
                "tokens_out": 0,
                "model": model,
            }
        )

    if not turns:
        return None

    artifact = Artifact.make(
        kind="chat_message",
        source="claude_ai",
        source_id=conv_id,
        created_at_ms=created_ms,
        uri=f"claude.ai/conversation/{conv_id}",
    )
    chat = ChatMessage.make(
        conversation_id=conv_id,
        turns=turns,
        title=title or (turns[0]["text"][:120] if turns else ""),
        model=last_model,
    )
    return [artifact, chat]


# ============================================================================
# ChatGPT Export
# ============================================================================


def load_chatgpt_export(
    export_path: str | Path,
) -> list[list]:
    """Read a ChatGPT data export and return component pairs.

    Accepts either a directory (unzipped export) or a path to
    ``conversations.json`` directly.

    Returns ``[[Artifact, ChatMessage], ...]``.
    """
    path = Path(export_path)
    if path.is_dir():
        conv_file = path / "conversations.json"
    else:
        conv_file = path

    if not conv_file.exists():
        raise FileNotFoundError(f"conversations.json not found at {conv_file}")

    data = json.loads(conv_file.read_text(encoding="utf-8"))
    results: list[list] = []

    for conv in data:
        pair = _parse_chatgpt_conversation(conv)
        if pair is not None:
            results.append(pair)

    return results


def _parse_chatgpt_conversation(conv: dict) -> list | None:
    """Parse a single ChatGPT conversation dict.

    ChatGPT exports use a tree structure via ``mapping`` — a dict of
    node_id -> {message, parent, children}.  We flatten to a linear
    sequence by following the first-child path from root to leaf.
    """
    conv_id = conv.get("id", conv.get("conversation_id", ""))
    title = conv.get("title", "")
    create_time = conv.get("create_time")
    created_ms = int(create_time * 1000) if create_time else 0

    mapping = conv.get("mapping", {})
    if not mapping:
        return None

    # Find root (node with no parent or parent not in mapping)
    root_id = None
    for nid, node in mapping.items():
        parent = node.get("parent")
        if parent is None or parent not in mapping:
            root_id = nid
            break
    if root_id is None:
        return None

    # Walk first-child path
    turns: list[dict[str, Any]] = []
    last_model = ""
    current = root_id
    visited: set[str] = set()

    while current and current not in visited:
        visited.add(current)
        node = mapping.get(current, {})
        msg = node.get("message")

        if msg and isinstance(msg, dict):
            author = msg.get("author", {})
            role = author.get("role", "") if isinstance(author, dict) else ""
            if role in ("user", "assistant", "system"):
                content = msg.get("content", {})
                parts = content.get("parts", []) if isinstance(content, dict) else []
                text = "\n".join(str(p) for p in parts if isinstance(p, str))

                if text.strip():
                    meta = msg.get("metadata", {})
                    model = meta.get("model_slug", "") if isinstance(meta, dict) else ""
                    if model:
                        last_model = model

                    ts = msg.get("create_time")
                    ts_ms = int(ts * 1000) if ts else 0

                    turns.append(
                        {
                            "role": role,
                            "text": text,
                            "timestamp_ms": ts_ms,
                            "tokens_in": 0,
                            "tokens_out": 0,
                            "model": model,
                        }
                    )

        children = node.get("children", [])
        current = children[0] if children else None

    if not turns:
        return None

    artifact = Artifact.make(
        kind="chat_message",
        source="chatgpt",
        source_id=conv_id,
        created_at_ms=created_ms,
        uri=f"chatgpt/conversation/{conv_id}",
    )
    chat = ChatMessage.make(
        conversation_id=conv_id,
        turns=turns,
        title=title or (turns[0]["text"][:120] if turns else ""),
        model=last_model,
    )
    return [artifact, chat]


# ============================================================================
# Apple Photos (osxphotos primary, folder fallback)
# ============================================================================

_PHOTO_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".heic",
    ".heif",
    ".tiff",
    ".tif",
    ".webp",
    ".gif",
    ".bmp",
}
_VIDEO_EXTENSIONS = {".mov", ".mp4", ".m4v", ".avi", ".mkv", ".wmv", ".webm"}


def load_apple_photos(
    library_path: str | Path | None = None,
) -> list[list]:
    """Read Apple Photos library and return component pairs.

    Tries ``osxphotos`` first for rich metadata (albums, faces, GPS).
    Falls back to walking the library's ``originals/`` folder (or a
    user-specified export folder) with EXIF extraction via Pillow.

    Returns ``[[Artifact, Photo], ...]`` and ``[[Artifact, Video], ...]``.
    """
    try:
        return _load_via_osxphotos(library_path)
    except (ImportError, PermissionError, OSError) as exc:
        logger.info("osxphotos unavailable (%s), falling back to folder walk", exc)

    # Fallback: walk a directory of exported photos/videos
    folder = Path(library_path) if library_path else _default_photos_folder()
    if folder is None or not folder.exists():
        raise FileNotFoundError(
            "No Apple Photos library or export folder found. "
            "Install osxphotos and grant Full Disk Access, "
            "or pass --apple-photos-dir to a folder of exported media."
        )
    return _load_via_folder(folder)


def _default_photos_folder() -> Path | None:
    """Try the default Photos Library originals path."""
    lib = Path.home() / "Pictures" / "Photos Library.photoslibrary" / "originals"
    return lib if lib.exists() else None


def _load_via_osxphotos(library_path: str | Path | None) -> list[list]:
    """Load via osxphotos (rich metadata)."""
    import osxphotos  # type: ignore[import-untyped]

    kwargs: dict[str, Any] = {}
    if library_path:
        kwargs["dbfile"] = str(library_path)
    photosdb = osxphotos.PhotosDB(**kwargs)

    results: list[list] = []
    for photo in photosdb.photos():
        is_video = photo.ismovie
        created_ms = int(photo.date.timestamp() * 1000) if photo.date else 0
        source_id = photo.uuid or ""
        path = photo.path or ""

        artifact = Artifact.make(
            kind="video" if is_video else "photo",
            source="apple_photos",
            source_id=source_id,
            created_at_ms=created_ms,
            uri=path,
        )

        albums = [a.title for a in photo.album_info] if photo.album_info else []
        persons = list(photo.persons) if photo.persons else []
        lat = photo.latitude or 0.0
        lon = photo.longitude or 0.0

        if is_video:
            component = Video(
                path=path,
                width=photo.width or 0,
                height=photo.height or 0,
                duration_ms=int((photo.duration or 0) * 1000),
                mime=mimetypes.guess_type(path)[0] or "video/quicktime",
                file_size=photo.original_filesize or 0,
                gps_lat=lat,
                gps_lon=lon,
                album_names_json=json.dumps(albums),
            )
        else:
            component = Photo(
                path=path,
                width=photo.width or 0,
                height=photo.height or 0,
                mime=mimetypes.guess_type(path)[0] or "image/jpeg",
                file_size=photo.original_filesize or 0,
                gps_lat=lat,
                gps_lon=lon,
                camera_make=photo.exif_info.camera_make if photo.exif_info else "",
                camera_model=photo.exif_info.camera_model if photo.exif_info else "",
                favorite=photo.favorite,
                hidden=photo.hidden,
                album_names_json=json.dumps(albums),
                persons_json=json.dumps(persons),
            )

        results.append([artifact, component])

    return results


def _load_via_folder(folder: Path) -> list[list]:
    """Fallback: walk a folder and extract EXIF via Pillow."""
    results: list[list] = []

    for path in sorted(folder.rglob("*")):
        if not path.is_file():
            continue
        ext = path.suffix.lower()

        if ext in _PHOTO_EXTENSIONS:
            pair = _photo_from_file(path)
            if pair:
                results.append(pair)
        elif ext in _VIDEO_EXTENSIONS:
            pair = _video_from_file(path)
            if pair:
                results.append(pair)

    return results


def _photo_from_file(path: Path) -> list | None:
    """Build (Artifact, Photo) from a file on disk using Pillow EXIF."""
    try:
        stat = path.stat()
    except OSError:
        return None

    created_ms = (
        int(stat.st_birthtime * 1000)
        if hasattr(stat, "st_birthtime")
        else int(stat.st_mtime * 1000)
    )
    width, height = 0, 0
    gps_lat, gps_lon = 0.0, 0.0
    camera_make, camera_model = "", ""

    try:
        from PIL import Image
        from PIL.ExifTags import GPS, Base

        with Image.open(path) as img:
            width, height = img.size
            exif = img.getexif()
            if exif:
                camera_make = str(exif.get(Base.Make, ""))
                camera_model = str(exif.get(Base.Model, ""))
                gps_info = exif.get_ifd(GPS.IFD)
                if gps_info:
                    gps_lat, gps_lon = _parse_gps(gps_info)

            # Try EXIF DateTimeOriginal for better timestamp
            dt_orig = exif.get(Base.DateTimeOriginal, "") if exif else ""
            if dt_orig:
                try:
                    dt = datetime.strptime(dt_orig, "%Y:%m:%d %H:%M:%S")
                    created_ms = int(dt.timestamp() * 1000)
                except ValueError:
                    pass
    except Exception:
        pass  # Pillow not available or file unreadable — use stat data

    artifact = Artifact.make(
        kind="photo",
        source="folder",
        source_id=str(path),
        created_at_ms=created_ms,
        uri=str(path),
    )
    photo = Photo(
        path=str(path),
        width=width,
        height=height,
        mime=mimetypes.guess_type(str(path))[0] or "",
        file_size=stat.st_size,
        gps_lat=gps_lat,
        gps_lon=gps_lon,
        camera_make=camera_make,
        camera_model=camera_model,
    )
    return [artifact, photo]


def _video_from_file(path: Path) -> list | None:
    """Build (Artifact, Video) from a video file on disk."""
    try:
        stat = path.stat()
    except OSError:
        return None

    created_ms = (
        int(stat.st_birthtime * 1000)
        if hasattr(stat, "st_birthtime")
        else int(stat.st_mtime * 1000)
    )

    artifact = Artifact.make(
        kind="video",
        source="folder",
        source_id=str(path),
        created_at_ms=created_ms,
        uri=str(path),
    )
    video = Video(
        path=str(path),
        mime=mimetypes.guess_type(str(path))[0] or "",
        file_size=stat.st_size,
    )
    return [artifact, video]


def _parse_gps(gps_info: dict) -> tuple[float, float]:
    """Convert EXIF GPS IFD to (lat, lon) floats."""
    try:
        from PIL.ExifTags import GPS

        lat_dms = gps_info.get(GPS.GPSLatitude)
        lat_ref = gps_info.get(GPS.GPSLatitudeRef, "N")
        lon_dms = gps_info.get(GPS.GPSLongitude)
        lon_ref = gps_info.get(GPS.GPSLongitudeRef, "E")

        if not lat_dms or not lon_dms:
            return 0.0, 0.0

        lat = _dms_to_decimal(lat_dms)
        lon = _dms_to_decimal(lon_dms)
        if lat_ref == "S":
            lat = -lat
        if lon_ref == "W":
            lon = -lon
        return lat, lon
    except Exception:
        return 0.0, 0.0


def _dms_to_decimal(dms: tuple) -> float:
    """Convert (degrees, minutes, seconds) to decimal degrees."""
    d, m, s = float(dms[0]), float(dms[1]), float(dms[2])
    return d + m / 60.0 + s / 3600.0


# ============================================================================
# Orchestrator
# ============================================================================


async def ingest_all(
    world: AsyncWorld,
    *,
    claude_code_dir: str | Path | None = None,
    claude_ai_export: str | Path | None = None,
    chatgpt_export: str | Path | None = None,
    apple_photos_library: str | Path | None = None,
    apple_photos_dir: str | Path | None = None,
) -> dict[str, int]:
    """Run all available loaders and spawn entities into the world.

    Each source is attempted independently; failures are logged and
    skipped.  Returns a dict of ``{source: count}`` for successfully
    ingested items.

    Args:
        world: The chronicle world to ingest into.
        claude_code_dir: Override for ``~/.claude/projects``.
        claude_ai_export: Path to claude.ai export (ZIP dir or
            conversations.json).
        chatgpt_export: Path to ChatGPT export (ZIP dir or
            conversations.json).
        apple_photos_library: Path to Photos Library (for osxphotos).
            Pass ``None`` to use default.
        apple_photos_dir: Path to an exported photos folder (fallback
            when osxphotos is unavailable).
    """
    counts: dict[str, int] = {}

    # Claude Code
    try:
        pairs = load_claude_code_sessions(claude_code_dir)
        for pair in pairs:
            await world.create_entity(pair)
        counts["claude_code"] = len(pairs)
        logger.info("Ingested %d Claude Code sessions", len(pairs))
    except FileNotFoundError:
        logger.info("Claude Code sessions not found, skipping")
    except Exception:
        logger.exception("Failed to ingest Claude Code sessions")

    # Claude.ai
    if claude_ai_export:
        try:
            pairs = load_claude_ai_export(claude_ai_export)
            for pair in pairs:
                await world.create_entity(pair)
            counts["claude_ai"] = len(pairs)
            logger.info("Ingested %d Claude.ai conversations", len(pairs))
        except Exception:
            logger.exception("Failed to ingest Claude.ai export")

    # ChatGPT
    if chatgpt_export:
        try:
            pairs = load_chatgpt_export(chatgpt_export)
            for pair in pairs:
                await world.create_entity(pair)
            counts["chatgpt"] = len(pairs)
            logger.info("Ingested %d ChatGPT conversations", len(pairs))
        except Exception:
            logger.exception("Failed to ingest ChatGPT export")

    # Apple Photos
    photos_source = apple_photos_library or apple_photos_dir
    if photos_source is not None or _default_photos_folder():
        try:
            pairs = load_apple_photos(photos_source)
            for pair in pairs:
                await world.create_entity(pair)
            counts["apple_photos"] = len(pairs)
            logger.info("Ingested %d photos/videos", len(pairs))
        except FileNotFoundError:
            logger.info("Apple Photos not found, skipping")
        except Exception:
            logger.exception("Failed to ingest Apple Photos")

    return counts
