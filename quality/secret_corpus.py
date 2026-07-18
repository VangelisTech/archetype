"""Synthetic credential corpus shared by redaction tests and capability evals.

Values are assembled at runtime so repository secret scanners do not mistake
the corpus for live credentials. None of these values grants access anywhere.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SecretLeakCase:
    name: str
    payload: str
    rule_id: str


SECRET_LEAK_CORPUS = (
    SecretLeakCase(
        "codex-openai-api-key",
        "OPENAI_API_KEY=" + "sk-proj-" + "A" * 32,
        "sensitive-assignment",
    ),
    SecretLeakCase(
        "claude-anthropic-api-key",
        "ANTHROPIC_API_KEY=" + "sk-ant-api03-" + "B" * 32,
        "sensitive-assignment",
    ),
    SecretLeakCase(
        "github-token",
        "clone credential " + "ghp_" + "C" * 36,
        "github-token",
    ),
    SecretLeakCase(
        "modal-token-secret",
        "MODAL_ENDPOINT_TOKEN_SECRET=" + "as-" + "D" * 32,
        "sensitive-assignment",
    ),
    SecretLeakCase(
        "modal-token-id",
        "request token " + "ak-" + "E" * 32,
        "modal-token",
    ),
    SecretLeakCase(
        "openrouter-api-key",
        "router=" + "sk-or-v1-" + "F" * 32,
        "openrouter-api-key",
    ),
    SecretLeakCase(
        "aws-access-key",
        "AWS_ACCESS_KEY_ID=" + "AKIA" + "G" * 16,
        "aws-access-key-id",
    ),
    SecretLeakCase(
        "google-api-key",
        "google=" + "AIza" + "H" * 35,
        "google-api-key",
    ),
    SecretLeakCase(
        "codex-oauth-refresh-token",
        '{"refresh_' + 'token":"codex-refresh-' + "I" * 32 + '"}',
        "sensitive-assignment",
    ),
    SecretLeakCase(
        "claude-oauth-access-token",
        '{"access_' + 'token":"claude-access-' + "J" * 32 + '"}',
        "sensitive-assignment",
    ),
    SecretLeakCase(
        "generic-bearer",
        "Authorization: Bearer " + "K" * 40,
        "authorization-header",
    ),
    SecretLeakCase(
        "signed-url",
        "https://objects.invalid/file?X-Amz-Signature=" + "L" * 64,
        "signed-url-query",
    ),
    SecretLeakCase(
        "azure-account-key",
        "AccountKey=" + "M" * 48,
        "sensitive-assignment",
    ),
    SecretLeakCase(
        "generic-cli-key",
        "agent --api-key " + "N" * 40,
        "cli-secret-argument",
    ),
    SecretLeakCase(
        "private-key",
        "-----BEGIN "
        + "PRIVATE KEY-----\n"
        + "TUlJRXZBSUJBREFOQmdrcWhraUc5dzBCQVFFRkFBU0M="
        + "\n-----END PRIVATE KEY-----",
        "private-key",
    ),
)


SAFE_REDACTION_CORPUS = (
    "OPENAI_API_KEY=sk-...",
    '"Modal-Secret":"{env:MODAL_ENDPOINT_TOKEN_SECRET}"',
    '"authorization":"<redacted:authorization-header>"',
    "idempotency_key=attempt-0001",
    "content_hash=" + "a" * 64,
    "No credentials are present in this session log.",
)
