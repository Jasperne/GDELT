from __future__ import annotations

import os
import ssl
import sys
from pathlib import Path

CA_BUNDLE_ENV_VARS = ("GDELT_CA_BUNDLE", "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE")


def configure_ssl_context(ca_bundle: str | None = None) -> tuple[ssl.SSLContext, str | None, str | None]:
    resolved_bundle, bundle_source = resolve_ca_bundle(ca_bundle)
    if resolved_bundle:
        return ssl.create_default_context(cafile=resolved_bundle), resolved_bundle, bundle_source
    return ssl.create_default_context(), None, None


def resolve_ca_bundle(ca_bundle: str | None = None) -> tuple[str | None, str | None]:
    if ca_bundle and ca_bundle.strip():
        return _validate_ca_bundle_path(ca_bundle), "cli"

    for env_var in CA_BUNDLE_ENV_VARS:
        value = os.environ.get(env_var)
        if value and value.strip():
            return _validate_ca_bundle_path(value), f"env:{env_var}"

    try:
        import certifi
    except ImportError:
        return None, None

    return _validate_ca_bundle_path(certifi.where()), "certifi"


def is_certificate_verification_error(exc: Exception) -> bool:
    reason = getattr(exc, "reason", None)
    return isinstance(exc, ssl.SSLCertVerificationError) or isinstance(
        reason,
        ssl.SSLCertVerificationError,
    ) or "CERTIFICATE_VERIFY_FAILED" in str(exc)


def build_certificate_error_message(
    target: str,
    ca_bundle: str | None,
    ca_bundle_source: str | None,
) -> str:
    install_certificates_command = (
        f'open "/Applications/Python {sys.version_info.major}.{sys.version_info.minor}/Install Certificates.command"'
    )
    bundle_hint = (
        f" Active CA bundle: {ca_bundle} ({ca_bundle_source})."
        if ca_bundle
        else " No CA bundle is configured yet."
    )
    return (
        f"TLS certificate verification failed while connecting to {target}.{bundle_hint} "
        f"If you are using the Python.org macOS build, run {install_certificates_command}. "
        f"Alternatively rerun with --ca-bundle $(python3 -m certifi) or set "
        f"SSL_CERT_FILE=$(python3 -m certifi)."
    )


def _validate_ca_bundle_path(value: str) -> str:
    path = Path(value).expanduser()
    if not path.is_file():
        raise ValueError(f"CA bundle file does not exist: {path}")
    return str(path)
