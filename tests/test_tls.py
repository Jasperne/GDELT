import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from gdelt_news.tls import build_certificate_error_message, resolve_ca_bundle


class TLSTests(unittest.TestCase):
    def test_resolve_ca_bundle_accepts_explicit_path(self) -> None:
        with tempfile.NamedTemporaryFile() as handle:
            bundle, source = resolve_ca_bundle(handle.name)

        self.assertEqual(bundle, handle.name)
        self.assertEqual(source, "cli")

    def test_resolve_ca_bundle_uses_environment_variable(self) -> None:
        with tempfile.NamedTemporaryFile() as handle:
            with patch.dict(os.environ, {"GDELT_CA_BUNDLE": handle.name}, clear=False):
                bundle, source = resolve_ca_bundle()

        self.assertEqual(bundle, handle.name)
        self.assertEqual(source, "env:GDELT_CA_BUNDLE")

    def test_build_certificate_error_message_contains_macos_fix_hint(self) -> None:
        message = build_certificate_error_message("the GDELT API", None, None)

        self.assertIn("Install Certificates.command", message)
        self.assertIn("SSL_CERT_FILE=$(python3 -m certifi)", message)


if __name__ == "__main__":
    unittest.main()
