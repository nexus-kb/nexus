#!/usr/bin/env python3
"""Exercise the mirror wrapper with isolated command stubs; no network or host writes."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


class GrokmirrorAuthenticationTests(unittest.TestCase):
    def test_requests_and_failures(self):
        source = Path(__file__).with_name("run-grokmirror.sh").read_text()
        token = "0123456789abcdef" * 4  # Public test fixture, not a credential.
        cases = [
            ("success", token, 0, 0, 0, 0, 2),
            ("missing", "", 0, 0, 0, 1, 0),
            ("malformed", "bad-token", 0, 0, 0, 1, 0),
            ("lore failure", token, 1, 0, 0, 1, 1),
            ("mainline failure", token, 0, 1, 0, 1, 1),
            ("HTTP failure", token, 0, 0, 22, 1, 2),
        ]
        for name, credential, grok, git, curl, status, requests in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                config = root / "environment"
                config.write_text(f"NEXUS_ADMIN_TOKEN={credential}\n")
                mainline = root / "mainline"
                mainline.mkdir()
                (mainline / "HEAD").touch()
                script = source.replace("/etc/default/nexus", str(config)).replace(
                    "/opt/nexus/mainline.git", str(mainline))
                for command, result in [("grok-pull", grok), ("git", git), ("curl", curl)]:
                    stub = root / command
                    body = f"#!/bin/bash\nprintf '%s\\n' '{command}' >> '{root}/calls'\n"
                    if command == "curl":
                        body += (
                            f"printf '%s\\n' \"$*\" >> '{root}/arguments'\n"
                            f"cat >> '{root}/headers'\n"
                        )
                    stub.write_text(body + f"exit {result}\n")
                    stub.chmod(0o755)
                    script = script.replace(f"/usr/bin/{command}", str(stub))
                wrapper = root / "run.sh"
                wrapper.write_text(script)
                result = subprocess.run(
                    ["bash", str(wrapper)], capture_output=True, text=True,
                    env={"PATH": os.environ["PATH"]}, check=False)
                self.assertEqual(result.returncode, status)
                self.assertNotIn(token, result.stdout + result.stderr)
                if requests:
                    arguments = (root / "arguments").read_text().splitlines()
                    self.assertEqual(len(arguments), requests)
                    for argument in arguments:
                        self.assertIn("--header @-", argument)
                        self.assertNotIn(token, argument)
                    self.assertEqual((root / "headers").read_text().splitlines(),
                                     [f"Authorization: Bearer {token}"] * requests)
                    expected_paths = []
                    if not grok:
                        expected_paths.append("/webhooks/grokmirror")
                    if not git:
                        expected_paths.append("/mainline/sync")
                    for argument, path in zip(arguments, expected_paths):
                        self.assertTrue(argument.endswith("/api/v1/admin" + path))
                else:
                    self.assertFalse((root / "calls").exists())


if __name__ == "__main__":
    unittest.main()
