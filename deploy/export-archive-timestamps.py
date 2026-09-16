#!/usr/bin/env python3
"""Export public-inbox v2 message blob/committer times for timestamp repair.

Read-only; writes CSV to stdout. Run against the same mirrors used by ingestion:
  python3 deploy/export-archive-timestamps.py /opt/nexus/lore > archive-timestamps.csv
Do not use a partial output if this command fails. No mail bodies are exported.
"""

import csv
from pathlib import Path
import subprocess
import sys


def export(root, output):
    repositories = sorted(root.glob("*/git/*.git"))
    if not repositories:
        raise ValueError(f"No public-inbox epoch repositories under {root}")
    writer = csv.writer(output)
    for repository in repositories:
        group = repository.parent.parent.name
        print(f"Scanning {repository}", file=sys.stderr)
        # --root includes the first message; --raw avoids opening message blobs.
        # The m path excludes deletion records (d) and non-message commits.
        command = [
            "git", "-c", f"safe.directory={repository}", "-C", str(repository),
            "log", "--format=%ct", "--raw", "--no-abbrev", "--root",
            "--diff-filter=AM", "--first-parent", "refs/heads/master", "--", "m",
        ]
        with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as process:
            timestamp = None
            for line in process.stdout:
                line = line.strip()
                if not line:
                    continue
                if line.startswith(":"):
                    fields = line.split()
                    if len(fields) != 6 or fields[5] != "m" or timestamp is None:
                        raise ValueError(f"Unexpected git record: {line}")
                    writer.writerow((group, fields[3], timestamp))
                else:
                    timestamp = int(line)
            if process.wait() != 0:
                raise RuntimeError(f"git log failed for {repository}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: export-archive-timestamps.py /opt/nexus/lore")
    export(Path(sys.argv[1]).resolve(), sys.stdout)
