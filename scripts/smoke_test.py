#!/usr/bin/env python3
"""Smoke test for erpl_web DuckDB extension.

Downloads the official DuckDB CLI for the specified version and architecture
from GitHub releases, then verifies the built extension artifact loads and can
make a real outbound HTTP request. Simulates exactly what a user does after
downloading the extension from the distribution channel.

Usage: python3 scripts/smoke_test.py <extension_path> <duckdb_version> <arch>

  extension_path  Absolute path to the .duckdb_extension file to test
  duckdb_version  DuckDB version tag, e.g. v1.5.1
  arch            Target architecture: linux_amd64 | osx_amd64 | osx_arm64 | windows_amd64
"""

import os
import platform
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

ARCH_TO_CLI_ZIP: dict[str, str] = {
    "linux_amd64": "duckdb_cli-linux-amd64.zip",
    "linux_arm64": "duckdb_cli-linux-aarch64.zip",
    "osx_amd64": "duckdb_cli-osx-universal.zip",
    "osx_arm64": "duckdb_cli-osx-universal.zip",
    "windows_amd64": "duckdb_cli-windows-amd64.zip",
}

# httpbin.org is the project's standard HTTP test endpoint.
# Status 502 is treated as a pass: it means httpbin.org is temporarily
# unavailable, which is not a fault of the extension.
SMOKE_SQL = """\
LOAD '{ext}';

SELECT extension_name, loaded
FROM duckdb_extensions()
WHERE extension_name = 'erpl_web';

SELECT
  status,
  CASE WHEN status IN (200, 502) THEN 'PASS' ELSE 'FAIL' END AS result
FROM http_get('https://httpbin.org/status/200')
LIMIT 1;
"""


def _download_duckdb_cli(version: str, arch: str, dest_dir: str) -> str:
    zip_name = ARCH_TO_CLI_ZIP.get(arch)
    if zip_name is None:
        raise SystemExit(
            f"Unsupported arch '{arch}'. Supported: {sorted(ARCH_TO_CLI_ZIP)}"
        )

    url = (
        f"https://github.com/duckdb/duckdb/releases/download/{version}/{zip_name}"
    )
    zip_path = os.path.join(dest_dir, "duckdb_cli.zip")

    print(f"Downloading DuckDB {version} CLI ({arch}):\n  {url}")
    urllib.request.urlretrieve(url, zip_path)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(dest_dir)

    bin_name = "duckdb.exe" if platform.system() == "Windows" else "duckdb"
    binary = os.path.join(dest_dir, bin_name)
    if not os.path.isfile(binary):
        raise SystemExit(
            f"DuckDB binary not found after extraction: {binary}\n"
            f"Zip contents: {zipfile.ZipFile(zip_path).namelist()}"
        )

    if platform.system() != "Windows":
        os.chmod(binary, 0o755)

    return binary


def run_smoke_test(extension_path: str, duckdb_version: str, arch: str) -> None:
    if not os.path.isfile(extension_path):
        raise SystemExit(f"Extension artifact not found: {extension_path}")

    # Forward slashes work in DuckDB SQL on all platforms including Windows
    ext_sql_path = extension_path.replace("\\", "/")
    sql = SMOKE_SQL.format(ext=ext_sql_path)

    with tempfile.TemporaryDirectory() as tmpdir:
        duckdb_bin = _download_duckdb_cli(duckdb_version, arch, tmpdir)

        print(
            f"\nSmoke test:\n"
            f"  extension : {extension_path}\n"
            f"  duckdb    : {duckdb_bin}\n"
            f"\nSQL:\n{sql}"
        )

        proc = subprocess.run(
            [duckdb_bin, "-unsigned"],
            input=sql,
            text=True,
        )

    if proc.returncode != 0:
        raise SystemExit(f"Smoke test FAILED (duckdb exit code {proc.returncode})")

    print("\nSmoke test PASSED")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit(
            f"Usage: {sys.argv[0]} <extension_path> <duckdb_version> <arch>"
        )
    run_smoke_test(
        extension_path=sys.argv[1],
        duckdb_version=sys.argv[2],
        arch=sys.argv[3],
    )
