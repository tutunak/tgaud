"""The wheel carries both packages and their SQL files; installed outside the checkout, both launchers run and
`tgaud --selfcheck` passes in an empty environment."""

import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def run(cmd, **kw):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=600, **kw)
    assert r.returncode == 0, f"{cmd}: {r.stdout}\n{r.stderr}"
    return r


def test_wheel_installs_and_runs_outside_the_checkout(tmp_path):
    uv = shutil.which("uv")
    assert uv, "uv is required"
    dist = tmp_path / "dist"
    run([uv, "build", "--wheel", "--out-dir", str(dist), str(REPO)])
    wheel = next(dist.glob("*.whl"))
    names = zipfile.ZipFile(wheel).namelist()
    for path in ("tgaud/sql/queries.sql", "tgaud/sql/admin.sql", "tgaud/sql/migrations/001_initial.sql",
                 "tgaud/sql/migrations/002_shared.sql", "tgaud/jobs/ingest.py", "tgstats/cli.py"):
        assert path in names, path
    venv = tmp_path / "venv"
    run([uv, "venv", "--python", sys.executable, str(venv)])
    run([uv, "pip", "install", "--python", str(venv / "bin" / "python"), str(wheel)])
    bin_ = venv / "bin"
    run([str(bin_ / "tgstats"), "--help"], cwd=tmp_path)
    run([str(bin_ / "tgaud"), "--help"], cwd=tmp_path)
    r = run([str(bin_ / "tgaud"), "--selfcheck"], cwd=tmp_path, env={})
    assert r.stdout.startswith("selfcheck ok:")
    where = run([str(bin_ / "python"), "-c", "import tgaud; print(tgaud.__file__)"], cwd=tmp_path, env={})
    assert str(venv) in where.stdout
