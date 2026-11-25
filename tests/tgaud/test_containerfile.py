"""The image's static contract (docs/deploy-contract.md, Image), read from the files: `mise run image-test` builds and
runs the image itself."""

import ast
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def read(name):
    return (ROOT / name).read_text("utf-8")


def test_entrypoint_and_cmd():
    text = read("Containerfile")
    assert re.search(r'^ENTRYPOINT \["tgaud"\]$', text, re.M)
    assert re.search(r'^CMD \["serve"\]$', text, re.M)


def test_no_user_line():
    # Rootless Podman maps uid 0 in the container to the host's tgaud account; a USER line would break that.
    assert not re.search(r"^\s*USER\s", read("Containerfile"), re.M)


def test_base_image_is_pinned_by_digest():
    assert re.search(r"python:3\.13-slim@sha256:[0-9a-f]{64}", read("Containerfile"))


def test_unbuffered_output_and_no_volume():
    # No volume: all state is in PostgreSQL. PYTHONDONTWRITEBYTECODE keeps the read-only root file system quiet.
    text = read("Containerfile")
    assert "PYTHONUNBUFFERED=1" in text
    assert "PYTHONDONTWRITEBYTECODE=1" in text
    assert not re.search(r"^VOLUME\b", text, re.M)


def test_final_stage_copies_only_the_virtualenv():
    final = read("Containerfile").split("\nFROM ")[-1]
    copies = [line for line in final.splitlines() if line.startswith("COPY ")]
    assert copies == ["COPY --from=build /opt/tgaud /opt/tgaud"]


def test_install_uses_the_lock_without_dev_dependencies():
    text = read("Containerfile")
    assert "uv sync --locked --no-dev" in text
    assert "--no-editable" in text


def test_containerignore_keeps_secrets_state_tests_and_docs_out():
    patterns = {line.strip() for line in read(".containerignore").splitlines()
                if line.strip() and not line.startswith("#")}
    for required in ("**/.env", "**/*.env", "**/*.session", "**/*.db", "**/.venv", "tests", "docs"):
        assert required in patterns, required


def test_dockerignore_is_the_containerignore():
    # BuildKit reads .dockerignore, podman .containerignore: one file, so the two builders get the same context
    # (scripts/release/test-release.sh builds the repo with buildx and checks what arrives).
    link = ROOT / ".dockerignore"
    assert link.is_symlink() and link.readlink() == Path(".containerignore")
    assert link.read_text("utf-8") == read(".containerignore")


def jobs(workflow):
    """The text of each job of a workflow, by name (the jobs are the two-space-indented keys under `jobs:`)."""
    text = read(f".github/workflows/{workflow}")
    body = text.split("\njobs:\n", 1)[1]
    parts = re.split(r"^  ([a-z][a-z-]*):\n", body, flags=re.M)
    return dict(zip(parts[1::2], parts[2::2]))


def test_forgejo_pipeline_is_gone():
    assert not (ROOT / ".forgejo").exists()


def test_ci_runs_on_pull_requests_to_main_only():
    text = read(".github/workflows/ci.yml")
    assert re.search(r'^"on":\n  pull_request:\n    branches: \[main\]\n\n', text, re.M)
    assert not re.search(r"^  (push|schedule|workflow_dispatch|pull_request_target):", text, re.M)
    assert re.search(r"^permissions:\n  contents: read\n", text, re.M)


def test_images_are_pushed_only_by_publishing_after_every_test_job():
    ci = jobs("ci.yml")
    assert set(ci) == {"lint", "tests", "container", "release-scripts", "publishing"}
    publishing = ci.pop("publishing")
    needs = re.search(r"^    needs: \[(.*)\]$", publishing, re.M).group(1)
    assert {n.strip() for n in needs.split(",")} == set(ci)
    assert "packages: write" in publishing
    assert "uses: docker/login-action@v4" in publishing
    assert "password: ${{ secrets.GITHUB_TOKEN }}" in publishing
    assert re.search(r"^        run: bash scripts/release/publish-images\.sh ghcr\.io/\$\{\{ github\.repository \}\}$",
                     publishing, re.M)
    for name, text in ci.items():
        assert "packages:" not in text and "login-action" not in text, name
        assert "publish-images" not in text and "docker push" not in text and "--push" not in text, name
    assert (ROOT / "scripts/release/images.txt").read_text("utf-8").splitlines()[-1] == "|.|Containerfile|"


def test_release_pushes_no_image_build():
    text = read(".github/workflows/release.yml")
    assert re.search(r'^"on":\n  push:\n    branches: \[main\]\n', text, re.M)
    assert re.search(r"^concurrency:\n  group: release\n  queue: max\n", text, re.M)
    assert not re.search(r"^\s*cancel-in-progress:", text, re.M)
    assert set(jobs("release.yml")) == {"release"}
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    for forbidden in ("publish-images", "buildx build", "setup-buildx", "build-push-action", "docker push", "--push",
                      "pytest", "podman"):
        assert forbidden not in code, forbidden
    assert re.search(r"^        run: bash scripts/release/release\.sh ghcr\.io/\$\{\{ github\.repository \}\}$", text, re.M)
    # retagging is by manifest and keeps the digest of the tree tag
    assert "docker buildx imagetools create --prefer-index=false --tag" in read("scripts/release/release.sh")
    assert ".github/workflows/release.yml" in read(".github/actionlint.yaml")


def test_ci_gate_runs_the_pushed_build_before_the_push():
    publish = read("scripts/release/publish-images.sh")
    build = publish.index("docker buildx build --load -t")
    gate = publish.index('"$RELEASE_DIR/gate-image.sh" "$image:$tag"')
    push = publish.index('docker push "$image:$tag"')
    assert build < gate < push
    assert "--push" not in publish
    gate_script = read("scripts/release/gate-image.sh")
    assert ("docker run --rm --network=none --read-only --tmpfs /tmp --cap-drop=all --security-opt=no-new-privileges "
            '"$1" --selfcheck') in gate_script
    assert '[[ $out == "selfcheck ok: "* && $out != *$\'\\n\'* ]]' in gate_script


def test_ci_database_tests_are_not_skipped():
    text = read(".github/workflows/ci.yml")
    assert "TGAUD_TEST_NO_DATABASE" not in text
    # a service container can't take the server arguments the timing tests need
    assert not re.search(r"^\s*services:", text, re.M)
    tests = jobs("ci.yml")["tests"]
    assert tests.index("bash scripts/ci-postgres.sh") < tests.index("uv run pytest -rs")
    assert "uv sync --locked" in tests
    container = jobs("ci.yml")["container"]
    assert container.index("bash scripts/ci-postgres.sh") < container.index("bash scripts/container-e2e.sh")
    assert "bash scripts/image-test.sh" in container


def test_ci_postgres_has_the_limits_of_the_local_fixture():
    # The script reads IMAGE and SERVER_SETTINGS from conftest.py and checks the cap and every setting is in effect.
    script = read("scripts/ci-postgres.sh")
    assert "tests/tgaud/conftest.py" in script and '"IMAGE", "SERVER_SETTINGS"' in script
    assert re.search(r'--memory 512m "\$image" "\$\{settings\[@\]\}"', script)
    assert "536870912" in script and "SHOW $key" in script
    assigned = {t.targets[0].id: ast.literal_eval(t.value)
                for t in ast.parse(read("tests/tgaud/conftest.py")).body
                if isinstance(t, ast.Assign) and isinstance(t.targets[0], ast.Name)
                and t.targets[0].id in ("IMAGE", "SERVER_SETTINGS")}
    assert assigned["IMAGE"] == "docker.io/library/postgres:18.6-alpine3.24"
    assert assigned["SERVER_SETTINGS"][::2] == ["-c"] * (len(assigned["SERVER_SETTINGS"]) // 2)


def test_ci_python_is_the_python_the_image_runs():
    image = re.search(r"python:(3\.\d+)-slim", read("Containerfile")).group(1)
    text = read(".github/workflows/ci.yml")
    assert f'UV_PYTHON: "{image}"' in text
    assert text.count(f'python-version: "{image}"') == 2
    assert "UV_VERSION: 0.11.16" in text
    assert "uv==0.11.16" in read("Containerfile") and 'uv = "0.11.16"' in read("mise.toml")
