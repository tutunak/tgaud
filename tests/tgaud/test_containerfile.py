"""The image's static contract (docs/deploy-contract.md, Image), read from the files: `mise run image-test` builds and
runs the image itself."""

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


def test_ci_uses_no_actions_and_pushes_only_from_main():
    text = read(".forgejo/workflows/ci.yml")
    assert not re.search(r"^\s*(?:-\s+)?uses:", text, re.M)
    assert re.search(r"if: github\.event_name == 'push' && github\.ref == 'refs/heads/main'", text)
    assert "secrets.REGISTRY_TOKEN" in text
    assert "vars.REGISTRY_USER" in text
    assert "IMAGE: git.firr.co/company/tgaud" in text


def test_ci_gate_runs_the_published_image_before_the_push():
    text = read(".forgejo/workflows/ci.yml")
    gate = text.index("docker run --rm --network=none --read-only --tmpfs /tmp --cap-drop=all "
                      "--security-opt=no-new-privileges")
    assert gate < text.index("buildah push \"$IMAGE:$GITHUB_SHA\"")
    assert "buildah push -q \"$IMAGE:$GITHUB_SHA\" docker-archive:" in text


def test_ci_database_tests_are_skipped_only_on_the_runner():
    text = read(".forgejo/workflows/ci.yml")
    assert 'TGAUD_TEST_NO_DATABASE: "1"' in text
    assert "services:" not in text
    assert "TGAUD_TEST_ADMIN_URL" not in text
