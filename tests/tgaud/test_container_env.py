"""The container end-to-end helper's cleanup: it removes only the database container it created, and a failed
removal fails the run (the review of the integrate task)."""
import subprocess

import pytest

import container_env


class Calls:
    def __init__(self, created=None, starts=True, rm_returncode=0):
        self.argv = []
        self.created = created  # the id podman wrote into --cidfile, None when it created nothing
        self.starts = starts
        self.rm_returncode = rm_returncode

    def __call__(self, argv, **kw):
        self.argv.append(argv)
        if argv[:2] == ["podman", "run"]:
            if self.created is not None:  # podman wrote the created container's id, even if the start failed
                cid = argv[argv.index("--cidfile") + 1]
                with open(cid, "w") as f:
                    f.write(self.created + "\n")
            if not self.starts:
                raise subprocess.CalledProcessError(125, argv, stderr="the container failed to start")
            return subprocess.CompletedProcess(argv, 0, stdout=(self.created or "") + "\n", stderr="")
        if argv[:2] == ["podman", "rm"]:
            return subprocess.CompletedProcess(argv, self.rm_returncode, stdout="",
                                               stderr="Error: cannot remove container: runtime error")
        raise AssertionError(argv)


@pytest.fixture
def no_admin_url(monkeypatch):
    monkeypatch.delenv("TGAUD_TEST_ADMIN_URL", raising=False)
    monkeypatch.setattr(container_env, "run", lambda *a, **k: [])
    monkeypatch.setattr(container_env, "PgServer", lambda *a, **k: object())


def test_a_launch_that_created_nothing_removes_no_container(monkeypatch, no_admin_url):
    """A name collision: podman created nothing, so cleanup touches no container (not the other run's)."""
    calls = Calls(created=None, starts=False)
    monkeypatch.setattr(container_env.subprocess, "run", calls)
    assert container_env.main("169.254.1.3") == 1
    assert not any(a[:2] == ["podman", "rm"] for a in calls.argv)


def test_a_failed_start_still_removes_the_container_it_created(monkeypatch, no_admin_url):
    calls = Calls(created="feedfacecafe0001", starts=False)
    monkeypatch.setattr(container_env.subprocess, "run", calls)
    assert container_env.main("169.254.1.3") == 1
    assert ["podman", "rm", "-f", "--volumes", "feedfacecafe0001"] in calls.argv


def test_cleanup_removes_the_created_container_and_its_volume_by_id(monkeypatch, no_admin_url):
    calls = Calls(created="0123456789abcdef")
    monkeypatch.setattr(container_env.subprocess, "run", calls)
    assert container_env.main("169.254.1.3") == 0
    assert ["podman", "rm", "-f", "--volumes", "0123456789abcdef"] in calls.argv
    launch = next(a for a in calls.argv if a[:2] == ["podman", "run"])
    assert "--rm" not in launch and "--image-volume=ignore" in launch  # this helper owns the removal


def test_a_failed_removal_fails_the_run(monkeypatch, no_admin_url, capsys):
    calls = Calls(created="0123456789abcdef", rm_returncode=125)
    monkeypatch.setattr(container_env.subprocess, "run", calls)
    assert container_env.main("169.254.1.3") == 1
    assert "podman rm of the test database 0123456789ab failed" in capsys.readouterr().err
