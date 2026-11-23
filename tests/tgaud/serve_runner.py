"""`tgaud serve` with the job modules of serve_fakes (TEST_MODE), through the normal CLI entry."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import serve_fakes  # noqa: E402

serve_fakes.install(os.environ.get("TEST_MODE", ""))

from tgaud import cli, jobs  # noqa: E402

if os.environ.get("TEST_HEALTH_WINDOW"):
    jobs.HEALTH_WINDOW = float(os.environ["TEST_HEALTH_WINDOW"])

sys.exit(cli.main(["serve"]))
