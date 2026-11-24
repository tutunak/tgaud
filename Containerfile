# tgaud: the Telegram Bot API service, the image of docs/deploy-contract.md. No USER line: rootless Podman maps
# uid 0 in the container to the host's tgaud account. The root file system is read-only and only /tmp is writable;
# all state is in PostgreSQL, so there is no volume.
ARG PYTHON_IMAGE=docker.io/library/python:3.13-slim@sha256:09a17cad02668ae61ece136b4fd36ac3849e14f670ebedb637ea3e15580525bc

# Build the package with uv from uv.lock; the final image gets only the virtualenv, not the repo or uv.
FROM ${PYTHON_IMAGE} AS build

ENV UV_NO_CACHE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/tgaud

RUN pip install --no-cache-dir --root-user-action=ignore uv==0.11.16

WORKDIR /src
COPY pyproject.toml uv.lock README.md ./
# The dependencies first, so a change of the source does not download them again.
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
# --no-editable: the installed package does not point back at /src.
RUN uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH=/opt/tgaud/bin:$PATH

COPY --from=build /opt/tgaud /opt/tgaud

ENTRYPOINT ["tgaud"]
CMD ["serve"]
