FROM python:3.11.9

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    \
    PDM_CHECK_UPDATE=false

RUN apt update \
    && apt install -y \
    git \
    # deps for building python deps
    build-essential \
    libcurl4-gnutls-dev \
    gnutls-dev \
    libmagic-dev

RUN curl -sSL https://install.python-poetry.org | python -

WORKDIR /main
COPY pdm.lock pyproject.toml ./

RUN pdm install --check --prod --no-editable

COPY . .
ENTRYPOINT pdm run python -OO mdbroker.py
