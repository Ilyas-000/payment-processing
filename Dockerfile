FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH=/app

RUN pip install --no-cache-dir "poetry>=2,<3" && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-ansi --without dev

COPY . .

RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 8000
