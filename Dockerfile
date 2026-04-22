FROM python:3.12-slim

WORKDIR /app
ENV PYTHONPATH=/app

RUN pip install poetry && \
    poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-interaction --no-ansi --without dev

COPY . .

EXPOSE 8000
