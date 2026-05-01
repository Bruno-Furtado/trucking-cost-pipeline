FROM apache/spark:4.1.1-python3

USER root

RUN mkdir -p /app /data && chmod 777 /data \
    && ln -sf "$(command -v python3)" /usr/local/bin/python

WORKDIR /app

COPY pyproject.toml /app/
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && mkdir -p /app/pipeline && touch /app/pipeline/__init__.py \
    && pip install --no-cache-dir ".[dev]"

ENTRYPOINT []
CMD ["bash"]
