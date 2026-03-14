FROM python:3.14-slim

# Keep Python behavior predictable in containers.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install Python dependencies first for better Docker layer caching.
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy only app code needed by FastAPI service.
COPY api /app/api

# Run as non-root for ECS/Fargate hardening.
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# ECS task definitions typically map this container port.
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
