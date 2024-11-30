FROM python:3.12.7-slim


ENV \
    # Disable buffers in stdout and stderr
    PYTHONUNBUFFERED=1 \
    # Prevents python creating .pyc files
    PYTHONDONTWRITEBYTECODE=1 \
    # PIP optimization
    PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /api_gateway

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first to leverage Docker's layer caching
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]