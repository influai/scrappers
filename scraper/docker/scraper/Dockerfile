FROM python:3.12

ARG PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=500

WORKDIR /src
COPY ./scraper ./scraper
COPY ./database ./database

RUN pip install --upgrade pip && \
    pip install -r ./scraper/requirements.txt

CMD ["python", "-m", "scraper.main"]