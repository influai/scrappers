# docker

## scraper

### build

```bash
cd scraper
docker build -t scraper-node:latest -f Dockerfile ./
```

### run

```bash
docker run -d --rm --env-file {path/to/.env} scraper-node:latest
```

## api gateway

### build

```bash
cd api_gateway
docker build -t scraper-api:latest -f Dockerfile ./
```

### run

```bash
docker run -d --rm --env-file {path/to/.env} -p 8000:8000 scraper-api:latest
```

## note

example of `.env` file located in [example.env](/example_env)
