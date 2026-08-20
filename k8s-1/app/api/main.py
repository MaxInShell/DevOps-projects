import json
import logging
import os
import secrets
import string
import time
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import RedirectResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, HttpUrl

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("shortener")

ALPHABET = string.ascii_letters + string.digits
CODE_LEN = 7
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "300"))

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clicks")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "https://links.lab.local")

REQUESTS = Counter(
    "shortener_requests_total", "HTTP requests", ["method", "route", "status"]
)
LATENCY = Histogram(
    "shortener_request_duration_seconds", "Request duration", ["route"]
)
CACHE_HITS = Counter("shortener_cache_hits_total", "Redis cache hits")
CACHE_MISSES = Counter("shortener_cache_misses_total", "Redis cache misses")
CLICKS_PUBLISHED = Counter("shortener_clicks_published_total", "Click events sent to Kafka")
CLICKS_FAILED = Counter("shortener_clicks_failed_total", "Click events that could not be sent")


def build_dsn() -> str:
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn
    host = os.getenv("PGHOST", "pg-cluster-rw")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "shortener")
    password = os.getenv("PGPASSWORD", "")
    db = os.getenv("PGDATABASE", "shortener")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = AsyncConnectionPool(build_dsn(), min_size=1, max_size=10, open=False)
    await app.state.pool.open(wait=True, timeout=30)
    app.state.redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    app.state.producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode(),
        linger_ms=50,
    )
    await app.state.producer.start()
    log.info("started: kafka=%s redis=%s", KAFKA_BOOTSTRAP, REDIS_URL)
    yield
    await app.state.producer.stop()
    await app.state.redis.aclose()
    await app.state.pool.close()


app = FastAPI(title="url-shortener-lab", lifespan=lifespan)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    route = request.url.path if request.url.path in ("/links", "/healthz", "/metrics") else "/{code}"
    start = time.perf_counter()
    response = await call_next(request)
    LATENCY.labels(route).observe(time.perf_counter() - start)
    REQUESTS.labels(request.method, route, str(response.status_code)).inc()
    return response


class LinkIn(BaseModel):
    url: HttpUrl


class LinkOut(BaseModel):
    code: str
    short_url: str
    url: str


@app.get("/healthz")
async def healthz():
    async with app.state.pool.connection() as conn:
        await conn.execute("SELECT 1")
    return {"status": "ok"}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/links", response_model=LinkOut, status_code=201)
async def create_link(payload: LinkIn):
    url = str(payload.url)
    for _ in range(5):
        code = "".join(secrets.choice(ALPHABET) for _ in range(CODE_LEN))
        async with app.state.pool.connection() as conn:
            cur = await conn.execute(
                "INSERT INTO links (code, url) VALUES (%s, %s) "
                "ON CONFLICT (code) DO NOTHING RETURNING code",
                (code, url),
            )
            row = await cur.fetchone()
        if row:
            return LinkOut(code=code, short_url=f"{PUBLIC_BASE_URL}/{code}", url=url)
    raise HTTPException(status_code=503, detail="could not allocate a free code")


@app.get("/links/{code}/stats")
async def link_stats(code: str):
    async with app.state.pool.connection() as conn:
        cur = await conn.execute(
            "SELECT day, clicks FROM click_stats WHERE code = %s ORDER BY day DESC LIMIT 30",
            (code,),
        )
        rows = await cur.fetchall()
    return {"code": code, "daily": [{"day": str(d), "clicks": c} for d, c in rows]}


@app.get("/{code}")
async def follow(code: str, request: Request):
    cache_key = f"link:{code}"
    url = await app.state.redis.get(cache_key)

    if url:
        CACHE_HITS.inc()
    else:
        CACHE_MISSES.inc()
        async with app.state.pool.connection() as conn:
            cur = await conn.execute("SELECT url FROM links WHERE code = %s", (code,))
            row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="unknown code")
        url = row[0]
        await app.state.redis.set(cache_key, url, ex=CACHE_TTL)

    event = {
        "code": code,
        "ts": time.time(),
        "ua": request.headers.get("user-agent", "")[:200],
        "referer": request.headers.get("referer", "")[:200],
    }
    try:
        await app.state.producer.send(KAFKA_TOPIC, event)
        CLICKS_PUBLISHED.inc()
    except Exception:
        CLICKS_FAILED.inc()
        log.exception("failed to publish click for %s", code)

    return RedirectResponse(url, status_code=307)
