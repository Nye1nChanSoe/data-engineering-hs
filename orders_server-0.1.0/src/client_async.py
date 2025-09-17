import asyncio
from aiolimiter import AsyncLimiter
import httpx
import time
import logging
import csv
from io import StringIO
import aiofiles

# 18 req per sec
RATE_LIMIT_REQUEST = 18
RATE_LIMIT_SEC = 1
SEMAPHORE_COUNT = 25

# API
API_BASE = "http://127.0.0.1:8000"

# Workers
MAX_WORKERS = 8

# retries
TIMEOUT = 5
MAX_RETRIES = 3

# log rotation
MAX_BYTES = 1024 * 1024  # 1MB
BACKUP_COUNT = 5

# csv
TARGET_ROWS = 1000
CSV_PATH = "output.csv"
COL_NAMES = [
    "order_id",
    "account_id",
    "company",
    "status",
    "currency",
    "subtotal",
    "tax",
    "total",
    "created_at",
]


async_limiter = AsyncLimiter(RATE_LIMIT_REQUEST, RATE_LIMIT_SEC)

http_limits = httpx.Limits(
    max_connections=MAX_WORKERS * 1.5, max_keepalive_connections=MAX_WORKERS
)

async_client = httpx.AsyncClient(
    base_url=API_BASE, timeout=httpx.Timeout(TIMEOUT), limits=http_limits, http2=False
)

# logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# file_handler = RotatingFileHandler("app.log", "a", MAX_BYTES, BACKUP_COUNT)
# file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
# logger.addHandler(file_handler)


def extract_csv_record(res: httpx.Response):
    payload = res.json()
    return {k: payload[k] for k in COL_NAMES}


async def write_csv_async(path: str, rows: list[dict]):
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=COL_NAMES)

    # only write header once
    writer.writeheader()
    writer.writerows(rows)
    async with aiofiles.open(path, "w", encoding="utf-8") as f:
        await f.write(buf.getvalue())


burst_sem = asyncio.Semaphore(SEMAPHORE_COUNT)


async def fetch_async(order_id: int):
    retries = 0
    while retries <= MAX_RETRIES:
        async with burst_sem:
            async with async_limiter:
                try:
                    res = await async_client.get(f"/item/{order_id}")
                except (httpx.TimeoutException, httpx.RequestError) as e:
                    retries += 1
                    logger.warning(
                        f"EXCEPTION {e} - attempt={retries} - endpoint=/item/{order_id}"
                    )
                    await asyncio.sleep(1)
                    continue

                if res.status_code == 200:
                    return res

                if res.status_code == 429:
                    delay = int(res.headers.get("Retry-After", 1))
                    logger.warning(
                        f"Rate_Limit {res.status_code} - attempt={retries} - endpoint=/item/{order_id}"
                    )
                    await asyncio.sleep(delay)
                    retries += 1
                    continue

                if 500 <= res.status_code < 600:
                    if retries < MAX_RETRIES:
                        logger.warning(
                            f"SERVER_ERROR {res.status_code} - attempt={retries} - endpoint=/item/{order_id}"
                        )
                    else:
                        logger.error(
                            f"FATAL {res.status_code} - attempts={retries} - endpoint=/item/{order_id}"
                        )
                    retries += 1
                    await asyncio.sleep(1)
                    continue

                return None

    return None


async def main():
    start = time.perf_counter()

    # coroutine objects
    tasks = {fetch_async(i) for i in range(1, TARGET_ROWS + 1)}

    # run tasks
    results = await asyncio.gather(*tasks)

    rows = [extract_csv_record(result) for result in results]

    # write asynchronously
    await write_csv_async(CSV_PATH, rows)

    end = time.perf_counter()
    print(f"Finished {len(rows)} rows in {end - start:.2f}s -> {CSV_PATH}")


def cli():
    asyncio.run(main())
