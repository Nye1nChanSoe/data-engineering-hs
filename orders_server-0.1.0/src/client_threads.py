from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import httpx
import time
import logging
import csv


# 18 req per sec
RATE_LIMIT_REQUEST = 18
RATE_LIMIT_SEC = 1

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


# http client connection pool
http_limits = httpx.Limits(
    max_connections=MAX_WORKERS * 1.5, max_keepalive_connections=MAX_WORKERS
)

client = httpx.Client(
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


@sleep_and_retry
@limits(RATE_LIMIT_REQUEST, RATE_LIMIT_SEC)
def fetch(endpoint: str):
    return client.get(endpoint)


def fetch_order_item(id: int):
    attempts = 0

    # retry on timeout / transport bounded errors
    while attempts < MAX_RETRIES:
        attempts += 1
        try:
            res = fetch(f"/item/{id}")

            # 429 - too many requests
            if res.status_code == 429:
                retry_after = int(res.headers.get("retry-after", 1))
                logger.warning(
                    f"Rate_Limit {res.status_code} - attempt={attempts} - endpoint=/item/{id}"
                )
                time.sleep(retry_after)
                continue

            # 5xx - internal server error
            if res.status_code >= 500:
                if attempts < MAX_RETRIES:
                    logger.warning(
                        f"SERVER_ERROR {res.status_code} - attempt={attempts} - endpoint=/item/{id}"
                    )
                else:
                    logger.error(
                        f"FATAL {res.status_code} - attempts={attempts} - endpoint=/item/{id}"
                    )
                time.sleep(1.0)
                continue

            # 4xx - non-retryable termination
            if 400 <= res.status_code <= 499:
                return

            record = extract_csv_record(res)
            return record

        except (httpx.TimeoutException, httpx.TransportError) as e:
            logger.warning(f"EXCEPTION {e} - attempt={attempts} - endpoint=/item/{id}")


def cli():
    start = time.perf_counter()

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COL_NAMES)
        writer.writeheader()

    written = 0
    next_id = 0

    with (
        ThreadPoolExecutor(MAX_WORKERS) as pool,
        open(CSV_PATH, "a", newline="", encoding="utf-8") as f,
    ):
        writer = csv.DictWriter(f, fieldnames=COL_NAMES)

        futures = {}
        for _ in range(MAX_WORKERS):
            futures[pool.submit(fetch_order_item, next_id)] = next_id
            next_id += 1

        while written < TARGET_ROWS:
            if not futures:
                futures[pool.submit(fetch_order_item, next_id)] = next_id
                next_id += 1
                continue

            for fut in as_completed(list(futures.keys())):
                futures.pop(fut)
                record = fut.result()

                if record:
                    writer.writerow(record)
                    written += 1
                    if written % 50 == 0:
                        logger.info(f"Progress: {written}/{TARGET_ROWS}")

                if written < TARGET_ROWS:
                    futures[pool.submit(fetch_order_item, next_id)] = next_id
                    next_id += 1

                if written >= TARGET_ROWS:
                    break

            f.flush()

    end = time.perf_counter()
    print(f"Concurrent tasks finish: {end - start:.2f}")
