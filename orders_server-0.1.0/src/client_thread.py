import httpx
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor

API_URL = "http://127.0.0.1:8000"
HEADERS = {"accept": "application/json"}
THROTTLE_LIMIT = 18
THROTTLE_TIME = 5


@sleep_and_retry
@limits(THROTTLE_LIMIT, THROTTLE_TIME)
def test_rate_limited_request(client: httpx.Client, item_id: int):
    return client.get(f"{API_URL}/item/{item_id}")


def ddox():
    with httpx.Client() as client:
        for i in range(1, 51):
            res = test_rate_limited_request(client, i)
            print(f"[{i}] {res.status_code} -> {res.text[:60]}")


def cli():
    ddox()
