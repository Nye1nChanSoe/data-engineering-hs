# generate_data_json.py
import json
import random
import gzip
from pathlib import Path
from datetime import datetime, timedelta, UTC
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)
OUT = Path("data")
(OUT / "events").mkdir(parents=True, exist_ok=True)

NUM_USERS = 20_000
NUM_ITEMS = 5_000
NUM_EVENTS = 200_000
START = datetime.now(UTC) - timedelta(days=30)

plans = ["free", "pro", "enterprise"]
countries = ["US", "DE", "IN", "BR", "TH", "VN", "FR", "GB"]
devices = ["web", "ios", "android"]
cats = ["electronics", "home", "toys", "fashion", "books", "sports"]

# users.jsonl
with open(OUT / "users.jsonl", "w") as f:
    for uid in range(1, NUM_USERS + 1):
        rec = {
            "id": uid,
            "signup_date": (START - timedelta(days=random.randint(1, 400)))
            .date()
            .isoformat(),
            "plan": random.choices(plans, weights=[70, 25, 5])[0],
            "country": random.choice(countries),
            "marketing_opt_in": bool(random.getrandbits(1)),
        }
        f.write(json.dumps(rec) + "\n")

# items.jsonl
with open(OUT / "items.jsonl", "w") as f:
    for iid in range(1, NUM_ITEMS + 1):
        rec = {
            "item_id": iid,
            "category": random.choice(cats),
            "tags": random.sample(
                ["sale", "new", "clearance", "gift", "popular"], k=random.randint(1, 3)
            ),
        }
        f.write(json.dumps(rec) + "\n")


# events/*.jsonl.gz
def event_row(i):
    ts = START + timedelta(seconds=i * random.randint(1, 30))
    et = random.choices(["view", "click", "purchase"], weights=[85, 12, 3])[0]
    rec = {
        "ts": ts.isoformat(),
        "event": et,
        "user_id": random.randint(1, NUM_USERS),
        "item_id": random.randint(1, NUM_ITEMS),
        "context": {
            "country": random.choices(countries, weights=[30, 10, 25, 8, 10, 7, 5, 5])[
                0
            ],  # skewed
            "device": random.choice(devices),
            "locale": random.choice(
                ["en_US", "en_GB", "de_DE", "th_TH", "vi_VN", "fr_FR", "pt_BR", "hi_IN"]
            ),
            "session_id": fake.uuid4(),
        },
        "props": {
            "price": round(random.uniform(3, 300), 2) if et == "purchase" else None,
            "payment_method": random.choice(["card", "wallet", "cod"])
            if et == "purchase"
            else None,
            "dwell_ms": random.randint(50, 5000) if et != "purchase" else None,
        },
        "exp": {"ab_group": random.choice(["A", "B"])},
    }
    # rare bad data:
    if random.random() < 0.001 and rec["props"]["price"] is not None:
        rec["props"]["price"] = -abs(rec["props"]["price"])
    return rec


files = [OUT / "events" / f"part-{i:02d}.jsonl.gz" for i in range(4)]
chunk = NUM_EVENTS // len(files)
for i, path in enumerate(files):
    with gzip.open(path, "wt") as f:
        for j in range(chunk):
            f.write(json.dumps(event_row(i * chunk + j)) + "\n")

print("Data ready under ./data")
