"""
Generate reproducible, intentionally-dirty sample data for the
Customer Lifetime Value / RFM sample transformation.

Produces three CSVs in this directory:

    customers.csv      - one row per customer
    orders.csv         - one row per order (many per customer)
    order_items.csv    - one row per line item (many per order)

The data matches the schemas consumed by
`transformations.gold.customer_lifetime_value.build_customer_lifetime_value`:

    customers   : customer_id, full_name, country, signup_date
    orders      : order_id,    customer_id, order_ts, currency
    order_items : order_id,    product_id, category, quantity, unit_price

"Dirty" traits baked in on purpose so the transformation's normalization
logic is exercised:
  * mixed-case / padded names  ->  normalize_name
  * country codes in several forms (sg, SG, Singapore, usa, US, uk, ...)
  * currency codes in several forms (usd, USD, sgd, rmb, yen, ...)
  * a subset of customers have zero orders (null-metric code path)
  * order recency is spread so some customers are "at risk" (> 180 days)

The generator is seeded so every run produces the same files. Change the
DEFAULTS block or pass --seed / --customers to scale it up or down.

Usage:

    python generate_sample_data.py
    python generate_sample_data.py --customers 20000 --seed 7
"""

from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

HERE = Path(__file__).resolve().parent

DEFAULTS = {
    "customers": 5_000,
    "seed": 42,
    "as_of_date": date(2024, 4, 23),
    "no_order_rate": 0.08,            # ~8% of customers never ordered
    "avg_orders_per_customer": 7,     # mean of the order count distribution
    "avg_items_per_order": 3,         # mean of the line-item count distribution
}

# ---------------------------------------------------------------------------
# Dictionaries
# ---------------------------------------------------------------------------

FIRST_NAMES = [
    "alice", "bob", "carol", "dan", "eve", "frank", "grace", "heidi", "ivan",
    "judy", "kenji", "leila", "mallory", "nina", "oscar", "peggy", "quentin",
    "ruth", "sybil", "trent", "uma", "victor", "wendy", "xander", "yara",
    "zach", "aisha", "bao", "chen", "daniela", "elena", "farid", "giulia",
    "haruto", "indira", "jun", "kiran", "liam", "mei", "noa", "omar", "priya",
    "rajesh", "satoshi", "tariq", "ursula", "valentina", "wei", "xiulan",
    "yusuf", "zara", "arjun", "bianca", "caleb", "devi",
]

LAST_NAMES = [
    "tan", "lee", "lim", "ng", "chen", "wong", "goh", "koh", "teo", "ho",
    "smith", "johnson", "williams", "brown", "davis", "miller", "garcia",
    "rodriguez", "nguyen", "patel", "khan", "kumar", "singh", "sato", "suzuki",
    "takahashi", "yamamoto", "zhang", "liu", "wang", "huang", "martin",
    "dubois", "bernard", "mueller", "schneider", "fischer", "hansen",
    "jensen", "pedersen", "andersen", "okafor", "adebayo", "abbas",
]

# Country samples map to the branches inside `normalize_nationality_code`
# where applicable, plus a long tail of unmapped values for realism.
COUNTRY_CHOICES = [
    "sg", "SG", "Singapore", "SINGAPORE", " sg ",
    "usa", "USA", "us", "US",
    "uk", "UK", "GB", "gb",
    "cn", "CN", "China", "china",
    "tw", "TW", "Taiwan",
    "fr", "FR", "france", "FRANCE",
    "dk", "DK", "denmark",
    "jp", "JP", "japan", "JAPAN",
    "my", "MY", "malaysia",
    "id", "ID", "indonesia",
    "th", "TH", "thailand",
    "in", "IN", "india",
]

CURRENCY_CHOICES = [
    "sgd", "SGD", " sgd", "sgd ",
    "usd", "USD",
    "cny", "CNY", "rmb", "RMB",
    "jpy", "JPY", "yen", "YEN",
    "gbp", "GBP",
    "eur", "EUR",
]

CATEGORIES = [
    "electronics", "books", "home", "fashion", "toys", "beauty", "sports",
    "grocery", "garden", "office", "automotive", "pet", "jewelry", "music",
]

# Per-category (low_price, high_price) in base currency units. Skewed so
# electronics/jewelry can dominate a customer's revenue and drive the
# top_categories ranking.
CATEGORY_PRICE_RANGE = {
    "electronics": (80, 1500),
    "books":       (8, 60),
    "home":        (15, 400),
    "fashion":     (20, 250),
    "toys":        (10, 120),
    "beauty":      (8, 150),
    "sports":      (15, 500),
    "grocery":     (3, 45),
    "garden":      (12, 300),
    "office":      (5, 200),
    "automotive":  (25, 900),
    "pet":         (6, 180),
    "jewelry":     (60, 2500),
    "music":       (10, 220),
}

# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


@dataclass
class Counts:
    customers: int
    orders: int
    order_items: int


def _messy_name(rng: random.Random, first: str, last: str) -> str:
    """Produce a realistically messy full_name (casing + padding)."""
    casing = rng.random()
    if casing < 0.20:
        name = f"{first} {last}"                    # all lower
    elif casing < 0.55:
        name = f"{first.capitalize()} {last.capitalize()}"
    elif casing < 0.80:
        name = f"{first.upper()} {last.upper()}"
    else:
        name = f"{first.capitalize()} {last.upper()}"

    padding = rng.random()
    if padding < 0.15:
        name = "  " + name
    elif padding < 0.30:
        name = name + "  "
    elif padding < 0.40:
        name = "  " + name + " "
    return name


def _random_signup(rng: random.Random, as_of: date) -> date:
    """Signup somewhere in the last 1-5 years."""
    days_back = rng.randint(30, 365 * 5)
    return as_of - timedelta(days=days_back)


def _random_order_ts(
    rng: random.Random,
    signup: date,
    as_of: date,
    bias_recent: float,
) -> datetime:
    """Pick an order timestamp between signup and as_of.

    `bias_recent` in [0, 1]: higher values concentrate orders closer to today,
    so we can produce a healthy mix of active vs at-risk customers.
    """
    earliest = datetime.combine(signup + timedelta(days=1), datetime.min.time())
    latest = datetime.combine(as_of, datetime.min.time())
    if latest <= earliest:
        return earliest
    span = (latest - earliest).total_seconds()
    # Triangular distribution biased toward `latest` when bias_recent is high.
    mode_offset = span * bias_recent
    offset = rng.triangular(0, span, mode_offset)
    return earliest + timedelta(seconds=offset)


def _order_count_for(rng: random.Random, no_order_rate: float, mean: int) -> int:
    if rng.random() < no_order_rate:
        return 0
    # Shifted geometric-ish distribution: most customers have a few orders,
    # a long tail of power buyers.
    n = int(rng.expovariate(1 / mean))
    return max(1, min(n, mean * 6))


def _items_for_order(rng: random.Random, mean: int) -> int:
    n = int(rng.expovariate(1 / mean))
    return max(1, min(n, mean * 5))


def _category_sampler(rng: random.Random, pref: list[str]) -> str:
    # 70% of the time pick from the customer's preferred short-list, otherwise
    # any category (keeps the top_categories ranking meaningful).
    if rng.random() < 0.7 and pref:
        return rng.choice(pref)
    return rng.choice(CATEGORIES)


def generate(
    out_dir: Path,
    n_customers: int,
    seed: int,
    as_of: date,
    no_order_rate: float,
    avg_orders_per_customer: int,
    avg_items_per_order: int,
) -> Counts:
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)

    customers_path = out_dir / "customers.csv"
    orders_path = out_dir / "orders.csv"
    items_path = out_dir / "order_items.csv"

    next_order_id = 1_000_000
    total_orders = 0
    total_items = 0

    with (
        customers_path.open("w", newline="", encoding="utf-8") as f_c,
        orders_path.open("w", newline="", encoding="utf-8") as f_o,
        items_path.open("w", newline="", encoding="utf-8") as f_i,
    ):
        cw = csv.writer(f_c)
        ow = csv.writer(f_o)
        iw = csv.writer(f_i)

        cw.writerow(["customer_id", "full_name", "country", "signup_date"])
        ow.writerow(["order_id", "customer_id", "order_ts", "currency"])
        iw.writerow(["order_id", "product_id", "category", "quantity", "unit_price"])

        for customer_id in range(1, n_customers + 1):
            first = rng.choice(FIRST_NAMES)
            last = rng.choice(LAST_NAMES)
            full_name = _messy_name(rng, first, last)
            country = rng.choice(COUNTRY_CHOICES)
            signup = _random_signup(rng, as_of)

            cw.writerow([customer_id, full_name, country, signup.isoformat()])

            n_orders = _order_count_for(rng, no_order_rate, avg_orders_per_customer)
            if n_orders == 0:
                continue

            # Each customer leans toward a short list of preferred categories.
            pref = rng.sample(CATEGORIES, k=rng.randint(1, 4))
            # Bias some customers to "active" and some to "at risk".
            bias_recent = rng.uniform(0.1, 0.95)
            currency = rng.choice(CURRENCY_CHOICES)

            for _ in range(n_orders):
                order_id = next_order_id
                next_order_id += 1
                order_ts = _random_order_ts(rng, signup, as_of, bias_recent)

                # 15% of orders have a different currency (e.g. travel purchases).
                order_ccy = currency if rng.random() > 0.15 else rng.choice(CURRENCY_CHOICES)

                ow.writerow([
                    order_id,
                    customer_id,
                    order_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    order_ccy,
                ])
                total_orders += 1

                n_items = _items_for_order(rng, avg_items_per_order)
                for _ in range(n_items):
                    category = _category_sampler(rng, pref)
                    low, high = CATEGORY_PRICE_RANGE[category]
                    unit_price = Decimal(f"{rng.uniform(low, high):.2f}")
                    quantity = rng.choices(
                        [1, 2, 3, 4, 5, 10],
                        weights=[50, 25, 12, 6, 4, 3],
                        k=1,
                    )[0]
                    product_id = 10_000 + hash((category, rng.randint(0, 999))) % 90_000
                    iw.writerow([
                        order_id,
                        product_id,
                        category,
                        quantity,
                        str(unit_price),
                    ])
                    total_items += 1

    return Counts(customers=n_customers, orders=total_orders, order_items=total_items)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--customers", type=int, default=DEFAULTS["customers"])
    p.add_argument("--seed", type=int, default=DEFAULTS["seed"])
    p.add_argument("--out-dir", type=Path, default=HERE)
    p.add_argument(
        "--as-of",
        type=lambda s: date.fromisoformat(s),
        default=DEFAULTS["as_of_date"],
    )
    p.add_argument("--no-order-rate", type=float, default=DEFAULTS["no_order_rate"])
    p.add_argument("--avg-orders", type=int, default=DEFAULTS["avg_orders_per_customer"])
    p.add_argument("--avg-items", type=int, default=DEFAULTS["avg_items_per_order"])
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    counts = generate(
        out_dir=args.out_dir,
        n_customers=args.customers,
        seed=args.seed,
        as_of=args.as_of,
        no_order_rate=args.no_order_rate,
        avg_orders_per_customer=args.avg_orders,
        avg_items_per_order=args.avg_items,
    )
    print(
        f"Wrote {counts.customers:,} customers, "
        f"{counts.orders:,} orders, "
        f"{counts.order_items:,} order items into {args.out_dir}"
    )


if __name__ == "__main__":
    main()
