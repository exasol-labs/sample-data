#!/usr/bin/env python3
"""
Generate category-aware reviews with personas and timestamps (Python 3 compatible).

This version uses pyarrow.parquet.ParquetFile.iter_batches(...) for streaming
to avoid relying on dataset.scan (which may not exist in older/newer pyarrow APIs).
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from typing import Iterator, List, Optional

import numpy as np
import pandas as pd

# -----------------------
# Config (same as before)
# -----------------------
MAX_REVIEW_CHARS = 100_000
DEFAULT_MAX_REVIEWS = 5
DEFAULT_BATCH_SIZE = 100_000

PERSONAS = [
    {"persona": "Tech Enthusiast", "first_names": ["Alex", "Jordan", "Taylor", "Sam", "Riley", "Casey"],
     "last_names": ["Ng", "Patel", "Garcia", "Smith", "Khan", "Brown"], "age_range": (22, 45),
     "locations": ["San Francisco, CA", "Berlin, DE", "London, UK", "Austin, TX"], "focus": "performance"},
    {"persona": "Bargain Hunter", "first_names": ["Jamie", "Morgan", "Avery", "Charlie"],
     "last_names": ["Diaz", "Chen", "O'Neill", "Lopez"], "age_range": (25, 60),
     "locations": ["Nashville, TN", "Madrid, ES", "Chicago, IL"], "focus": "value"},
    {"persona": "Parent Reviewer", "first_names": ["Pat", "Kelly", "Dana", "Robin"],
     "last_names": ["Williams", "Miller", "Wilson", "Davis"], "age_range": (30, 50),
     "locations": ["Seattle, WA", "Paris, FR", "Toronto, CA"], "focus": "durability"},
    {"persona": "Outdoor Enthusiast", "first_names": ["Blake", "Drew", "Harper", "Parker"],
     "last_names": ["Hunt", "Stone", "Wells", "Fisher"], "age_range": (20, 55),
     "locations": ["Denver, CO", "Vancouver, CA", "Rotorua, NZ"], "focus": "weatherproofing"},
    {"persona": "Home Chef", "first_names": ["Maya", "Noah", "Liam", "Ivy"],
     "last_names": ["Singh", "Martinez", "Rossi", "Keller"], "age_range": (24, 65),
     "locations": ["Rome, IT", "Melbourne, AU", "Boston, MA"], "focus": "ease_of_use"},
    {"persona": "Audiophile", "first_names": ["Evan", "Kai", "Zoe", "Nina"],
     "last_names": ["Park", "Hernandez", "Lopez", "Morris"], "age_range": (18, 50),
     "locations": ["Tokyo, JP", "Seoul, KR", "Brooklyn, NY"], "focus": "sound_quality"},
    {"persona": "Frequent Traveler", "first_names": ["Chris", "Patrice", "Jo", "Samir"],
     "last_names": ["Baker", "Singh", "Ali", "Johnson"], "age_range": (28, 58),
     "locations": ["Dubai, AE", "Singapore, SG", "Los Angeles, CA"], "focus": "portability"},
    {"persona": "Pet Owner", "first_names": ["Lena", "Omar", "Bea", "Hannah"],
     "last_names": ["Ramirez", "Gonzalez", "Brown", "Evans"], "age_range": (21, 70),
     "locations": ["Minneapolis, MN", "Dublin, IE", "Lima, PE"], "focus": "safety"},
]

BASE_RATING_PROBS = np.array([0.07, 0.11, 0.22, 0.33, 0.27])  # for 1..5

CATEGORY_BIASES = {
    "Electronics": -0.10, "Automotive": -0.08, "Tools": -0.05,
    "Clothing": 0.02, "Shoes": 0.01, "Toys": 0.03, "Books": 0.08,
    "Music": 0.06, "Movies": 0.04, "Health": 0.00, "Beauty": 0.03,
    "Grocery": 0.02, "Home": 0.03, "Garden": 0.01, "Sports": 0.00,
    "Office": 0.02, "Pets": 0.04, "Baby": 0.05, "Outdoors": -0.02,
    "Jewelry": 0.05, "General": 0.0,
}

BASE_TEMPLATES = {
    1: {"openers": ["Terrible experience with", "I regret buying", "Completely unsatisfied with"],
        "middles": ["It broke after a few uses and support did nothing.", "The build felt cheap and failed.", "Multiple defects and poor reliability."],
        "closers": ["Would not recommend.", "Save your money.", "I returned it and asked for a refund."]},
    2: {"openers": ["Disappointing overall for", "Not what I expected from", "Mixed feelings about"],
        "middles": ["Works sometimes but has flaws.", "Some features are fine but execution lacks.", "Disappointing quality in several places."],
        "closers": ["There are better options.", "I probably won't buy again.", "Needs improvement."]},
    3: {"openers": ["It's okay for", "Average experience with", "Works as expected for"],
        "middles": ["It does the job but doesn't excel.", "Reasonable quality at this price.", "Has pros and cons."],
        "closers": ["A decent neutral pick.", "Satisfactory if you need something simple.", "Not exceptional but usable."]},
    4: {"openers": ["Pretty pleased with", "Good value in", "Solid performance for"],
        "middles": ["Performs reliably and feels well constructed.", "Met expectations and pleasant to use.", "A few small issues, but overall positive."],
        "closers": ["Would recommend.", "Good buy.", "I'd purchase again."]},
    5: {"openers": ["Absolutely love", "Fantastic product:", "Exceeded expectations with"],
        "middles": ["Top-tier quality and attention to detail.", "Performs flawlessly and is a joy to use.", "Everything feels premium and reliable."],
        "closers": ["Highly recommended!", "Five stars.", "Worth every penny."]},
}


# -----------------------
# Helper functions
# -----------------------
def sample_persona(rng: np.random.Generator) -> dict:
    persona = rng.choice(PERSONAS)
    first = rng.choice(persona["first_names"])
    last = rng.choice(persona["last_names"])
    full_name = f"{first} {last}"
    age = int(rng.integers(persona["age_range"][0], persona["age_range"][1] + 1))
    location = rng.choice(persona["locations"])
    return {
        "reviewer_name": full_name,
        "reviewer_persona": persona["persona"],
        "reviewer_age": age,
        "reviewer_location": location,
        "persona_focus": persona["focus"],
    }


def adjust_probs_for_category(base_probs: np.ndarray, category: str) -> np.ndarray:
    bias = CATEGORY_BIASES.get(category, CATEGORY_BIASES.get("General", 0.0))
    probs = base_probs.copy()
    if bias == 0:
        return probs
    shift = max(min(bias * 0.5, 0.15), -0.15)
    if shift > 0:
        move_from_low = shift * (probs[0] + probs[1])
        if move_from_low > 0:
            reduction = np.array([probs[0], probs[1]]) * (move_from_low / (probs[0] + probs[1]))
            probs[0] -= reduction[0]; probs[1] -= reduction[1]
            addition_total = probs[3] + probs[4]
            if addition_total <= 0:
                probs[3] += move_from_low * 0.5; probs[4] += move_from_low * 0.5
            else:
                probs[3] += move_from_low * (probs[3] / addition_total)
                probs[4] += move_from_low * (probs[4] / addition_total)
    else:
        move_from_high = (-shift) * (probs[3] + probs[4])
        if move_from_high > 0:
            reduction = np.array([probs[3], probs[4]]) * (move_from_high / (probs[3] + probs[4]))
            probs[3] -= reduction[0]; probs[4] -= reduction[1]
            addition_total = probs[0] + probs[1]
            if addition_total <= 0:
                probs[0] += move_from_high * 0.5; probs[1] += move_from_high * 0.5
            else:
                probs[0] += move_from_high * (probs[0] / addition_total)
                probs[1] += move_from_high * (probs[1] / addition_total)
    probs = np.clip(probs, 1e-6, 1.0)
    probs /= probs.sum()
    return probs


def choose_num_reviews(rng: np.random.Generator, max_reviews: int) -> int:
    if max_reviews < 1:
        return 0
    weights = [0.20, 0.28, 0.22, 0.15, 0.10, 0.05][: max_reviews + 1]
    weights = np.array(weights, dtype=float)
    weights /= weights.sum()
    return int(rng.choice(list(range(max_reviews + 1)), p=weights))


def choose_rating_for_category(rng: np.random.Generator, category: str) -> int:
    probs = adjust_probs_for_category(BASE_RATING_PROBS.copy(), category)
    return int(rng.choice([1, 2, 3, 4, 5], p=probs))


def sample_review_date(rng: np.random.Generator, years_back: int = 3) -> str:
    now = datetime.now()
    start = now - timedelta(days=365 * years_back)
    random_seconds = int(rng.integers(0, int((now - start).total_seconds()) + 1))
    dt = start + timedelta(seconds=random_seconds)
    return dt.strftime("%Y-%m-%d %I:%M:%S %p")  # 12-hour with AM/PM


def safe_truncate(text: str, max_chars: int = MAX_REVIEW_CHARS) -> str:
    return text if len(text) <= max_chars else text[: max_chars - 1]


def make_review_text(rng: np.random.Generator, rating: int, product_name: str, product_category: str, persona_info: dict) -> str:
    tpl = BASE_TEMPLATES[rating]
    opener = rng.choice(tpl["openers"])
    middle = rng.choice(tpl["middles"])
    closer = rng.choice(tpl["closers"])
    focus = persona_info.get("persona_focus", "")
    persona_phrase = ""
    if focus == "performance":
        persona_phrase = "As someone who values performance, "
    elif focus == "value":
        persona_phrase = "As someone who looks for value, "
    elif focus == "durability":
        persona_phrase = "With kids in the house, "
    elif focus == "weatherproofing":
        persona_phrase = "I use it outdoors often, so "
    elif focus == "ease_of_use":
        persona_phrase = "I cook a lot and care about ease of use, so "
    elif focus == "sound_quality":
        persona_phrase = "As an audiophile, "
    elif focus == "portability":
        persona_phrase = "I travel a lot, so "
    elif focus == "safety":
        persona_phrase = "With pets around, "
    category_phrase = f" In the {product_category} category, this product"
    if rating >= 4:
        extra = rng.choice(["It consistently performs well in daily use.", "Setup was straightforward and painless.", "Materials and fit/finish are impressive for the price."])
    elif rating == 3:
        extra = rng.choice(["It's serviceable for common tasks but not outstanding.", "It does what it needs to, but don't expect surprises.", "Good for occasional use or budget setups."])
    else:
        extra = rng.choice(["It caused repeated issues during normal use.", "Support and documentation were inadequate.", "I encountered multiple defects and usability problems."])
    persona_line = f" - {persona_info['reviewer_name']}, {persona_info['reviewer_persona']}, {persona_info['reviewer_location']}"
    full = " ".join([f"{opener} {product_name}.", f"{persona_phrase}{middle}", category_phrase + ".", extra, closer]) + " " + persona_line
    return safe_truncate(full)


# -----------------------
# Parquet streaming using ParquetFile.iter_batches
# -----------------------
def iter_products_from_parquet(parquet_path: str, id_col: str, name_col: str, category_col: Optional[str], batch_size: int) -> Iterator[pd.DataFrame]:
    """
    Stream (id, name, optional category) batches from a Parquet file
    using pyarrow.parquet.ParquetFile.iter_batches for compatibility.
    """
    try:
        import pyarrow.parquet as pq
        import pyarrow as pa
    except Exception as e:
        raise RuntimeError("pyarrow is required. Install with: pip install pyarrow") from e

    pf = pq.ParquetFile(parquet_path)
    # determine columns to read; parquet API requires existing columns
    columns = [id_col, name_col]
    if category_col:
        columns.append(category_col)

    # iterate row-groups and within them iterate record batches
    for rg_index in range(pf.num_row_groups):
        # read the row-group into a table limited to columns
        # using read_row_group then iter_batches provides predictable memory usage
        row_group_table = pf.read_row_group(rg_index, columns=columns)
        for batch in row_group_table.to_batches(max_chunksize=batch_size):
            # convert batch to pandas
            tb = pa.Table.from_batches([batch], schema=row_group_table.schema)
            yield tb.to_pandas()

# -----------------------
# Main entry
# -----------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Generate category-aware reviews with personas and timestamps.")
    parser.add_argument("--input-parquet", required=True, help="Input Parquet file path (products).")
    parser.add_argument("--id-column", default="id", help="Product id column name.")
    parser.add_argument("--name-column", default="product_name", help="Product name column name.")
    parser.add_argument("--category-column", default="product_category", help="Optional product category column name (if present).")
    parser.add_argument("--output", required=True, help="Output file path (.parquet or .csv).")
    parser.add_argument("--max-reviews-per-product", type=int, default=DEFAULT_MAX_REVIEWS, help="Maximum reviews per product (0..N).")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="How many products to read per batch.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible outputs.")
    parser.add_argument("--years-back", type=int, default=3, help="How many years back to sample review dates from (default 3).")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    out_is_parquet = args.output.lower().endswith(".parquet")
    out_is_csv = args.output.lower().endswith(".csv")
    if not (out_is_parquet or out_is_csv):
        raise ValueError("Output must end with .parquet or .csv")

    parquet_writer = None
    csv_header_written = False
    review_id_counter = 1

    for df_batch in iter_products_from_parquet(args.input_parquet, args.id_column, args.name_column, args.category_column, args.batch_size):
        rows_out: List[dict] = []
        has_category = args.category_column in df_batch.columns
        for _, p_row in df_batch.iterrows():
            pid = int(p_row[args.id_column])
            pname = str(p_row[args.name_column])
            if has_category:
                pcat = str(p_row[args.category_column]) if pd.notna(p_row[args.category_column]) else "General"
            else:
                pcat = pname.split("_", 1)[0] if "_" in pname else "General"
            pcat = pcat if pcat in CATEGORY_BIASES else "General"
            n_reviews = choose_num_reviews(rng, args.max_reviews_per_product)
            for _ in range(n_reviews):
                persona_info = sample_persona(rng)
                rating = choose_rating_for_category(rng, pcat)
                review_text = make_review_text(rng, rating, pname, pcat, persona_info)
                review_date = sample_review_date(rng, years_back=args.years_back)
                rows_out.append({
                    "review_id": review_id_counter,
                    "product_id": pid,
                    "product_name": pname,
                    "product_category": pcat,
                    "rating": int(rating),
                    "review_text": review_text,
                    "reviewer_name": persona_info["reviewer_name"],
                    "reviewer_persona": persona_info["reviewer_persona"],
                    "reviewer_age": int(persona_info["reviewer_age"]),
                    "reviewer_location": persona_info["reviewer_location"],
                    "review_date": review_date,
                })
                review_id_counter += 1

        if not rows_out:
            continue

        out_df = pd.DataFrame(rows_out)
        if out_is_csv:
            out_df.to_csv(args.output, mode="a", header=not csv_header_written, index=False, encoding="utf-8")
            csv_header_written = True
        else:
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pandas(out_df, preserve_index=False)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(args.output, table.schema, compression="snappy", use_dictionary=True)
            parquet_writer.write_table(table)

    if parquet_writer is not None:
        parquet_writer.close()

    print(f"Done. Generated {review_id_counter - 1} reviews -> {args.output}")


if __name__ == "__main__":
    main()
