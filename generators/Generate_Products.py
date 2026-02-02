#!/usr/bin/env python3
"""
Generate a 1,000,000-row Amazon-like product dataset and write it to Parquet.

Columns:
- id (int64) unique
- product_category (string) 20 human-readable generalized e-commerce categories
- product_name (string) human-readable e-commerce-style product titles aligned with category
- price_usd (float64)
- inventory_count (int32)
- margin (float64)

Requires:
  pip install pandas numpy pyarrow
"""

from __future__ import annotations

import argparse
from typing import Dict, List

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# -----------------------
# Category -> product title templates (Amazon-like, human readable)
# -----------------------
CATEGORY_PRODUCTS: Dict[str, List[str]] = {
    "Electronics": [
        "Wireless Bluetooth Earbuds",
        "Noise Cancelling Headphones",
        "Portable Power Bank 20000mAh",
        "USB-C Fast Charger 65W",
        "4K Streaming Stick",
        "Smart Speaker with Alexa",
        "1080p USB Webcam",
        "Mechanical Gaming Keyboard",
        "Ergonomic Wireless Mouse",
        "27-inch 4K Monitor",
        "Wi-Fi 6 Router",
        "External SSD 1TB",
    ],
    "Computers & Accessories": [
        "Laptop Stand Adjustable Aluminum",
        "USB-C Hub Multiport Adapter",
        "HDMI Cable 6ft 4K",
        "Bluetooth Keyboard for Tablet",
        "Gaming Mouse Pad Extended",
        "Webcam Privacy Cover (Pack of 3)",
        "Portable Laptop Charger",
        "DisplayPort Cable 8K",
        "NVMe SSD Enclosure USB-C",
        "Ethernet Adapter USB 3.0",
    ],
    "Smart Home": [
        "Smart Plug Mini (4 Pack)",
        "Smart LED Light Bulb Color Changing",
        "Video Doorbell Camera",
        "Indoor Security Camera 1080p",
        "Smart Thermostat",
        "Smart Light Switch",
        "Robot Vacuum Cleaner",
        "Smart Smoke Detector",
        "Smart Motion Sensor",
        "Smart Door Lock Keyless Entry",
    ],
    "Home & Kitchen": [
        "Stainless Steel Water Bottle 32oz",
        "Nonstick Frying Pan 12-inch",
        "Air Fryer 6 Quart",
        "Electric Kettle Temperature Control",
        "Memory Foam Pillow",
        "Vacuum Storage Bags (10 Pack)",
        "Kitchen Knife Set with Block",
        "Dish Drying Rack",
        "Bamboo Cutting Board Set",
        "LED Desk Lamp Dimmable",
    ],
    "Furniture": [
        "Ergonomic Office Chair with Lumbar Support",
        "Standing Desk Converter",
        "Bookshelf 5-Tier Industrial",
        "Storage Ottoman Bench",
        "Side Table with Charging Station",
        "Adjustable Bar Stools Set of 2",
        "Computer Desk with Shelves",
        "Accent Chair Modern",
    ],
    "Tools & Home Improvement": [
        "Cordless Drill Driver Kit",
        "Digital Laser Measuring Tool",
        "Socket Wrench Set 108-Piece",
        "Utility Knife Retractable",
        "Heavy Duty Extension Cord 25ft",
        "LED Work Light Rechargeable",
        "Stud Finder with Deep Scan",
        "Smart Tape Measure",
        "Screwdriver Set Magnetic",
    ],
    "Sports & Outdoors": [
        "Yoga Mat Non-Slip",
        "Adjustable Dumbbells Pair",
        "Resistance Bands Set",
        "Insulated Water Bottle",
        "Camping Lantern LED",
        "Hiking Backpack 40L",
        "Trekking Poles Collapsible",
        "Fitness Tracker Watch",
        "Inflatable Sleeping Pad",
        "Bike Phone Mount",
    ],
    "Clothing": [
        "Men's Performance T-Shirt",
        "Women's High-Waisted Leggings",
        "Unisex Hoodie Fleece",
        "Men's Slim Fit Jeans",
        "Women's Summer Dress",
        "Athletic Socks Cushioned (6 Pack)",
        "Winter Beanie Knit",
        "Rain Jacket Lightweight",
    ],
    "Shoes": [
        "Men's Running Shoes Breathable",
        "Women's Walking Shoes",
        "Slip-On Sneakers",
        "Hiking Boots Waterproof",
        "Casual Loafers",
        "Training Shoes Lightweight",
    ],
    "Beauty & Personal Care": [
        "Electric Toothbrush Rechargeable",
        "Facial Cleanser Gentle",
        "Vitamin C Serum for Face",
        "Hair Dryer Ionic",
        "Beard Trimmer Kit",
        "Sunscreen SPF 50",
        "Moisturizing Body Lotion",
        "Makeup Brush Set",
    ],
    "Health & Household": [
        "Digital Thermometer",
        "Blood Pressure Monitor",
        "First Aid Kit 200 Piece",
        "Hand Sanitizer Gel",
        "Air Purifier HEPA",
        "Laundry Detergent Pods",
        "Disinfecting Wipes",
        "Pain Relief Patches",
    ],
    "Baby": [
        "Baby Diapers Size 3 (120 Count)",
        "Baby Wipes Sensitive (8 Pack)",
        "Convertible Car Seat",
        "Baby Monitor with Camera",
        "Portable Changing Pad",
        "Silicone Baby Bibs (3 Pack)",
    ],
    "Pet Supplies": [
        "Dry Dog Food 20lb",
        "Cat Litter Clumping 40lb",
        "Dog Training Pads (100 Count)",
        "Interactive Cat Toy",
        "Pet Grooming Brush",
        "Dog Leash Heavy Duty",
        "Cat Water Fountain",
    ],
    "Toys & Games": [
        "Building Blocks Set 500pcs",
        "Remote Control Car",
        "Puzzle 1000 Pieces",
        "Board Game Family Edition",
        "STEM Science Kit",
        "Dollhouse Furniture Set",
        "Kids Art Supplies Kit",
    ],
    "Books": [
        "Hardcover Notebook Dotted",
        "Cookbook: Quick & Easy Meals",
        "Science Fiction Novel Bestseller",
        "Children's Picture Book",
        "Productivity Planner Weekly",
        "Language Learning Workbook",
    ],
    "Office Products": [
        "Ballpoint Pens Black (12 Pack)",
        "Wireless Label Maker",
        "Desk Organizer Mesh",
        "Sticky Notes Assorted Colors",
        "Printer Paper 500 Sheets",
        "Ergonomic Wrist Rest",
        "Whiteboard Markers Set",
    ],
    "Video Games": [
        "Gaming Controller Wireless",
        "Gaming Headset Surround Sound",
        "Mechanical Keyboard RGB",
        "Console Charging Dock",
        "Gaming Chair Mat",
    ],
    "Automotive": [
        "Car Phone Mount Magnetic",
        "Jump Starter Battery Pack",
        "Tire Inflator Portable Air Compressor",
        "Windshield Sun Shade",
        "Car Vacuum Cleaner Handheld",
        "OBD2 Scanner Diagnostic Tool",
    ],
    "Industrial & Scientific": [
        "Safety Glasses Anti-Fog",
        "Nitrile Gloves Box of 100",
        "Digital Caliper Stainless Steel",
        "Labeling Tape Refill",
        "ESD Anti-Static Wrist Strap",
        "Infrared Thermometer Gun",
    ],
    "Arts, Crafts & Sewing": [
        "Acrylic Paint Set 24 Colors",
        "Sketchbook Hardcover A4",
        "Hot Glue Gun Kit",
        "Knitting Needles Set",
        "Craft Scissors Titanium",
        "Sewing Thread Set 60 Spools",
    ],
}

# Typical Amazon-like “title enhancers”
BRANDS = [
    "Anker", "Samsung", "Sony", "Logitech", "Amazon Basics", "Apple",
    "HP", "Dell", "ASUS", "Lenovo", "Bose", "Philips", "Nike", "Adidas",
    "Under Armour", "Instant Pot", "Shark", "Dyson", "Fitbit", "Ring",
    "TP-Link", "Roku", "JBL", "Belkin", "Xiaomi", "Spigen", "Hydro Flask",
    "Brita", "KitchenAid", "LEGO",
]
QUALIFIERS = [
    "New", "2026 Model", "Upgraded", "Premium", "Ultra", "Pro",
    "Compact", "Lightweight", "Heavy Duty", "High Performance",
    "Fast Charging", "Waterproof", "Wireless", "Rechargeable",
]
BUNDLE_HINTS = [
    "", "", "",  # more likely no bundle
    " (2 Pack)",
    " (3 Pack)",
    " (4 Pack)",
    " Bundle",
]
COLORS = [
    "", "", "",  # many listings omit color
    " - Black",
    " - White",
    " - Blue",
    " - Gray",
    " - Red",
]
SIZES = [
    "", "", "", "",
    " Small",
    " Medium",
    " Large",
    " XL",
    " 1TB",
    " 2TB",
    " 64GB",
    " 128GB",
    " 20000mAh",
]


def make_amazon_title(rng: np.random.Generator, base: str) -> str:
    """
    Assemble an Amazon-like product title.
    Example:
      "Anker Upgraded Wireless Bluetooth Earbuds - Black (2 Pack)"
    """
    brand = rng.choice(BRANDS)
    qualifier = rng.choice(QUALIFIERS) if rng.random() < 0.75 else ""
    size = rng.choice(SIZES)
    color = rng.choice(COLORS)
    bundle = rng.choice(BUNDLE_HINTS)

    parts = [brand]
    if qualifier:
        parts.append(qualifier)
    parts.append(base + size)

    title = " ".join(p for p in parts if p).strip()
    title = f"{title}{color}{bundle}".strip()

    # Clean up double spaces if any slipped in
    return " ".join(title.split())


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a Parquet e-commerce (Amazon-like) product dataset.")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows to generate.")
    parser.add_argument("--output", default="amazon_like_products.parquet", help="Output Parquet file path.")
    parser.add_argument("--seed", type=int, default=42, help="Seed for reproducible generation.")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)

    categories: List[str] = list(CATEGORY_PRODUCTS.keys())
    if len(categories) != 20:
        raise ValueError(f"Expected 20 categories, got {len(categories)}.")

    rows = args.rows
    ids = np.arange(1, rows + 1, dtype=np.int64)

    # Stable, even distribution: round-robin category by id
    cat_idx = (ids - 1) % len(categories)
    product_categories = [categories[i] for i in cat_idx]

    product_names: List[str] = []
    for cat in product_categories:
        base = rng.choice(CATEGORY_PRODUCTS[cat])
        product_names.append(make_amazon_title(rng, base))

    # More e-commerce-ish price range; still synthetic
    df = pd.DataFrame(
        {
            "id": ids,
            "product_category": product_categories,
            "product_name": product_names,
            "price_usd": np.round(rng.uniform(4.99, 999.99, rows), 2),
            "inventory_count": rng.integers(0, 250_000, rows, dtype=np.int32),
            "margin": np.round(rng.uniform(0.05, 0.75, rows), 4),
        }
    )

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(
        table,
        args.output,
        compression="snappy",
        use_dictionary=True,
    )

    print(f"Parquet written: {args.output}")
    print("Schema:")
    print(table.schema)
    print("\nSample rows:")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
