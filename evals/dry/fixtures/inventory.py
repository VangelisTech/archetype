# Fixture: structurally duplicates orders.py despite renamed identifiers.
# The eval's oracle should detect:
#   inventory.restock_item  ~  orders.fulfill_order
#   inventory.format_summary ~ shipping.format_label
#   inventory.deactivate    ~  shipping.cancel
# The non-duplicate (compute_inventory_value) is a control — it should
# not match anything else above threshold.
from __future__ import annotations


def restock_item(stock, sku, count):
    if count <= 0:
        raise ValueError("count must be positive")
    if sku not in stock:
        stock[sku] = 0
    stock[sku] += count
    return stock[sku]


def format_summary(items):
    parts = []
    for item in items:
        parts.append(f"{item.name}: {item.qty}")
    return ", ".join(parts)


def deactivate(record):
    record.active = False
    record.updated_at = "now"
    return record


def compute_inventory_value(items, prices):
    total = 0.0
    for item in items:
        if item.sku in prices:
            total += item.qty * prices[item.sku]
        else:
            total += 0.0
    return round(total, 2)
