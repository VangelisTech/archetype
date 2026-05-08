# Fixture: deliberate structural duplicates of inventory.py forms,
# with renamed identifiers and changed literals.  See inventory.py
# for the expected oracle pairings.
from __future__ import annotations


def fulfill_order(ledger, order_id, amount):
    if amount <= 0:
        raise ValueError("amount must be positive")
    if order_id not in ledger:
        ledger[order_id] = 0
    ledger[order_id] += amount
    return ledger[order_id]


def render_receipt(lines):
    chunks = []
    for line in lines:
        chunks.append(f"{line.label}: {line.total}")
    return " | ".join(chunks)


def average_order_value(orders):
    if not orders:
        return 0.0
    running = 0.0
    for o in orders:
        running += o.total
    return running / len(orders)
