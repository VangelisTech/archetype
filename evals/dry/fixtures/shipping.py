# Fixture: deliberate structural duplicates of inventory.py forms.
from __future__ import annotations


def format_label(parcels):
    out = []
    for parcel in parcels:
        out.append(f"{parcel.tracking}: {parcel.weight}")
    return ", ".join(out)


def cancel(shipment):
    shipment.active = False
    shipment.updated_at = "later"
    return shipment


def estimate_arrival(start, distance, speed):
    if speed <= 0:
        return None
    travel_hours = distance / speed
    return start + travel_hours
