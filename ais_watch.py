#!/usr/bin/env python3
"""
Riga repair-zone vessel watch.

Connects to aisstream.io for a short window, records which vessels are sitting
still inside the configured repair zones, and writes a row to events.csv once a
vessel has been stationary there long enough to count as a repair event.

Designed to run on a schedule (e.g. GitHub Actions every 3 hours). State is kept
in state.json between runs.
"""

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import websockets

BASE = Path(__file__).resolve().parent
ZONES_FILE = BASE / "zones.json"
STATE_FILE = BASE / "state.json"
EVENTS_FILE = BASE / "events.csv"
STREAM_URL = "wss://stream.aisstream.io/v0/stream"

EVENT_COLUMNS = [
    "detected_utc", "imo", "mmsi", "vessel_name", "ship_type",
    "length_m", "zone", "first_seen_utc", "hours_in_zone",
]


def now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(ts):
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def load_json(path, default):
    if path.exists():
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    return default


def find_zone(lat, lon, zones):
    for z in zones:
        if z["lat_min"] <= lat <= z["lat_max"] and z["lon_min"] <= lon <= z["lon_max"]:
            return z["name"]
    return None


def is_interesting(vessel, rules):
    """Filter out harbour craft, small vessels and anything still unidentified."""
    ship_type = vessel.get("ship_type")
    if ship_type is not None and ship_type in rules["excluded_ship_types"]:
        return False
    length = vessel.get("length_m")
    if length is not None and length < rules["min_length_m"]:
        return False
    return True


# ---------------------------------------------------------------- collection

async def collect(api_key, cfg):
    """Listen for a fixed window and return the latest snapshot per MMSI."""
    seen = {}
    rules = cfg["rules"]
    deadline = now() + timedelta(seconds=rules["listen_seconds"])

    subscription = {
        "APIKey": api_key,
        "BoundingBoxes": [cfg["subscription_box"]],
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    async with websockets.connect(STREAM_URL, ping_interval=20) as ws:
        await ws.send(json.dumps(subscription))
        while now() < deadline:
            remaining = (deadline - now()).total_seconds()
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break
            handle_message(json.loads(raw), seen)

    return {mmsi: v for mmsi, v in seen.items() if is_interesting(v, rules)}


def handle_message(msg, seen):
    meta = msg.get("MetaData", {})
    mmsi = str(meta.get("MMSI", "")).strip()
    if not mmsi:
        return

    rec = seen.setdefault(mmsi, {
        "mmsi": mmsi, "name": None, "imo": None,
        "ship_type": None, "length_m": None,
        "lat": None, "lon": None, "sog": None,
    })

    name = (meta.get("ShipName") or "").strip()
    if name:
        rec["name"] = name
    if meta.get("latitude") is not None:
        rec["lat"] = meta["latitude"]
        rec["lon"] = meta["longitude"]

    body = msg.get("Message", {})

    if msg.get("MessageType") == "PositionReport":
        pr = body.get("PositionReport", {})
        if pr.get("Sog") is not None:
            rec["sog"] = pr["Sog"]

    elif msg.get("MessageType") == "ShipStaticData":
        sd = body.get("ShipStaticData", {})
        imo = sd.get("ImoNumber")
        if imo:
            rec["imo"] = imo
        if sd.get("Type") is not None:
            rec["ship_type"] = sd["Type"]
        dim = sd.get("Dimension") or {}
        if dim.get("A") is not None and dim.get("B") is not None:
            rec["length_m"] = dim["A"] + dim["B"]


# ---------------------------------------------------------------- state logic

def update_state(snapshot, state, cfg, run_time):
    """Merge this run's snapshot into the dwell state. Returns new events."""
    rules = cfg["rules"]
    gap = timedelta(hours=rules["session_gap_hours"])
    events = []

    for mmsi, v in snapshot.items():
        if v["lat"] is None:
            continue
        zone = find_zone(v["lat"], v["lon"], cfg["zones"])
        sog = v.get("sog")
        stationary = sog is None or sog <= rules["max_speed_knots"]
        if zone is None or not stationary:
            continue

        entry = state.get(mmsi)
        if entry is None or entry["zone"] != zone or run_time - parse(entry["last_seen"]) > gap:
            entry = {
                "zone": zone,
                "first_seen": iso(run_time),
                "last_seen": iso(run_time),
                "alerted": False,
                "name": v["name"],
                "imo": v["imo"],
                "ship_type": v["ship_type"],
                "length_m": v["length_m"],
            }
        else:
            entry["last_seen"] = iso(run_time)
            for field in ("name", "imo", "ship_type", "length_m"):
                if v.get(field) and not entry.get(field):
                    entry[field] = v[field]

        hours = (run_time - parse(entry["first_seen"])).total_seconds() / 3600
        if hours >= rules["dwell_hours_to_alert"] and not entry["alerted"]:
            entry["alerted"] = True
            events.append({
                "detected_utc": iso(run_time),
                "imo": entry.get("imo") or "",
                "mmsi": mmsi,
                "vessel_name": entry.get("name") or "",
                "ship_type": entry.get("ship_type") or "",
                "length_m": entry.get("length_m") or "",
                "zone": entry["zone"],
                "first_seen_utc": entry["first_seen"],
                "hours_in_zone": round(hours, 1),
            })

        state[mmsi] = entry

    cutoff = run_time - timedelta(days=30)
    state = {m: e for m, e in state.items() if parse(e["last_seen"]) > cutoff}
    return state, events


def append_events(events):
    new_file = not EVENTS_FILE.exists()
    with open(EVENTS_FILE, "a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=EVENT_COLUMNS)
        if new_file:
            writer.writeheader()
        for row in events:
            writer.writerow(row)


def write_current(state, cfg, run_time):
    """Human-readable snapshot of who is sitting in the zones right now."""
    fresh = timedelta(hours=cfg["rules"]["session_gap_hours"])
    lines = [f"# Vessels in Riga repair zones — updated {iso(run_time)}", ""]
    rows = []
    for mmsi, e in state.items():
        if run_time - parse(e["last_seen"]) > fresh:
            continue
        hours = (parse(e["last_seen"]) - parse(e["first_seen"])).total_seconds() / 3600
        rows.append((hours, mmsi, e))
    for hours, mmsi, e in sorted(rows, reverse=True):
        lines.append(
            f"- {e.get('name') or 'UNKNOWN'} | IMO {e.get('imo') or '?'} | MMSI {mmsi} "
            f"| {e['zone']} | {hours:.0f} h | type {e.get('ship_type') or '?'} "
            f"| {e.get('length_m') or '?'} m"
        )
    if not rows:
        lines.append("_No qualifying vessels right now._")
    (BASE / "current.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------- entry point

async def main():
    api_key = os.environ.get("AISSTREAM_API_KEY")
    if not api_key:
        sys.exit("AISSTREAM_API_KEY is not set")

    cfg = load_json(ZONES_FILE, None)
    state = load_json(STATE_FILE, {})
    run_time = now()

    try:
        snapshot = await collect(api_key, cfg)
    except Exception as exc:
        print(f"stream error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"collected {len(snapshot)} qualifying vessels in the subscription box")

    state, events = update_state(snapshot, state, cfg, run_time)

    STATE_FILE.write_text(json.dumps(state, indent=1, ensure_ascii=False), encoding="utf-8")
    write_current(state, cfg, run_time)
    if events:
        append_events(events)
    print(f"new repair events: {len(events)}")


if __name__ == "__main__":
    asyncio.run(main())
