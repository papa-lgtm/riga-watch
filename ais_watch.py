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


def normalise_zones(cfg):
    """Swap min/max if they were entered the wrong way round, and say so."""
    for z in cfg["zones"]:
        for lo, hi in (("lat_min", "lat_max"), ("lon_min", "lon_max")):
            if z[lo] > z[hi]:
                z[lo], z[hi] = z[hi], z[lo]
                print(f"note: {lo}/{hi} were swapped in zone '{z['name']}' — corrected")
    return cfg


def find_zone(lat, lon, zones):
    for z in zones:
        if z["lat_min"] <= lat <= z["lat_max"] and z["lon_min"] <= lon <= z["lon_max"]:
            return z["name"]
    return None


def rejection_reason(vessel, rules):
    """Return why a vessel is not interesting, or None if it is."""
    ship_type = vessel.get("ship_type")
    if ship_type is not None and ship_type in rules["excluded_ship_types"]:
        return f"ship type {ship_type} excluded"
    length = vessel.get("length_m")
    if length is not None and length < rules["min_length_m"]:
        return f"length {length} m below {rules['min_length_m']} m"
    return None


def is_interesting(vessel, rules):
    return rejection_reason(vessel, rules) is None


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

    stats = {"raw": 0, "position": 0, "static": 0, "other": 0}
    first_shown = False

    async with websockets.connect(STREAM_URL, ping_interval=20) as ws:
        await ws.send(json.dumps(subscription))
        print(f"subscribed with box {cfg['subscription_box']}, "
              f"listening for {rules['listen_seconds']} s")

        while now() < deadline:
            remaining = (deadline - now()).total_seconds()
            if remaining <= 0:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                break

            msg = json.loads(raw)
            stats["raw"] += 1

            # aisstream reports a rejected subscription as a plain error frame
            if isinstance(msg, dict) and ("error" in msg or "Error" in msg):
                raise RuntimeError(f"aisstream rejected the subscription: {msg}")

            if not first_shown:
                print(f"first message looks like: {json.dumps(msg)[:400]}")
                first_shown = True

            mtype = msg.get("MessageType")
            if mtype == "PositionReport":
                stats["position"] += 1
            elif mtype == "ShipStaticData":
                stats["static"] += 1
            else:
                stats["other"] += 1

            handle_message(msg, seen)

    print(f"messages: {stats['raw']} total "
          f"({stats['position']} position, {stats['static']} static, {stats['other']} other)")
    print(f"unique vessels seen in box: {len(seen)}")

    kept, dropped = {}, []
    for mmsi, v in seen.items():
        reason = rejection_reason(v, rules)
        if reason is None:
            kept[mmsi] = v
        else:
            dropped.append(f"{v.get('name') or mmsi}: {reason}")

    if dropped:
        print(f"filtered out {len(dropped)}: " + "; ".join(dropped[:15]))

    notes = [
        f"- messages received: {stats['raw']} "
        f"({stats['position']} position, {stats['static']} static, {stats['other']} other)",
        f"- unique vessels in box: {len(seen)}",
        f"- passed the filters: {len(kept)}",
        f"- filtered out: {len(dropped)}",
    ]
    return kept, seen, notes


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

    in_zone = moving = 0

    for mmsi, v in snapshot.items():
        if v["lat"] is None:
            continue
        zone = find_zone(v["lat"], v["lon"], cfg["zones"])
        sog = v.get("sog")
        stationary = sog is None or sog <= rules["max_speed_knots"]
        if zone is not None:
            in_zone += 1
            if not stationary:
                moving += 1
                print(f"  in zone but moving: {v.get('name') or mmsi} "
                      f"{sog} kn, {zone}")
        if zone is None or not stationary:
            continue
        print(f"  standing in zone: {v.get('name') or mmsi} "
              f"IMO {v.get('imo') or '?'}, {zone}")

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

    print(f"inside repair zones: {in_zone} ({moving} of them moving)")

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


def write_debug(snapshot, cfg, run_time, notes):
    """Dump what this run actually saw, so the result can be checked later."""
    lines = [f"# Debug report — {iso(run_time)}", ""]
    lines += notes + ["", "## Zones in use", ""]
    for z in cfg["zones"]:
        lines.append(
            f"- {z['name']}: lat {z['lat_min']}–{z['lat_max']}, "
            f"lon {z['lon_min']}–{z['lon_max']}"
        )
    lines += ["", "## Vessels seen in the subscription box", ""]
    if not snapshot:
        lines.append("_None._")
    else:
        lines.append("| MMSI | Name | IMO | Lat | Lon | Speed | Type | Length | Zone |")
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for mmsi, v in sorted(snapshot.items()):
            zone = find_zone(v["lat"], v["lon"], cfg["zones"]) if v["lat"] else None
            lines.append(
                f"| {mmsi} | {v.get('name') or ''} | {v.get('imo') or ''} "
                f"| {v.get('lat')} | {v.get('lon')} | {v.get('sog')} "
                f"| {v.get('ship_type')} | {v.get('length_m')} | {zone or '—'} |"
            )
    (BASE / "debug.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    cfg = normalise_zones(cfg)
    override = os.environ.get("LISTEN_SECONDS")
    if override:
        cfg["rules"]["listen_seconds"] = int(override)
    state = load_json(STATE_FILE, {})
    run_time = now()

    try:
        snapshot, everything, notes = await collect(api_key, cfg)
    except Exception as exc:
        print(f"stream error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"collected {len(snapshot)} qualifying vessels in the subscription box")

    write_debug(everything, cfg, run_time, notes)
    state, events = update_state(snapshot, state, cfg, run_time)

    STATE_FILE.write_text(json.dumps(state, indent=1, ensure_ascii=False), encoding="utf-8")
    write_current(state, cfg, run_time)
    if events:
        append_events(events)
    print(f"new repair events: {len(events)}")


if __name__ == "__main__":
    asyncio.run(main())
