"""
Synthetic real estate market data generator for the Databricks lakehouse project.

Simulates what a brokerage receives from an MLS (Multiple Listing Service) feed:

  reference/ (static, loaded once)
    markets.csv                 8 metros x 5 ZIPs, region used for row-level security
    offices.csv                 brokerage offices
    agents.csv                  initial agent roster (SCD2 source)
    properties.csv              property master (beds, baths, sqft, lat/lon ...)
    mortgage_rates_weekly.csv   synthetic 30y / 15y fixed rates

  landing/ (one file per day -> simulates incremental arrival for Auto Loader)
    listing_events/listing_events_YYYY-MM-DD.jsonl   NEW / PRICE_CHANGE / STATUS_CHANGE (CDC feed)
    sales/sales_YYYY-MM-DD.csv                       closed transactions (some late, some corrected)
    agent_updates/agent_updates_YYYY-MM-DD.csv       agent office moves / new hires (SCD2 CDC)

Deliberate "real world" problems baked in (so you have something to engineer):
  * ~0.5% duplicate events (same event_id re-sent, sometimes in the next day's file)
  * ~0.5% bad prices (null / negative), ~0.3% events missing property_id
  * late-arriving sales: close_date up to 10 days before the file date
  * corrected sales: same sale_id re-sent with a higher record_version
  * schema evolution: from 2026-08-15 NEW events carry hoa_fee + virtual_tour_url

Everything is deterministic (fixed seed) so your numbers match between runs.
Usage:  python data/generate_data.py
"""

import csv
import json
import math
import os
import random
from datetime import date, datetime, timedelta

SEED = 42
START = date(2026, 7, 1)
DAYS = 90
SCHEMA_V2_DATE = date(2026, 8, 15)

HERE = os.path.dirname(os.path.abspath(__file__))
REF = os.path.join(HERE, "reference")
LAND = os.path.join(HERE, "landing")

rng = random.Random(SEED)

# --------------------------------------------------------------------------- markets
METROS = [
    # metro, state, region, base median price, lat, lon, zips
    ("Austin", "TX", "Texas", 520_000, 30.27, -97.74, ["78701", "78704", "78745", "78759", "78748"]),
    ("Dallas", "TX", "Texas", 430_000, 32.78, -96.80, ["75201", "75204", "75214", "75230", "75248"]),
    ("Phoenix", "AZ", "West", 450_000, 33.45, -112.07, ["85004", "85016", "85018", "85032", "85044"]),
    ("Denver", "CO", "West", 590_000, 39.74, -104.99, ["80202", "80205", "80210", "80220", "80237"]),
    ("Seattle", "WA", "West", 820_000, 47.61, -122.33, ["98101", "98103", "98115", "98118", "98144"]),
    ("Atlanta", "GA", "Southeast", 410_000, 33.75, -84.39, ["30303", "30306", "30309", "30318", "30342"]),
    ("Charlotte", "NC", "Southeast", 400_000, 35.23, -80.84, ["28202", "28203", "28205", "28210", "28277"]),
    ("Tampa", "FL", "Southeast", 420_000, 27.95, -82.46, ["33602", "33606", "33609", "33611", "33629"]),
]

STREETS = ["Oak", "Maple", "Cedar", "Pine", "Elm", "Willow", "Lakeview", "Sunset", "Highland", "Park",
           "River", "Hillcrest", "Meadow", "Ridge", "Forest", "Spring", "Valley", "Main", "Church", "Mill"]
SUFFIX = ["St", "Ave", "Dr", "Ln", "Ct", "Blvd", "Way", "Pl"]
FIRST = ["James", "Maria", "Robert", "Priya", "Michael", "Aisha", "David", "Mei", "Daniel", "Sofia",
         "Arjun", "Emily", "Carlos", "Fatima", "Kevin", "Grace", "Luis", "Hannah", "Omar", "Chloe",
         "Ravi", "Olivia", "Ethan", "Zara", "Noah", "Isabel", "Samuel", "Leah", "Victor", "Nina"]
LAST = ["Smith", "Garcia", "Patel", "Johnson", "Nguyen", "Brown", "Khan", "Lee", "Martinez", "Davis",
        "Reddy", "Wilson", "Lopez", "Chen", "Taylor", "Ali", "Anderson", "Thomas", "Moore", "Kim"]
BRANDS = ["Keystone Realty", "Cornerstone Homes", "Bluegate Properties", "Summit Realty Group"]
PTYPES = [("Single Family", 0.65), ("Condo", 0.20), ("Townhouse", 0.12), ("Multi-Family", 0.03)]
FINANCING = [("Conventional", 0.55), ("FHA", 0.14), ("VA", 0.07), ("Cash", 0.18), ("Jumbo", 0.06)]


def weighted(choices):
    r, acc = rng.random(), 0.0
    for val, w in choices:
        acc += w
        if r <= acc:
            return val
    return choices[-1][0]


def write_csv(path, rows, header):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def ts(d, hour_lo=7, hour_hi=21):
    t = datetime(d.year, d.month, d.day, rng.randint(hour_lo, hour_hi), rng.randint(0, 59), rng.randint(0, 59))
    return t.isoformat()


# --------------------------------------------------------------------------- reference: markets
markets, zip_info = [], {}
for metro, state, region, base, lat, lon, zips in METROS:
    for z in zips:
        mult = round(rng.uniform(0.75, 1.45), 2)
        markets.append({"zip_code": z, "city": metro, "state": state, "metro": metro, "region": region,
                        "price_index": mult, "center_lat": lat, "center_lon": lon})
        zip_info[z] = (metro, region, base * mult, lat, lon)
write_csv(os.path.join(REF, "markets.csv"), markets,
          ["zip_code", "city", "state", "metro", "region", "price_index", "center_lat", "center_lon"])

# --------------------------------------------------------------------------- offices + agents
offices = []
for i, (metro, state, region, *_rest) in enumerate(METROS):
    for j, side in enumerate(["North", "South"]):
        oid = f"OF{len(offices) + 1:03d}"
        offices.append({"office_id": oid, "office_name": f"{BRANDS[(i + j) % len(BRANDS)]} - {metro} {side}",
                        "brand": BRANDS[(i + j) % len(BRANDS)], "metro": metro, "region": region})
write_csv(os.path.join(REF, "offices.csv"), offices, ["office_id", "office_name", "brand", "metro", "region"])

agents = {}
for n in range(160):
    aid = f"AG{n + 1:04d}"
    office = offices[n % len(offices)]
    fn, ln = rng.choice(FIRST), rng.choice(LAST)
    agents[aid] = {"agent_id": aid, "first_name": fn, "last_name": ln,
                   "email": f"{fn.lower()}.{ln.lower()}{n + 1}@example.com",
                   "office_id": office["office_id"],
                   "license_date": (date(2008, 1, 1) + timedelta(days=rng.randint(0, 6200))).isoformat(),
                   "status": "Active", "updated_at": "2026-06-30T00:00:00"}
write_csv(os.path.join(REF, "agents.csv"), agents.values(),
          ["agent_id", "first_name", "last_name", "email", "office_id", "license_date", "status", "updated_at"])
agents_by_metro = {}
office_metro = {o["office_id"]: o["metro"] for o in offices}
for a in agents.values():
    agents_by_metro.setdefault(office_metro[a["office_id"]], []).append(a["agent_id"])

# --------------------------------------------------------------------------- properties
properties = {}
for n in range(6000):
    pid = f"P{n + 1:06d}"
    z = rng.choice(list(zip_info))
    metro, region, zip_median, lat, lon = zip_info[z]
    ptype = weighted(PTYPES)
    beds = {"Condo": rng.choice([1, 1, 2, 2, 3]), "Townhouse": rng.choice([2, 3, 3, 4])}.get(
        ptype, rng.choice([2, 3, 3, 4, 4, 5]))
    baths = max(1.0, beds - rng.choice([0, 0.5, 1, 1.5]))
    sqft = int(rng.gauss(650 + beds * 480, 220))
    sqft = max(450, sqft)
    lot = 0 if ptype == "Condo" else int(rng.uniform(1500, 3500) if ptype == "Townhouse" else rng.uniform(4500, 15000))
    # "true" market value used by the simulation (not published)
    value = zip_median * (sqft / 1900) ** 0.85 * rng.uniform(0.85, 1.15) * (0.8 if ptype == "Condo" else 1.0)
    properties[pid] = {
        "property_id": pid,
        "street_address": f"{rng.randint(100, 9999)} {rng.choice(STREETS)} {rng.choice(SUFFIX)}",
        "zip_code": z, "property_type": ptype, "beds": beds, "baths": baths, "sqft": sqft,
        "lot_sqft": lot, "year_built": rng.randint(1925, 2025),
        "latitude": round(lat + rng.uniform(-0.12, 0.12), 5),
        "longitude": round(lon + rng.uniform(-0.12, 0.12), 5),
        "_value": value, "_metro": metro,
    }
write_csv(os.path.join(REF, "properties.csv"),
          [{k: v for k, v in p.items() if not k.startswith("_")} for p in properties.values()],
          ["property_id", "street_address", "zip_code", "property_type", "beds", "baths", "sqft",
           "lot_sqft", "year_built", "latitude", "longitude"])

# --------------------------------------------------------------------------- mortgage rates (synthetic)
rates, week = [], START - timedelta(days=START.weekday())
rate = 6.62
for w in range(14):
    # rises into August, then eases: gives the BI side a story to find
    rate += 0.06 if w < 6 else -0.09
    rate += rng.uniform(-0.04, 0.04)
    rates.append({"week_start": (week + timedelta(weeks=w)).isoformat(),
                  "rate_30yr_fixed": round(rate, 2), "rate_15yr_fixed": round(rate - 0.72, 2)})
write_csv(os.path.join(REF, "mortgage_rates_weekly.csv"), rates,
          ["week_start", "rate_30yr_fixed", "rate_15yr_fixed"])


def rate_on(d):
    for r in reversed(rates):
        if date.fromisoformat(r["week_start"]) <= d:
            return r["rate_30yr_fixed"]
    return rates[0]["rate_30yr_fixed"]


# --------------------------------------------------------------------------- simulation
events_by_day = {START + timedelta(days=i): [] for i in range(DAYS)}
sales_by_day = {START + timedelta(days=i): [] for i in range(DAYS)}
agent_updates_by_day = {START + timedelta(days=i): [] for i in range(DAYS)}
event_seq, listing_seq, sale_seq = [0], [0], [0]
listings = {}       # listing_id -> state
available = set(properties)
REMARKS = ["Updated kitchen", "Corner lot", "Open floor plan", "Near light rail", "New roof 2024",
           "Walk to schools", "Mountain views", "Pool and spa", "Move-in ready", "Investor special"]


def emit(day, ev):
    event_seq[0] += 1
    ev = {"event_id": f"EV{event_seq[0]:08d}", **ev, "source_system": "MLS_FEED_V2"}
    events_by_day[day].append(ev)
    return ev


def new_listing(day, list_day):
    pid = rng.choice(sorted(available))
    available.discard(pid)
    p = properties[pid]
    listing_seq[0] += 1
    lid = f"L{listing_seq[0]:07d}"
    markup = rng.uniform(0.96, 1.12)               # some sellers overprice
    price = int(round(p["_value"] * markup, -3))
    agent = rng.choice(agents_by_metro[p["_metro"]])
    listings[lid] = {"pid": pid, "price": price, "status": "Active", "agent": agent, "markup": markup,
                     "list_day": list_day, "pending_day": None}
    ev = {"listing_id": lid, "property_id": pid, "event_type": "NEW", "status": "Active",
          "list_price": price, "previous_price": None, "listing_agent_id": agent,
          "list_date": list_day.isoformat(), "remarks": rng.choice(REMARKS),
          "event_ts": ts(list_day) if list_day == day else f"{list_day.isoformat()}T09:00:00"}
    if day >= SCHEMA_V2_DATE:                       # schema evolution
        ev["hoa_fee"] = rng.choice([0, 0, 0, 85, 150, 240, 410]) if p["property_type"] != "Condo" else rng.randint(250, 700)
        ev["virtual_tour_url"] = f"https://tours.example.com/{lid}"
    emit(day, ev)


# backlog: listings already active on day 1 (initial load)
for _ in range(900):
    new_listing(START, START - timedelta(days=rng.randint(1, 75)))

for i in range(DAYS):
    day = START + timedelta(days=i)
    weekday_factor = 0.55 if day.weekday() >= 5 else 1.1
    rate_drag = 1 - (rate_on(day) - 6.5) * 0.9    # higher rates -> slower market
    for _ in range(int(rng.gauss(34 * weekday_factor, 5))):
        if available:
            new_listing(day, day)

    for lid, L in list(listings.items()):
        if L["list_day"] >= day and L["status"] == "Active":
            continue
        if L["status"] == "Active":
            p_pending = 0.032 * rate_drag * (1.6 if L["markup"] < 1.0 else 1.0 if L["markup"] < 1.06 else 0.45)
            r = rng.random()
            if r < p_pending:
                L["status"], L["pending_day"] = "Pending", day
                emit(day, {"listing_id": lid, "property_id": L["pid"], "event_type": "STATUS_CHANGE",
                           "status": "Pending", "list_price": L["price"], "previous_price": None,
                           "listing_agent_id": L["agent"], "event_ts": ts(day)})
            elif r < p_pending + (0.022 if L["markup"] > 1.04 else 0.008):
                old = L["price"]
                L["price"] = int(round(old * rng.uniform(0.94, 0.985), -3))
                L["markup"] *= L["price"] / old
                emit(day, {"listing_id": lid, "property_id": L["pid"], "event_type": "PRICE_CHANGE",
                           "status": "Active", "list_price": L["price"], "previous_price": old,
                           "listing_agent_id": L["agent"], "event_ts": ts(day)})
            elif r < p_pending + 0.025 and (day - L["list_day"]).days > 45:
                L["status"] = "Withdrawn"
                emit(day, {"listing_id": lid, "property_id": L["pid"], "event_type": "STATUS_CHANGE",
                           "status": "Withdrawn", "list_price": L["price"], "previous_price": None,
                           "listing_agent_id": L["agent"], "event_ts": ts(day)})
        elif L["status"] == "Pending":
            days_pending = (day - L["pending_day"]).days
            if days_pending > 5 and rng.random() < 0.004:      # deal fell through
                L["status"] = "Active"
                emit(day, {"listing_id": lid, "property_id": L["pid"], "event_type": "STATUS_CHANGE",
                           "status": "Active", "list_price": L["price"], "previous_price": None,
                           "listing_agent_id": L["agent"], "event_ts": ts(day)})
            elif days_pending >= 21 and rng.random() < 0.09:
                L["status"] = "Sold"
                ev = emit(day, {"listing_id": lid, "property_id": L["pid"], "event_type": "STATUS_CHANGE",
                                "status": "Sold", "list_price": L["price"], "previous_price": None,
                                "listing_agent_id": L["agent"], "event_ts": ts(day)})
                p = properties[L["pid"]]
                sale_price = int(round(L["price"] * rng.uniform(0.95, 1.03) * (1.02 if L["markup"] < 1 else 1), -3))
                fin = weighted(FINANCING)
                ltv = {"Cash": 0, "FHA": rng.uniform(0.9, 0.965), "VA": 1.0}.get(fin, rng.uniform(0.7, 0.9))
                sale_seq[0] += 1
                buyer_metro_agents = agents_by_metro[p["_metro"]]
                sale = {"sale_id": f"S{sale_seq[0]:07d}", "listing_id": lid, "property_id": L["pid"],
                        "close_date": day.isoformat(), "sale_price": sale_price,
                        "listing_agent_id": L["agent"], "buyer_agent_id": rng.choice(buyer_metro_agents),
                        "financing_type": fin, "loan_amount": int(sale_price * ltv),
                        "record_version": 1, "reported_at": None}
                # late arrival: 10% of sales show up 1-10 days after closing
                lag = rng.randint(1, 10) if rng.random() < 0.10 else 0
                arrive = min(day + timedelta(days=lag), START + timedelta(days=DAYS - 1))
                sale["reported_at"] = ts(arrive)
                sales_by_day[arrive].append(sale)
                # correction: 2% re-sent later with a fixed price
                if rng.random() < 0.02:
                    fix_day = min(arrive + timedelta(days=rng.randint(1, 6)), START + timedelta(days=DAYS - 1))
                    fixed = dict(sale, sale_price=sale_price + rng.choice([-5000, 2500, 10000]),
                                 record_version=2, reported_at=ts(fix_day))
                    sales_by_day[fix_day].append(fixed)

    # agent CDC: office moves + new hires
    if rng.random() < 0.18:
        aid = rng.choice(sorted(agents))
        a = agents[aid]
        same_metro = [o["office_id"] for o in offices
                      if o["metro"] == office_metro[a["office_id"]] and o["office_id"] != a["office_id"]]
        a = dict(a, office_id=rng.choice(same_metro), updated_at=ts(day))
        agents[aid] = a
        agent_updates_by_day[day].append({**a, "operation": "UPDATE"})
    if rng.random() < 0.08:
        aid = f"AG{len(agents) + 1:04d}"
        office = rng.choice(offices)
        fn, ln = rng.choice(FIRST), rng.choice(LAST)
        a = {"agent_id": aid, "first_name": fn, "last_name": ln,
             "email": f"{fn.lower()}.{ln.lower()}{len(agents) + 1}@example.com",
             "office_id": office["office_id"], "license_date": day.isoformat(),
             "status": "Active", "updated_at": ts(day)}
        agents[aid] = a
        agents_by_metro[office["metro"]].append(aid)
        agent_updates_by_day[day].append({**a, "operation": "INSERT"})

# --------------------------------------------------------------------------- inject data-quality problems
all_days = sorted(events_by_day)
for d in all_days:
    evs = events_by_day[d]
    for ev in list(evs):
        r = rng.random()
        if r < 0.005:                                   # duplicate, same or next day
            target = d if rng.random() < 0.5 or d == all_days[-1] else d + timedelta(days=1)
            events_by_day[target].append(dict(ev))
        elif r < 0.010:
            ev["list_price"] = rng.choice([None, -ev["list_price"], 0])
        elif r < 0.013:
            ev["property_id"] = None
    rng.shuffle(evs)

# --------------------------------------------------------------------------- write landing files
for d in all_days:
    p = os.path.join(LAND, "listing_events", f"listing_events_{d.isoformat()}.jsonl")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        for ev in events_by_day[d]:
            f.write(json.dumps(ev) + "\n")
    write_csv(os.path.join(LAND, "sales", f"sales_{d.isoformat()}.csv"), sales_by_day[d],
              ["sale_id", "listing_id", "property_id", "close_date", "sale_price", "listing_agent_id",
               "buyer_agent_id", "financing_type", "loan_amount", "record_version", "reported_at"])
    write_csv(os.path.join(LAND, "agent_updates", f"agent_updates_{d.isoformat()}.csv"), agent_updates_by_day[d],
              ["agent_id", "first_name", "last_name", "email", "office_id", "license_date", "status",
               "updated_at", "operation"])

n_ev = sum(len(v) for v in events_by_day.values())
n_sales = sum(len(v) for v in sales_by_day.values())
n_ag = sum(len(v) for v in agent_updates_by_day.values())
print(f"properties={len(properties)} listings={len(listings)} events={n_ev} sales_rows={n_sales} agent_updates={n_ag}")
