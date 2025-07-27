import os
import json
import math
import csv
import time
import yaml
import pytz
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from typing import Dict, Any, List, Tuple, Optional

from amadeus_client import AmadeusClient

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, "output")
os.makedirs(OUT_DIR, exist_ok=True)

def load_yaml(path: str) -> Any:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def date_range(start: date, end: date, step_days: int = 1):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=step_days)

def mondays_thursdays_saturdays(start: date, end: date) -> List[date]:
    allowed = {0, 3, 5}  # Mon/Thu/Sat
    return [d for d in date_range(start, end) if d.weekday() in allowed]

def sliding_windows(config: Dict[str, Any]) -> List[date]:
    year = config["search"]["year"]
    months = config["search"]["summer_months"]
    los = config["search"]["length_of_stay_nights"]
    full_daily_scan = config["search"]["full_daily_scan"]
    coarse_dows = set(config["search"]["coarse_days_of_week"])

    start = date(year, months[0], 1)
    # end is the last day that allows LOS nights within the last month
    last_month = months[-1]
    # Get last day of last month
    first_of_next = date(year, last_month, 1) + relativedelta(months=1)
    last_day_of_last = first_of_next - timedelta(days=1)
    end = last_day_of_last - timedelta(days=los - 1)

    if full_daily_scan:
        return list(date_range(start, end, 1))
    else:
        return [d for d in date_range(start, end, 1) if d.weekday() in coarse_dows]

def summarize_offer_price(offer: Dict[str, Any]) -> Optional[Tuple[float, str]]:
    """
    Returns (total_price_float, currency) if available.
    """
    try:
        price = offer["price"]["total"]
        currency = offer["price"]["currency"]
        return (float(price), currency)
    except Exception:
        return None

def parse_search_response(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalizes v3 response into a list of hotel-level summaries:
    { 'hotelId': 'HLPAR266', 'hotelName': '...', 'offerId': '...', 'total': 123.45, 'currency': 'EUR' }
    """
    out = []
    data = resp.get("data", [])
    for hotel in data:
        hotel_id = None
        hotel_name = None
        try:
            hotel_id = hotel.get("hotel", {}).get("hotelId")
            hotel_name = hotel.get("hotel", {}).get("name")
        except Exception:
            pass
        offers = hotel.get("offers", []) if isinstance(hotel, dict) else []
        # Take the cheapest offer for this hotel (usually the API already returns cheapest)
        best = None
        for off in offers:
            s = summarize_offer_price(off)
            if not s:
                continue
            total, currency = s
            if (best is None) or (total < best[0]):
                best = (total, currency, off.get("id"))
        if best:
            out.append({
                "hotelId": hotel_id,
                "hotelName": hotel_name,
                "offerId": best[2],
                "total": best[0],
                "currency": best[1],
            })
    return out

def median(values: List[float]) -> float:
    s = sorted(values)
    n = len(s)
    if n == 0: return float("nan")
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid-1] + s[mid]) / 2.0

def main():
    cfg = load_yaml(os.path.join(ROOT, "config.yaml"))
    env = os.environ.get("AMADEUS_ENV", "test")
    client = AmadeusClient(
        env=env,
        host_test=cfg["api"]["host_test"],
        host_prod=cfg["api"]["host_prod"],
        sleep_between_calls_ms=cfg["api"]["sleep_between_calls_ms"],
    )
    destinations = load_yaml(os.path.join(ROOT, "destinations.yaml"))
    # Prepare check-in dates
    base_windows = sliding_windows(cfg)
    los = cfg["search"]["length_of_stay_nights"]
    adults = 2
    room_qty = 1

    # Shortlist filters
    amenities = cfg["shortlist"]["amenities"]
    ratings = cfg["shortlist"]["ratings"]
    max_hotels = cfg["shortlist"]["max_hotels_per_place"]
    batch_size = cfg["api"]["batch_size"]

    tz = pytz.timezone("Europe/Vienna")
    verified_at = datetime.now(tz).isoformat()

    cheapest_by_place = {}
    raw_quotes_path = os.path.join(OUT_DIR, "raw_quotes.jsonl")
    with open(raw_quotes_path, "w") as rawf:
        pass  # truncate

    for country, places in destinations.items():
        for place in places:
            pname = place["name"]
            print(f"\n=== {country} – {pname} ===")
            lat = place["latitude"]; lon = place["longitude"]
            radius_km = int(place.get("radius_km") or cfg["shortlist"]["radius_km_default"])

            # Step 1: shortlist hotels by geocode + amenities/ratings
            hotels = client.hotel_list_by_geocode(
                latitude=lat, longitude=lon, radius_km=radius_km,
                amenities=amenities, ratings=ratings, hotel_source="ALL"
            )
            hotel_ids = [h.get("hotelId") for h in hotels if h.get("hotelId")]
            # Deduplicate & cap
            uniq_ids = []
            seen = set()
            for hid in hotel_ids:
                if hid not in seen:
                    uniq_ids.append(hid); seen.add(hid)
            hotel_ids = uniq_ids[:max_hotels]
            print(f"Shortlisted {len(hotel_ids)} hotels")

            if not hotel_ids:
                print("No hotels found for this place; skipping.")
                continue

            # Step 2: coarse scan across check-ins
            best_total = math.inf
            best = None  # (check_in_date, hotelId, hotelName, offerId, total, currency)
            # Batch hotelIds to reduce API calls per date
            def chunks(lst, n):
                for i in range(0, len(lst), n):
                    yield lst[i:i+n]

            for d in base_windows:
                totals_for_date = []
                for batch in chunks(hotel_ids, batch_size):
                    resp = client.hotel_search_offers(batch, check_in_date=d.isoformat(), adults=adults, room_quantity=room_qty)
                    if resp is None:
                        print(f"No offers found for {d.isoformat()} on {pname}; skipping.")
                        continue
                    parsed = parse_search_response(resp)
                    totals_for_date.extend(parsed)
                if totals_for_date:
                    # find best for this date
                    winner = min(totals_for_date, key=lambda x: x["total"])
                    with open(raw_quotes_path, "a") as rawf:
                        rawf.write(json.dumps({
                            "country": country, "place": pname, "checkIn": d.isoformat(),
                            "winner": winner
                        }) + "\n")
                    if winner["total"] < best_total:
                        best_total = winner["total"]
                        best = (d.isoformat(), winner["hotelId"], winner["hotelName"], winner["offerId"], winner["total"], winner["currency"])

            if not best:
                print("No priced offers found in coarse scan; skipping refinement.")
                continue

            # Step 3: refine around the best fortnight (± N days)
            refine_days = int(cfg["search"]["refine_window_days"])
            d0 = datetime.fromisoformat(best[0]).date()
            refine_start = d0 - timedelta(days=refine_days)
            refine_end   = d0 + timedelta(days=refine_days)
            for d in date_range(refine_start, refine_end, 1):
                totals_for_date = []
                for batch in chunks(hotel_ids, batch_size):
                    resp = client.hotel_search_offers(batch, check_in_date=d.isoformat(), adults=adults, room_quantity=room_qty)
                    parsed = parse_search_response(resp)
                    totals_for_date.extend(parsed)
                if totals_for_date:
                    winner = min(totals_for_date, key=lambda x: x["total"])
                    with open(raw_quotes_path, "a") as rawf:
                        rawf.write(json.dumps({
                            "country": country, "place": pname, "checkIn": d.isoformat(),
                            "winner": winner
                        }) + "\n")
                    if winner["total"] < best_total:
                        best_total = winner["total"]
                        best = (d.isoformat(), winner["hotelId"], winner["hotelName"], winner["offerId"], winner["total"], winner["currency"])

            if best:
                check_in, hid, hname, offer_id, total, currency = best
                check_out = (datetime.fromisoformat(check_in).date() + timedelta(days=los)).isoformat()
                cheapest_by_place.setdefault(country, {})
                cheapest_by_place[country][pname] = {
                    "checkIn": check_in,
                    "checkOut": check_out,
                    "hotelId": hid,
                    "hotelName": hname,
                    "offerId": offer_id,
                    "total": total,
                    "currency": currency,
                    "verifiedAt": verified_at,
                    "notes": "Price for 2 adults; verify child policy before booking."
                }
                print(f"Cheapest: {check_in} → {check_out} | {hname} ({hid}) | {total} {currency}")
            else:
                print("No winner found after refinement.")

    # Save per-place results
    out_path = os.path.join(OUT_DIR, "cheapest_by_place.json")
    with open(out_path, "w") as f:
        json.dump(cheapest_by_place, f, indent=2)

    # Country-level aggregation (median of places)
    by_country = {}
    for country, places in cheapest_by_place.items():
        totals = []
        for pname, info in places.items():
            totals.append(info["total"])
        if totals:
            by_country[country] = {
                "medianCheapestWeekTotal": median(totals),
                "currency": next(iter(places.values()))["currency"],
                "places": places
            }
    out2_path = os.path.join(OUT_DIR, "cheapest_by_country.json")
    with open(out2_path, "w") as f:
        json.dump(by_country, f, indent=2)

    # CSV report
    csv_path = os.path.join(OUT_DIR, "report.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["country","place","check_in","check_out","hotel_name","hotel_id","offer_id","total","currency","verified_at"])
        for country, places in cheapest_by_place.items():
            for pname, info in places.items():
                w.writerow([country, pname, info["checkIn"], info["checkOut"],
                            info["hotelName"], info["hotelId"], info["offerId"],
                            info["total"], info["currency"], info["verifiedAt"]])
    print(f"\nWrote:\n- {out_path}\n- {out2_path}\n- {csv_path}\n- {os.path.join(OUT_DIR,'raw_quotes.jsonl')}")

if __name__ == "__main__":
    main()
