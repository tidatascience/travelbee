\
import os
import time
import requests
from typing import Dict, Any, List, Optional

class AmadeusClient:
    """
    Minimal Amadeus Self‑Service client for:
      - OAuth2 token
      - Hotel List (by geocode)
      - Hotel Search v3
      - (optional) Offer details
    """

    def __init__(self, env: str = "test", host_test: str = "https://test.api.amadeus.com", host_prod: str = "https://api.amadeus.com", sleep_between_calls_ms: int = 50):
       #self.client_id = os.environ.get("AMADEUS_CLIENT_ID")
       # self.client_secret = os.environ.get("AMADEUS_CLIENT_SECRET")
        self.client_id = "Z4YTYJSofVXRQbq4KoimDyAs5et70sAR"
        self.client_secret = "LGQkzYXrmwAmo60E"
        if not self.client_id or not self.client_secret:
            raise RuntimeError("Set AMADEUS_CLIENT_ID and AMADEUS_CLIENT_SECRET env vars.")
        self.host = host_test if env == "test" else host_prod
        self._token: Optional[str] = None
        self._token_expiry_ts: float = 0.0
        self.sleep_between_calls_ms = sleep_between_calls_ms

    def _token_valid(self) -> bool:
        return self._token and (time.time() < self._token_expiry_ts - 30)

    def _get_token(self) -> str:
        if self._token_valid():
            return self._token
        url = f"{self.host}/v1/security/oauth2/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        resp = requests.post(url, data=data, headers=headers, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        self._token_expiry_ts = time.time() + int(payload.get("expires_in", 1800))
        return self._token

    def _headers(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._get_token()}"}

    def hotel_list_by_geocode(self, latitude: float, longitude: float, radius_km: int, amenities: List[str], ratings: List[int], hotel_source: str = "ALL") -> List[Dict[str, Any]]:
        """
        Returns hotels with name, id, geocode, etc. Supports amenities and ratings filters.
        """
        url = f"{self.host}/v1/reference-data/locations/hotels/by-geocode"
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "radius": radius_km,
            "radiusUnit": "KM",
            "amenities": ",".join(amenities) if amenities else None,
            "ratings": ",".join(map(str, ratings)) if ratings else None,
            "hotelSource": hotel_source,
        }
        # Remove None
        params = {k: v for k, v in params.items() if v is not None}
        time.sleep(self.sleep_between_calls_ms / 1000.0)
        r = requests.get(url, headers=self._headers(), params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        # Some responses use 'data', others 'hotelIds' - standardize
        items = data.get("data", [])
        return items

    def hotel_search_offers(self, hotel_ids: List[str], check_in_date: str, adults: int = 2, room_quantity: int = 1) -> Dict[str, Any]:
        """
        Calls v3 Hotel Search. Accepts up to a batch of hotelIds (tune batch size).
        Returns parsed JSON.
        """
        url = f"{self.host}/v3/shopping/hotel-offers"
        params = {
            "hotelIds": ",".join(hotel_ids),
            "adults": adults,
            "checkInDate": check_in_date,
            "roomQuantity": room_quantity,
            # You can add currency=EUR if desired
        }
        time.sleep(self.sleep_between_calls_ms / 1000.0)
        r = requests.get(url, headers=self._headers(), params=params, timeout=30)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            if r.status_code == 429:
                raise RuntimeError("Rate limit exceeded. Try again later.") from e
            elif r.status_code == 400:
                raise ValueError(f"Bad request: {r.text}") from e
            else:
                return None
       
        return r.json()

    def hotel_offer_details(self, offer_id: str) -> Dict[str, Any]:
        url = f"{self.host}/v3/shopping/hotel-offers/{offer_id}"
        time.sleep(self.sleep_between_calls_ms / 1000.0)
        r = requests.get(url, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json()
