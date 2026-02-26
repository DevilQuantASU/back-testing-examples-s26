import argparse
import csv
import json
import logging
import requests
import datetime
import time
from typing import Optional

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
CLOB_API_BASE = "https://clob.polymarket.com"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_event_data(slug: str) -> dict:
    url = f"{GAMMA_API_BASE}/events/slug/{slug}"
    response = requests.get(url)
    if response.status_code == 404:
        raise ValueError(f"Event with slug '{slug}' not found.")
    response.raise_for_status()
    return response.json()

def extract_clob_tokens(market: dict) -> list[str]:
    clob_token_ids_str = market.get("clobTokenIds")
    if not clob_token_ids_str:
        raise ValueError(f"No clobTokenIds found for market {market.get('id')}")
    # The API returns this as a JSON string array like '["0x...", "0x..."]' or comma separated sometimes.
    try:
        tokens = json.loads(clob_token_ids_str)
        if isinstance(tokens, list):
            return tokens
    except json.JSONDecodeError:
        pass
    
    # Fallback if it's just a comma separated string
    return [t.strip() for t in clob_token_ids_str.split(",") if t.strip()]

def fetch_price_history(token_id: str, start_ts: int, end_ts: int, fidelity: int = 10) -> list[dict]:
    url = f"{CLOB_API_BASE}/prices-history"
    params = {
        "market": token_id,
        "startTs": start_ts,
        "endTs": end_ts,
        "fidelity": fidelity
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data.get("history", [])

def main():
    parser = argparse.ArgumentParser(description="Ingest Polymarket price history for an event slug.")
    parser.add_argument("slug", help="The event slug (e.g., 'will-bitcoin-hit-100k-in-2026')")
    parser.add_argument("--outcome-index", type=int, default=0, help="0 for Yes, 1 for No (default: 0)")
    parser.add_argument("--market-index", type=int, default=0, help="Index of the market in the event (default: 0)")
    parser.add_argument("--output", type=str, default="market_data.csv", help="Output CSV filename")
    
    args = parser.parse_args()
    
    logger.info(f"Fetching event data for slug: {args.slug}")
    try:
        event = fetch_event_data(args.slug)
    except Exception as e:
        logger.error(f"Failed to fetch event data: {e}")
        return

    markets = event.get("markets", [])
    if not markets:
        logger.error("No markets found in this event.")
        return

    if args.market_index >= len(markets):
        logger.error(f"Market index {args.market_index} out of range. The event has {len(markets)} markets.")
        return

    market = markets[args.market_index]
    logger.info(f"Using market: {market.get('question')} (ID: {market.get('id')})")
    
    try:
        tokens = extract_clob_tokens(market)
    except Exception as e:
        logger.error(f"Failed to extract clob tokens: {e}")
        return
        
    if args.outcome_index >= len(tokens):
        logger.error(f"Outcome index {args.outcome_index} out of range. The market has {len(tokens)} token ids.")
        return

    selected_token_id = tokens[args.outcome_index]
    logger.info(f"Selected Token ID: {selected_token_id}")

    # Determine pagination bounds
    start_date_str = market.get("startDate")
    end_date_str = market.get("endDate")
    
    # Fallback to a default old date if startDate is missing to ensure we start far back enough
    if start_date_str:
        try:
            # Handle ISO timestamp, sometimes ending with Z
            dt_start = datetime.datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
            current_start_ts = int(dt_start.timestamp())
        except ValueError:
             # Fallback
             current_start_ts = int(time.time()) - (365 * 24 * 60 * 60) # 1 year ago
    else:
         current_start_ts = int(time.time()) - (365 * 24 * 60 * 60)
         
    # Determine the hard end date
    current_time_ts = int(time.time())
    if end_date_str:
         try:
            dt_end = datetime.datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            market_end_ts = int(dt_end.timestamp())
            final_end_ts = min(market_end_ts, current_time_ts)
         except ValueError:
            final_end_ts = current_time_ts
    else:
         final_end_ts = current_time_ts

    logger.info(f"Fetching price history from {current_start_ts} to {final_end_ts}...")
    
    all_history = []
    chunk_size_seconds = 15 * 24 * 60 * 60 # 15 days
    
    while current_start_ts < final_end_ts:
        chunk_end_ts = min(current_start_ts + chunk_size_seconds, final_end_ts)
        
        try:
             logger.info(f"Fetching chunk: {current_start_ts} to {chunk_end_ts}")
             # Fidelity = 10 (10 minutes) per requirements
             history_chunk = fetch_price_history(selected_token_id, start_ts=current_start_ts, end_ts=chunk_end_ts, fidelity=10)
             if history_chunk:
                all_history.extend(history_chunk)
        except Exception as e:
             logger.error(f"Failed to fetch price history chunk: {e}")
             # Decide if we want to break or continue; we'll continue for now
             
        # Move forward
        current_start_ts = chunk_end_ts

    logger.info(f"Retrieved {len(all_history)} data points total. Writing to {args.output}...")
    
    # Define required columns for standard Backtesting Pipeline format
    fieldnames = [
        "timestamp", "event_id", "last_price", 
        "best_bid", "best_ask", "bid_size", 
        "ask_size", "volume", "is_settled"
    ]

    with open(args.output, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for point in all_history:
            t = point.get("t")
            p = point.get("p")
            
            # Polymarket API returns integer unix timestamps in seconds.
            try:
                dt = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc)
                iso_time = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                iso_time = ""
                
            # Polymarket prices are generally expressed as decimals between 0 and 1, 
            # though some interfaces scale them to 0-100. 
            is_settled = 1 if (p is not None and (p == 0.0 or p == 1.0 or p == 100.0)) else 0
            
            writer.writerow({
                "timestamp": iso_time,
                "event_id": selected_token_id,  # Using CLOB token as the unique identifier
                "last_price": p,
                # The prices-history endpoint only provides aggregate timestamps and prices, 
                # so the orderbook depth fields will be recorded as empty for now.
                "best_bid": "",
                "best_ask": "",
                "bid_size": "",
                "ask_size": "",
                "volume": "",
                "is_settled": is_settled
            })

    logger.info("Success! CSV file generated.")

if __name__ == "__main__":
    main()
