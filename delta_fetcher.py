"""
delta_fetcher.py — Multi-Account Delta Exchange Trade Fetcher
=============================================================
Uses /v2/orders/history (not /v2/fills) because fills endpoint
requires trading-permission API keys which have IP restrictions.
Orders endpoint gives us everything we need:
  - meta_data.pnl       → realized PnL per trade
  - meta_data.entry_price → position open price
  - average_fill_price  → actual fill price (exit)
  - paid_commission     → fees

Reads account configs from accounts.json
API secrets from environment variables (never in code)
Fetches all orders incrementally (cursor-based pagination)
Matches exit orders with entry orders to form complete trades
Writes: data/{account_id}/dashboard_data.json
        accounts_list.json (for dashboard account switcher)

Run locally (PowerShell):
  $env:NATARAJ_API_KEY = "your_key"
  $env:NATARAJ_API_SECRET = "your_secret"
  python delta_fetcher.py
"""

import json
import os
import time
import hmac
import hashlib
import math
import socket
from datetime import datetime, timezone
from collections import defaultdict

# ── FORCE IPv4 (avoid IPv6 which is not whitelisted) ──────────────────────────
_orig_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **k: [r for r in _orig_getaddrinfo(*a, **k) if r[0] == socket.AF_INET]

try:
    import requests
except ImportError:
    raise SystemExit("Missing dependency: pip install requests")


# ── CONSTANTS ─────────────────────────────────────────────────────────────────
USD_INR = 85.0


# ── ACCOUNT CONFIG ────────────────────────────────────────────────────────────
def load_accounts():
    cfg_path = os.path.join(os.path.dirname(__file__), "accounts.json")
    with open(cfg_path) as f:
        return json.load(f)["accounts"]


# ── HMAC AUTH ─────────────────────────────────────────────────────────────────
def _sign(secret, method, path, query, timestamp, body=""):
    message = method + timestamp + path + (("?" + query) if query else "") + body
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


def _auth_headers(api_key, api_secret, method, path, query="", body=""):
    ts = str(int(time.time()))
    sig = _sign(api_secret, method, path, query, ts, body)
    return {
        "api-key": api_key,
        "timestamp": ts,
        "signature": sig,
        "Content-Type": "application/json",
    }


# ── ORDERS FETCH ──────────────────────────────────────────────────────────────
def fetch_orders_page(endpoint, api_key, api_secret, after_cursor=None, page_size=100):
    """Fetch one page of order history. Returns (status_code, data_dict)."""
    path = "/v2/orders/history"
    params = {"page_size": page_size}
    if after_cursor:
        params["after"] = after_cursor

    query = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{endpoint}{path}?{query}"
    headers = _auth_headers(api_key, api_secret, "GET", path, query)

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.status_code, resp.json()
    except requests.exceptions.RequestException as e:
        print(f"  [fetch_orders] Request error: {e}")
        return 0, {"error": str(e)}


def fetch_all_orders(endpoint, api_key, api_secret, known_ids=None):
    """
    Fetch all order history via cursor pagination.
    known_ids: set of order IDs already cached (skip if seen).
    Returns list of new orders (most recent first from API, we'll sort later).
    """
    if known_ids is None:
        known_ids = set()

    all_orders = []
    after = None
    page_num = 0
    max_pages = 500
    hit_known = False

    for _ in range(max_pages):
        code, data = fetch_orders_page(endpoint, api_key, api_secret, after_cursor=after)

        if code != 200:
            err = data.get("error", data)
            print(f"  [fetch_orders] Error on page {page_num + 1}: {err}")
            break

        orders = data.get("result", [])
        if not orders:
            break

        page_num += 1
        new_on_page = 0
        for order in orders:
            oid = order.get("id")
            if oid in known_ids:
                hit_known = True
                continue
            all_orders.append(order)
            new_on_page += 1

        print(f"  [fetch_orders] Page {page_num}: {len(orders)} orders, {new_on_page} new (total new: {len(all_orders)})")

        # Stop if we've hit already-known orders
        if hit_known:
            print(f"  [fetch_orders] Hit cached orders — stopping pagination")
            break

        meta = data.get("meta", {})
        after = meta.get("after")
        if not after:
            break

        time.sleep(0.3)  # Polite rate limiting

    return all_orders


def fetch_open_positions(endpoint, api_key, api_secret):
    """Fetch current open positions."""
    path = "/v2/positions/margined"
    headers = _auth_headers(api_key, api_secret, "GET", path)
    url = f"{endpoint}{path}"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        data = resp.json()
        return data.get("result", [])
    except Exception as e:
        print(f"  [fetch_positions] Error: {e}")
        return []


# ── TIMESTAMP HELPERS ─────────────────────────────────────────────────────────
def _parse_dt(ts_str):
    """Parse ISO timestamp string to datetime."""
    if not ts_str:
        return datetime.now(tz=timezone.utc)
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


def _dt_str(dt):
    return dt.strftime("%Y-%m-%d %H:%M")


# ── TRADE BUILDER FROM ORDERS ─────────────────────────────────────────────────
def build_trades_from_orders(orders):
    """
    Convert order history into closed trades and open trades.

    Exit order:  state=closed, meta_data.pnl != 0, reduce_only=true
                 → has entry_price, avg_exit_price, pnl in meta_data
    Entry order: state=closed, meta_data.pnl == 0 or not present, reduce_only=false
                 → average_fill_price = entry price

    For each exit order we build a complete trade.
    We try to match it with its corresponding entry order for accurate entry_time.
    If no match, entry_time falls back to exit_time (conservative).

    Returns: (closed_trades, open_trades)
    """
    exit_orders = []
    entry_orders = []

    for order in orders:
        # Skip if not actually filled
        avg_fill = order.get("average_fill_price")
        if avg_fill is None:
            continue

        state = order.get("state", "")
        # Some cancelled orders sneak through; skip them
        if state == "cancelled":
            continue

        meta = order.get("meta_data") or {}
        pnl_str = meta.get("pnl")

        is_exit = False
        if pnl_str is not None:
            try:
                if float(pnl_str) != 0.0:
                    is_exit = True
            except (ValueError, TypeError):
                pass

        # reduce_only=True is a strong signal for exit orders
        if order.get("reduce_only") is True:
            is_exit = True

        if is_exit:
            exit_orders.append(order)
        else:
            entry_orders.append(order)

    # Sort entry orders oldest first for FIFO matching
    entry_orders.sort(key=lambda o: o.get("created_at", ""))

    used_entries = set()
    closed_trades = []

    for exit_ord in exit_orders:
        meta = exit_ord.get("meta_data") or {}
        sym = exit_ord["product_symbol"]
        exit_side = (exit_ord.get("side") or "").lower()   # sell=long exit, buy=short exit
        entry_side = "buy" if exit_side == "sell" else "sell"
        trade_dir = "LONG" if exit_side == "sell" else "SHORT"

        qty = float(exit_ord.get("size") or 0)
        exit_px = float(exit_ord.get("average_fill_price") or 0)

        # PnL and entry price come from meta_data
        pnl_usd = float(meta.get("pnl") or 0)
        entry_px_meta = float(meta.get("entry_price") or 0)
        exit_commission = float(exit_ord.get("paid_commission") or 0)
        exit_time_str = exit_ord.get("created_at", "")

        # Try to find matching entry order (same symbol, opposite side, any qty, earlier)
        # We only use the entry order for entry_time — PnL and prices come from meta_data.
        # Relaxed qty matching: one entry position may generate multiple partial exits.
        matched_entry = None
        for idx, ent in enumerate(entry_orders):
            if idx in used_entries:
                continue
            ent_sym = ent.get("product_symbol", "")
            ent_side = (ent.get("side") or "").lower()
            ent_time = ent.get("created_at", "")

            if (ent_sym == sym
                    and ent_side == entry_side
                    and ent_time <= exit_time_str):
                matched_entry = ent
                used_entries.add(idx)
                break

        if matched_entry:
            entry_time_str = matched_entry.get("created_at", exit_time_str)
            entry_px = float(matched_entry.get("average_fill_price") or entry_px_meta)
            entry_commission = float(matched_entry.get("paid_commission") or 0)
        else:
            # Fall back to meta_data entry price, exit time as entry time
            entry_time_str = exit_time_str
            entry_px = entry_px_meta
            entry_commission = 0.0

        # Use meta_data entry_price if it's more reliable (non-zero)
        if entry_px_meta > 0:
            entry_px = entry_px_meta

        total_fees = exit_commission + entry_commission

        entry_dt = _parse_dt(entry_time_str)
        exit_dt = _parse_dt(exit_time_str)
        duration_hrs = round((exit_dt - entry_dt).total_seconds() / 3600, 1)

        closed_trades.append({
            "contract": sym,
            "direction": trade_dir,
            "qty": qty,
            "entry_px": round(entry_px, 6),
            "exit_px": round(exit_px, 6),
            "entry": _dt_str(entry_dt),
            "exit": _dt_str(exit_dt),
            "entry_time": entry_dt.isoformat(),
            "exit_time": exit_dt.isoformat(),
            "pnl_usd": round(pnl_usd, 4),
            "pnl_inr": round(pnl_usd * USD_INR, 2),
            "entry_val_usd": round(qty * entry_px, 2),
            "entry_val_inr": round(qty * entry_px * USD_INR, 0),
            "fees_usd": round(total_fees, 4),
            "fees_inr": round(total_fees * USD_INR, 2),
            "duration_hrs": duration_hrs,
        })

    # Filter out options contracts (symbol contains '-' like C-BTC-98200-111224)
    closed_trades = [t for t in closed_trades if "-" not in t["contract"]]

    # Sort by exit time, number trades
    closed_trades.sort(key=lambda t: t["exit_time"])
    for i, t in enumerate(closed_trades):
        t["num"] = i + 1

    return closed_trades, []


def build_open_trades_from_positions(positions, all_orders=None):
    """Convert live positions to open trades format.
    Cross-references orders history to find entry_time for each position."""
    # Build a lookup: symbol -> most recent entry order timestamp + fees
    entry_lookup = {}
    if all_orders:
        for o in all_orders:
            sym = o.get("product_symbol", "")
            side = (o.get("side") or "").lower()
            status = (o.get("state") or o.get("status") or "").lower()
            if status not in ("filled", "closed"):
                continue
            # For each symbol, track most recent buy and sell order
            ts = o.get("created_at", "")
            fees = float(o.get("paid_commission") or 0)
            key_buy = sym + "_buy"
            key_sell = sym + "_sell"
            k = sym + "_" + side
            if k not in entry_lookup or ts > entry_lookup[k]["ts"]:
                entry_lookup[k] = {"ts": ts, "fees": fees}

    open_trades = []
    for pos in positions:
        size = float(pos.get("size") or 0)
        if size == 0:
            continue
        sym = pos.get("product_symbol", "")
        entry_px = float(pos.get("entry_price") or 0)
        direction = "LONG" if (pos.get("side") or "").lower() == "buy" else "SHORT"

        # Find entry time from orders history
        order_side = "buy" if direction == "LONG" else "sell"
        lookup = entry_lookup.get(sym + "_" + order_side, {})
        entry_time_str = lookup.get("ts", "")
        entry_dt = _parse_dt(entry_time_str) if entry_time_str else None
        entry_display = _dt_str(entry_dt) if entry_dt else "—"
        fees_usd = lookup.get("fees", 0)

        open_trades.append({
            "contract": sym,
            "direction": direction,
            "qty": abs(size),
            "entry_px": round(entry_px, 6),
            "entry": entry_display,
            "entry_time": entry_dt.isoformat() if entry_dt else "",
            "entry_val_usd": round(abs(size) * entry_px, 2),
            "entry_val_inr": round(abs(size) * entry_px * USD_INR, 0),
            "fees_usd": round(fees_usd, 4),
            "fees_inr": round(fees_usd * USD_INR, 2),
            "unrealized_pnl_usd": round(float(pos.get("unrealized_pnl") or pos.get("realized_pnl") or 0), 4),
            "unrealized_pnl_inr": round(float(pos.get("unrealized_pnl") or pos.get("realized_pnl") or 0) * USD_INR, 2),
            "source": "live_position",
        })
    return open_trades


# ── METRICS ENGINE ────────────────────────────────────────────────────────────
def compute_metrics(closed_trades, assumed_capital):
    if not closed_trades:
        return {}

    pnls = [t["pnl_usd"] for t in closed_trades]
    wins = [t for t in closed_trades if t["pnl_usd"] > 0]
    losses = [t for t in closed_trades if t["pnl_usd"] <= 0]
    total_pnl = sum(pnls)
    gross_win = sum(t["pnl_usd"] for t in wins)
    gross_loss = abs(sum(t["pnl_usd"] for t in losses))

    # NAV curve + drawdown
    nav = assumed_capital
    peak = assumed_capital
    max_dd_pct = 0.0
    max_dd_usd = 0.0
    nav_curve = []

    for t in closed_trades:
        nav += t["pnl_usd"]
        if nav > peak:
            peak = nav
        dd_pct = (nav - peak) / peak * 100 if peak > 0 else 0.0
        dd_usd = nav - peak
        max_dd_pct = min(max_dd_pct, dd_pct)
        max_dd_usd = min(max_dd_usd, dd_usd)
        nav_curve.append({
            "date": t["exit"][:10],
            "nav_usd": round(nav, 2),
            "nav_inr": round(nav * USD_INR, 0),
            "pnl": t["pnl_usd"],
            "pnl_inr": t["pnl_inr"],
            "contract": t["contract"],
            "direction": t["direction"],
            "trade_num": t["num"],
            "drawdown_pct": round(dd_pct, 2),
        })

    final_nav = assumed_capital + total_pnl

    # Recovery period from max drawdown trough
    recovery_period_days = None  # None = still in recovery / not yet recovered
    if max_dd_pct < 0 and nav_curve:
        trough_idx = min(range(len(nav_curve)), key=lambda i: nav_curve[i]["drawdown_pct"])
        trough_nav = nav_curve[trough_idx]["nav_usd"]
        trough_dd_pct = nav_curve[trough_idx]["drawdown_pct"]
        # peak_at_trough: the NAV level we need to recover back to
        peak_at_trough = trough_nav * 100.0 / (100.0 + trough_dd_pct) if trough_dd_pct != 0 else trough_nav
        trough_date = datetime.strptime(nav_curve[trough_idx]["date"], "%Y-%m-%d")
        for i in range(trough_idx + 1, len(nav_curve)):
            if nav_curve[i]["nav_usd"] >= peak_at_trough:
                rec_date = datetime.strptime(nav_curve[i]["date"], "%Y-%m-%d")
                recovery_period_days = (rec_date - trough_date).days
                break

    # Period
    start_dt = _parse_dt(closed_trades[0]["entry_time"])
    end_dt = _parse_dt(closed_trades[-1]["exit_time"])
    years = max((end_dt - start_dt).days / 365.25, 1 / 365)

    # CAGR
    cagr = ((final_nav / assumed_capital) ** (1.0 / years) - 1) * 100 if assumed_capital > 0 else 0

    # Sharpe
    sharpe = 0.0
    if len(pnls) > 1:
        returns = [p / assumed_capital for p in pnls]
        avg_r = sum(returns) / len(returns)
        variance = sum((r - avg_r) ** 2 for r in returns) / (len(returns) - 1)
        std_r = math.sqrt(variance) if variance > 0 else 0
        trades_per_year = len(closed_trades) / years
        sharpe = (avg_r / std_r) * math.sqrt(trades_per_year) if std_r > 0 else 0

    # Monthly PnL
    monthly_map = defaultdict(lambda: {"pnl_usd": 0.0, "pnl_inr": 0.0, "trades": 0, "wins": 0})
    for t in closed_trades:
        m = t["exit"][:7]
        monthly_map[m]["pnl_usd"] += t["pnl_usd"]
        monthly_map[m]["pnl_inr"] += t["pnl_inr"]
        monthly_map[m]["trades"] += 1
        if t["pnl_usd"] > 0:
            monthly_map[m]["wins"] += 1

    monthly = []
    for m in sorted(monthly_map.keys()):
        d = monthly_map[m]
        monthly.append({
            "month": m,
            "pnl_usd": round(d["pnl_usd"], 2),
            "pnl_inr": round(d["pnl_inr"], 0),
            "trades": d["trades"],
            "wins": d["wins"],
        })

    # PnL distribution (bucket by $1 increments if all trades are small)
    if pnls:
        spread = max(pnls) - min(pnls)
        bucket = 1 if spread < 20 else (10 if spread < 200 else 100)
        min_b = math.floor(min(pnls) / bucket) * bucket
        max_b = math.ceil(max(pnls) / bucket) * bucket
        dist = []
        b = min_b
        while b < max_b:
            count = sum(1 for p in pnls if b <= p < b + bucket)
            dist.append({"range": f"{round(b,1)} to {round(b+bucket,1)}", "count": count})
            b = round(b + bucket, 6)
    else:
        dist = []

    # Contract breakdown
    contract_map = defaultdict(lambda: {"trades": 0, "pnl_usd": 0.0, "wins": 0, "losses": 0,
                                         "long_trades": 0, "short_trades": 0,
                                         "long_pnl": 0.0, "short_pnl": 0.0})
    for t in closed_trades:
        c = t["contract"]
        contract_map[c]["trades"] += 1
        contract_map[c]["pnl_usd"] += t["pnl_usd"]
        if t["pnl_usd"] > 0:
            contract_map[c]["wins"] += 1
        else:
            contract_map[c]["losses"] += 1
        if t["direction"] == "LONG":
            contract_map[c]["long_trades"] += 1
            contract_map[c]["long_pnl"] += t["pnl_usd"]
        else:
            contract_map[c]["short_trades"] += 1
            contract_map[c]["short_pnl"] += t["pnl_usd"]

    contracts = {}
    for sym, d in contract_map.items():
        contracts[sym] = {
            "trades": d["trades"],
            "pnl_usd": round(d["pnl_usd"], 2),
            "wins": d["wins"],
            "losses": d["losses"],
            "win_rate_pct": round(d["wins"] / d["trades"] * 100, 1) if d["trades"] > 0 else 0,
            "avg_trade_usd": round(d["pnl_usd"] / d["trades"], 2) if d["trades"] > 0 else 0,
            "long_trades": d["long_trades"],
            "short_trades": d["short_trades"],
            "long_pnl_usd": round(d["long_pnl"], 2),
            "short_pnl_usd": round(d["short_pnl"], 2),
        }

    long_trades = [t for t in closed_trades if t["direction"] == "LONG"]
    short_trades = [t for t in closed_trades if t["direction"] == "SHORT"]

    # Consecutive wins/losses
    max_cw = max_cl = cw = cl = 0
    for t in closed_trades:
        if t["pnl_usd"] > 0:
            cw += 1; cl = 0
        else:
            cl += 1; cw = 0
        max_cw = max(max_cw, cw)
        max_cl = max(max_cl, cl)

    # Avg duration: only include trades with a matched entry (duration > 0)
    matched_durs = [t["duration_hrs"] for t in closed_trades if t["duration_hrs"] > 0]
    avg_duration = sum(matched_durs) / len(matched_durs) if matched_durs else 0
    total_fees_usd = sum(t["fees_usd"] for t in closed_trades)

    stats = {
        "total_pnl_usd": round(total_pnl, 2),
        "total_pnl_inr": round(total_pnl * USD_INR, 0),
        "assumed_capital_usd": assumed_capital,
        "final_nav_usd": round(final_nav, 2),
        "nav_growth_pct": round((final_nav / assumed_capital - 1) * 100, 1),
        "cagr_pct": round(cagr, 1),
        "sharpe": round(sharpe, 2),
        "max_drawdown_pct": round(max_dd_pct, 1),
        "max_drawdown_usd": round(max_dd_usd, 0),
        "recovery_period_days": recovery_period_days,
        "profit_factor": round(gross_win / gross_loss, 2) if gross_loss > 0 else 999.0,
        "total_trades": len(closed_trades),
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "win_rate_pct": round(len(wins) / len(closed_trades) * 100, 1) if closed_trades else 0,
        "total_fees_usd": round(total_fees_usd, 2),
        "total_fees_inr": round(total_fees_usd * USD_INR, 0),
        "fees_pct_of_pnl": round(total_fees_usd / abs(total_pnl) * 100, 1) if total_pnl != 0 else 0,
        "avg_win_usd": round(gross_win / len(wins), 2) if wins else 0,
        "avg_loss_usd": round(-gross_loss / len(losses), 2) if losses else 0,
        "rr_ratio": round((gross_win / len(wins)) / (gross_loss / len(losses)), 2) if wins and losses else 0,
        "max_win_usd": round(max(t["pnl_usd"] for t in wins), 2) if wins else 0,
        "max_loss_usd": round(min(t["pnl_usd"] for t in losses), 2) if losses else 0,
        "max_consec_wins": max_cw,
        "max_consec_losses": max_cl,
        "expectancy_usd": round(total_pnl / len(closed_trades), 2) if closed_trades else 0,
        "expectancy_inr": round(total_pnl * USD_INR / len(closed_trades), 0) if closed_trades else 0,
        "avg_duration_hrs": round(avg_duration, 1),
        "trades_per_month": round(len(closed_trades) / max(len(monthly), 1), 1),
        "period_start": closed_trades[0]["entry"][:10],
        "period_end": closed_trades[-1]["exit"][:10],
        "long_trades": len(long_trades),
        "short_trades": len(short_trades),
        "long_pnl_usd": round(sum(t["pnl_usd"] for t in long_trades), 2),
        "short_pnl_usd": round(sum(t["pnl_usd"] for t in short_trades), 2),
        "long_pnl_pct": round(len(long_trades) / len(closed_trades) * 100, 1) if closed_trades else 0,
        "contracts": contracts,
    }

    return {
        "stats": stats,
        "nav": nav_curve,
        "monthly": monthly,
        "dist": dist,
    }


# ── ACCOUNT RUNNER ────────────────────────────────────────────────────────────
def run_account(account):
    account_id = account["id"]
    account_name = account.get("name", account_id)
    endpoint = account.get("endpoint", "https://api.india.delta.exchange")
    capital = float(account.get("assumed_capital_usd", 10000))

    api_key = os.environ.get(account.get("api_key_env", ""), "")
    api_secret = os.environ.get(account.get("api_secret_env", ""), "")

    if not api_key or not api_secret:
        print(f"[{account_id}] Skipping — no credentials in env "
              f"({account.get('api_key_env')} / {account.get('api_secret_env')})")
        return False

    print(f"\n[{account_id}] ── {account_name}")

    out_dir = os.path.join("data", account_id)
    os.makedirs(out_dir, exist_ok=True)

    # Load cached orders
    orders_file = os.path.join(out_dir, "orders_store.json")
    if os.path.exists(orders_file):
        with open(orders_file) as f:
            store = json.load(f)
        print(f"[{account_id}] Loaded {len(store.get('orders', []))} cached orders")
    else:
        store = {"orders": []}
        print(f"[{account_id}] No cache — full fetch from beginning")

    # Incremental fetch: skip orders we've already seen
    known_ids = {o["id"] for o in store.get("orders", []) if "id" in o}
    new_orders = fetch_all_orders(endpoint, api_key, api_secret, known_ids=known_ids)

    if new_orders:
        store["orders"].extend(new_orders)
        with open(orders_file, "w") as f:
            json.dump(store, f, indent=2)
        print(f"[{account_id}] Saved {len(new_orders)} new orders (total: {len(store['orders'])})")
    else:
        print(f"[{account_id}] No new orders")

    all_orders = store.get("orders", [])
    if not all_orders:
        print(f"[{account_id}] No order data — skipping")
        return False

    # Build trades
    closed_trades, open_from_orders = build_trades_from_orders(all_orders)
    print(f"[{account_id}] Built: {len(closed_trades)} closed trades")

    # Fetch live open positions only — never fall back to unmatched orders
    positions = fetch_open_positions(endpoint, api_key, api_secret)
    open_trades = build_open_trades_from_positions(positions, all_orders=all_orders)
    print(f"[{account_id}] Open trades: {len(open_trades)}")

    if not closed_trades:
        print(f"[{account_id}] No closed trades — skipping metrics")
        return False

    metrics = compute_metrics(closed_trades, capital)

    now_iso = datetime.now(timezone.utc).isoformat()
    dashboard = {
        "meta": {
            "account_id": account_id,
            "account_name": account_name,
            "last_updated": now_iso,
            "usd_inr_rate": USD_INR,
            "assumed_capital_usd": capital,
            "active": account.get("active", True),
            "data_source": "orders_history",
        },
        "stats": metrics["stats"],
        "nav": metrics["nav"],
        "monthly": metrics["monthly"],
        "dist": metrics["dist"],
        "trades": closed_trades,
        "open_trades": open_trades,
    }

    out_file = os.path.join(out_dir, "dashboard_data.json")
    with open(out_file, "w") as f:
        json.dump(dashboard, f, indent=2)

    pnl = metrics["stats"]["total_pnl_usd"]
    wr = metrics["stats"]["win_rate_pct"]
    cagr = metrics["stats"]["cagr_pct"]
    total = metrics["stats"]["total_trades"]
    print(f"[{account_id}] Done → {total} trades | PnL: ${pnl:,.2f} | WR: {wr}% | CAGR: {cagr}%")
    return True


# ── COMBINE ALL ACCOUNTS ──────────────────────────────────────────────────────
def combine_accounts(accounts):
    all_closed = []
    all_open = []
    total_capital = 0.0
    account_names = []

    for acc in accounts:
        acc_id = acc["id"]
        data_file = os.path.join("data", acc_id, "dashboard_data.json")
        if not os.path.exists(data_file):
            continue

        with open(data_file) as f:
            d = json.load(f)

        acc_name = acc.get("name", acc_id)
        account_names.append(acc_name)
        total_capital += float(d.get("meta", {}).get("assumed_capital_usd", 10000))

        for t in d.get("trades", []):
            t2 = dict(t)
            t2["account"] = acc_name
            t2["account_id"] = acc_id
            all_closed.append(t2)

        for t in d.get("open_trades", []):
            t2 = dict(t)
            t2["account"] = acc_name
            t2["account_id"] = acc_id
            all_open.append(t2)

    if not all_closed:
        print("[combined] No closed trades — skipping")
        return False

    all_closed.sort(key=lambda t: t.get("exit_time", t.get("exit", "")))
    for i, t in enumerate(all_closed):
        t["num"] = i + 1

    metrics = compute_metrics(all_closed, total_capital)

    now_iso = datetime.now(timezone.utc).isoformat()
    combined = {
        "meta": {
            "account_id": "combined",
            "account_name": "All Accounts",
            "accounts_included": account_names,
            "last_updated": now_iso,
            "usd_inr_rate": USD_INR,
            "assumed_capital_usd": total_capital,
            "active": True,
        },
        "stats": metrics["stats"],
        "nav": metrics["nav"],
        "monthly": metrics["monthly"],
        "dist": metrics["dist"],
        "trades": all_closed,
        "open_trades": all_open,
    }

    os.makedirs(os.path.join("data", "combined"), exist_ok=True)
    out_file = os.path.join("data", "combined", "dashboard_data.json")
    with open(out_file, "w") as f:
        json.dump(combined, f, indent=2)

    pnl = metrics["stats"]["total_pnl_usd"]
    total = metrics["stats"]["total_trades"]
    print(f"[combined] {len(account_names)} account(s) | {total} trades | PnL: ${pnl:,.2f}")
    return True


# ── ACCOUNTS LIST ─────────────────────────────────────────────────────────────
def write_accounts_list(accounts, has_combined=False):
    items = []
    if has_combined and len(accounts) > 1:
        items.append({"id": "combined", "name": "All Accounts (Combined)",
                      "active": True, "is_combined": True})
    for acc in accounts:
        items.append({"id": acc["id"], "name": acc.get("name", acc["id"]),
                      "active": acc.get("active", True), "is_combined": False})

    with open("accounts_list.json", "w") as f:
        json.dump(items, f, indent=2)
    print(f"\nWrote accounts_list.json ({len(items)} entries)")


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  100X Algo — Delta Exchange Trade Fetcher")
    print("  Using: /v2/orders/history (orders-based)")
    print("=" * 60)

    accounts = load_accounts()
    print(f"Loaded {len(accounts)} account(s)\n")

    results = {}
    for acc in accounts:
        try:
            results[acc["id"]] = run_account(acc)
        except Exception as e:
            print(f"[{acc['id']}] ERROR: {e}")
            import traceback
            traceback.print_exc()
            results[acc["id"]] = False

    print("\n[combined] Building combined view...")
    try:
        combined_ok = combine_accounts(accounts)
    except Exception as e:
        print(f"[combined] ERROR: {e}")
        combined_ok = False

    write_accounts_list(accounts, has_combined=combined_ok)

    print("\n" + "=" * 60)
    for acc_id, ok in results.items():
        print(f"  {acc_id}: {'OK' if ok else 'SKIPPED/ERROR'}")
    print(f"  combined: {'OK' if combined_ok else 'SKIPPED/ERROR'}")
    print("=" * 60)
