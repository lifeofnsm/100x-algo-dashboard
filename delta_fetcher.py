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


def fetch_wallet_balance(endpoint, api_key, api_secret):
    """
    Fetch current wallet balances from /v2/wallet/balances.
    Returns total available balance in USD (USDT asset).
    Also returns raw balances list for inspection.
    """
    path = "/v2/wallet/balances"
    headers = _auth_headers(api_key, api_secret, "GET", path)
    url = f"{endpoint}{path}"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        print(f"  [fetch_wallet] HTTP {resp.status_code}")
        if resp.status_code != 200:
            print(f"  [fetch_wallet] Error response: {resp.text[:300]}")
            return None, []
        data = resp.json()
        balances = data.get("result", [])
        if not balances:
            print(f"  [fetch_wallet] Empty result. Full response: {str(data)[:300]}")
            return None, []

        # Look for USD/USDT balance (main margin currency on Delta India)
        # Fields confirmed from live API response:
        #   available_balance = free cash only (not in any position)
        #   balance           = available_balance + position_margin = total USD in account
        #   balance_inr       = balance converted to INR by Delta's own rate
        #   position_margin   = margin locked in open positions
        # Use `balance` as the true wallet equity (matches computed NAV closely)
        usdt_balance = None
        for b in balances:
            sym = (b.get("asset_symbol") or b.get("currency") or "").upper()
            if sym in ("USDT", "USD"):
                balance_usd     = float(b.get("balance") or b.get("available_balance") or 0)
                balance_inr     = float(b.get("balance_inr") or 0)
                available       = float(b.get("available_balance") or 0)
                position_margin = float(b.get("position_margin") or b.get("blocked_margin") or 0)
                # unrealized_pnl may be directly in the balance object on some API versions
                unrealized_pnl  = float(b.get("unrealized_pnl") or b.get("unrealized_funding") or 0)
                # equity = balance + unrealized_pnl (true mark-to-market value)
                equity_usd      = round(balance_usd + unrealized_pnl, 4)
                usdt_balance = {
                    "asset": sym,
                    "balance": round(balance_usd, 4),        # collateral only (available + position_margin)
                    "balance_inr": round(balance_inr, 2),    # INR from Delta's own rate
                    "available": round(available, 4),         # free margin
                    "position_margin": round(position_margin, 4),
                    "unrealized_pnl": round(unrealized_pnl, 4),
                    "equity": equity_usd,                     # true mark-to-market value
                }
                break

        return usdt_balance, balances
    except Exception as e:
        print(f"  [fetch_wallet] Error: {e}")
        return None, []


def fetch_wallet_transactions(endpoint, api_key, api_secret):
    """
    Try to fetch wallet transaction history from /v2/wallet/transactions.
    Returns net deposited capital (deposits - withdrawals) in USD.
    Returns None if endpoint is not available or returns no data.
    """
    path = "/v2/wallet/transactions"
    all_txns = []
    after = None

    for _ in range(50):   # max 50 pages of transactions
        params = {"page_size": 100}
        if after:
            params["after"] = after
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{endpoint}{path}?{query}"
        headers = _auth_headers(api_key, api_secret, "GET", path, query)

        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code != 200:
                print(f"  [fetch_wallet_txns] HTTP {resp.status_code} — endpoint may not be available")
                return None
            data = resp.json()
            txns = data.get("result", [])
            if not txns:
                break
            all_txns.extend(txns)
            meta = data.get("meta", {})
            after = meta.get("after")
            if not after:
                break
            time.sleep(0.2)
        except Exception as e:
            print(f"  [fetch_wallet_txns] Error: {e}")
            return None

    if not all_txns:
        return None

    # Print unique transaction types seen — helps debug field name mismatches
    seen_types = sorted(set(
        (txn.get("transaction_type") or txn.get("type") or "unknown").lower()
        for txn in all_txns
    ))
    print(f"  [fetch_wallet_txns] {len(all_txns)} txns | Types seen: {seen_types}")

    # Sum deposits and withdrawals
    # Delta Exchange India known types (may vary by account):
    # deposit-like:    "deposit", "inward_transfer", "credit", "transfer_in"
    # withdrawal-like: "withdrawal", "outward_transfer", "debit", "transfer_out"
    DEPOSIT_TYPES    = {"deposit", "inward_transfer", "credit", "transfer_in",
                        "fund_deposit", "usdt_deposit"}
    WITHDRAWAL_TYPES = {"withdrawal", "outward_transfer", "debit", "transfer_out",
                        "fund_withdrawal", "usdt_withdrawal"}

    total_deposited = 0.0
    total_withdrawn = 0.0
    for txn in all_txns:
        txn_type = (txn.get("transaction_type") or txn.get("type") or "").lower()
        amount = float(txn.get("amount") or 0)
        if txn_type in DEPOSIT_TYPES:
            total_deposited += abs(amount)
        elif txn_type in WITHDRAWAL_TYPES:
            total_withdrawn += abs(amount)

    net_capital = total_deposited - total_withdrawn
    print(f"  [fetch_wallet_txns] Deposits: ${total_deposited:,.2f} | Withdrawals: ${total_withdrawn:,.2f} | Net: ${net_capital:,.2f}")
    return {
        "total_deposited_usd": round(total_deposited, 2),
        "total_withdrawn_usd": round(total_withdrawn, 2),
        "net_capital_usd": round(net_capital, 2),
        "transaction_count": len(all_txns),
        "types_seen": seen_types,
    } if net_capital > 0 else {
        "total_deposited_usd": round(total_deposited, 2),
        "total_withdrawn_usd": round(total_withdrawn, 2),
        "net_capital_usd": 0,
        "transaction_count": len(all_txns),
        "types_seen": seen_types,
    }


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
    FIFO Position Tracking — handles staggered entries AND staggered exits.

    Logic:
      - Process all filled orders chronologically per symbol.
      - Maintain a per-symbol FIFO lot queue (entry lots with qty + price + time + fees).
      - Each exit order consumes lots FIFO by qty:
          * Staggered entry, single exit  → multiple lots consumed, weighted avg entry price
          * Single entry, staggered exits → one lot partially consumed per exit
          * Mixed                         → handled naturally by FIFO queue
      - PnL always comes from meta_data.pnl (exchange-reported, authoritative).
      - Entry time = earliest lot consumed for that exit.
      - Entry price = weighted average of consumed lots.

    Returns: (closed_trades, open_trades)
    """
    from collections import defaultdict, deque

    # ── Step 1: Filter valid filled orders ────────────────────────────────────
    valid = []
    for o in orders:
        if o.get("average_fill_price") is None:
            continue
        if (o.get("state") or "") == "cancelled":
            continue
        valid.append(o)

    # Sort chronologically — critical for correct FIFO matching
    valid.sort(key=lambda o: o.get("created_at", ""))

    # ── Step 2: Classify each order as entry or exit ───────────────────────────
    # Exit signals: reduce_only=True  OR  meta_data.pnl != 0
    def is_exit_order(o):
        if o.get("reduce_only") is True:
            return True
        meta = o.get("meta_data") or {}
        pnl_str = meta.get("pnl")
        if pnl_str is not None:
            try:
                return float(pnl_str) != 0.0
            except (ValueError, TypeError):
                pass
        return False

    # ── Step 3: FIFO lot queue per symbol ─────────────────────────────────────
    # ledger[sym] = deque of lots: {qty, price, time, fees, side}
    ledger = defaultdict(deque)
    closed_trades = []

    for order in valid:
        sym = order.get("product_symbol", "")
        side = (order.get("side") or "").lower()          # "buy" or "sell"
        qty  = float(order.get("size") or 0)
        px   = float(order.get("average_fill_price") or 0)
        fees = float(order.get("paid_commission") or 0)
        ts   = order.get("created_at", "")
        meta = order.get("meta_data") or {}

        if not is_exit_order(order):
            # ── Entry: push lot onto FIFO queue ───────────────────────────────
            ledger[sym].append({
                "qty": qty, "price": px,
                "time": ts, "fees": fees, "side": side,
            })
        else:
            # ── Exit: consume FIFO lots by qty ────────────────────────────────
            pnl_usd       = float(meta.get("pnl") or 0)
            entry_px_meta = float(meta.get("entry_price") or 0)
            exit_px       = px
            exit_fees     = fees
            exit_ts       = ts

            # Entry side is opposite of exit side
            entry_side = "buy" if side == "sell" else "sell"
            direction  = "LONG" if side == "sell" else "SHORT"

            remaining = qty
            consumed  = []      # lots consumed for this exit

            q = ledger[sym]
            temp_skipped = []   # lots with wrong side — put back after

            while remaining > 0.001 and q:
                lot = q.popleft()
                if lot["side"] != entry_side:
                    temp_skipped.append(lot)
                    continue
                if lot["qty"] <= remaining + 0.001:
                    # Consume entire lot
                    consumed.append(dict(lot))
                    remaining -= lot["qty"]
                else:
                    # Partial consume — split the lot
                    consumed.append({**lot, "qty": remaining})
                    lot["qty"] -= remaining
                    remaining = 0
                    q.appendleft(lot)   # put remainder back

            # Restore skipped lots (wrong-side) to front of queue
            for lot in reversed(temp_skipped):
                q.appendleft(lot)

            # ── Build trade record ─────────────────────────────────────────────
            if consumed:
                total_entry_qty  = sum(l["qty"] for l in consumed)
                wavg_entry_px    = (sum(l["price"] * l["qty"] for l in consumed)
                                    / total_entry_qty) if total_entry_qty > 0 else entry_px_meta
                entry_ts         = consumed[0]["time"]   # earliest lot = FIFO entry time
                entry_fees       = sum(l["fees"] for l in consumed)
            else:
                # No matching lots — fall back to meta_data
                wavg_entry_px = entry_px_meta if entry_px_meta > 0 else exit_px
                entry_ts      = exit_ts
                entry_fees    = 0

            # Always trust meta_data entry_price if available (exchange is authoritative)
            if entry_px_meta > 0:
                wavg_entry_px = entry_px_meta

            entry_dt     = _parse_dt(entry_ts)
            exit_dt      = _parse_dt(exit_ts)
            duration_hrs = max(0, round((exit_dt - entry_dt).total_seconds() / 3600, 4))
            total_fees   = exit_fees + entry_fees

            closed_trades.append({
                "contract":    sym,
                "direction":   direction,
                "qty":         round(qty, 6),
                "entry_px":    round(wavg_entry_px, 6),
                "exit_px":     round(exit_px, 6),
                "entry":       _dt_str(entry_dt),
                "exit":        _dt_str(exit_dt),
                "entry_time":  entry_dt.isoformat(),
                "exit_time":   exit_dt.isoformat(),
                "pnl_usd":     round(pnl_usd, 4),
                "pnl_inr":     round(pnl_usd * USD_INR, 2),
                "entry_val_usd": round(qty * wavg_entry_px, 2),
                "entry_val_inr": round(qty * wavg_entry_px * USD_INR, 0),
                "fees_usd":    round(total_fees, 4),
                "fees_inr":    round(total_fees * USD_INR, 2),
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
    period_days = (end_dt - start_dt).days
    years = max(period_days / 365.25, 1 / 365)

    # CAGR — only meaningful if we have at least 30 days of data
    if assumed_capital > 0 and period_days >= 30:
        cagr = ((final_nav / assumed_capital) ** (1.0 / years) - 1) * 100
    elif assumed_capital > 0:
        # Too short to annualise meaningfully — show simple return instead
        cagr = (final_nav / assumed_capital - 1) * 100
    else:
        cagr = 0

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

    net_pnl_after_fees = total_pnl - total_fees_usd   # gross trade PnL minus commissions

    stats = {
        "total_pnl_usd": round(total_pnl, 2),           # gross PnL from trades (before commission)
        "total_pnl_inr": round(total_pnl * USD_INR, 0),
        "net_pnl_usd": round(net_pnl_after_fees, 2),    # after commission, before funding
        "net_pnl_inr": round(net_pnl_after_fees * USD_INR, 0),
        "assumed_capital_usd": assumed_capital,
        "final_nav_usd": round(final_nav, 2),            # capital + gross PnL (ignores funding)
        "nav_growth_pct": round((final_nav / assumed_capital - 1) * 100, 1),
        "cagr_pct": round(cagr, 1),
        "sharpe": round(sharpe, 2),
        "max_drawdown_pct": round(max_dd_pct, 1),
        "max_drawdown_usd": round(max_dd_usd, 0),
        "recovery_period_days": recovery_period_days,
        "period_days": period_days,
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
    profit_transfer_out_inr = float(account.get("profit_transfer_out_inr", 0))
    profit_transfer_out_usd = round(profit_transfer_out_inr / USD_INR, 2) if profit_transfer_out_inr else 0

    # ── Capital events: deposits and withdrawals after initial investment ─────
    # Format in accounts.json:
    # "capital_events": [
    #   {"date": "2026-04-01", "type": "deposit",    "inr": 100000},
    #   {"date": "2026-05-15", "type": "withdrawal",  "inr": 50000}
    # ]
    capital_events     = account.get("capital_events", [])
    total_deposits_inr = sum(e["inr"] for e in capital_events if e.get("type") == "deposit")
    total_withdrawals_inr = sum(e["inr"] for e in capital_events if e.get("type") == "withdrawal")
    net_events_inr     = total_deposits_inr - total_withdrawals_inr
    net_events_usd     = round(net_events_inr / USD_INR, 2)
    # Adjusted capital = initial + deposits - withdrawals
    assumed_capital_inr = round(capital * USD_INR, 0)
    net_capital_inr    = assumed_capital_inr + net_events_inr
    net_capital_usd    = round(net_capital_inr / USD_INR, 2)
    if net_events_inr != 0:
        print(f"[{account_id}] Capital events: +₹{total_deposits_inr:,.0f} deposits, "
              f"-₹{total_withdrawals_inr:,.0f} withdrawals → Net capital: ₹{net_capital_inr:,.0f}")
        capital = net_capital_usd   # use adjusted capital for all PnL and CAGR calculations

    # Skip inactive accounts entirely
    if not account.get("active", True):
        print(f"[{account_id}] Skipping — account marked inactive")
        return False

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

    # ── Fetch wallet balance + add unrealized PnL from live positions ──────────
    print(f"[{account_id}] Fetching wallet balance...")
    wallet_balance, raw_balances = fetch_wallet_balance(endpoint, api_key, api_secret)
    if wallet_balance:
        # Sum unrealized PnL from all open positions to get true mark-to-market equity
        position_unrealized = sum(
            float(p.get("unrealized_pnl") or p.get("realized_pnl") or 0)
            for p in positions
            if (p.get("unrealized_pnl") is not None)
        )
        # If API balance object already has unrealized_pnl, don't double-count
        if wallet_balance["unrealized_pnl"] == 0 and position_unrealized != 0:
            wallet_balance["unrealized_pnl"] = round(position_unrealized, 4)
            wallet_balance["equity"] = round(wallet_balance["balance"] + position_unrealized, 4)
        print(f"[{account_id}] Wallet: balance=${wallet_balance['balance']:,.2f} "
              f"| unrealized=${wallet_balance['unrealized_pnl']:,.2f} "
              f"| equity=${wallet_balance['equity']:,.2f} USDT")
    else:
        print(f"[{account_id}] Wallet balance not available")

    # ── Fetch deposit/withdrawal history from API (auto capital tracking) ───────
    print(f"[{account_id}] Fetching wallet transactions...")
    wallet_txns = fetch_wallet_transactions(endpoint, api_key, api_secret)

    api_deposited_usd  = wallet_txns["total_deposited_usd"] if wallet_txns else 0
    api_withdrawn_usd  = wallet_txns["total_withdrawn_usd"] if wallet_txns else 0
    api_net_capital    = wallet_txns["net_capital_usd"]     if wallet_txns else 0

    if api_net_capital > 0:
        # API has valid net capital (deposits > withdrawals) — use as primary source
        # This auto-captures all future deposits and withdrawals without any manual config
        actual_capital       = round(api_deposited_usd, 2)
        actual_withdrawn_usd = round(api_withdrawn_usd, 2)
        capital              = round(api_net_capital, 2)
        print(f"[{account_id}] Capital from API: deposited=${api_deposited_usd:,.2f} "
              f"| withdrawn=${api_withdrawn_usd:,.2f} | net=${capital:,.2f}")
        # Override the manual capital_events — API is always more accurate
        total_deposits_inr    = round(api_deposited_usd * USD_INR, 0)
        total_withdrawals_inr = round(api_withdrawn_usd * USD_INR, 0)
        net_capital_inr       = round(capital * USD_INR, 0)
    else:
        # API has no net capital (USDT transfers like Shalini/Vinay, or deposits=withdrawals)
        # Fall back to assumed_capital + manual capital_events from accounts.json
        actual_capital       = None
        actual_withdrawn_usd = 0
        if api_deposited_usd > 0:
            print(f"[{account_id}] API deposits cancelled by withdrawals (USDT cycle) — using assumed capital ₹{net_capital_inr:,.0f}")
        else:
            print(f"[{account_id}] No deposit records in API — using assumed capital ₹{net_capital_inr:,.0f}")

    if not closed_trades:
        print(f"[{account_id}] No closed trades — skipping metrics")
        return False

    metrics = compute_metrics(closed_trades, capital)

    # ── Wallet-based NAV (equity = balance + unrealized PnL = true mark-to-market)
    # equity is the number that matches what you see on the Delta Exchange app live
    wallet_equity_usd   = wallet_balance["equity"]       if wallet_balance else None
    wallet_balance_usd  = wallet_balance["balance"]      if wallet_balance else None  # collateral only
    wallet_balance_inr  = wallet_balance["balance_inr"]  if wallet_balance else None
    wallet_unrealized   = wallet_balance["unrealized_pnl"] if wallet_balance else 0
    wallet_nav_growth_pct = None

    wallet_cagr_pct = None
    if wallet_equity_usd is not None and wallet_equity_usd > 0:
        wallet_nav_growth_pct = round((wallet_equity_usd / capital - 1) * 100, 1)
        # Wallet-based CAGR — override the trade-log CAGR with wallet equity
        period_days = metrics["stats"].get("period_days", 0)
        if period_days >= 30:
            years = period_days / 365.0
            wallet_cagr_pct = round(((wallet_equity_usd / capital) ** (1.0 / years) - 1) * 100, 1)
        else:
            wallet_cagr_pct = wallet_nav_growth_pct   # simple return for short periods
        # Override trade-log CAGR in stats with wallet-based CAGR
        metrics["stats"]["cagr_pct"] = wallet_cagr_pct
        print(f"[{account_id}] Equity: ${wallet_equity_usd:,.2f} "
              f"(balance=${wallet_balance_usd:,.2f} + unrealized=${wallet_unrealized:,.2f}) "
              f"| Return: {wallet_nav_growth_pct}% | Wallet CAGR: {wallet_cagr_pct}%")

    # ── Wallet-based Net PnL = equity - starting_capital (true source of truth) ─
    wallet_net_pnl_usd = round(wallet_equity_usd - capital, 2) if wallet_equity_usd is not None else None
    wallet_net_pnl_inr = round(wallet_net_pnl_usd * USD_INR, 0) if wallet_net_pnl_usd is not None else None
    # Use equity INR: equity_usd * rate (balance_inr is only for collateral portion)
    wallet_equity_inr  = round(wallet_equity_usd * USD_INR, 0) if wallet_equity_usd is not None else wallet_balance_inr

    now_iso = datetime.now(timezone.utc).isoformat()
    dashboard = {
        "meta": {
            "account_id": account_id,
            "account_name": account_name,
            "last_updated": now_iso,
            "usd_inr_rate": USD_INR,
            "assumed_capital_usd": capital,
            "assumed_capital_inr": net_capital_inr,
            "profit_transfer_out_inr": profit_transfer_out_inr if profit_transfer_out_inr else None,
            "total_deposits_inr": round(total_deposits_inr, 0) if total_deposits_inr else None,
            "total_withdrawals_inr": round(total_withdrawals_inr, 0) if total_withdrawals_inr else None,
            "net_capital_inr": net_capital_inr,
            "capital_source": "api_transactions" if api_net_capital > 0 else "manual_config",
            "wallet_balance_usd": wallet_equity_usd,        # equity = balance + unrealized PnL
            "wallet_balance_inr": wallet_equity_inr,
            "wallet_collateral_usd": wallet_balance_usd,    # collateral only (for reference)
            "wallet_unrealized_usd": wallet_unrealized,
            "wallet_net_pnl_usd": wallet_net_pnl_usd,
            "wallet_net_pnl_inr": wallet_net_pnl_inr,
            "wallet_nav_growth_pct": wallet_nav_growth_pct,
            "actual_capital_usd": actual_capital,
            "actual_capital_inr": round(actual_capital * USD_INR, 0) if actual_capital else None,
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
    total_capital_inr = 0.0
    account_names = []
    # Aggregate wallet data across accounts
    combined_wallet_equity_usd = 0.0
    combined_wallet_equity_inr = 0.0
    combined_wallet_net_pnl_usd = 0.0
    combined_wallet_net_pnl_inr = 0.0
    has_wallet_data = False

    for acc in accounts:
        # Skip inactive accounts from combined view
        if not acc.get("active", True):
            continue
        acc_id = acc["id"]
        data_file = os.path.join("data", acc_id, "dashboard_data.json")
        if not os.path.exists(data_file):
            continue

        with open(data_file) as f:
            d = json.load(f)

        # Skip if no trades (empty/reset account)
        if not d.get("trades"):
            continue

        acc_name = acc.get("name", acc_id)
        account_names.append(acc_name)
        acc_meta = d.get("meta", {})
        acc_capital_usd = float(acc.get("assumed_capital_usd", acc_meta.get("assumed_capital_usd", 10000)))
        acc_capital_inr = float(acc_meta.get("net_capital_inr") or acc_meta.get("assumed_capital_inr") or (acc_capital_usd * USD_INR))
        total_capital     += acc_capital_usd
        total_capital_inr += acc_capital_inr

        # Aggregate wallet equity
        # If wallet API failed for an account, fall back to trade log PnL + capital as estimate
        w_eq_usd  = acc_meta.get("wallet_balance_usd")
        w_eq_inr  = acc_meta.get("wallet_balance_inr")
        w_pnl_usd = acc_meta.get("wallet_net_pnl_usd")
        w_pnl_inr = acc_meta.get("wallet_net_pnl_inr")
        if w_eq_usd is not None:
            combined_wallet_equity_usd += w_eq_usd
            combined_wallet_equity_inr += (w_eq_inr or w_eq_usd * USD_INR)
            combined_wallet_net_pnl_usd += (w_pnl_usd or 0)
            combined_wallet_net_pnl_inr += (w_pnl_inr or 0)
            has_wallet_data = True
        else:
            # No live wallet data — use trade log PnL + capital as best estimate
            trade_pnl_inr = float(d.get("stats", {}).get("total_pnl_inr") or 0)
            trade_pnl_usd = round(trade_pnl_inr / USD_INR, 2)
            combined_wallet_net_pnl_inr += trade_pnl_inr
            combined_wallet_net_pnl_usd += trade_pnl_usd
            combined_wallet_equity_inr  += acc_capital_inr + trade_pnl_inr
            combined_wallet_equity_usd  += acc_capital_usd + trade_pnl_usd
            has_wallet_data = True
            print(f"[combined] {acc_id}: no wallet data — using trade PnL fallback (Rs.{trade_pnl_inr:,.0f})")

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

    # Combined wallet NAV
    combined_wallet_nav_growth = None
    combined_wallet_cagr = None
    if has_wallet_data and total_capital > 0:
        combined_wallet_nav_growth = round((combined_wallet_equity_usd / total_capital - 1) * 100, 1)
        period_days = metrics["stats"].get("period_days", 0)
        if period_days >= 30:
            years = period_days / 365.0
            combined_wallet_cagr = round(((combined_wallet_equity_usd / total_capital) ** (1.0 / years) - 1) * 100, 1)
        else:
            combined_wallet_cagr = combined_wallet_nav_growth
        metrics["stats"]["cagr_pct"] = combined_wallet_cagr
        print(f"[combined] Wallet equity: ${combined_wallet_equity_usd:,.2f} | "
              f"Return: {combined_wallet_nav_growth}% | CAGR: {combined_wallet_cagr}%")

    now_iso = datetime.now(timezone.utc).isoformat()
    combined = {
        "meta": {
            "account_id": "combined",
            "account_name": "All Accounts",
            "accounts_included": account_names,
            "last_updated": now_iso,
            "usd_inr_rate": USD_INR,
            "assumed_capital_usd": total_capital,
            "assumed_capital_inr": round(total_capital_inr, 0),
            "net_capital_inr": round(total_capital_inr, 0),
            "wallet_balance_usd": round(combined_wallet_equity_usd, 2) if has_wallet_data else None,
            "wallet_balance_inr": round(combined_wallet_equity_inr, 0) if has_wallet_data else None,
            "wallet_net_pnl_usd": round(combined_wallet_net_pnl_usd, 2) if has_wallet_data else None,
            "wallet_net_pnl_inr": round(combined_wallet_net_pnl_inr, 0) if has_wallet_data else None,
            "wallet_nav_growth_pct": combined_wallet_nav_growth,
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
        # Only include active accounts in the dashboard dropdown
        if acc.get("active", True):
            items.append({"id": acc["id"], "name": acc.get("name", acc["id"]),
                          "active": True, "is_combined": False})

    with open("accounts_list.json", "w") as f:
        json.dump(items, f, indent=2)
    print(f"\nWrote accounts_list.json ({len(items)} entries)")


# ── DISCORD NOTIFICATION ──────────────────────────────────────────────────────
def send_discord_summary(results, combined_ok):
    webhook_url = os.environ.get("DISCORD_WEBHOOK", "").strip()
    if not webhook_url:
        print("[discord] No webhook set — skipping.")
        return

    # Normalise URL (discordapp.com -> discord.com)
    webhook_url = webhook_url.replace("discordapp.com", "discord.com")
    print("[discord] Sending summary...")

    def inr(v):
        try:
            return "Rs.{:,.0f}".format(float(v or 0))
        except:
            return "Rs.0"

    try:
        from datetime import timezone, timedelta
        now_ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        ts = now_ist.strftime("%d %b %Y, %I:%M %p IST")

        lines = ["**100X Algo — Daily Update** | " + ts, ""]

        for acc_id, ok in results.items():
            if not ok:
                continue
            acc_path = os.path.join(DATA_DIR, acc_id, "dashboard_data.json")
            if not os.path.exists(acc_path):
                continue
            try:
                with open(acc_path, encoding="utf-8") as f:
                    d = json.load(f)
                s    = d.get("stats", {})
                m    = d.get("meta", {})
                name = m.get("account_name", acc_id.capitalize())
                cap  = float(m.get("assumed_capital_inr") or m.get("net_capital_inr") or 0)
                nav  = float(m.get("wallet_balance_inr") or 0)
                pnl  = float(m.get("wallet_net_pnl_inr") or s.get("total_pnl_inr") or 0)
                sign = "+" if pnl >= 0 else "-"
                lines.append("**" + name + "**  |  Capital: " + inr(cap) + "  |  NAV: " + inr(nav))
                lines.append("PnL: " + sign + inr(abs(pnl)) + "  |  CAGR: " + str(s.get("cagr_pct", 0)) + "%  |  " + str(s.get("total_trades", 0)) + " trades  |  " + str(s.get("win_rate_pct", 0)) + "% WR")
                lines.append("")
            except Exception as e2:
                print("[discord] Skipping " + acc_id + ": " + str(e2))

        combined_path = os.path.join(DATA_DIR, "combined", "dashboard_data.json")
        if combined_ok and os.path.exists(combined_path):
            try:
                with open(combined_path, encoding="utf-8") as f:
                    cd = json.load(f)
                cs = cd.get("stats", {})
                cm = cd.get("meta", {})
                c_nav  = float(cm.get("wallet_balance_inr") or 0)
                c_cap  = float(cm.get("assumed_capital_inr") or cm.get("net_capital_inr") or 0)
                c_pnl  = float(cm.get("wallet_net_pnl_inr") or cs.get("total_pnl_inr") or 0)
                c_sign = "+" if c_pnl >= 0 else "-"
                lines.append("**Combined**  |  Capital: " + inr(c_cap) + "  |  NAV: " + inr(c_nav))
                lines.append("PnL: " + c_sign + inr(abs(c_pnl)) + "  |  CAGR: " + str(cs.get("cagr_pct", 0)) + "%  |  " + str(cs.get("total_trades", 0)) + " trades  |  " + str(cs.get("win_rate_pct", 0)) + "% WR")
                lines.append("")
            except Exception as e3:
                print("[discord] Combined error: " + str(e3))

        lines.append("natarajmalavade.in/100x-algo-dashboard")

        resp = requests.post(webhook_url, json={"content": "\n".join(lines)}, timeout=15)
        if resp.status_code in (200, 204):
            print("[discord] Sent successfully.")
        else:
            print("[discord] Failed: HTTP " + str(resp.status_code) + " — " + resp.text)

    except Exception as e:
        import traceback
        print("[discord] ERROR: " + str(e))
        traceback.print_exc()


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

    # Send Discord summary
    send_discord_summary(results, combined_ok)
