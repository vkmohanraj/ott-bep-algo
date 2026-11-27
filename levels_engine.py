import time
import json
import os
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import requests

IST = ZoneInfo("Asia/Kolkata")
DHAN_BASE = "https://api.dhan.co/v2"


def load_config(path: str = "config.json") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    token_path = cfg.get("token_file_path")
    if token_path and os.path.exists(token_path):
        try:
            with open(token_path, "r", encoding="utf-8") as tf:
                tdata = json.load(tf)
                if tdata.get("access_token"):
                    cfg["dhan_access_token"] = tdata["access_token"]
                if tdata.get("client_id"):
                    cfg["dhan_client_id"] = str(tdata["client_id"])
        except Exception:
            pass
    return cfg



def _safe_post(url, headers, json_payload):
    for attempt in range(5):
        time.sleep(1.15)
        r = requests.post(url, headers=headers, json=json_payload, timeout=15)
        if r.status_code != 429:
            return r
        time.sleep(2.5 * (attempt + 1))
    return r

def get_headers(cfg: dict) -> dict:
    return {
        "access-token": cfg["dhan_access_token"],
        "client-id": str(cfg["dhan_client_id"]),
        "Content-Type": "application/json",
    }


def init_db(db_path: str):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_levels (
            trade_date TEXT PRIMARY KEY,
            expiry TEXT,
            atm_strike INTEGER,
            strike_gap REAL,
            synthetic_future REAL,
            bep_points REAL,
            bep_upper REAL,
            bep_lower REAL,
            straddle_mid REAL,
            atm_ce_iv REAL,
            atm_pe_iv REAL,
            erz_ce_strike INTEGER,
            erz_pe_strike INTEGER,
            erz_ce_iv REAL,
            erz_pe_iv REAL,
            wzr_ce_strike INTEGER,
            wzr_pe_strike INTEGER,
            wzr_ce_iv REAL,
            wzr_pe_iv REAL,
            instruments_json TEXT,
            created_at TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT,
            signal_type TEXT,
            option_type TEXT,
            strike INTEGER,
            security_id TEXT,
            qty INTEGER,
            entry_time TEXT,
            entry_price REAL,
            sl_price REAL,
            target_price REAL,
            breakeven_moved INTEGER DEFAULT 0,
            status TEXT,
            exit_time TEXT,
            exit_price REAL,
            exit_reason TEXT,
            pnl REAL DEFAULT 0.0
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT,
            level TEXT,
            message TEXT
        )
    """)
    for col_def in ("prev_day_mid REAL DEFAULT 0.0", "straddle_sum REAL DEFAULT 0.0", "prev_erz_ce_iv REAL DEFAULT 0.0", "prev_erz_pe_iv REAL DEFAULT 0.0", "today_erz_ce_iv REAL DEFAULT 0.0", "today_erz_pe_iv REAL DEFAULT 0.0", "erz_total_range REAL DEFAULT 0.0", "tz_mid REAL DEFAULT 0.0", "tz_low REAL DEFAULT 0.0", "tz_high REAL DEFAULT 0.0", "opt_r1 REAL DEFAULT 0.0", "opt_s1 REAL DEFAULT 0.0"):
        try:
            cur.execute(f"ALTER TABLE daily_levels ADD COLUMN {col_def}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()


def fetch_prev_ohlc(headers: dict, security_id: str) -> dict:
    today = datetime.now(IST).date()
    from_date = (today - timedelta(days=10)).strftime("%Y-%m-%d")
    to_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": "NSE_FNO",
        "instrument": "OPTIDX",
        "expiryCode": 0,
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date,
    }
    resp = _safe_post(f"{DHAN_BASE}/charts/historical", headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return {
        "open": float(data["open"][-1]),
        "high": float(data["high"][-1]),
        "low": float(data["low"][-1]),
        "close": float(data["close"][-1]),
    }


def fetch_spot_prev_ohlc(headers: dict) -> dict:
    today = datetime.now(IST).date()
    from_date = (today - timedelta(days=10)).strftime("%Y-%m-%d")
    to_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    payload = {
        "securityId": "13",
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "expiryCode": 0,
        "oi": False,
        "fromDate": from_date,
        "toDate": to_date,
    }
    resp = _safe_post(f"{DHAN_BASE}/charts/historical", headers=headers, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return {
        "open": float(data["open"][-1]),
        "high": float(data["high"][-1]),
        "low": float(data["low"][-1]),
        "close": float(data["close"][-1]),
    }


def compute_and_store_levels(cfg: dict) -> dict:
    init_db(cfg["db_path"])
    headers = get_headers(cfg)
    today_str = datetime.now(IST).strftime("%Y-%m-%d")

    exp_resp = _safe_post(
        f"{DHAN_BASE}/optionchain/expirylist",
        headers=headers,
        json={"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I"},
        timeout=15,
    )
    exp_resp.raise_for_status()
    expiries = exp_resp.json().get("data", [])
    if not expiries:
        raise RuntimeError("No NIFTY expiries returned by Dhan API")
    nearest_expiry = sorted(expiries)[0]

    oc_resp = _safe_post(
        f"{DHAN_BASE}/optionchain",
        headers=headers,
        json={"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I", "Expiry": nearest_expiry},
        timeout=15,
    )
    oc_resp.raise_for_status()
    oc_data = oc_resp.json().get("data", {})
    oc_map = oc_data.get("oc", {})
    spot_ref = float(oc_data.get("last_price") or 0)

    strikes = {}
    for k, legs in oc_map.items():
        strike = int(round(float(k)))
        ce = legs.get("ce") or {}
        pe = legs.get("pe") or {}
        ce_prev = float(ce.get("previous_close_price") or ce.get("last_price") or 0)
        pe_prev = float(pe.get("previous_close_price") or pe.get("last_price") or 0)
        if ce.get("security_id") and pe.get("security_id") and ce_prev > 0 and pe_prev > 0:
            strikes[strike] = {
                "ce_id": str(ce["security_id"]),
                "pe_id": str(pe["security_id"]),
                "ce_prev": ce_prev,
                "pe_prev": pe_prev,
                "gap": round(abs(ce_prev - pe_prev), 2),
            }

    if not strikes:
        raise RuntimeError("Empty option chain strikes from Dhan API")

    near_strikes = {s: d for s, d in strikes.items() if abs(s - spot_ref) <= 250} if spot_ref > 0 else strikes
    pool = near_strikes or strikes
    atm_strike, atm_info = min(pool.items(), key=lambda x: x[1]["gap"])

    ce_ohlc = fetch_prev_ohlc(headers, atm_info["ce_id"])
    pe_ohlc = fetch_prev_ohlc(headers, atm_info["pe_id"])
    prelim_fut = atm_strike + (ce_ohlc["close"] - pe_ohlc["close"])
    if abs(atm_strike - prelim_fut) > 100:
        re_pool = {s: d for s, d in strikes.items() if abs(s - prelim_fut) <= 150} or strikes
        atm_strike, atm_info = min(re_pool.items(), key=lambda x: x[1]["gap"])
        ce_ohlc = fetch_prev_ohlc(headers, atm_info["ce_id"])
        pe_ohlc = fetch_prev_ohlc(headers, atm_info["pe_id"])

    all_8 = [
        ce_ohlc["open"], ce_ohlc["high"], ce_ohlc["low"], ce_ohlc["close"],
        pe_ohlc["open"], pe_ohlc["high"], pe_ohlc["low"], pe_ohlc["close"],
    ]
    bep_points = round((max(all_8) + min(all_8)) / 2.0, 2)
    synth_fut = round(atm_strike + (ce_ohlc["close"] - pe_ohlc["close"]), 2)
    straddle_sum = round(ce_ohlc["close"] + pe_ohlc["close"], 2)
    straddle_mid = round(straddle_sum / 2.0, 2)
    try:
        spot_ohlc = fetch_spot_prev_ohlc(headers)
        prev_day_mid = round((spot_ohlc["high"] + spot_ohlc["low"]) / 2.0, 2)
    except Exception:
        prev_day_mid = round(synth_fut, 2)

    if cfg.get("dynamic_triangle_width", True):
        erz_off = max(100, int(round(straddle_mid / 50.0) * 50))
        wzr_off = max(200, int(round(straddle_sum / 50.0) * 50))
    else:
        erz_off = int(cfg.get("erz_offset", 150))
        wzr_off = int(cfg.get("wzr_offset", 300))

    erz_ce_strike = atm_strike - erz_off
    erz_pe_strike = atm_strike + erz_off
    wzr_ce_strike = atm_strike - wzr_off
    wzr_pe_strike = atm_strike + wzr_off

    atm_ce_iv = round(max(0.0, synth_fut - atm_strike), 2)
    atm_pe_iv = round(max(0.0, atm_strike - synth_fut), 2)
    erz_ce_iv = round(max(0.0, synth_fut - erz_ce_strike), 2)
    erz_pe_iv = round(max(0.0, erz_pe_strike - synth_fut), 2)
    prev_erz_ce_iv, prev_erz_pe_iv = erz_ce_iv, erz_pe_iv
    today_erz_ce_iv, today_erz_pe_iv = erz_ce_iv, erz_pe_iv
    erz_total_range = float(round(erz_off * 2))
    straddle_mid = float(round(straddle_mid))
    straddle_sum = float(round(straddle_sum))
    bep_points = float(round(bep_points))
    bep_upper = float(round(atm_strike + bep_points))
    bep_lower = float(round(atm_strike - bep_points))
    synthetic_future = float(round(atm_strike + (round(ce_ohlc["close"]) - round(pe_ohlc["close"]))))
    prev_erz_ce_iv = float(round(prev_erz_ce_iv))
    prev_erz_pe_iv = float(round(prev_erz_pe_iv))
    tz_mid = float(round(prev_day_mid / 50.0) * 50)
    tz_low, tz_high = float(tz_mid - 30), float(tz_mid + 30)
    opt_r1 = float(round(atm_strike + round(ce_ohlc["low"])))
    opt_s1 = float(round(atm_strike - round(pe_ohlc["low"])))
    wzr_ce_iv = round(max(0.0, synth_fut - wzr_ce_strike), 2)
    wzr_pe_iv = round(max(0.0, wzr_pe_strike - synth_fut), 2)

    instruments = {
        "NIFTY_SPOT": {"security_id": "13", "segment": "IDX_I", "label": "NIFTY 50"},
        "ATM_CE": {"security_id": atm_info["ce_id"], "segment": "NSE_FNO", "strike": atm_strike, "type": "CE", "iv": atm_ce_iv, "prev_high": ce_ohlc["high"], "prev_low": ce_ohlc["low"], "prev_close": ce_ohlc["close"]},
        "ATM_PE": {"security_id": atm_info["pe_id"], "segment": "NSE_FNO", "strike": atm_strike, "type": "PE", "iv": atm_pe_iv, "prev_high": pe_ohlc["high"], "prev_low": pe_ohlc["low"], "prev_close": pe_ohlc["close"]},
        "ERZ_CE": {**{"security_id": strikes[erz_ce_strike]["ce_id"], "segment": "NSE_FNO", "strike": erz_ce_strike, "type": "CE", "iv": erz_ce_iv}, **{f"prev_{k}": v for k, v in fetch_prev_ohlc(headers, strikes[erz_ce_strike]["ce_id"]).items() if k in ("high", "low")}},
        "ERZ_PE": {**{"security_id": strikes[erz_pe_strike]["pe_id"], "segment": "NSE_FNO", "strike": erz_pe_strike, "type": "PE", "iv": erz_pe_iv}, **{f"prev_{k}": v for k, v in fetch_prev_ohlc(headers, strikes[erz_pe_strike]["pe_id"]).items() if k in ("high", "low")}},
        "WZR_CE": {"security_id": strikes[wzr_ce_strike]["ce_id"], "segment": "NSE_FNO", "strike": wzr_ce_strike, "type": "CE", "iv": wzr_ce_iv},
        "WZR_PE": {"security_id": strikes[wzr_pe_strike]["pe_id"], "segment": "NSE_FNO", "strike": wzr_pe_strike, "type": "PE", "iv": wzr_pe_iv},
    }

    result = {
        "trade_date": today_str,
        "expiry": nearest_expiry,
        "atm_strike": atm_strike,
        "strike_gap": atm_info["gap"],
        "synthetic_future": synth_fut,
        "bep_points": bep_points,
        "bep_upper": round(atm_strike + bep_points, 2),
        "bep_lower": round(atm_strike - bep_points, 2),
        "straddle_mid": straddle_mid,
        "straddle_sum": straddle_sum,
        "prev_day_mid": prev_day_mid,
        "atm_ce_iv": atm_ce_iv,
        "atm_pe_iv": atm_pe_iv,
        "erz_ce_strike": erz_ce_strike,
        "erz_pe_strike": erz_pe_strike,
        "erz_ce_iv": erz_ce_iv,
        "erz_pe_iv": erz_pe_iv,
        "prev_erz_ce_iv": prev_erz_ce_iv,
        "prev_erz_pe_iv": prev_erz_pe_iv,
        "today_erz_ce_iv": today_erz_ce_iv,
        "today_erz_pe_iv": today_erz_pe_iv,
        "erz_total_range": erz_total_range, "tz_mid": tz_mid, "tz_low": tz_low, "tz_high": tz_high, "opt_r1": opt_r1, "opt_s1": opt_s1,
        "wzr_ce_strike": wzr_ce_strike,
        "wzr_pe_strike": wzr_pe_strike,
        "wzr_ce_iv": wzr_ce_iv,
        "wzr_pe_iv": wzr_pe_iv,
        "instruments": instruments,
    }

    conn = sqlite3.connect(cfg["db_path"])
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO daily_levels (
            trade_date, expiry, atm_strike, strike_gap,
            synthetic_future, bep_points, bep_upper, bep_lower,
            straddle_mid, atm_ce_iv, atm_pe_iv,
            erz_ce_strike, erz_pe_strike, erz_ce_iv, erz_pe_iv,
            wzr_ce_strike, wzr_pe_strike, wzr_ce_iv, wzr_pe_iv,
            instruments_json, created_at, prev_day_mid, straddle_sum, prev_erz_ce_iv, prev_erz_pe_iv, today_erz_ce_iv, today_erz_pe_iv, erz_total_range, tz_mid, tz_low, tz_high, opt_r1, opt_s1
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    """, (
        result["trade_date"], result["expiry"], result["atm_strike"], result["strike_gap"],
        result["synthetic_future"], result["bep_points"], result["bep_upper"], result["bep_lower"],
        result["straddle_mid"], result["atm_ce_iv"], result["atm_pe_iv"],
        result["erz_ce_strike"], result["erz_pe_strike"], result["erz_ce_iv"], result["erz_pe_iv"],
        result["wzr_ce_strike"], result["wzr_pe_strike"], result["wzr_ce_iv"], result["wzr_pe_iv"],
        json.dumps(instruments), datetime.now(IST).isoformat(),
        result["prev_day_mid"], result["straddle_sum"], result["prev_erz_ce_iv"], result["prev_erz_pe_iv"], result["today_erz_ce_iv"], result["today_erz_pe_iv"], result["erz_total_range"], result["tz_mid"], result["tz_low"], result["tz_high"], result["opt_r1"], result["opt_s1"],
    ))
    conn.commit()
    conn.close()
    return result


def get_today_levels(cfg: dict) -> dict:
    init_db(cfg["db_path"])
    today_str = datetime.now(IST).strftime("%Y-%m-%d")
    conn = sqlite3.connect(cfg["db_path"])
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM daily_levels WHERE trade_date = ?", (today_str,)).fetchone()
    conn.close()
    if row:
        d = dict(row)
        d["instruments"] = json.loads(d["instruments_json"])
        return d
    return compute_and_store_levels(cfg)
