import json
import sqlite3
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
from levels_engine import load_config, get_headers

IST = ZoneInfo("Asia/Kolkata")
DHAN_BASE = "https://api.dhan.co/v2"


def fetch_oc_map(headers: dict, expiry: str) -> dict:
    r = requests.post(
        f"{DHAN_BASE}/optionchain",
        headers=headers,
        json={"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I", "Expiry": expiry},
        timeout=15,
    )
    r.raise_for_status()
    oc = r.json().get("data", {}).get("oc", {})
    out = {}
    for k, legs in oc.items():
        s = int(round(float(k)))
        ce, pe = legs.get("ce") or {}, legs.get("pe") or {}
        if ce.get("security_id") and pe.get("security_id"):
            out[s] = {"ce_id": str(ce["security_id"]), "pe_id": str(pe["security_id"])}
    return out


def fetch_3m(headers: dict, sec_id: str, segment: str, trade_date: str) -> dict:
    payload = {
        "securityId": str(sec_id),
        "exchangeSegment": segment,
        "instrument": "INDEX" if segment == "IDX_I" else "OPTIDX",
        "interval": "3",
        "oi": False,
        "fromDate": f"{trade_date} 09:15:00",
        "toDate": f"{trade_date} 15:30:00",
    }
    d = {}
    for attempt in range(4):
        time.sleep(0.45 * (attempt + 1))
        r = requests.post(f"{DHAN_BASE}/charts/intraday", headers=headers, json=payload, timeout=15)
        if r.status_code == 429:
            time.sleep(2.0 * (attempt + 1))
            continue
        r.raise_for_status()
        d = r.json()
        break
    bars = {}
    ts_list = d.get("timestamp") or []
    vols = d.get("volume") or [0] * len(ts_list)
    for i, ts in enumerate(ts_list):
        hm = datetime.fromtimestamp(int(ts), tz=IST).strftime("%H:%M")
        if not ("09:15" <= hm <= "15:30"):
            continue
        bars[hm] = {
            "hm": hm,
            "open": float(d["open"][i]),
            "high": float(d["high"][i]),
            "low": float(d["low"][i]),
            "close": float(d["close"][i]),
            "volume": int(vols[i]),
        }
    return bars


def run_backtest(trade_date: str = "2026-11-25"):
    cfg = load_config()
    headers = get_headers(cfg)

    conn = sqlite3.connect(cfg["db_path"])
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM daily_levels WHERE trade_date = ?", (trade_date,)).fetchone()
    conn.close()
    if not row:
        raise RuntimeError(f"No daily_levels row found for {trade_date}")

    lv = dict(row)
    oc_map = fetch_oc_map(headers, lv["expiry"])
    cache = {}

    def get_opt_bars(strike: int, side: str) -> dict:
        key = (strike, side)
        if key not in cache:
            sid = oc_map[strike]["ce_id" if side == "CE" else "pe_id"]
            cache[key] = fetch_3m(headers, sid, "NSE_FNO", trade_date)
        return cache[key]

    spot_bars = fetch_3m(headers, "13", "IDX_I", trade_date)
    cur_atm = int(lv["atm_strike"])
    erz_off = int(cfg.get("erz_offset", 100))
    cur_erz_ce = cur_atm - erz_off
    cur_erz_pe = cur_atm + erz_off

    timeline = sorted(spot_bars.keys())
    print(f"=== BACKTEST {trade_date} | Initial ATM {cur_atm} | BEP {lv['bep_lower']}–{lv['bep_upper']} | WZR {lv['wzr_ce_strike']}–{lv['wzr_pe_strike']} ===")
    print(f"Loaded {len(timeline)} spot 3m bars ({timeline[0]} to {timeline[-1]} IST)\n")

    bep_upper = float(lv["bep_upper"])
    bep_lower = float(lv["bep_lower"])
    wzr_upper = float(lv["wzr_pe_strike"])
    wzr_lower = float(lv["wzr_ce_strike"])

    buf = float(cfg.get("boundary_buffer_pts", 20.0))
    mult = float(cfg.get("sweep_vol_multiplier", 1.3))
    wzr_mult = max(1.5, mult)
    erz_width = float(erz_off) * 2.0
    qty = int(cfg.get("lot_size", 65)) * int(cfg.get("lots", 1))
    max_sl = float(cfg.get("max_sl_points", 35.0))
    def_sl = float(cfg.get("sl_points", 25.0))
    be_pts = float(cfg.get("breakeven_points", 15.0))
    tgt_pts = float(cfg.get("target_points", 60.0))

    ce_state, pe_state = "IDLE", "IDLE"
    upper_abs, lower_abs = False, False
    sweep_pe_low, sweep_ce_low = None, None
    open_trade = None
    closed_trades = []

    for idx, hm in enumerate(timeline):
        sc = spot_bars[hm]
        atm_ce_b = get_opt_bars(cur_atm, "CE")
        atm_pe_b = get_opt_bars(cur_atm, "PE")
        erz_ce_b = get_opt_bars(cur_erz_ce, "CE")
        erz_pe_b = get_opt_bars(cur_erz_pe, "PE")
        if not all(hm in b for b in (atm_ce_b, atm_pe_b, erz_ce_b, erz_pe_b)):
            continue

        ac, ap, ec, ep = atm_ce_b[hm], atm_pe_b[hm], erz_ce_b[hm], erz_pe_b[hm]

        # 1. Manage open trade intrabar
        if open_trade:
            t_bars = get_opt_bars(open_trade["strike"], open_trade["side"])
            leg = t_bars.get(hm)
            if leg:
                if not open_trade["be_locked"] and leg["high"] >= open_trade["entry"] + be_pts:
                    open_trade["sl"] = open_trade["entry"]
                    open_trade["be_locked"] = True
                    print(f"[{hm}] BREAKEVEN LOCK: {open_trade['strike']} {open_trade['side']} SL moved to {open_trade['sl']:.2f}")

                exit_price, reason = None, None
                if leg["low"] <= open_trade["sl"]:
                    exit_price, reason = open_trade["sl"], ("BREAKEVEN_SL" if open_trade["be_locked"] else "SL_HIT")
                elif leg["high"] >= open_trade["target"]:
                    exit_price, reason = open_trade["target"], "TARGET_HIT"
                elif hm >= cfg.get("eod_exit", "15:15"):
                    exit_price, reason = leg["close"], "EOD_SQUAREOFF"

                if exit_price is not None:
                    pnl = round((exit_price - open_trade["entry"]) * qty, 2)
                    open_trade.update({"exit_time": hm, "exit": exit_price, "reason": reason, "pnl": pnl})
                    closed_trades.append(open_trade)
                    print(f"[{hm}] EXIT ({reason}): {open_trade['strike']} {open_trade['side']} @ {exit_price:.2f} | PnL: Rs {pnl:+.2f}")
                    open_trade = None

        # 2. Live Synthetic Future & Intraday ATM Roll (>= 150 pt drift while flat)
        live_fut = round(cur_atm + (ac["close"] - ap["close"]), 2)
        if open_trade is None and abs(live_fut - cur_atm) >= 150:
            new_atm = int(round(live_fut / 50.0) * 50)
            if new_atm != cur_atm and all(s in oc_map for s in (new_atm, new_atm - erz_off, new_atm + erz_off)):
                print(f"[{hm}] ATM ROLL: {cur_atm} -> {new_atm} (Live Synth Fut {live_fut:.2f})")
                cur_atm = new_atm
                cur_erz_ce = cur_atm - erz_off
                cur_erz_pe = cur_atm + erz_off
                ce_state, pe_state = "IDLE", "IDLE"
                atm_ce_b = get_opt_bars(cur_atm, "CE")
                atm_pe_b = get_opt_bars(cur_atm, "PE")
                erz_ce_b = get_opt_bars(cur_erz_ce, "CE")
                erz_pe_b = get_opt_bars(cur_erz_pe, "PE")
                if not all(hm in b for b in (atm_ce_b, atm_pe_b, erz_ce_b, erz_pe_b)):
                    continue
                ac, ap, ec, ep = atm_ce_b[hm], atm_pe_b[hm], erz_ce_b[hm], erz_pe_b[hm]
                live_fut = round(cur_atm + (ac["close"] - ap["close"]), 2)

        # 3. 3m Boundary Sweep / Near-Boundary Rejection + Volume Absorption
        prev_hms = [h for h in timeline[:idx] if h in atm_pe_b and h in atm_ce_b][-5:]
        rng = max(0.01, sc["high"] - sc["low"])
        top_wick = sc["high"] - max(sc["open"], sc["close"])
        bot_wick = min(sc["open"], sc["close"]) - sc["low"]
        bear_rej = (sc["close"] < sc["open"]) or (top_wick / rng >= 0.40)
        bull_rej = (sc["close"] > sc["open"]) or (bot_wick / rng >= 0.40)

        up_bep = (sc["high"] >= bep_upper - buf) and (sc["close"] < bep_upper) and bear_rej
        up_wzr = (not up_bep) and hm >= "09:30" and (sc["high"] >= wzr_upper - buf) and (sc["close"] < wzr_upper) and bear_rej
        if (up_bep or up_wzr) and len(prev_hms) >= 2:
            req_m = mult if up_bep else wzr_mult
            avg_v = sum(atm_pe_b[h]["volume"] for h in prev_hms) / len(prev_hms)
            ratio = round(ap["volume"] / avg_v, 2) if avg_v > 0 else 0.0
            if ratio >= req_m:
                upper_abs = True
                sweep_pe_low = round(ap["low"] - 2.0, 2)
                zone = f"Upper BEP ({bep_upper})" if up_bep else f"Upper WZR ({wzr_upper:.0f})"
                print(f"[{hm}] ABSORB: {zone} ({sc['high']:.1f}->{sc['close']:.1f}) | Vol {ratio}x | {cur_atm} PE Low {sweep_pe_low}")

        dn_bep = (sc["low"] <= bep_lower + buf) and (sc["close"] > bep_lower) and bull_rej
        dn_wzr = (not dn_bep) and hm >= "09:30" and (sc["low"] <= wzr_lower + buf) and (sc["close"] > wzr_lower) and bull_rej
        if (dn_bep or dn_wzr) and len(prev_hms) >= 2:
            req_m = mult if dn_bep else wzr_mult
            avg_v = sum(atm_ce_b[h]["volume"] for h in prev_hms) / len(prev_hms)
            ratio = round(ac["volume"] / avg_v, 2) if avg_v > 0 else 0.0
            if ratio >= req_m:
                lower_abs = True
                sweep_ce_low = round(ac["low"] - 2.0, 2)
                zone = f"Lower BEP ({bep_lower})" if dn_bep else f"Lower WZR ({wzr_lower:.0f})"
                print(f"[{hm}] ABSORB: {zone} ({sc['low']:.1f}->{sc['close']:.1f}) | Vol {ratio}x | {cur_atm} CE Low {sweep_ce_low}")

        # 4. Dynamic ERZ Compression & Slingshot Reclaim Check
        straddle_mid = round((ac["close"] + ap["close"]) / 2.0, 2)
        in_win = cfg.get("trade_start", "09:20") <= hm <= cfg.get("trade_end", "14:45")
        ep_hist = [erz_pe_b[h] for h in prev_hms if h in erz_pe_b]
        ec_hist = [erz_ce_b[h] for h in prev_hms if h in erz_ce_b]

        if len(ep_hist) >= 2:
            ep_avg = sum(x["close"] for x in ep_hist) / len(ep_hist)
            if min(ep["low"], ep["close"]) < ep_avg and pe_state == "IDLE":
                pe_state = "ARMED_OTM"
                print(f"[{hm}] ARM PE: {cur_erz_pe} PE compressed < 5-bar avg {ep_avg:.2f} (Low {ep['low']:.2f})")
            elif pe_state == "ARMED_OTM" and ep["close"] > max(ep_avg, ep_hist[-1]["high"]):
                if upper_abs and ap["close"] >= straddle_mid:
                    pe_state = "TRIGGERED"
                    upper_abs = False
                    entry = ap["close"]
                    sl = max(round(entry - max_sl, 2), sweep_pe_low) if (sweep_pe_low and sweep_pe_low < entry) else round(entry - def_sl, 2)
                    tgt = round(entry + tgt_pts, 2)
                    print(f"[{hm}] SIGNAL BUY PE: {cur_atm} PE @ {entry:.2f} | SL {sl:.2f} | TGT {tgt:.2f}")
                    if in_win and not open_trade:
                        open_trade = {"side": "PE", "strike": cur_atm, "entry_time": hm, "entry": entry, "sl": sl, "target": tgt, "be_locked": False}

        if len(ec_hist) >= 2:
            ec_avg = sum(x["close"] for x in ec_hist) / len(ec_hist)
            if min(ec["low"], ec["close"]) < ec_avg and ce_state == "IDLE":
                ce_state = "ARMED_OTM"
                print(f"[{hm}] ARM CE: {cur_erz_ce} CE compressed < 5-bar avg {ec_avg:.2f} (Low {ec['low']:.2f})")
            elif ce_state == "ARMED_OTM" and ec["close"] > max(ec_avg, ec_hist[-1]["high"]):
                if lower_abs and ac["close"] >= straddle_mid:
                    ce_state = "TRIGGERED"
                    lower_abs = False
                    entry = ac["close"]
                    sl = max(round(entry - max_sl, 2), sweep_ce_low) if (sweep_ce_low and sweep_ce_low < entry) else round(entry - def_sl, 2)
                    tgt = round(entry + tgt_pts, 2)
                    print(f"[{hm}] SIGNAL BUY CE: {cur_atm} CE @ {entry:.2f} | SL {sl:.2f} | TGT {tgt:.2f}")
                    if in_win and not open_trade:
                        open_trade = {"side": "CE", "strike": cur_atm, "entry_time": hm, "entry": entry, "sl": sl, "target": tgt, "be_locked": False}

    total_pnl = round(sum(t["pnl"] for t in closed_trades), 2)
    print(f"\n=== SUMMARY ({trade_date}) ===")
    print(f"Total Trades: {len(closed_trades)} | Net PnL: Rs {total_pnl:+.2f}")
    for t in closed_trades:
        print(f"  {t['entry_time']}->{t['exit_time']} | BUY {t['strike']} {t['side']} | Entry {t['entry']:.2f} -> Exit {t['exit']:.2f} ({t['reason']}) | PnL Rs {t['pnl']:+.2f}")


if __name__ == "__main__":
    run_backtest("2026-11-25")
