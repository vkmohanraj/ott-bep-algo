import asyncio
import json
import struct
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import websockets
from levels_engine import get_headers

IST = ZoneInfo("Asia/Kolkata")


class Candle3M:
    def __init__(self, bucket: int, ts_epoch: int, price: float, cum_vol: int = 0):
        self.bucket = bucket
        self.time = ts_epoch
        self.open = price
        self.high = price
        self.low = price
        self.close = price
        self.start_vol = cum_vol
        self.end_vol = cum_vol

    def update(self, price: float, cum_vol: int = 0):
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        if cum_vol > 0:
            if self.start_vol == 0:
                self.start_vol = cum_vol
            self.end_vol = max(self.end_vol, cum_vol)

    @property
    def volume(self) -> int:
        return max(0, self.end_vol - self.start_vol)

    def to_dict(self) -> dict:
        return {
            "time": self.time,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }


class SignalEngine:
    def __init__(self, cfg: dict, levels: dict, trader, notifier=None):
        self.cfg = cfg
        self.levels = levels
        self.trader = trader
        self.notifier = notifier
        self.prices = {k: 0.0 for k in levels["instruments"]}
        self.cum_vols = {k: 0 for k in levels["instruments"]}
        self.sec_to_key = {v["security_id"]: k for k, v in levels["instruments"].items()}

        self.ce_state = "IDLE"
        self.pe_state = "IDLE"
        self.active_signal = "WAITING"
        self.ws_connected = False
        self.last_tick_time = "--:--:--"
        self._running = False

        tracked = ("NIFTY_SPOT", "ATM_CE", "ATM_PE", "ERZ_CE", "ERZ_PE")
        self.cur_candles = {}
        self.hist_candles = {k: deque(maxlen=150) for k in tracked}
        self.upper_bep_absorbed = False
        self.lower_bep_absorbed = False
        self.sweep_atm_pe_low = None
        self.sweep_atm_ce_low = None
        self._force_resubscribe = False
        self._rolling_atm = False

    def _roll_intraday_atm(self, new_atm: int, live_fut: float):
        try:
            headers = get_headers(self.cfg)
            r = requests.post(
                "https://api.dhan.co/v2/optionchain",
                headers=headers,
                json={"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I", "Expiry": self.levels["expiry"]},
                timeout=10,
            )
            r.raise_for_status()
            oc = r.json().get("data", {}).get("oc", {})
            smap = {}
            for k, legs in oc.items():
                s = int(round(float(k)))
                ce, pe = legs.get("ce") or {}, legs.get("pe") or {}
                if ce.get("security_id") and pe.get("security_id"):
                    smap[s] = {"ce_id": str(ce["security_id"]), "pe_id": str(pe["security_id"])}
            erz_off = int(self.cfg.get("erz_offset", 100))
            if not all(s in smap for s in (new_atm, new_atm - erz_off, new_atm + erz_off)):
                return
            old_atm = self.levels["atm_strike"]
            inst = self.levels["instruments"]
            inst["ATM_CE"].update({"security_id": smap[new_atm]["ce_id"], "strike": new_atm})
            inst["ATM_PE"].update({"security_id": smap[new_atm]["pe_id"], "strike": new_atm})
            inst["ERZ_CE"].update({"security_id": smap[new_atm - erz_off]["ce_id"], "strike": new_atm - erz_off})
            inst["ERZ_PE"].update({"security_id": smap[new_atm + erz_off]["pe_id"], "strike": new_atm + erz_off})
            self.levels["atm_strike"] = new_atm
            self.levels["erz_ce_strike"] = new_atm - erz_off
            self.levels["erz_pe_strike"] = new_atm + erz_off
            self.sec_to_key = {v["security_id"]: k for k, v in inst.items()}
            for k in ("ATM_CE", "ATM_PE", "ERZ_CE", "ERZ_PE"):
                self.prices[k] = 0.0
                self.cum_vols[k] = 0
                self.cur_candles.pop(k, None)
                self.hist_candles[k].clear()
            self.ce_state = "IDLE"
            self.pe_state = "IDLE"
            self.sweep_atm_pe_low = None
            self.sweep_atm_ce_low = None
            self._force_resubscribe = True
            self.trader.log_event("SYS", f"Intraday ATM Roll: {old_atm} -> {new_atm} (Synth Fut {live_fut:.2f})")
        except Exception as e:
            self.trader.log_event("WARN", f"ATM Roll failed: {e}")
        finally:
            self._rolling_atm = False

    def _in_trade_window(self) -> bool:
        now_hm = datetime.now(IST).strftime("%H:%M")
        return self.cfg.get("trade_start", "09:20") <= now_hm <= self.cfg.get("trade_end", "14:45")

    def _update_3m_candles(self, key: str, price: float, cum_vol: int):
        if key not in self.hist_candles or price <= 0:
            return
        now = datetime.now(IST)
        hm = now.strftime("%H:%M")
        if hm < "09:15" or hm > "15:30":
            return
        bucket = (now.hour * 60 + now.minute) // 3
        floored = now.replace(minute=(now.minute // 3) * 3, second=0, microsecond=0)
        ts_ist = int(floored.timestamp()) + 19800

        c = self.cur_candles.get(key)
        if c is None:
            self.cur_candles[key] = Candle3M(bucket, ts_ist, price, cum_vol)
            return
        if c.bucket == bucket:
            c.update(price, cum_vol)
        else:
            self.hist_candles[key].append(c)
            self.cur_candles[key] = Candle3M(bucket, ts_ist, price, cum_vol)
            if key == "NIFTY_SPOT":
                self._on_3m_spot_close(c)

    def _on_3m_spot_close(self, spot_c: Candle3M):
        bar_hm = datetime.fromtimestamp(spot_c.time - 19800, tz=IST).strftime("%H:%M")
        if "09:15" <= bar_hm < "09:30":
            oh, ol = getattr(self, "orb_high", None), getattr(self, "orb_low", None)
            self.orb_high = spot_c.high if oh is None else max(oh, spot_c.high)
            self.orb_low = spot_c.low if ol is None else min(ol, spot_c.low)
            return

        oh, ol = getattr(self, "orb_high", None), getattr(self, "orb_low", None)
        if oh and spot_c.close > oh and spot_c.open <= oh:
            self.lower_bep_absorbed, self.upper_bep_absorbed = True, False
            self.trader.log_event("ABSORB", f"3m ORB High Breakout ({spot_c.close:.1f} > {oh:.1f})")
        elif ol and spot_c.close < ol and spot_c.open >= ol:
            self.upper_bep_absorbed, self.lower_bep_absorbed = True, False
            self.trader.log_event("ABSORB", f"3m ORB Low Breakdown ({spot_c.close:.1f} < {ol:.1f})")

    def evaluate_strategy(self):
        import threading
        spot = self.prices.get("NIFTY_SPOT", 0.0)
        atm_ce, atm_pe = self.prices.get("ATM_CE", 0.0), self.prices.get("ATM_PE", 0.0)
        erz_ce, erz_pe = self.prices.get("ERZ_CE", 0.0), self.prices.get("ERZ_PE", 0.0)
        if spot <= 0:
            return

        open_trade = self.trader.get_open_trade()
        if getattr(self, "_had_open_trade", False) and not open_trade:
            self.ce_state, self.pe_state, self.active_signal = "IDLE", "IDLE", "WAITING"
        self._had_open_trade = bool(open_trade)

        atm_strike = float(self.levels["atm_strike"])
        live_fut = round(atm_strike + (atm_ce - atm_pe), 2) if (atm_ce > 0 and atm_pe > 0) else spot
        now_hm = datetime.now(IST).strftime("%H:%M")

        if not open_trade and not self._rolling_atm and "09:15" <= now_hm <= "14:45" and abs(live_fut - atm_strike) >= 100.0:
            new_atm = int(round(live_fut / 50.0) * 50)
            if new_atm != int(atm_strike):
                self._rolling_atm = True
                threading.Thread(target=self._roll_intraday_atm, args=(new_atm, live_fut), daemon=True).start()
                return

        if min(spot, atm_ce, atm_pe, erz_ce, erz_pe) <= 0:
            return

        erz_off = float(self.cfg.get("erz_offset", 100))
        self.levels["wzr_ce_strike"], self.levels["wzr_pe_strike"] = atm_strike - erz_off, atm_strike + erz_off
        self.levels["synthetic_future"] = live_fut
        self.levels["erz_ce_iv"] = round(max(10.0, live_fut - float(self.levels["erz_ce_strike"])), 2)
        self.levels["erz_pe_iv"] = round(max(10.0, float(self.levels["erz_pe_strike"]) - live_fut), 2)
        self.levels["straddle_mid"] = round((atm_ce + atm_pe) / 2.0, 2)
        self.levels["live_straddle_sum"] = round(atm_ce + atm_pe, 2)

        qty = int(self.cfg.get("lot_size", 65)) * int(self.cfg.get("lots", 1))
        ep_hist, ec_hist = list(self.hist_candles["ERZ_PE"]), list(self.hist_candles["ERZ_CE"])

        if not open_trade and self.upper_bep_absorbed and self.pe_state != "TRIGGERED":
            pe_brk = round(ep_hist[-1].high, 2) if ep_hist else erz_pe
            self.pe_state, self.upper_bep_absorbed, self.active_signal = "TRIGGERED", False, "BUY PE"
            self.trader.log_event("SIGNAL", f"BUY PE! ORB Breakdown + ERZ PE ({erz_pe:.2f})")
            atm_leg = self.levels["instruments"]["ATM_PE"]
            trade = self.trader.open_trade("BUY PE", "PE", atm_leg["strike"], atm_leg["security_id"], atm_pe) if self._in_trade_window() else None
            if self.notifier:
                sl_p = trade["sl_price"] if trade else round(max(1.0, atm_pe - float(self.cfg.get("sl_points", 20.0))), 2)
                tgt_p = trade["target_price"] if trade else round(atm_pe + float(self.cfg.get("target_points", 60.0)), 2)
                self.notifier.alert_signal("BUY PE", atm_leg["strike"], "PE", atm_pe, sl_p, tgt_p, qty, self.levels["erz_pe_strike"], erz_pe, pe_brk, spot, bool(trade))

        if not open_trade and self.lower_bep_absorbed and self.ce_state != "TRIGGERED":
            ce_brk = round(ec_hist[-1].high, 2) if ec_hist else erz_ce
            self.ce_state, self.lower_bep_absorbed, self.active_signal = "TRIGGERED", False, "BUY CE"
            self.trader.log_event("SIGNAL", f"BUY CE! ORB Breakout + ERZ CE ({erz_ce:.2f})")
            atm_leg = self.levels["instruments"]["ATM_CE"]
            trade = self.trader.open_trade("BUY CE", "CE", atm_leg["strike"], atm_leg["security_id"], atm_ce) if self._in_trade_window() else None
            if self.notifier:
                sl_p = trade["sl_price"] if trade else round(max(1.0, atm_ce - float(self.cfg.get("sl_points", 20.0))), 2)
                tgt_p = trade["target_price"] if trade else round(atm_ce + float(self.cfg.get("target_points", 60.0)), 2)
                self.notifier.alert_signal("BUY CE", atm_leg["strike"], "CE", atm_ce, sl_p, tgt_p, qty, self.levels["erz_ce_strike"], erz_ce, ce_brk, spot, bool(trade))

        self.trader.on_tick(self.prices)

    def _parse_binary_packet(self, data: bytes):
        if len(data) < 12:
            return
        feed_code = data[0]
        if feed_code in (2, 4, 8):
            sec_id = str(struct.unpack("<i", data[4:8])[0])
            ltp = round(struct.unpack("<f", data[8:12])[0], 2)
            cum_vol = struct.unpack("<i", data[22:26])[0] if (feed_code in (4, 8) and len(data) >= 26) else 0
            key = self.sec_to_key.get(sec_id)
            if key and ltp > 0:
                self.prices[key] = ltp
                if cum_vol > 0:
                    self.cum_vols[key] = cum_vol
                self.last_tick_time = datetime.now(IST).strftime("%H:%M:%S")
                self._update_3m_candles(key, ltp, self.cum_vols.get(key, 0))
                self.evaluate_strategy()

    def _poll_rest_fallback(self):
        try:
            headers = get_headers(self.cfg)
            payload = {"IDX_I": [13], "NSE_FNO": []}
            for info in self.levels["instruments"].values():
                if info["segment"] == "NSE_FNO":
                    payload["NSE_FNO"].append(int(info["security_id"]))
            r = requests.post("https://api.dhan.co/v2/marketfeed/ltp", headers=headers, json=payload, timeout=5)
            if r.status_code == 200:
                d = r.json().get("data", {})
                for seg in ("IDX_I", "NSE_FNO"):
                    for sid, val in d.get(seg, {}).items():
                        key = self.sec_to_key.get(str(sid))
                        if key and val.get("last_price", 0) > 0:
                            ltp = float(val["last_price"])
                            self.prices[key] = ltp
                            self._update_3m_candles(key, ltp, self.cum_vols.get(key, 0))
                self.last_tick_time = datetime.now(IST).strftime("%H:%M:%S")
                self.evaluate_strategy()
        except Exception:
            pass

    def fetch_intraday_candles(self, leg_key: str) -> list:
        info = self.levels["instruments"].get(leg_key)
        if not info:
            return []
        if not hasattr(self, "_candle_cache"):
            self._candle_cache = {}
        now_ts = datetime.now(IST).timestamp()
        ckey = f"{leg_key}:{info['security_id']}"
        cached = self._candle_cache.get(ckey)
        merged = dict(cached[1]) if (cached and now_ts - cached[0] < 30.0) else {}
        if not merged:
            try:
                headers = get_headers(self.cfg)
                today_str = datetime.now(IST).strftime("%Y-%m-%d")
                payload = {
                    "securityId": str(info["security_id"]),
                    "exchangeSegment": info["segment"],
                    "instrument": "INDEX" if info["segment"] == "IDX_I" else "OPTIDX",
                    "interval": "3",
                    "oi": False,
                    "fromDate": f"{today_str} 09:15:00",
                    "toDate": f"{today_str} 15:30:00",
                }
                r = requests.post(
                    "https://api.dhan.co/v2/charts/intraday",
                    headers=headers,
                    json=payload,
                    timeout=4,
                )
                if r.status_code == 200:
                    d = r.json()
                    ts_l = d.get("timestamp") or []
                    op_l, hi_l = d.get("open") or [], d.get("high") or []
                    lo_l, cl_l = d.get("low") or [], d.get("close") or []
                    for i in range(min(len(ts_l), len(op_l), len(hi_l), len(lo_l), len(cl_l))):
                        if None in (op_l[i], hi_l[i], lo_l[i], cl_l[i]):
                            continue
                        o, h, l, c = float(op_l[i]), float(hi_l[i]), float(lo_l[i]), float(cl_l[i])
                        if min(o, h, l, c) <= 0:
                            continue
                        hm = datetime.fromtimestamp(int(ts_l[i]), tz=IST).strftime("%H:%M")
                        if not ("09:15" <= hm <= "15:30"):
                            continue
                        t = int(ts_l[i]) + 19800
                        merged[t] = {"time": t, "open": o, "high": h, "low": l, "close": c}
                if merged:
                    self._candle_cache[ckey] = (now_ts, dict(merged))
            except Exception:
                if cached:
                    merged = dict(cached[1])
        for c in list(self.hist_candles.get(leg_key, [])):
            if c.open > 0 and c.close > 0:
                merged[c.time] = c.to_dict()
        cur = self.cur_candles.get(leg_key)
        if cur and cur.open > 0 and cur.close > 0:
            merged[cur.time] = cur.to_dict()
        return [merged[k] for k in sorted(merged.keys())]

    async def run(self):
        self._running = True
        ws_url = f"wss://api-feed.dhan.co?version=2&token={self.cfg['dhan_access_token']}&clientId={self.cfg['dhan_client_id']}&authType=2"
        while self._running:
            try:
                sub_list = [{"ExchangeSegment": info["segment"], "SecurityId": info["security_id"]} for info in self.levels["instruments"].values()]
                sub_msg = json.dumps({"RequestCode": 17, "InstrumentCount": len(sub_list), "InstrumentList": sub_list})
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self.ws_connected = True
                    self._force_resubscribe = False
                    await ws.send(sub_msg)
                    self.trader.log_event("SYS", "DhanHQ v2 Quote WebSocket connected (3m Absorption Filter ON)")
                    await asyncio.to_thread(self._poll_rest_fallback)
                    while self._running:
                        if self._force_resubscribe:
                            self._force_resubscribe = False
                            sub_list = [{"ExchangeSegment": info["segment"], "SecurityId": info["security_id"]} for info in self.levels["instruments"].values()]
                            await ws.send(json.dumps({"RequestCode": 17, "InstrumentCount": len(sub_list), "InstrumentList": sub_list}))
                            await asyncio.to_thread(self._poll_rest_fallback)
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=25)
                            if isinstance(msg, bytes):
                                self._parse_binary_packet(msg)
                        except asyncio.TimeoutError:
                            await asyncio.to_thread(self._poll_rest_fallback)
            except Exception as e:
                self.ws_connected = False
                err_txt = str(e) or type(e).__name__
                self.trader.log_event("WARN", f"WS reconnecting ({err_txt}); running REST LTP sync")
                await asyncio.to_thread(self._poll_rest_fallback)
                await asyncio.sleep(3)

    def get_snapshot(self) -> dict:
        open_trade = self.trader.get_open_trade()
        trades = self.trader.get_today_trades()
        realized_pnl = sum(t["pnl"] for t in trades if t["status"] == "CLOSED")
        unrealized_pnl = 0.0
        if open_trade:
            cur_ltp = self.prices.get(f"ATM_{open_trade['option_type']}", open_trade["entry_price"])
            unrealized_pnl = round((cur_ltp - open_trade["entry_price"]) * open_trade["qty"], 2)
        live_bars = {k: c.to_dict() for k, c in self.cur_candles.items()}
        return {
            "time": datetime.now(IST).strftime("%H:%M:%S"),
            "ws_connected": self.ws_connected,
            "last_tick": self.last_tick_time,
            "active_signal": self.active_signal,
            "ce_state": f"{self.ce_state}{'[ABS]' if self.lower_bep_absorbed else ''}",
            "pe_state": f"{self.pe_state}{'[ABS]' if self.upper_bep_absorbed else ''}",
            "levels": self.levels,
            "prices": self.prices,
            "live_bars": live_bars,
            "open_trade": open_trade,
            "trades": trades,
            "realized_pnl": round(realized_pnl, 2),
            "unrealized_pnl": unrealized_pnl,
            "net_pnl": round(realized_pnl + unrealized_pnl, 2),
            "capital": round(float(self.cfg["initial_capital"]) + realized_pnl + unrealized_pnl, 2),
            "logs": self.trader.get_recent_logs(25),
        }
