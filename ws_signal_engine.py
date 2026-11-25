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
        bep_upper = float(self.levels["bep_upper"])
        bep_lower = float(self.levels["bep_lower"])
        wzr_upper = float(self.levels.get("wzr_pe_strike", bep_upper))
        wzr_lower = float(self.levels.get("wzr_ce_strike", bep_lower))
        buf = float(self.cfg.get("boundary_buffer_pts", 20.0))
        mult = float(self.cfg.get("sweep_vol_multiplier", 1.3))
        wzr_mult = max(1.5, mult)
        hm = datetime.now(IST).strftime("%H:%M")

        rng = max(0.01, spot_c.high - spot_c.low)
        top_wick = spot_c.high - max(spot_c.open, spot_c.close)
        bot_wick = min(spot_c.open, spot_c.close) - spot_c.low
        bear_rej = (spot_c.close < spot_c.open) or (top_wick / rng >= 0.40)
        bull_rej = (spot_c.close > spot_c.open) or (bot_wick / rng >= 0.40)

        up_bep = (spot_c.high >= bep_upper - buf) and (spot_c.close < bep_upper) and bear_rej
        up_wzr = (not up_bep) and hm >= "09:30" and (spot_c.high >= wzr_upper - buf) and (spot_c.close < wzr_upper) and bear_rej
        if up_bep or up_wzr:
            pe_hist = list(self.hist_candles["ATM_PE"])
            req_mult = mult if up_bep else wzr_mult
            vol_ok, vol_ratio = False, 0.0
            if len(pe_hist) >= 3:
                last_v = pe_hist[-1].volume
                avg_v = sum(x.volume for x in pe_hist[-6:-1]) / max(1, len(pe_hist[-6:-1]))
                if avg_v > 0:
                    vol_ratio = round(last_v / avg_v, 2)
                    vol_ok = vol_ratio >= req_mult
            if vol_ok:
                self.upper_bep_absorbed = True
                self.sweep_atm_pe_low = round(pe_hist[-1].low - 2.0, 2)
                lvl = bep_upper if up_bep else wzr_upper
                tag = "Sweep" if spot_c.high >= lvl else f"Near-Rej (-{lvl - spot_c.high:.1f}p)"
                zone = f"Upper BEP ({bep_upper})" if up_bep else f"Upper WZR ({wzr_upper:.0f})"
                self.trader.log_event("ABSORB", f"3m {zone} {tag} ({spot_c.high:.1f}->{spot_c.close:.1f}) | Vol {vol_ratio}x")

        dn_bep = (spot_c.low <= bep_lower + buf) and (spot_c.close > bep_lower) and bull_rej
        dn_wzr = (not dn_bep) and hm >= "09:30" and (spot_c.low <= wzr_lower + buf) and (spot_c.close > wzr_lower) and bull_rej
        if dn_bep or dn_wzr:
            ce_hist = list(self.hist_candles["ATM_CE"])
            req_mult = mult if dn_bep else wzr_mult
            vol_ok, vol_ratio = False, 0.0
            if len(ce_hist) >= 3:
                last_v = ce_hist[-1].volume
                avg_v = sum(x.volume for x in ce_hist[-6:-1]) / max(1, len(ce_hist[-6:-1]))
                if avg_v > 0:
                    vol_ratio = round(last_v / avg_v, 2)
                    vol_ok = vol_ratio >= req_mult
            if vol_ok:
                self.lower_bep_absorbed = True
                self.sweep_atm_ce_low = round(ce_hist[-1].low - 2.0, 2)
                lvl = bep_lower if dn_bep else wzr_lower
                tag = "Sweep" if spot_c.low <= lvl else f"Near-Rej (+{spot_c.low - lvl:.1f}p)"
                zone = f"Lower BEP ({bep_lower})" if dn_bep else f"Lower WZR ({wzr_lower:.0f})"
                self.trader.log_event("ABSORB", f"3m {zone} {tag} ({spot_c.low:.1f}->{spot_c.close:.1f}) | Vol {vol_ratio}x")

    def evaluate_strategy(self):
        spot = self.prices.get("NIFTY_SPOT", 0.0)
        atm_ce = self.prices.get("ATM_CE", 0.0)
        atm_pe = self.prices.get("ATM_PE", 0.0)
        erz_ce = self.prices.get("ERZ_CE", 0.0)
        erz_pe = self.prices.get("ERZ_PE", 0.0)
        if min(spot, atm_ce, atm_pe, erz_ce, erz_pe) <= 0:
            return

        atm_strike = float(self.levels["atm_strike"])
        live_fut = round(atm_strike + (atm_ce - atm_pe), 2)
        erz_ce_iv = round(max(10.0, live_fut - float(self.levels["erz_ce_strike"])), 2)
        erz_pe_iv = round(max(10.0, float(self.levels["erz_pe_strike"]) - live_fut), 2)
        self.levels["synthetic_future"] = live_fut
        self.levels["erz_ce_iv"] = erz_ce_iv
        self.levels["erz_pe_iv"] = erz_pe_iv
        erz_width = float(self.cfg.get("erz_offset", 100)) * 2.0
        straddle_mid = round((atm_ce + atm_pe) / 2.0, 2)
        self.levels["straddle_mid"] = straddle_mid
        qty = int(self.cfg.get("lot_size", 65)) * int(self.cfg.get("lots", 1))
        use_abs = bool(self.cfg.get("use_absorption_filter", True))
        use_dyn_sl = bool(self.cfg.get("use_dynamic_sweep_sl", True))

        ep_hist = list(self.hist_candles["ERZ_PE"])[-5:]
        ec_hist = list(self.hist_candles["ERZ_CE"])[-5:]
        pe_ref = round(sum(x.close for x in ep_hist) / len(ep_hist), 2) if len(ep_hist) >= 2 else erz_pe_iv
        ce_ref = round(sum(x.close for x in ec_hist) / len(ec_hist), 2) if len(ec_hist) >= 2 else erz_ce_iv
        pe_brk = max(pe_ref, ep_hist[-1].high) if ep_hist else erz_pe_iv
        ce_brk = max(ce_ref, ec_hist[-1].high) if ec_hist else erz_ce_iv

        if erz_pe < pe_ref and self.pe_state == "IDLE":
            self.pe_state = "ARMED_OTM"
            self.trader.log_event("ARM", f"ERZ PE ({self.levels['erz_pe_strike']} PE) compressed (< {pe_ref})")
            if self.notifier:
                self.notifier.alert_armed("PE", self.levels["erz_pe_strike"], erz_pe, pe_ref, spot, self.levels)
        elif self.pe_state == "ARMED_OTM" and erz_pe > pe_brk:
            abs_ok = (not use_abs) or self.upper_bep_absorbed
            if abs_ok and atm_pe >= straddle_mid:
                self.pe_state = "TRIGGERED"
                self.upper_bep_absorbed = False
                self.active_signal = "BUY PE"
                self.trader.log_event("SIGNAL", f"BUY PE TRIGGERED! ERZ PE slingshot ({erz_pe:.2f} > {pe_brk:.2f})")
                in_win = self._in_trade_window()
                atm_leg = self.levels["instruments"]["ATM_PE"]
                custom_sl = self.sweep_atm_pe_low if use_dyn_sl else None
                trade = self.trader.open_trade("BUY PE", "PE", atm_leg["strike"], atm_leg["security_id"], atm_pe, custom_sl=custom_sl) if in_win else None
                sl_p = trade["sl_price"] if trade else round(max(1.0, atm_pe - float(self.cfg.get("sl_points", 25.0))), 2)
                tgt_p = trade["target_price"] if trade else round(atm_pe + float(self.cfg.get("target_points", 60.0)), 2)
                if self.notifier:
                    self.notifier.alert_signal("BUY PE", atm_leg["strike"], "PE", atm_pe, sl_p, tgt_p, qty, self.levels["erz_pe_strike"], erz_pe, pe_brk, spot, bool(trade))

        if erz_ce < ce_ref and self.ce_state == "IDLE":
            self.ce_state = "ARMED_OTM"
            self.trader.log_event("ARM", f"ERZ CE ({self.levels['erz_ce_strike']} CE) compressed (< {ce_ref})")
            if self.notifier:
                self.notifier.alert_armed("CE", self.levels["erz_ce_strike"], erz_ce, ce_ref, spot, self.levels)
        elif self.ce_state == "ARMED_OTM" and erz_ce > ce_brk:
            abs_ok = (not use_abs) or self.lower_bep_absorbed
            if abs_ok and atm_ce >= straddle_mid:
                self.ce_state = "TRIGGERED"
                self.lower_bep_absorbed = False
                self.active_signal = "BUY CE"
                self.trader.log_event("SIGNAL", f"BUY CE TRIGGERED! ERZ CE slingshot ({erz_ce:.2f} > {ce_brk:.2f})")
                in_win = self._in_trade_window()
                atm_leg = self.levels["instruments"]["ATM_CE"]
                custom_sl = self.sweep_atm_ce_low if use_dyn_sl else None
                trade = self.trader.open_trade("BUY CE", "CE", atm_leg["strike"], atm_leg["security_id"], atm_ce, custom_sl=custom_sl) if in_win else None
                sl_p = trade["sl_price"] if trade else round(max(1.0, atm_ce - float(self.cfg.get("sl_points", 25.0))), 2)
                tgt_p = trade["target_price"] if trade else round(atm_ce + float(self.cfg.get("target_points", 60.0)), 2)
                if self.notifier:
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
        merged = {}
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
            r = requests.post("https://api.dhan.co/v2/charts/intraday", headers=headers, json=payload, timeout=10)
            if r.status_code == 200:
                d = r.json()
                ts_list = d.get("timestamp") or []
                for i in range(len(ts_list)):
                    t = int(ts_list[i]) + 19800
                    merged[t] = {
                        "time": t,
                        "open": float(d["open"][i]),
                        "high": float(d["high"][i]),
                        "low": float(d["low"][i]),
                        "close": float(d["close"][i]),
                    }
        except Exception:
            pass

        for c in list(self.hist_candles.get(leg_key, [])):
            merged[c.time] = c.to_dict()
        cur = self.cur_candles.get(leg_key)
        if cur:
            merged[cur.time] = cur.to_dict()

        return [merged[k] for k in sorted(merged.keys())]

    async def run(self):
        self._running = True
        ws_url = f"wss://api-feed.dhan.co?version=2&token={self.cfg['dhan_access_token']}&clientId={self.cfg['dhan_client_id']}&authType=2"
        sub_list = [{"ExchangeSegment": info["segment"], "SecurityId": info["security_id"]} for info in self.levels["instruments"].values()]
        sub_msg = json.dumps({"RequestCode": 17, "InstrumentCount": len(sub_list), "InstrumentList": sub_list})

        while self._running:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    self.ws_connected = True
                    await ws.send(sub_msg)
                    self.trader.log_event("SYS", "DhanHQ v2 Quote WebSocket connected (3m Absorption Filter ON)")
                    await asyncio.to_thread(self._poll_rest_fallback)
                    while self._running:
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
