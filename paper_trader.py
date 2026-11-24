import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")


class PaperTrader:
    def __init__(self, cfg: dict, notifier=None):
        self.cfg = cfg
        self.db_path = cfg["db_path"]
        self.notifier = notifier

    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def log_event(self, level: str, message: str):
        conn = self._conn()
        conn.execute(
            "INSERT INTO signal_logs (ts, level, message) VALUES (?, ?, ?)",
            (datetime.now(IST).strftime("%H:%M:%S"), level, message),
        )
        conn.commit()
        conn.close()

    def get_open_trade(self):
        today = datetime.now(IST).strftime("%Y-%m-%d")
        conn = self._conn()
        row = conn.execute(
            "SELECT * FROM paper_trades WHERE trade_date = ? AND status = 'OPEN' ORDER BY id DESC LIMIT 1",
            (today,),
        ).fetchone()
        conn.close()
        return dict(row) if row else None

    def get_today_trades(self):
        today = datetime.now(IST).strftime("%Y-%m-%d")
        conn = self._conn()
        rows = conn.execute(
            "SELECT * FROM paper_trades WHERE trade_date = ? ORDER BY id DESC",
            (today,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def get_recent_logs(self, limit: int = 30):
        conn = self._conn()
        rows = conn.execute(
            "SELECT ts, level, message FROM signal_logs ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def open_trade(self, signal_type: str, option_type: str, strike: int, security_id: str, ltp: float, custom_sl: float = None):
        if ltp <= 0 or self.get_open_trade():
            return None
        if len(self.get_today_trades()) >= int(self.cfg.get("max_trades_per_day", 2)):
            return None

        qty = int(self.cfg.get("lot_size", 65)) * int(self.cfg.get("lots", 1))
        default_sl = round(max(1.0, ltp - float(self.cfg.get("sl_points", 25.0))), 2)
        if custom_sl is not None and custom_sl > 0:
            max_risk = float(self.cfg.get("max_sl_points", 35.0))
            min_allowed_sl = round(max(1.0, ltp - max_risk), 2)
            sl_price = round(max(min_allowed_sl, min(ltp - 5.0, custom_sl)), 2)
        else:
            sl_price = default_sl

        target_price = round(ltp + float(self.cfg.get("target_points", 60.0)), 2)
        now_str = datetime.now(IST).strftime("%H:%M:%S")
        today = datetime.now(IST).strftime("%Y-%m-%d")

        conn = self._conn()
        conn.execute(
            "INSERT INTO paper_trades (trade_date, signal_type, option_type, strike, security_id, qty, entry_time, entry_price, sl_price, target_price, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')",
            (today, signal_type, option_type, strike, security_id, qty, now_str, ltp, sl_price, target_price),
        )
        conn.commit()
        conn.close()

        self.log_event("TRADE", f"PAPER BUY {strike} {option_type} @ {ltp:.2f} | SL {sl_price:.2f} | TGT {target_price:.2f}")
        return self.get_open_trade()

    def on_tick(self, prices: dict):
        trade = self.get_open_trade()
        if not trade:
            return
        ltp = prices.get(f"ATM_{trade['option_type']}", 0.0)
        if ltp <= 0:
            return
        if datetime.now(IST).strftime("%H:%M") >= self.cfg.get("squareoff_time", "15:15"):
            self.close_trade(trade["id"], ltp, "EOD_SQUAREOFF")
            return

        be_trig = float(self.cfg.get("breakeven_trigger_points", 15.0))
        if not trade["breakeven_moved"] and (ltp - trade["entry_price"]) >= be_trig:
            conn = self._conn()
            conn.execute("UPDATE paper_trades SET sl_price = ?, breakeven_moved = 1 WHERE id = ?", (trade["entry_price"], trade["id"]))
            conn.commit()
            conn.close()
            trade["sl_price"] = trade["entry_price"]
            self.log_event("RISK", f"Moved SL to Breakeven ({trade['entry_price']:.2f}) on {trade['strike']} {trade['option_type']}")
            if self.notifier:
                self.notifier.send_async(f"🛡️ <b>SL Moved to Breakeven</b>\nNIFTY {trade['strike']} {trade['option_type']} SL locked at ₹{trade['entry_price']:.2f}")

        if ltp <= trade["sl_price"]:
            self.close_trade(trade["id"], ltp, "SL_HIT")
        elif ltp >= trade["target_price"]:
            self.close_trade(trade["id"], ltp, "TARGET_HIT")

    def close_trade(self, trade_id: int, exit_price: float, reason: str):
        conn = self._conn()
        row = conn.execute("SELECT * FROM paper_trades WHERE id = ?", (trade_id,)).fetchone()
        if not row or row["status"] != "OPEN":
            conn.close()
            return
        pnl = round((exit_price - row["entry_price"]) * row["qty"], 2)
        now_str = datetime.now(IST).strftime("%H:%M:%S")
        conn.execute("UPDATE paper_trades SET status = 'CLOSED', exit_time = ?, exit_price = ?, exit_reason = ?, pnl = ? WHERE id = ?", (now_str, exit_price, reason, pnl, trade_id))
        conn.commit()
        conn.close()
        self.log_event("EXIT", f"CLOSED {row['strike']} {row['option_type']} @ {exit_price:.2f} ({reason}) | PnL: ₹{pnl:+.2f}")
        if self.notifier:
            self.notifier.alert_exit(row["strike"], row["option_type"], row["entry_price"], exit_price, row["qty"], reason, pnl)
