import threading
from datetime import datetime
from zoneinfo import ZoneInfo
import requests

IST = ZoneInfo("Asia/Kolkata")


class TelegramNotifier:
    def __init__(self, cfg: dict):
        tg = cfg.get("telegram") or {}
        self.enabled = bool(tg.get("enabled", False))
        self.bot_token = str(tg.get("bot_token", "")).strip()
        self.chat_id = str(tg.get("chat_id", "")).strip()

    def _post(self, text: str):
        if not (self.enabled and self.bot_token and self.chat_id):
            return
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        try:
            requests.post(url, json=payload, timeout=8)
        except Exception:
            pass

    def send_async(self, text: str):
        if not (self.enabled and self.bot_token and self.chat_id):
            return
        threading.Thread(target=self._post, args=(text,), daemon=True).start()

    def alert_armed(self, side: str, strike: int, ltp: float, iv: float, spot: float, levels: dict):
        now_str = datetime.now(IST).strftime("%H:%M:%S")
        direction = "Bearish Watch (PE)" if side == "PE" else "Bullish Watch (CE)"
        msg = (
            f"🟡 <b>OTT BEP — ERZ ARMED IN OTM</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Setup:</b> {direction}\n"
            f"<b>Trigger Leg:</b> {strike} {side}\n"
            f"<b>Current LTP:</b> ₹{ltp:.2f} (IV: ₹{iv:.2f})\n"
            f"<b>NIFTY Spot:</b> {spot:.2f}\n"
            f"<b>BEP Band:</b> {levels['bep_lower']} – {levels['bep_upper']}\n"
            f"<b>Next Action:</b> Waiting for reclaim above ₹{iv:.2f}\n"
            f"🕒 <i>{now_str} IST</i>"
        )
        self.send_async(msg)

    def alert_signal(
        self,
        signal_type: str,
        atm_strike: int,
        option_type: str,
        entry_price: float,
        sl_price: float,
        target_price: float,
        qty: int,
        erz_strike: int,
        erz_ltp: float,
        erz_iv: float,
        spot: float,
        in_window: bool,
    ):
        now_str = datetime.now(IST).strftime("%H:%M:%S")
        icon = "🟢" if option_type == "CE" else "🔴"
        exec_note = (
            f"✅ <b>PAPER ORDER EXECUTED</b> (Qty: {qty})"
            if in_window
            else "⏸ <b>OUTSIDE TRADE WINDOW</b> (Alert Only)"
        )
        msg = (
            f"{icon} <b>OTT BEP — {signal_type} SIGNAL!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Order Strike:</b> NIFTY {atm_strike} {option_type}\n"
            f"<b>Entry Price:</b> ₹{entry_price:.2f}\n"
            f"<b>Stop-Loss:</b> ₹{sl_price:.2f}\n"
            f"<b>Target:</b> ₹{target_price:.2f}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Slingshot Proof:</b> {erz_strike} {option_type} reclaimed ₹{erz_ltp:.2f} (&gt; IV ₹{erz_iv:.2f})\n"
            f"<b>NIFTY Spot:</b> {spot:.2f}\n"
            f"{exec_note}\n"
            f"🕒 <i>{now_str} IST</i>"
        )
        self.send_async(msg)

    def alert_exit(self, strike: int, option_type: str, entry: float, exit_p: float, qty: int, reason: str, pnl: float):
        now_str = datetime.now(IST).strftime("%H:%M:%S")
        icon = "💰" if pnl >= 0 else "🛑"
        msg = (
            f"{icon} <b>OTT BEP — PAPER TRADE CLOSED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Contract:</b> NIFTY {strike} {option_type} (Qty: {qty})\n"
            f"<b>Entry → Exit:</b> ₹{entry:.2f} → ₹{exit_p:.2f}\n"
            f"<b>Reason:</b> {reason}\n"
            f"<b>Net P&amp;L:</b> ₹{pnl:+.2f}\n"
            f"🕒 <i>{now_str} IST</i>"
        )
        self.send_async(msg)
