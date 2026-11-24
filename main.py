import asyncio
import uvicorn
from levels_engine import load_config, get_today_levels
from paper_trader import PaperTrader
from ws_signal_engine import SignalEngine
from dashboard_server import create_app
from telegram_notifier import TelegramNotifier


async def main():
    cfg = load_config("config.json")
    levels = get_today_levels(cfg)
    notifier = TelegramNotifier(cfg)
    trader = PaperTrader(cfg, notifier=notifier)

    startup_msg = (
        f"Loaded ATM {levels['atm_strike']} (Gap {levels['strike_gap']}) | "
        f"BEP {levels['bep_lower']}–{levels['bep_upper']}"
    )
    trader.log_event("SYS", startup_msg)
    notifier.send_async(
        f"🚀 <b>OTT BEP Engine Started</b>\n"
        f"<b>ATM Strike:</b> {levels['atm_strike']} (Gap: {levels['strike_gap']} pts)\n"
        f"<b>Synth Future:</b> {levels['synthetic_future']}\n"
        f"<b>BEP Band:</b> {levels['bep_lower']} – {levels['bep_upper']}\n"
        f"<b>ERZ IVs:</b> {levels['erz_ce_strike']} CE ₹{levels['erz_ce_iv']} | "
        f"{levels['erz_pe_strike']} PE ₹{levels['erz_pe_iv']}"
    )

    engine = SignalEngine(cfg, levels, trader, notifier=notifier)
    app = create_app(cfg, engine)

    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="0.0.0.0",
            port=int(cfg.get("port", 7740)),
            log_level="warning",
            timeout_graceful_shutdown=2,
        )
    )

    engine_task = asyncio.create_task(engine.run())
    try:
        await server.serve()
    finally:
        engine._running = False
        engine_task.cancel()
        await asyncio.gather(engine_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
