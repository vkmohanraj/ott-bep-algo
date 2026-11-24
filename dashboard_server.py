import asyncio
import json
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from levels_engine import compute_and_store_levels

templates = Jinja2Templates(directory="templates")


def create_app(cfg: dict, engine) -> FastAPI:
    app = FastAPI(title="OTT BEP NIFTY Paper Trader")

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request):
        return templates.TemplateResponse("index.html", {"request": request})

    @app.get("/api/stream")
    async def stream():
        async def event_gen():
            while True:
                payload = json.dumps(engine.get_snapshot())
                yield f"data: {payload}\n\n"
                await asyncio.sleep(1)

        return StreamingResponse(event_gen(), media_type="text/event-stream")

    @app.get("/api/candles/{leg_key}")
    async def get_candles(leg_key: str):
        bars = await asyncio.to_thread(engine.fetch_intraday_candles, leg_key)
        return JSONResponse({"leg": leg_key, "bars": bars})

    @app.post("/api/recalc")
    async def recalc_levels():
        new_levels = await asyncio.to_thread(compute_and_store_levels, cfg)
        engine.levels = new_levels
        engine.sec_to_key = {v["security_id"]: k for k, v in new_levels["instruments"].items()}
        engine.ce_state = "IDLE"
        engine.pe_state = "IDLE"
        engine.active_signal = "WAITING"
        engine.upper_bep_absorbed = False
        engine.lower_bep_absorbed = False
        engine.trader.log_event("SYS", f"Levels recalculated | ATM {new_levels['atm_strike']} (Gap {new_levels['strike_gap']})")
        return JSONResponse({"ok": True, "atm": new_levels["atm_strike"]})

    @app.post("/api/squareoff")
    async def manual_squareoff():
        trade = engine.trader.get_open_trade()
        if not trade:
            return JSONResponse({"ok": False, "msg": "No open trade"})
        ltp = engine.prices.get(f"ATM_{trade['option_type']}", trade["entry_price"])
        engine.trader.close_trade(trade["id"], ltp, "MANUAL_EXIT")
        return JSONResponse({"ok": True})

    @app.post("/api/reset_states")
    async def reset_states():
        engine.ce_state = "IDLE"
        engine.pe_state = "IDLE"
        engine.active_signal = "WAITING"
        engine.upper_bep_absorbed = False
        engine.lower_bep_absorbed = False
        engine.trader.log_event("SYS", "Signal state machine reset to IDLE")
        return JSONResponse({"ok": True})

    return app
