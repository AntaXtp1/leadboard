import asyncio
import hashlib
import json
import logging
import time
from typing import Set

import httpx
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

EVENT_ID = "50"
WS_URL = "wss://livetiming.azurewebsites.net/"
PAGE_URL = "https://livetiming.azurewebsites.net/event=50?config=w3"

# ── Mapping kode TRACKSTATE → label ─────────────────────────────────────────
TRACKSTATE_MAP = {
    "0": "Green",
    "1": "Yellow",
    "2": "SC",
    "3": "Red",
    "4": "FCY",
    "5": "Finish",
}

# ── Shared state ────────────────────────────────────────────────────────────
state = {
    "connected": False,
    "race_info": {
        "cup":         "54. ADAC RAVENOL 24h Nürburgring",
        "heat":        "Race",
        "track":       "Nürburgring",
        "track_state": "Green",
    },
    "end_time_ms":   0,
    "server_time_ms": 0,
    "positions":     [],
    "all_positions": [],
    "total_cars":    0,
    "raw_pids":      {},
    "last_update_ms": 0,
}

clients: Set[WebSocket] = set()

# ── State change tracking ───────────────────────────────────────────────────
_last_hash = ""


def _state_signature() -> str:
    """Hash dari bagian state yang relevan — buat deteksi perubahan."""
    payload = {
        "connected":   state["connected"],
        "race_info":   state["race_info"],
        "end_time_ms": state["end_time_ms"],
        "positions":   state["positions"],
        "total_cars":  state["total_cars"],
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.md5(raw.encode()).hexdigest()


def _build_payload() -> str:
    state["server_time_ms"] = int(time.time() * 1000)
    return json.dumps(state, default=str)


# ── Helpers ─────────────────────────────────────────────────────────────────
def map_trackstate(code) -> str:
    return TRACKSTATE_MAP.get(str(code), str(code))


async def maybe_broadcast(force: bool = False):
    """Broadcast hanya saat state berubah (atau force=True)."""
    global _last_hash
    sig = _state_signature()
    if not force and sig == _last_hash:
        return
    _last_hash = sig
    state["last_update_ms"] = int(time.time() * 1000)

    if not clients:
        return
    msg = _build_payload()
    dead = set()
    for c in clients:
        try:
            await c.send_text(msg)
        except Exception:
            dead.add(c)
    clients.difference_update(dead)


# ── Message parser ──────────────────────────────────────────────────────────
def process_message(data: dict):
    pid = str(data.get("PID", ""))

    # Simpan paket mentah utk debug, truncate RESULT biar nggak gede
    snap = dict(data)
    if isinstance(snap.get("RESULT"), list):
        snap["RESULT"] = f"<{len(snap['RESULT'])} cars>"
    state["raw_pids"][pid] = snap

    if pid == "0":
        _parse_pid0(data)
    elif pid == "4":
        _parse_pid4(data)


def _parse_pid0(data: dict):
    """Header race + leaderboard."""
    ri = state["race_info"]
    ri["cup"]   = data.get("CUP",        ri["cup"])
    ri["heat"]  = data.get("HEAT",       ri["heat"])
    ri["track"] = data.get("TRACKNAME",  ri["track"])
    if "TRACKSTATE" in data:
        ri["track_state"] = map_trackstate(data["TRACKSTATE"])

    result = data.get("RESULT")
    if isinstance(result, list) and result:
        positions = []
        for e in result:
            if not isinstance(e, dict):
                continue
            positions.append({
                "pos":      e.get("POSITION", "?"),
                "no":       e.get("STNR", "?"),
                "name":     e.get("NAME", ""),
                "team":     e.get("TEAM", ""),
                "car":      e.get("CAR", ""),
                "cls":      e.get("CLASSNAME", ""),
                "pro":      e.get("PRO", ""),
                "laps":     e.get("LAPS", "0"),
                "gap":      e.get("GAP", ""),
                "interval": e.get("INT", ""),
                "last":     e.get("LASTLAPTIME", ""),
                "fastest":  e.get("FASTESTLAP", ""),
                "pits":     e.get("PITSTOPCOUNT", ""),
                "chg":      e.get("CHG", "0"),
            })

        def pos_key(x):
            try:
                return int(x["pos"])
            except Exception:
                return 999
        positions.sort(key=pos_key)

        state["all_positions"] = positions
        state["positions"]     = positions[:10]
        state["total_cars"]    = len(positions)


def _parse_pid4(data: dict):
    """Track state + race end time."""
    if "TRACKSTATE" in data:
        state["race_info"]["track_state"] = map_trackstate(data["TRACKSTATE"])

    if "ENDTIME" in data:
        try:
            state["end_time_ms"] = int(data["ENDTIME"])
        except Exception:
            pass


# ── Live timing worker ──────────────────────────────────────────────────────
async def livetiming_worker():
    while True:
        try:
            logger.info("Fetching session cookies from live timing page...")
            async with httpx.AsyncClient(follow_redirects=True, timeout=15) as http:
                resp = await http.get(PAGE_URL)
                cookies = dict(resp.cookies)
                logger.info(f"Cookies received: {list(cookies.keys())}")

            cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
            if not cookie_str:
                cookie_str = "x-ms-routing-name=self"

            headers = {
                "Cookie":        cookie_str,
                "Origin":        "https://livetiming.azurewebsites.net",
                "User-Agent":    (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/148.0.0.0 Safari/537.36"
                ),
                "Cache-Control": "no-cache",
                "Pragma":        "no-cache",
            }

            logger.info(f"Connecting WebSocket → {WS_URL}")
            async with websockets.connect(
                WS_URL,
                extra_headers=headers,
                ping_interval=25,
                ping_timeout=10,
                max_size=10 * 1024 * 1024,
            ) as ws:
                state["connected"] = True
                logger.info("WS connected ✓  sending handshake...")

                handshake = {
                    "eventId":         EVENT_ID,
                    "eventPid":        [0, 4],
                    "clientLocalTime": int(time.time() * 1000),
                }
                await ws.send(json.dumps(handshake))
                await maybe_broadcast(force=True)

                async for raw in ws:
                    try:
                        data = json.loads(raw)
                        if isinstance(data, dict) and "PID" in data:
                            process_message(data)
                        elif isinstance(data, dict):
                            for v in data.values():
                                if isinstance(v, dict) and "PID" in v:
                                    process_message(v)
                        # broadcast cuma kalau ada perubahan signifikan
                        await maybe_broadcast()
                    except json.JSONDecodeError:
                        logger.debug(f"Non-JSON frame: {raw[:120]}")
                    except Exception as exc:
                        logger.error(f"Process error: {exc}")

        except Exception as exc:
            state["connected"] = False
            logger.error(f"WS error: {exc} — retrying in 5 s")
            await maybe_broadcast(force=True)
            await asyncio.sleep(5)


# ── FastAPI routes ──────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    asyncio.create_task(livetiming_worker())


@app.get("/")
async def index():
    with open("index.html", encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/api/state")
async def api_state():
    state["server_time_ms"] = int(time.time() * 1000)
    return state


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)
    logger.info(f"Frontend client connected ({len(clients)} total)")
    try:
        await websocket.send_text(_build_payload())   # snapshot awal
        while True:
            await websocket.receive_text()            # keep-alive
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        clients.discard(websocket)
        logger.info(f"Frontend client disconnected ({len(clients)} remaining)")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="info")
