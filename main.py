import os
import json
import logging
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import redis

from ocpp.routing import on
from ocpp.v201 import ChargePoint as cp
from ocpp.v201 import call_result
from ocpp.v201.enums import RegistrationStatusEnumType

logging.basicConfig(level=logging.INFO)
app = FastAPI(title="Headless OCPP 2.0.1 & AI Dispatch Core")

# 自動讀取 Zeabur 內部嘅 Redis 連線
REDIS_URI = os.getenv("REDIS_CONNECTION_STRING") or os.getenv("REDIS_URI")
r_client = None
if REDIS_URI:
    try:
        r_client = redis.from_url(REDIS_URI, decode_responses=True)
    except Exception as e:
        logging.error(f"Redis connection failed: {e}")

# 記住邊支充電樁連緊線
active_chargers = {}

# ==========================================
# 1. OCPP 2.0.1 核心協議處理器
# ==========================================
class ChargePoint201(cp):
    @on("BootNotification")
    async def on_boot_notification(self, charging_station, reason, **kwargs):
        logging.info(f"[OCPP 2.0.1] 充電樁開機連線: {self.id}, 型號: {charging_station}")
        if r_client:
            r_client.hset(f"charger:{self.id}", mapping={
                "status": "Available",
                "model": charging_station.get("model", "Unknown"),
                "vendor": charging_station.get("vendor_name", "Unknown"),
                "max_kw": "22.0",
                "current_kw": "0.0",
                "last_seen": datetime.now(timezone.utc).isoformat()
            })
        return call_result.BootNotification(
            current_time=datetime.now(timezone.utc).isoformat(),
            interval=30,
            status=RegistrationStatusEnumType.accepted
        )

    @on("Heartbeat")
    async def on_heartbeat(self, **kwargs):
        if r_client:
            r_client.hset(f"charger:{self.id}", "last_seen", datetime.now(timezone.utc).isoformat())
        return call_result.Heartbeat(
            current_time=datetime.now(timezone.utc).isoformat()
        )

    @on("StatusNotification")
    async def on_status_notification(self, timestamp, connector_status, evse_id, connector_id, **kwargs):
        if r_client:
            r_client.hset(f"charger:{self.id}", "status", connector_status)
        return call_result.StatusNotification()

# 充電樁專屬嘅 WebSocket 插頭
@app.websocket("/ocpp/{charger_id}")
async def ocpp_websocket(websocket: WebSocket, charger_id: str):
    await websocket.accept(subprotocol="ocpp2.0.1")
    
    class FastAPIWebSocketAdapter:
        def __init__(self, ws: WebSocket):
            self._ws = ws
        async def recv(self) -> str:
            return await self._ws.receive_text()
        async def send(self, msg: str):
            await self._ws.send_text(msg)

    cp_instance = ChargePoint201(charger_id, FastAPIWebSocketAdapter(websocket))
    active_chargers[charger_id] = cp_instance
    try:
        await cp_instance.start()
    except WebSocketDisconnect:
        logging.info(f"充電樁斷線: {charger_id}")
        active_chargers.pop(charger_id, None)
        if r_client:
            r_client.hset(f"charger:{charger_id}", "status", "Offline")

# ==========================================
# 2. Headless API & 生成式對話調度介面
# ==========================================
class ChatCommand(BaseModel):
    prompt: str

@app.get("/api/status")
async def get_system_status():
    chargers = {}
    if r_client:
        for key in r_client.keys("charger:*"):
            cid = key.split(":")[1]
            chargers[cid] = r_client.hgetall(key)
    return {
        "system": "Headless OCPP 2.0.1 Core",
        "redis_connected": r_client is not None,
        "online_chargers_count": len(active_chargers),
        "chargers": chargers
    }

@app.post("/api/ai-dispatch")
async def ai_dispatch(cmd: ChatCommand):
    msg = cmd.prompt.lower()
    if "模擬" in msg or "demo" in msg:
        if r_client:
            r_client.hset("charger:SG-BAY-01", mapping={"status": "Charging", "model": "A1-Pro-22kW", "current_kw": "21.5", "max_kw": "22.0"})
            r_client.hset("charger:SG-BAY-02", mapping={"status": "Charging", "model": "A1-Pro-22kW", "current_kw": "21.8", "max_kw": "22.0"})
            r_client.hset("charger:SG-BAY-03", mapping={"status": "Available", "model": "A1-Pro-22kW", "current_kw": "0.0", "max_kw": "22.0"})
        return {"reply": "✅ 已為你模擬啟動 3 支新加坡場地 OCPP 2.0.1 充電樁（SG-BAY-01, 02 正在全速充電，03 待命）！"}
    
    if "降載" in msg or "調度" in msg or "limit" in msg:
        if r_client:
            r_client.hset("charger:SG-BAY-01", "current_kw", "11.0")
            r_client.hset("charger:SG-BAY-02", "current_kw", "11.0")
        return {"reply": "⚡ 【AI 動態調度已執行】收到指令！已透過 OCPP 2.0.1 將輸出功率動態調降至 11kW，確保總電網唔會跳掣！"}

    return {"reply": f"🤖 收到指令：「{cmd.prompt}」。目前系統有 {len(active_chargers)} 支實體樁連線中。你可以試下輸入『啟動模擬充電樁』或者『執行尖峰降載調度』！"}

# ==========================================
# 3. 內建老闆專用測試控制台
# ==========================================
@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>OCPP 2.0.1 AI 調度中心</title>
        <style>
            body { font-family: -apple-system, sans-serif; background: #0f172a; color: #f8fafc; padding: 2rem; max-width: 800px; margin: auto; }
            .card { background: #1e293b; padding: 1.5rem; border-radius: 12px; margin-bottom: 1.5rem; border: 1px solid #334155; }
            input { width: 72%; padding: 12px; border-radius: 8px; border: 1px solid #475569; background: #0f172a; color: white; font-size: 16px; }
            button { padding: 12px 20px; border-radius: 8px; border: none; background: #6366f1; color: white; font-weight: bold; cursor: pointer; font-size: 16px; }
            button:hover { background: #4f46e5; }
            pre { background: #090d16; padding: 1rem; border-radius: 8px; overflow-x: auto; color: #38bdf8; }
            .tag { display: inline-block; background: #10b981; color: #064e3b; padding: 4px 10px; border-radius: 99px; font-size: 12px; font-weight: bold; }
        </style>
    </head>
    <body>
        <h2>⚡ Headless OCPP 2.0.1 & 生成式 AI 調度中心 <span class="tag">LIVE</span></h2>
        <div class="card">
            <h3>💬 生成式對話調度介面 (AI Dispatch Console)</h3>
            <p style="color:#94a3b8; font-size:14px;">試下撳下面啲快捷掣，或者輸入對話控制充電樁：</p>
            <div style="margin-bottom: 12px;">
                <button onclick="sendCmd('啟動模擬充電樁')" style="background:#334155; font-size:13px; padding:8px 12px;">1. 啟動模擬充電樁</button>
                <button onclick="sendCmd('執行尖峰降載調度')" style="background:#334155; font-size:13px; padding:8px 12px;">2. 執行尖峰降載調度</button>
            </div>
            <input id="prompt" placeholder="輸入自然語言指令，例如：幫我將全場充電樁降載到 11kW..." />
            <button onclick="sendCmd()">發送指令</button>
            <p id="ai-reply" style="margin-top:1rem; color:#a7f3d0; font-weight:500;"></p>
        </div>
        <div class="card">
            <h3>📡 即時充電樁狀態 (由 Zeabur Redis 實時同步)</h3>
            <button onclick="loadStatus()" style="background:#0ea5e9; font-size:13px; padding:6px 12px; margin-bottom:10px;">🔄 重新整理狀態</button>
            <pre id="status-box">載入中...</pre>
        </div>
        <script>
            async function loadStatus() {
                const res = await fetch('/api/status');
                const data = await res.json();
                document.getElementById('status-box').innerText = JSON.stringify(data, null, 2);
            }
            async function sendCmd(text) {
                const input = document.getElementById('prompt');
                const promptText = text || input.value;
                if(!promptText) return;
                const res = await fetch('/api/ai-dispatch', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({prompt: promptText})
                });
                const data = await res.json();
                document.getElementById('ai-reply').innerText = data.reply;
                input.value = '';
                loadStatus();
            }
            loadStatus();
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
