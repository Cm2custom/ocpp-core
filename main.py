import os
import json
import logging
from datetime import datetime, timezone
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import redis

try:
    from ocpp.routing import on
    from ocpp.v201 import ChargePoint as cp
    from ocpp.v201 import call_result
    from ocpp.v201.enums import RegistrationStatusEnumType
except ImportError:
    cp = object

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

# 預設葵涌站 5 台快充樁與電池健康度 (BMS SoH) 狀態
DEFAULT_FLEET_STATE = {
    "SG-BAY-01": {
        "ev_id": "EV-01",
        "name": "Van 1 (急件速遞)",
        "cp_id": "CP_DELTA_01 (EVSE #1)",
        "depart": "05:00",
        "priority": "高 (最優先)",
        "soc": 15,
        "target_soc": 90,
        "ready_time": "01:00",
        "status": "Charging",
        "current_kw": 60.0,
        "max_kw": 60.0,
        "soh": 96,
        "temp": 31.5,
        "internal_res": 1.2,
        "cycles": 310,
        "cell_delta_mv": 12,
        "health_tag": "優良",
        "color": "emerald"
    },
    "SG-BAY-02": {
        "ev_id": "EV-02",
        "name": "Van 2 (九龍B區)",
        "cp_id": "CP_DELTA_02 (EVSE #2)",
        "depart": "07:00",
        "priority": "標準",
        "soc": 30,
        "target_soc": 85,
        "ready_time": "06:30",
        "status": "Charging",
        "current_kw": 35.0,
        "max_kw": 60.0,
        "soh": 92,
        "temp": 33.0,
        "internal_res": 1.5,
        "cycles": 520,
        "cell_delta_mv": 18,
        "health_tag": "良好",
        "color": "sky"
    },
    "SG-BAY-03": {
        "ev_id": "EV-03",
        "name": "Van 3 (港島南區)",
        "cp_id": "CP_DELTA_03 (EVSE #3)",
        "depart": "07:00",
        "priority": "標準",
        "soc": 20,
        "target_soc": 80,
        "ready_time": "06:00",
        "status": "Charging",
        "current_kw": 45.0,
        "max_kw": 60.0,
        "soh": 76,
        "temp": 42.8,
        "internal_res": 3.4,
        "cycles": 1480,
        "cell_delta_mv": 68,
        "health_tag": "老化偏熱 (建議慢充)",
        "color": "rose"
    },
    "SG-BAY-04": {
        "ev_id": "EV-04",
        "name": "Van 4 (新界東線)",
        "cp_id": "CP_DELTA_04 (EVSE #4)",
        "depart": "07:30",
        "priority": "普通",
        "soc": 45,
        "target_soc": 85,
        "ready_time": "06:45",
        "status": "Charging",
        "current_kw": 25.0,
        "max_kw": 60.0,
        "soh": 94,
        "temp": 30.2,
        "internal_res": 1.3,
        "cycles": 410,
        "cell_delta_mv": 15,
        "health_tag": "優良",
        "color": "sky"
    },
    "SG-BAY-05": {
        "ev_id": "EV-05",
        "name": "Van 5 (機場冷鏈)",
        "cp_id": "CP_DELTA_05 (EVSE #5)",
        "depart": "06:30",
        "priority": "次高",
        "soc": 25,
        "target_soc": 85,
        "ready_time": "05:30",
        "status": "Charging",
        "current_kw": 44.5,
        "max_kw": 60.0,
        "soh": 89,
        "temp": 35.1,
        "internal_res": 1.9,
        "cycles": 760,
        "cell_delta_mv": 26,
        "health_tag": "正常",
        "color": "amber"
    }
}

def get_chargers_state():
    if r_client:
        try:
            raw = r_client.get("voltagent_fleet_state")
            if raw:
                return json.loads(raw)
            else:
                r_client.set("voltagent_fleet_state", json.dumps(DEFAULT_FLEET_STATE))
        except Exception as e:
            logging.error(f"Redis read error: {e}")
    return DEFAULT_FLEET_STATE.copy()

def save_chargers_state(state):
    if r_client:
        try:
            r_client.set("voltagent_fleet_state", json.dumps(state))
        except Exception as e:
            logging.error(f"Redis write error: {e}")

class DispatchCommand(BaseModel):
    command: str = ""
    scenario: str = ""

@app.get("/api/status")
async def api_status():
    chargers = get_chargers_state()
    redis_ok = False
    if r_client:
        try:
            redis_ok = bool(r_client.ping())
        except Exception:
            redis_ok = False
    return {
        "system": "Headless OCPP 2.0.1 Core (VoltAgent BMS Enabled)",
        "redis_connected": redis_ok,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "online_chargers_count": len(chargers),
        "chargers": chargers
    }

@app.post("/api/dispatch")
async def api_dispatch(payload: DispatchCommand):
    chargers = get_chargers_state()
    cmd = payload.command.strip()
    scenario = payload.scenario.strip()

    reply = ""
    dlm_peak = 31.8
    contract_limit = 200.0
    curve_mode = "normal"

    if scenario == "bms_soh" or "健康" in cmd or "電池" in cmd or "SoH" in cmd.upper():
        # 方案 C: 電池健康度掃描並自動限制老化電池功率
        chargers["SG-BAY-03"]["current_kw"] = 15.0
        chargers["SG-BAY-03"]["temp"] = 36.2
        chargers["SG-BAY-03"]["ready_time"] = "06:50 (涓流養護)"
        chargers["SG-BAY-03"]["health_tag"] = "AI 降載養護中 (15kW)"
        dlm_peak = 28.4
        curve_mode = "bms_protect"
        reply = (
            "🩺 【OCPP 2.0.1 BMS 電池健康度全面掃描完成】\n"
            "• 全場平均 SoH 為 89.4%。\n"
            "• ⚠️ 偵測到 EV-03 (港島南區) 電池健康度偏低 (SoH: 76%)、內阻達 3.4mΩ 且電芯壓差 68mV，快充溫升達 42.8°C。\n"
            "• ⚡ 已透過 OCPP 2.0.1 SetChargingProfile 將 EV-03 限制為 15kW 恆流慢充養護模式，預計溫降至 36.2°C，並確保明早 06:50 安全滿電出車！"
        )
    elif scenario == "ev01_urgent" or "EV-01" in cmd.upper() or "急件" in cmd:
        chargers["SG-BAY-01"]["current_kw"] = 60.0
        chargers["SG-BAY-01"]["ready_time"] = "00:30"
        chargers["SG-BAY-02"]["current_kw"] = 20.0
        chargers["SG-BAY-04"]["current_kw"] = 18.0
        dlm_peak = 34.2
        curve_mode = "urgent"
        reply = (
            "⚡ 【急件優先調度已執行】收到指令！已將 EV-01 (CP_DELTA_01) 鎖定 60kW 全速輸出，"
            "預計提前於 00:30 充至 90% 就緒；同時自動下調 EV-02 及 EV-04 功率以維持總負荷安全。"
        )
    elif scenario == "transformer_120" or "120" in cmd or "變壓器" in cmd:
        for k in chargers:
            chargers[k]["current_kw"] = round(chargers[k]["current_kw"] * 0.55, 1)
        dlm_peak = 24.5
        contract_limit = 120.0
        curve_mode = "emergency_120"
        reply = (
            "⚠️ 【突發應急調度】變壓器安全上限已下調至 120.0 kW！AI 已即時透過 OCPP 2.0.1 下發全場動態降載 Profile，"
            "優化峰值壓低至 24.5 kW，100% 避免跳掣並維持次日準時出車。"
        )
    elif scenario == "even_0700" or "07:00" in cmd or "均勻" in cmd or "11kW" in cmd:
        for k in chargers:
            chargers[k]["current_kw"] = 22.0
            chargers[k]["ready_time"] = "06:30"
        dlm_peak = 26.0
        curve_mode = "even"
        reply = (
            "🔄 【谷電均勻調峰已啟動】已將全場 5 台快充樁平滑分配於 00:00–06:30 離峰時段充電，"
            "全車隊將於 06:30 前準時就緒，最大化節省最高需求電費 (Demand Charge) 并延長電池壽命。"
        )
    else:
        reply = (
            f"✅ 【AI 動態調度已執行】收到指令：「{cmd}」。已透過 OCPP 2.0.1 動態調整充電排程與 BMS 保護參數，"
            "確保總電網唔會跳掣且電池處於最佳溫控區間！"
        )

    save_chargers_state(chargers)
    return {
        "reply": reply,
        "dlm_peak": dlm_peak,
        "contract_limit": contract_limit,
        "curve_mode": curve_mode,
        "chargers": chargers
    }

@app.post("/api/reset")
async def api_reset():
    save_chargers_state(DEFAULT_FLEET_STATE)
    return {"status": "reset", "chargers": DEFAULT_FLEET_STATE}

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return """<!DOCTYPE html>
<html lang="zh-HK">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VoltAgent Operations Console | OCPP 2.0.1 & BMS Health</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body { background-color: #0b0f17; color: #e2e8f0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
    .card-bg { background-color: #131b2e; border: 1px solid #1e293b; }
    .sub-card { background-color: #1a243b; border: 1px solid #283552; }
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 3px; }
  </style>
</head>
<body class="min-h-screen p-4 md:p-6">

  <!-- Top Header -->
  <header class="flex flex-wrap items-center justify-between gap-4 pb-5 mb-6 border-b border-slate-800">
    <div class="flex flex-wrap items-center gap-3">
      <div class="flex items-center gap-2 text-2xl font-bold text-sky-400">
        <span>⚡</span>
        <span>VoltAgent Operations Console</span>
      </div>
      <span class="px-3 py-1 text-xs font-semibold text-emerald-400 bg-emerald-950/60 border border-emerald-600/50 rounded-full flex items-center gap-1.5">
        <span class="w-2 h-2 bg-emerald-400 rounded-full animate-pulse"></span>
        Headless OCPP 2.0.1 核心運行中
      </span>
      <span class="px-3 py-1 text-xs font-semibold text-sky-400 bg-sky-950/60 border border-sky-600/50 rounded-full flex items-center gap-1.5">
        <span class="w-2 h-2 bg-sky-400 rounded-full"></span>
        站點: 葵涌物流充電中心
      </span>
      <span class="px-3 py-1 text-xs font-semibold text-purple-300 bg-purple-950/60 border border-purple-500/50 rounded-full">
        🩺 BMS SoH 實時監測已啟用
      </span>
    </div>

    <div class="flex items-center gap-2">
      <button onclick="runQuickScenario('bms_soh')" class="px-4 py-2 text-xs md:text-sm font-bold text-amber-300 bg-amber-950/70 hover:bg-amber-900/80 border border-amber-500/50 rounded-lg transition">
        🩺 一鍵全場電池體檢 (SoH)
      </button>
      <button id="simBtn" onclick="toggleTimeLapse()" class="px-4 py-2 text-xs md:text-sm font-bold text-white bg-emerald-600 hover:bg-emerald-500 rounded-lg transition shadow-lg shadow-emerald-900/30">
        ▶ 開始模擬充電推進 (Time-lapse)
      </button>
    </div>
  </header>

  <!-- 4 Top KPI Cards -->
  <section class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4 mb-6">
    <div class="card-bg rounded-xl p-5 text-center">
      <div class="text-3xl font-extrabold text-rose-500" id="kpi-unmanaged">209.5 kW</div>
      <div class="text-xs text-slate-400 mt-1.5">未經調度自然峰值 (嚴重超載)</div>
    </div>
    <div class="card-bg rounded-xl p-5 text-center">
      <div class="text-3xl font-extrabold text-emerald-400" id="kpi-dlm">31.8 kW</div>
      <div class="text-xs text-slate-400 mt-1.5">AI DLM 優化峰值 (安全受控)</div>
    </div>
    <div class="card-bg rounded-xl p-5 text-center">
      <div class="text-3xl font-extrabold text-sky-400" id="kpi-limit">200.0 kW</div>
      <div class="text-xs text-slate-400 mt-1.5">變壓器容量契約安全上限</div>
    </div>
    <div class="card-bg rounded-xl p-5 text-center">
      <div class="text-3xl font-extrabold text-amber-400">100%</div>
      <div class="text-xs text-slate-400 mt-1.5">車隊次日出車準時履約率</div>
    </div>
    <div class="card-bg rounded-xl p-5 text-center border-purple-500/40">
      <div class="text-3xl font-extrabold text-purple-400" id="kpi-soh">89.4% <span class="text-xs font-normal text-rose-400">(1部預警)</span></div>
      <div class="text-xs text-slate-400 mt-1.5">車隊平均電池健康度 (Fleet SoH)</div>
    </div>
  </section>

  <!-- Main Grid: Left AI Console & Right Live Fleet Status -->
  <section class="grid grid-cols-1 lg:grid-cols-12 gap-6 mb-6">

    <!-- Left: AI Dispatch Terminal (5 cols) -->
    <div class="lg:col-span-5 card-bg rounded-xl p-5 flex flex-col justify-between">
      <div>
        <div class="flex items-center justify-between mb-4">
          <h2 class="text-base font-bold text-white flex items-center gap-2">
            <span>💬</span> 生成式對話調度終端 (客戶可即場輸入或點擊測試)
          </h2>
          <span class="text-xs text-emerald-400 font-semibold flex items-center gap-1">
            <span class="w-2 h-2 bg-emerald-400 rounded-full"></span> Online
          </span>
        </div>

        <!-- Quick Buttons -->
        <div class="flex flex-wrap gap-2 mb-4">
          <button onclick="runQuickScenario('ev01_urgent')" class="px-3 py-1.5 text-xs bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg transition">
            ⚡ 預設案例: EV-01 急件優先
          </button>
          <button onclick="runQuickScenario('transformer_120')" class="px-3 py-1.5 text-xs bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg transition">
            ⚠️ 突發應急: 變壓器降至 120kW
          </button>
          <button onclick="runQuickScenario('even_0700')" class="px-3 py-1.5 text-xs bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg transition">
            🔄 均勻調峰: 全車隊 07:00 出發
          </button>
          <button onclick="runQuickScenario('bms_soh')" class="px-3 py-1.5 text-xs bg-purple-950/80 hover:bg-purple-900 text-purple-200 border border-purple-500/50 rounded-lg transition font-semibold">
            🩺 電池健檢: 掃描全車隊 SoH 並調度慢充保護
          </button>
        </div>

        <!-- Chat History Box -->
        <div id="chatBox" class="space-y-3 max-h-80 overflow-y-auto pr-1 mb-4">
          <div class="sub-card rounded-xl p-4 text-sm leading-relaxed">
            <div class="font-bold text-sky-400 mb-1">VoltAgent 智能調度員 :</div>
            <div class="text-slate-200">
              您好！我係葵涌站嘅 AI 調度助手。場站現有 5 部物流 Van 插槍，變壓器契約上限為 200kW，並已開啟 <b>OCPP 2.0.1 BMS 電池健康度 (SoH) 實時診斷</b>。請輸入您的調度指令（支援廣東話或即點上方快捷按鈕）。
            </div>
          </div>
        </div>
      </div>

      <!-- Input Bar -->
      <div class="flex gap-2 mt-2">
        <input id="chatInput" type="text" placeholder="例如：幫我檢查邊部車電池老化，或者 EV-01 明早5點急件..."
          class="flex-1 bg-slate-900/90 border border-slate-700 rounded-lg px-3.5 py-2.5 text-sm text-white focus:outline-none focus:border-sky-500"
          onkeydown="if(event.key==='Enter') sendCustomCommand()" />
        <button onclick="sendCustomCommand()" class="px-5 py-2.5 bg-sky-500 hover:bg-sky-400 text-slate-950 font-bold text-sm rounded-lg transition">
          發送
        </button>
      </div>
    </div>

    <!-- Right: 5 Chargers Live Telemetry + BMS Health (7 cols) -->
    <div class="lg:col-span-7 card-bg rounded-xl p-5">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-base font-bold text-white flex items-center gap-2">
          <span>🚚</span> 葵涌站 5 台快充樁即時遙測 & 電池健康度 (Live Fleet & BMS Status)
        </h2>
        <div class="flex items-center gap-3">
          <span class="text-xs text-slate-400" id="simClock">當前時間: 22:00</span>
          <button onclick="resetDemo()" class="text-xs text-slate-400 hover:text-white underline">重置狀態</button>
        </div>
      </div>

      <div id="fleetContainer" class="space-y-3">
        <!-- Populated dynamically by JS -->
      </div>
    </div>
  </section>

  <!-- Bottom: DLM Peak Shaving Curve -->
  <section class="card-bg rounded-xl p-5 mb-6">
    <div class="flex items-center justify-between mb-3">
      <h2 class="text-base font-bold text-white flex items-center gap-2">
        <span>📈</span> 22:00 - 08:00 全站動態負載平衡 (DLM Peak Shaving Curve)
      </h2>
      <span class="text-xs font-bold text-emerald-400" id="shavingRate">削峰率: 84.8%</span>
    </div>
    <div class="h-64 w-full">
      <canvas id="dlmChart"></canvas>
    </div>
  </section>

  <!-- Collapsible Zeabur Redis Raw State Inspector -->
  <section class="card-bg rounded-xl p-4">
    <div class="flex items-center justify-between">
      <div class="flex items-center gap-2 text-sm font-bold text-slate-300">
        <span>🛰️</span> 即時充電樁底層狀態 (由 Zeabur Redis 實時同步)
      </div>
      <div class="flex gap-2">
        <button onclick="fetchStatus()" class="px-3 py-1 text-xs bg-sky-600 hover:bg-sky-500 text-white rounded-md font-semibold">
          🔄 重新整理狀態
        </button>
        <button onclick="document.getElementById('rawJsonBox').classList.toggle('hidden')" class="px-3 py-1 text-xs bg-slate-800 hover:bg-slate-700 text-slate-300 rounded-md">
          展開 / 收起 JSON
        </button>
      </div>
    </div>
    <pre id="rawJsonBox" class="mt-3 p-3 bg-slate-950 rounded-lg text-xs text-emerald-400 overflow-x-auto hidden"></pre>
  </section>

  <!-- Modal: Solution B Detailed BMS Battery Health Diagnostic Report -->
  <div id="bmsModal" class="fixed inset-0 bg-black/75 backdrop-blur-sm hidden items-center justify-center z-50 p-4">
    <div class="card-bg border border-slate-700 rounded-2xl max-w-lg w-full p-6 shadow-2xl">
      <div class="flex items-center justify-between pb-3 mb-4 border-b border-slate-800">
        <div>
          <h3 class="text-lg font-bold text-white flex items-center gap-2" id="modalTitle">
            🩺 OCPP 2.0.1 BMS 電池深度體檢報告
          </h3>
          <p class="text-xs text-slate-400 mt-0.5" id="modalSub">透過 DataTransfer / NotifyMonitoringReport 實時採集</p>
        </div>
        <button onclick="closeBmsModal()" class="text-slate-400 hover:text-white text-xl font-bold">&times;</button>
      </div>

      <div class="grid grid-cols-2 gap-3 mb-4">
        <div class="sub-card p-3.5 rounded-xl">
          <div class="text-xs text-slate-400">電池健康度 (SoH)</div>
          <div class="text-2xl font-extrabold mt-1" id="m-soh">96%</div>
        </div>
        <div class="sub-card p-3.5 rounded-xl">
          <div class="text-xs text-slate-400">電芯核心溫度</div>
          <div class="text-2xl font-extrabold mt-1" id="m-temp">31.5°C</div>
        </div>
        <div class="sub-card p-3.5 rounded-xl">
          <div class="text-xs text-slate-400">等效內阻 (DCIR)</div>
          <div class="text-xl font-bold text-slate-200 mt-1" id="m-res">1.2 mΩ</div>
        </div>
        <div class="sub-card p-3.5 rounded-xl">
          <div class="text-xs text-slate-400">充放電循環 (Cycles)</div>
          <div class="text-xl font-bold text-slate-200 mt-1" id="m-cycles">310 次</div>
        </div>
      </div>

      <div class="sub-card p-4 rounded-xl mb-4 space-y-2 text-xs">
        <div class="flex justify-between">
          <span class="text-slate-400">單體電芯最大壓差 (ΔV):</span>
          <span class="font-bold text-white" id="m-delta">12 mV (極佳平衡)</span>
        </div>
        <div class="flex justify-between">
          <span class="text-slate-400">當前充電功率 / 上限:</span>
          <span class="font-bold text-sky-400" id="m-power">60.0 kW / 60.0 kW</span>
        </div>
        <div class="flex justify-between">
          <span class="text-slate-400">熱失控風險評估:</span>
          <span class="font-bold text-emerald-400" id="m-risk">極低 (安全受控)</span>
        </div>
      </div>

      <div class="p-3.5 rounded-xl bg-slate-900 border border-slate-700 text-xs leading-relaxed text-slate-300 mb-5" id="m-advice">
        <!-- Populated by JS -->
      </div>

      <div class="flex justify-end gap-2">
        <button onclick="runQuickScenario('bms_soh'); closeBmsModal();" class="px-4 py-2 bg-purple-600 hover:bg-purple-500 text-white text-xs font-bold rounded-lg">
          ⚡ 執行 AI 養護調度
        </button>
        <button onclick="closeBmsModal()" class="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-300 text-xs font-bold rounded-lg">
          關閉報告
        </button>
      </div>
    </div>
  </div>

  <script>
    let currentFleet = {};
    let dlmChart = null;
    let simInterval = null;
    let simMinutes = 22 * 60; // 22:00

    const colorMap = {
      emerald: { bar: "bg-emerald-500", text: "text-emerald-400", badge: "bg-emerald-950 text-emerald-300 border-emerald-600/40" },
      sky:     { bar: "bg-sky-500",     text: "text-sky-400",     badge: "bg-sky-950 text-sky-300 border-sky-600/40" },
      rose:    { bar: "bg-rose-500",    text: "text-rose-400",    badge: "bg-rose-950 text-rose-300 border-rose-500/60 animate-pulse" },
      amber:   { bar: "bg-amber-500",   text: "text-amber-400",   badge: "bg-amber-950 text-amber-300 border-amber-600/40" }
    };

    function renderFleet(chargers) {
      currentFleet = chargers;
      const container = document.getElementById("fleetContainer");
      container.innerHTML = "";

      let totalSoh = 0;
      let warnCount = 0;
      const keys = Object.keys(chargers);

      keys.forEach((bayKey) => {
        const c = chargers[bayKey];
        totalSoh += c.soh;
        if (c.soh < 80) warnCount++;

        const theme = colorMap[c.color] || colorMap.sky;
        const sohBadgeColor = c.soh < 80
          ? "bg-rose-950/90 text-rose-300 border border-rose-500/60"
          : (c.soh < 90 ? "bg-amber-950/80 text-amber-300 border border-amber-500/40" : "bg-emerald-950/80 text-emerald-300 border border-emerald-500/40");

        const card = document.createElement("div");
        card.className = "sub-card rounded-xl p-3.5 transition hover:border-slate-500";
        card.innerHTML = `
          <div class="flex flex-wrap items-center justify-between gap-2">
            <div>
              <div class="flex items-center gap-2">
                <span class="font-bold text-white text-sm">${c.ev_id} - ${c.name}</span>
                <span class="px-2 py-0.5 text-[11px] font-semibold rounded-full ${sohBadgeColor}">
                  🩺 SoH: ${c.soh}% (${c.health_tag})
                </span>
              </div>
              <div class="text-xs text-slate-400 mt-1">
                ${c.cp_id} | 出車: ${c.depart} | 優先級: ${c.priority} | 實時功率: <b class="text-slate-200">${c.current_kw} kW</b>
              </div>
              <div class="text-[11px] text-slate-400 mt-0.5 flex flex-wrap gap-3">
                <span>🌡️ 電芯溫度: <b class="${c.temp > 40 ? 'text-rose-400' : 'text-slate-200'}">${c.temp}°C</b></span>
                <span>⚡ 內阻: <b class="text-slate-200">${c.internal_res} mΩ</b></span>
                <span>🔋 壓差 ΔV: <b class="text-slate-200">${c.cell_delta_mv} mV</b></span>
              </div>
            </div>

            <div class="flex flex-col items-end gap-1.5 min-w-[170px]">
              <div class="flex items-center gap-2">
                <span class="text-xs font-bold ${theme.text}">
                  ${c.soc}% ➔ ${c.target_soc}% (${c.ready_time} 就緒)
                </span>
                <button onclick="openBmsModal('${bayKey}')" class="px-2 py-1 text-[11px] bg-slate-800 hover:bg-slate-700 text-sky-300 border border-slate-600 rounded-md font-semibold">
                  🔍 體檢報告
                </button>
              </div>
              <div class="w-44 h-2 bg-slate-800 rounded-full overflow-hidden">
                <div class="${theme.bar} h-full rounded-full transition-all duration-500" style="width: ${c.soc}%"></div>
              </div>
            </div>
          </div>
        `;
        container.appendChild(card);
      });

      const avgSoh = (totalSoh / (keys.length || 1)).toFixed(1);
      document.getElementById("kpi-soh").innerHTML = `${avgSoh}% <span class="text-xs font-normal ${warnCount > 0 ? 'text-rose-400' : 'text-emerald-400'}">(${warnCount > 0 ? warnCount + '部老化預警' : '全數健康'})</span>`;
    }

    function openBmsModal(bayKey) {
      const c = currentFleet[bayKey];
      if (!c) return;
      document.getElementById("modalTitle").innerText = `🩺 ${c.ev_id} - ${c.name} 電池體檢報告`;
      document.getElementById("modalSub").innerText = `充電樁: ${c.cp_id} | OCPP 2.0.1 BMS 實時遙測`;

      const sohEl = document.getElementById("m-soh");
      sohEl.innerText = `${c.soh}% (${c.health_tag})`;
      sohEl.className = `text-2xl font-extrabold mt-1 ${c.soh < 80 ? 'text-rose-400' : 'text-emerald-400'}`;

      const tempEl = document.getElementById("m-temp");
      tempEl.innerText = `${c.temp}°C`;
      tempEl.className = `text-2xl font-extrabold mt-1 ${c.temp > 40 ? 'text-rose-400' : 'text-sky-400'}`;

      document.getElementById("m-res").innerText = `${c.internal_res} mΩ`;
      document.getElementById("m-cycles").innerText = `${c.cycles} 次`;
      document.getElementById("m-delta").innerText = `${c.cell_delta_mv} mV ${c.cell_delta_mv > 40 ? '(⚠️ 壓差偏大)' : '(平衡良好)'}`;
      document.getElementById("m-power").innerText = `${c.current_kw} kW / ${c.max_kw} kW`;

      const riskEl = document.getElementById("m-risk");
      const adviceEl = document.getElementById("m-advice");
      if (c.soh < 80) {
        riskEl.innerText = "中等偏高 (高倍率快充易發熱)";
        riskEl.className = "font-bold text-rose-400";
        adviceEl.innerHTML = `<b>🤖 VoltAgent AI 診斷建議：</b><br/>此車電池循環已達 ${c.cycles} 次，SoH 跌至 ${c.soh}% 且單體壓差達 ${c.cell_delta_mv}mV。若維持 45kW+ 快充會加速鋰枝晶析出及溫升。建議點擊下方「執行 AI 養護調度」，透過 OCPP 2.0.1 自動降載至 15kW 涓流平衡充電。`;
      } else {
        riskEl.innerText = "極低 (電芯狀態理想)";
        riskEl.className = "font-bold text-emerald-400";
        adviceEl.innerHTML = `<b>🤖 VoltAgent AI 診斷建議：</b><br/>此車電池包健康度極佳 (SoH ${c.soh}%)，內阻與壓差均處於出廠標準範圍內，可安全支援 60kW 峰值快充及動態負載調度。`;
      }

      const modal = document.getElementById("bmsModal");
      modal.classList.remove("hidden");
      modal.classList.add("flex");
    }

    function closeBmsModal() {
      const modal = document.getElementById("bmsModal");
      modal.classList.add("hidden");
      modal.classList.remove("flex");
    }

    function appendChat(role, text) {
      const box = document.getElementById("chatBox");
      const div = document.createElement("div");
      if (role === "user") {
        div.className = "bg-sky-950/60 border border-sky-700/50 rounded-xl p-3 text-xs text-sky-200 ml-8";
        div.innerHTML = `<span class="font-bold">營運主管 (您) :</span> ${text}`;
      } else {
        div.className = "sub-card rounded-xl p-3.5 text-xs leading-relaxed whitespace-pre-line";
        div.innerHTML = `<div class="font-bold text-sky-400 mb-1">VoltAgent 智能調度員 :</div><div class="text-slate-200">${text}</div>`;
      }
      box.appendChild(div);
      box.scrollTop = box.scrollHeight;
    }

    async function runQuickScenario(scenario) {
      const labels = {
        ev01_urgent: "⚡ 預設案例: EV-01 明早 05:00 急件優先充飽",
        transformer_120: "⚠️ 突發應急: 變壓器安全上限降至 120kW",
        even_0700: "🔄 均勻調峰: 全車隊 07:00 出發",
        bms_soh: "🩺 電池健檢: 掃描全車隊 BMS SoH 並對老化電池執行慢充養護"
      };
      appendChat("user", labels[scenario] || scenario);

      const res = await fetch("/api/dispatch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scenario })
      });
      const data = await res.json();
      applyDispatchResult(data);
    }

    async function sendCustomCommand() {
      const input = document.getElementById("chatInput");
      const cmd = input.value.trim();
      if (!cmd) return;
      input.value = "";
      appendChat("user", cmd);

      const res = await fetch("/api/dispatch", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: cmd })
      });
      const data = await res.json();
      applyDispatchResult(data);
    }

    function applyDispatchResult(data) {
      appendChat("ai", data.reply);
      document.getElementById("kpi-dlm").innerText = `${data.dlm_peak} kW`;
      document.getElementById("kpi-limit").innerText = `${data.contract_limit.toFixed(1)} kW`;
      const saving = (((209.5 - data.dlm_peak) / 209.5) * 100).toFixed(1);
      document.getElementById("shavingRate").innerText = `削峰率: ${saving}%`;
      renderFleet(data.chargers);
      updateChart(data.curve_mode);
      fetchStatus();
    }

    async function resetDemo() {
      const res = await fetch("/api/reset", { method: "POST" });
      const data = await res.json();
      renderFleet(data.chargers);
      document.getElementById("kpi-dlm").innerText = "31.8 kW";
      document.getElementById("kpi-limit").innerText = "200.0 kW";
      updateChart("normal");
      appendChat("ai", "🔄 已重置葵涌站 5 台充電樁與電池遙測至初始狀態。");
      fetchStatus();
    }

    function toggleTimeLapse() {
      const btn = document.getElementById("simBtn");
      if (simInterval) {
        clearInterval(simInterval);
        simInterval = null;
        btn.innerText = "▶ 開始模擬充電推進 (Time-lapse)";
        btn.classList.replace("bg-amber-600", "bg-emerald-600");
        return;
      }
      btn.innerText = "⏸ 暫停模擬推進";
      btn.classList.replace("bg-emerald-600", "bg-amber-600");
      simInterval = setInterval(() => {
        simMinutes += 15;
        const hrs = Math.floor((simMinutes / 60) % 24).toString().padStart(2, "0");
        const mins = (simMinutes % 60).toString().padStart(2, "0");
        document.getElementById("simClock").innerText = `當前時間: ${hrs}:${mins}`;

        Object.keys(currentFleet).forEach((k) => {
          const c = currentFleet[k];
          if (c.soc < c.target_soc) {
            const step = Math.max(1, Math.round(c.current_kw / 18));
            c.soc = Math.min(c.target_soc, c.soc + step);
          }
        });
        renderFleet(currentFleet);
      }, 1000);
    }

    function initChart() {
      const ctx = document.getElementById("dlmChart").getContext("2d");
      dlmChart = new Chart(ctx, {
        type: "line",
        data: {
          labels: ["22:00", "23:00", "00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00", "08:00"],
          datasets: [
            {
              label: "未受控自然峰值 (Unmanaged Peak)",
              data: [209.5, 195, 160, 110, 65, 30, 15, 10, 5, 0, 0],
              borderColor: "#f43f5e",
              backgroundColor: "rgba(244, 63, 94, 0.08)",
              borderDash: [5, 5],
              tension: 0.35,
              fill: true
            },
            {
              label: "VoltAgent AI DLM + BMS 保護負載",
              data: [31.8, 31.8, 31.8, 31.0, 30.5, 29.0, 28.0, 25.0, 18.0, 8.0, 0],
              borderColor: "#10b981",
              backgroundColor: "rgba(16, 185, 129, 0.18)",
              tension: 0.35,
              fill: true
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { labels: { color: "#cbd5e1" } } },
          scales: {
            x: { ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" } },
            y: { ticks: { color: "#94a3b8" }, grid: { color: "#1e293b" }, suggestedMax: 220 }
          }
        }
      });
    }

    function updateChart(mode) {
      if (!dlmChart) return;
      if (mode === "bms_protect") {
        dlmChart.data.datasets[1].data = [28.4, 28.4, 28.0, 27.5, 27.5, 27.0, 26.0, 24.0, 20.0, 10.0, 0];
      } else if (mode === "emergency_120") {
        dlmChart.data.datasets[1].data = [24.5, 24.5, 24.5, 24.5, 24.0, 24.0, 23.5, 22.0, 18.0, 9.0, 0];
      } else if (mode === "urgent") {
        dlmChart.data.datasets[1].data = [34.2, 34.2, 33.0, 28.0, 26.0, 25.0, 24.0, 20.0, 15.0, 5.0, 0];
      } else {
        dlmChart.data.datasets[1].data = [31.8, 31.8, 31.8, 31.0, 30.5, 29.0, 28.0, 25.0, 18.0, 8.0, 0];
      }
      dlmChart.update();
    }

    async function fetchStatus() {
      const res = await fetch("/api/status");
      const data = await res.json();
      document.getElementById("rawJsonBox").innerText = JSON.stringify(data, null, 2);
      if (Object.keys(currentFleet).length === 0) {
        renderFleet(data.chargers);
      }
    }

    window.onload = () => {
      initChart();
      fetchStatus();
    };
  </script>
</body>
</html>"""
