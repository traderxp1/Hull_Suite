"""
Hull Suite Binance Bot - V1 (Based on V28 FINAL AUDITED) (V27 + pending TTL + no zero-qty liquidation)
+ قاعدة بيانات PostgreSQL كاملة
+ داشبورد تحليلي شامل
+ FINAL: Pending-only entry, no market fallback, OCO after fill
"""

import os
import logging
import threading
import queue
import time as time_module
import json
from flask import Flask, request, jsonify
from binance.client import Client
from binance.exceptions import BinanceAPIException
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_DOWN
from html import escape
import psycopg
from psycopg.rows import dict_row

# ====================================================================
# الإعدادات
# ====================================================================
API_KEY        = os.environ.get("BINANCE_API_KEY", "")
API_SECRET     = os.environ.get("BINANCE_API_SECRET", "")
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "renko2026")
USE_TESTNET    = os.environ.get("USE_TESTNET", "true").lower() == "true"
DATABASE_URL   = os.environ.get("DATABASE_URL", "")
WATCHED_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]

# ====================================================================
# NET PROFIT / COST FILTER
# ====================================================================
# Blocks trades where the expected net TP is too weak compared to expected net SL.
# Defaults are conservative for Binance Spot regular fee (0.10% per side).
def env_float(name, default):
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return float(default)

NET_FILTER_ENABLED = os.environ.get("NET_FILTER_ENABLED", "true").lower() == "true"
MIN_RISK_PCT       = env_float("MIN_RISK_PCT", 0.20)       # percent of entry price
MIN_NET_RR         = env_float("MIN_NET_RR", 1.20)         # net profit / net loss minimum
FEE_RATE_PER_SIDE  = env_float("FEE_RATE_PER_SIDE", 0.001) # decimal: 0.001 = 0.10% per side
SLIPPAGE_PCT_RT    = env_float("SLIPPAGE_PCT_RT", 0.00)    # percent round trip, e.g. 0.05 = 0.05%
FIXED_COST_RT      = env_float("FIXED_COST_RT", 0.0)       # fixed quote cost round trip

# ====================================================================
# BROKER-SIDE PROTECTION
# ====================================================================
# OCO means: after a buy fills, Binance receives BOTH TP and SL at broker side.
# If OCO fails, the bot closes immediately so no trade stays naked/unprotected.
BROKER_PROTECTION_MODE = os.environ.get("BROKER_PROTECTION_MODE", "OCO").upper()
REQUIRE_BROKER_PROTECTION = os.environ.get("REQUIRE_BROKER_PROTECTION", "true").lower() == "true"
OCO_STOP_LIMIT_BUFFER_PCT = env_float("OCO_STOP_LIMIT_BUFFER_PCT", 0.05)  # sell stop-limit below stop price

# ====================================================================
# BACKTEST MIRROR MODE
# ====================================================================
# هدفه: لا ننفذ الصفقة لايف إلا إذا السعر الحقيقي قريب من سعر دخول TradingView.
# هذا يمنع دخول متأخر يسبب خسائر R كبيرة مقارنة بالباك تست.
BACKTEST_MIRROR_MODE = os.environ.get("BACKTEST_MIRROR_MODE", "true").lower() == "true"
MAX_ENTRY_DEVIATION_PCT = env_float("MAX_ENTRY_DEVIATION_PCT", 0.05)  # max allowed live-vs-TV entry deviation %
REJECT_IF_PRICE_BEYOND_SL_TP = os.environ.get("REJECT_IF_PRICE_BEYOND_SL_TP", "true").lower() == "true"
MIRROR_REJECT_IF_NO_PRICE = os.environ.get("MIRROR_REJECT_IF_NO_PRICE", "true").lower() == "true"

# ====================================================================
# FINAL PENDING-ONLY ENTRY CONTROL
# ====================================================================
# Never chase entry with market orders. The bot places a real waiting buy order
# at TradingView entry. If price already passed the entry, the signal is rejected.
PENDING_ONLY_ENTRY = os.environ.get("PENDING_ONLY_ENTRY", "true").lower() == "true"
REJECT_IF_ENTRY_ALREADY_PASSED = os.environ.get("REJECT_IF_ENTRY_ALREADY_PASSED", "true").lower() == "true"
ENTRY_LIMIT_BUFFER_PCT = env_float("ENTRY_LIMIT_BUFFER_PCT", 0.00)  # 0.00 = exact entry, 0.02 = tiny fill buffer
NEAR_ENTRY_TOLERANCE_PCT = env_float("NEAR_ENTRY_TOLERANCE_PCT", 0.05)  # if live is slightly above entry, place LIMIT at entry instead of rejecting

# ====================================================================
# BOT-SIDE BREAKEVEN GUARD
# ====================================================================
# Independent BE system inside the bot. It does not wait for TradingView UPDATE_BACKUP_SL.
# When live price reaches +BOT_BE_TRIGGER_R, the bot moves broker-side SL to entry + BOT_BE_LOCK_R.
BOT_BE_ENABLED = os.environ.get("BOT_BE_ENABLED", "true").lower() == "true"
BOT_BE_TRIGGER_R = env_float("BOT_BE_TRIGGER_R", 0.30)
BOT_BE_LOCK_R = env_float("BOT_BE_LOCK_R", 0.00)
BOT_BE_UPDATE_BROKER = os.environ.get("BOT_BE_UPDATE_BROKER", "true").lower() == "true"
MONITOR_INTERVAL_SEC = env_float("MONITOR_INTERVAL_SEC", 1.0)
# V28: safety expiry for broker pending orders if TradingView CANCEL is missed. 0 = disabled.
PENDING_MAX_AGE_MIN = env_float("PENDING_MAX_AGE_MIN", 180.0)

# V20 FINAL HARDENED: broker reconcile checks the broker itself, not only bot memory.
BROKER_RECONCILE_ENABLED = os.environ.get("BROKER_RECONCILE_ENABLED", "true").lower() == "true"
RECONCILE_UNPROTECTED_ACTION = os.environ.get("RECONCILE_UNPROTECTED_ACTION", "CLOSE").upper()  # CLOSE or REPROTECT
EXECUTION_ERROR_R_THRESHOLD = env_float("EXECUTION_ERROR_R_THRESHOLD", -1.05)


# ====================================================================
# SINGLE VARIABLE CONFIG OVERRIDE
# ====================================================================
# بدل ما تضيف Variables كثيرة في Railway، تقدر تضيف متغير واحد فقط:
# BOT_SETTINGS=mirror=true;max_dev=0.05;near=0.05;protect=true;be=true;be_trigger=0.30;be_lock=0.00;monitor=1;reconcile=true;unprotected=CLOSE;exec_err_r=-1.05
# يدعم أيضاً JSON لو تحب لاحقاً.
def _bot_bool(v, default=False):
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def _bot_float(v, default):
    try:
        return float(str(v).strip())
    except Exception:
        return default

def _load_bot_settings():
    raw = os.environ.get("BOT_SETTINGS", "").strip()
    if not raw:
        return {}
    try:
        if raw.startswith("{"):
            obj = json.loads(raw)
            return {str(k).strip().lower(): v for k, v in obj.items()}
    except Exception:
        pass
    out = {}
    for part in raw.replace("\n", ";").replace(",", ";").split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        out[k.strip().lower()] = v.strip()
    return out

def _apply_bot_settings():
    global NET_FILTER_ENABLED, MIN_RISK_PCT, MIN_NET_RR, FEE_RATE_PER_SIDE, SLIPPAGE_PCT_RT, FIXED_COST_RT
    global BROKER_PROTECTION_MODE, REQUIRE_BROKER_PROTECTION, OCO_STOP_LIMIT_BUFFER_PCT
    global BACKTEST_MIRROR_MODE, MAX_ENTRY_DEVIATION_PCT, REJECT_IF_PRICE_BEYOND_SL_TP, MIRROR_REJECT_IF_NO_PRICE
    global PENDING_ONLY_ENTRY, REJECT_IF_ENTRY_ALREADY_PASSED, NEAR_ENTRY_TOLERANCE_PCT
    global BOT_BE_ENABLED, BOT_BE_TRIGGER_R, BOT_BE_LOCK_R, BOT_BE_UPDATE_BROKER, MONITOR_INTERVAL_SEC, PENDING_MAX_AGE_MIN
    global BROKER_RECONCILE_ENABLED, RECONCILE_UNPROTECTED_ACTION, EXECUTION_ERROR_R_THRESHOLD
    cfg = _load_bot_settings()
    if not cfg:
        return

    # aliases عربية/مختصرة بالإنجليزي
    if "net" in cfg or "net_filter" in cfg:
        NET_FILTER_ENABLED = _bot_bool(cfg.get("net", cfg.get("net_filter")), NET_FILTER_ENABLED)
    if "min_risk" in cfg:
        MIN_RISK_PCT = _bot_float(cfg.get("min_risk"), MIN_RISK_PCT)
    if "min_net_rr" in cfg:
        MIN_NET_RR = _bot_float(cfg.get("min_net_rr"), MIN_NET_RR)
    if "fee" in cfg:
        FEE_RATE_PER_SIDE = _bot_float(cfg.get("fee"), FEE_RATE_PER_SIDE)
    if "slippage" in cfg:
        SLIPPAGE_PCT_RT = _bot_float(cfg.get("slippage"), SLIPPAGE_PCT_RT)
    if "fixed_cost" in cfg:
        FIXED_COST_RT = _bot_float(cfg.get("fixed_cost"), FIXED_COST_RT)

    if "broker" in cfg:
        BROKER_PROTECTION_MODE = str(cfg.get("broker")).strip().upper()
    if "oco_buffer" in cfg:
        OCO_STOP_LIMIT_BUFFER_PCT = _bot_float(cfg.get("oco_buffer"), OCO_STOP_LIMIT_BUFFER_PCT)
    if "protect" in cfg or "require_protection" in cfg:
        REQUIRE_BROKER_PROTECTION = _bot_bool(cfg.get("protect", cfg.get("require_protection")), REQUIRE_BROKER_PROTECTION)

    if "mirror" in cfg:
        BACKTEST_MIRROR_MODE = _bot_bool(cfg.get("mirror"), BACKTEST_MIRROR_MODE)
    if "max_dev" in cfg:
        MAX_ENTRY_DEVIATION_PCT = _bot_float(cfg.get("max_dev"), MAX_ENTRY_DEVIATION_PCT)
    if "reject_sl_tp" in cfg:
        REJECT_IF_PRICE_BEYOND_SL_TP = _bot_bool(cfg.get("reject_sl_tp"), REJECT_IF_PRICE_BEYOND_SL_TP)
    if "reject_no_price" in cfg:
        MIRROR_REJECT_IF_NO_PRICE = _bot_bool(cfg.get("reject_no_price"), MIRROR_REJECT_IF_NO_PRICE)

    if "pending" in cfg:
        PENDING_ONLY_ENTRY = _bot_bool(cfg.get("pending"), PENDING_ONLY_ENTRY)
    if "reject_passed" in cfg:
        REJECT_IF_ENTRY_ALREADY_PASSED = _bot_bool(cfg.get("reject_passed"), REJECT_IF_ENTRY_ALREADY_PASSED)
    if "near" in cfg:
        NEAR_ENTRY_TOLERANCE_PCT = _bot_float(cfg.get("near"), NEAR_ENTRY_TOLERANCE_PCT)

    if "be" in cfg:
        BOT_BE_ENABLED = _bot_bool(cfg.get("be"), BOT_BE_ENABLED)
    if "be_trigger" in cfg:
        BOT_BE_TRIGGER_R = _bot_float(cfg.get("be_trigger"), BOT_BE_TRIGGER_R)
    if "be_lock" in cfg:
        BOT_BE_LOCK_R = _bot_float(cfg.get("be_lock"), BOT_BE_LOCK_R)
    if "be_update" in cfg:
        BOT_BE_UPDATE_BROKER = _bot_bool(cfg.get("be_update"), BOT_BE_UPDATE_BROKER)
    if "monitor" in cfg:
        MONITOR_INTERVAL_SEC = _bot_float(cfg.get("monitor"), MONITOR_INTERVAL_SEC)
    if "pending_expiry" in cfg:
        PENDING_MAX_AGE_MIN = _bot_float(cfg.get("pending_expiry"), PENDING_MAX_AGE_MIN)
        globals()["UNPROTECTED_GRACE_SEC"] = _bot_float(cfg.get("unprotected_grace"), globals().get("UNPROTECTED_GRACE_SEC", 20))


    if "reconcile" in cfg:
        BROKER_RECONCILE_ENABLED = _bot_bool(cfg.get("reconcile"), BROKER_RECONCILE_ENABLED)
    if "unprotected" in cfg:
        RECONCILE_UNPROTECTED_ACTION = str(cfg.get("unprotected")).strip().upper()
    if "exec_err_r" in cfg:
        EXECUTION_ERROR_R_THRESHOLD = _bot_float(cfg.get("exec_err_r"), EXECUTION_ERROR_R_THRESHOLD)

_apply_bot_settings()




# ====================================================================
# TIMEZONE DISPLAY
# ====================================================================
APP_TZ_NAME = os.environ.get("TZ", "Asia/Dubai")
try:
    APP_TZ = ZoneInfo(APP_TZ_NAME)
except Exception:
    APP_TZ_NAME = "Asia/Dubai"
    APP_TZ = ZoneInfo("Asia/Dubai")
UTC_TZ = ZoneInfo("UTC")

def app_now():
    return datetime.now(APP_TZ)

def to_app_time(dt):
    if dt is None:
        return None
    try:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC_TZ)
        return dt.astimezone(APP_TZ)
    except Exception:
        return dt

# V6 Dashboard: Active positions + performance analytics + signal/action logs.
# Compatible with latest TradingView Renko strategy alerts:
# PLACE_BUY_STOP / PENDING_ENTRY = place real waiting buy-stop order
# ENTRY = immediate market entry only when strategy is in Fast Green Close mode
# CANCEL_PENDING / UPDATE_BACKUP_SL / ADD_ON / EXIT are supported

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
app = Flask(__name__)

# ====================================================================
# قاعدة البيانات
# ====================================================================
def get_db():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def init_db():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS trades (
                        id SERIAL PRIMARY KEY,
                        open_time TIMESTAMP,
                        close_time TIMESTAMP DEFAULT NOW(),
                        symbol VARCHAR(20),
                        entry_price FLOAT,
                        exit_price FLOAT,
                        sl_price FLOAT,
                        initial_sl_price FLOAT,
                        current_sl_price FLOAT,
                        tp_price FLOAT,
                        exit_reason TEXT,
                        qty FLOAT,
                        pnl FLOAT,
                        pnl_pct FLOAT,
                        duration_min INT,
                        rr_actual FLOAT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS active_states (
                        symbol VARCHAR(20) PRIMARY KEY,
                        state_json TEXT,
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS signal_events (
                        id SERIAL PRIMARY KEY,
                        received_at TIMESTAMP DEFAULT NOW(),
                        symbol VARCHAR(20),
                        action TEXT,
                        status TEXT,
                        reason TEXT,
                        entry_price FLOAT,
                        sl_price FLOAT,
                        initial_sl_price FLOAT,
                        current_sl_price FLOAT,
                        tp_price FLOAT,
                        qty FLOAT,
                        exit_price FLOAT,
                        pnl FLOAT,
                        raw_json TEXT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS action_events (
                        id SERIAL PRIMARY KEY,
                        created_at TIMESTAMP DEFAULT NOW(),
                        symbol VARCHAR(20),
                        action TEXT,
                        details TEXT
                    )
                """)

                # --- V6 FIX: migrate old PostgreSQL tables instead of only CREATE IF NOT EXISTS ---
                # Some old Railway databases already have a trades table without close_time.
                # CREATE TABLE IF NOT EXISTS does not add missing columns, so we add them safely here.
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS open_time TIMESTAMP")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS close_time TIMESTAMP DEFAULT NOW()")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS symbol VARCHAR(20)")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS sl_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS initial_sl_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS current_sl_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS tp_price FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_reason TEXT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS qty FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS pnl FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS pnl_pct FLOAT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS duration_min INT")
                cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS rr_actual FLOAT")

                cur.execute("ALTER TABLE active_states ADD COLUMN IF NOT EXISTS state_json TEXT")
                cur.execute("ALTER TABLE active_states ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")

                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS received_at TIMESTAMP DEFAULT NOW()")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS symbol VARCHAR(20)")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS action TEXT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS status TEXT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS reason TEXT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS entry_price FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS sl_price FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS tp_price FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS qty FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS exit_price FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS pnl FLOAT")
                cur.execute("ALTER TABLE signal_events ADD COLUMN IF NOT EXISTS raw_json TEXT")

                cur.execute("ALTER TABLE action_events ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()")
                cur.execute("ALTER TABLE action_events ADD COLUMN IF NOT EXISTS symbol VARCHAR(20)")
                cur.execute("ALTER TABLE action_events ADD COLUMN IF NOT EXISTS action TEXT")
                cur.execute("ALTER TABLE action_events ADD COLUMN IF NOT EXISTS details TEXT")

                # V4/V29 DB SAFE: old tables may still be VARCHAR(20/40/60).
                # Convert text fields so long guard/reconcile reasons never crash saving.
                cur.execute("ALTER TABLE trades ALTER COLUMN exit_reason TYPE TEXT")
                cur.execute("ALTER TABLE signal_events ALTER COLUMN action TYPE TEXT")
                cur.execute("ALTER TABLE signal_events ALTER COLUMN status TYPE TEXT")
                cur.execute("ALTER TABLE action_events ALTER COLUMN action TYPE TEXT")
            conn.commit()
        log.info("✅ قاعدة البيانات جاهزة")
    except Exception as e:
        log.error(f"❌ فشل: {e}")

def save_trade(symbol, entry, exit_price, exit_reason, qty, pnl, sl=None, tp=None, open_time=None, current_sl=None, trade_quality="Clean"):
    """
    sl = initial/original SL used for true R calculation.
    current_sl = latest backup SL after BE/SL updates, saved only for audit.
    Older trades may not have these fields, so we keep sl_price as initial SL for compatibility.
    """
    try:
        exit_reason = str(exit_reason or "UNKNOWN")
        initial_sl = sl
        pnl_pct = round((pnl / (entry * qty)) * 100, 4) if entry and qty and entry * qty > 0 else None
        risk = entry - initial_sl if initial_sl and entry else None
        reward = exit_price - entry if exit_price and entry else None
        rr_actual = round(reward / risk, 3) if risk and risk > 0 and reward is not None else None
        duration_min = None
        if open_time:
            try:
                if isinstance(open_time, str):
                    open_time = datetime.fromisoformat(open_time.replace("Z", "+00:00")).replace(tzinfo=None)
                delta = datetime.utcnow() - open_time
                duration_min = max(0, int(delta.total_seconds() / 60))
            except Exception:
                pass
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades
                    (open_time, symbol, entry_price, exit_price, sl_price, initial_sl_price, current_sl_price, tp_price,
                     exit_reason, qty, pnl, pnl_pct, duration_min, rr_actual)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (open_time, symbol, entry, exit_price, initial_sl, initial_sl, current_sl, tp,
                      exit_reason, qty, pnl, pnl_pct, duration_min, rr_actual))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حفظ الصفقة: {e}")

def load_trades(limit=100):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM trades ORDER BY close_time DESC LIMIT %s", (limit,))
                return cur.fetchall()
    except Exception as e:
        log.error(f"فشل تحميل الصفقات: {e}")
        return []

def save_state(symbol, state):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO active_states (symbol, state_json, updated_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (symbol) DO UPDATE
                    SET state_json = EXCLUDED.state_json, updated_at = NOW()
                """, (symbol, json.dumps(state, default=str)))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حفظ الحالة: {e}")

def load_all_states():
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, state_json FROM active_states")
                rows = cur.fetchall()
                return {r["symbol"]: json.loads(r["state_json"]) for r in rows}
    except Exception as e:
        log.error(f"فشل تحميل الحالات: {e}")
        return {}

def delete_state(symbol):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM active_states WHERE symbol = %s", (symbol,))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حذف الحالة: {e}")


def safe_float(value, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default

def safe_dt(value):
    if not value:
        return None
    if hasattr(value, "strftime"):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None

def fmt_num(value, digits=4, default="—"):
    v = safe_float(value)
    if v is None:
        return default
    return f"{v:,.{digits}f}"

def fmt_money(value, digits=4, default="—"):
    v = safe_float(value)
    if v is None:
        return default
    sign = "+" if v > 0 else ""
    return f"{sign}{v:,.{digits}f}"

def pct(value, digits=2, default="—"):
    v = safe_float(value)
    if v is None:
        return default
    return f"{v:.{digits}f}%"

def load_signal_events(limit=80):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM signal_events ORDER BY received_at DESC LIMIT %s", (limit,))
                return cur.fetchall()
    except Exception as e:
        log.error(f"فشل تحميل الإشارات: {e}")
        return []

def load_action_events(limit=80):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM action_events ORDER BY created_at DESC LIMIT %s", (limit,))
                return cur.fetchall()
    except Exception as e:
        log.error(f"فشل تحميل الأحداث: {e}")
        return []

def save_action_event(symbol, action, details=""):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO action_events (symbol, action, details)
                    VALUES (%s, %s, %s)
                """, (symbol, action, details))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حفظ الحدث: {e}")

def log_signal_event(symbol, action, status="", reason="", data=None):
    try:
        data = data or {}
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO signal_events
                    (symbol, action, status, reason, entry_price, sl_price, tp_price,
                     qty, exit_price, pnl, raw_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    symbol, action, status, reason,
                    safe_float(data.get("entry")),
                    safe_float(data.get("backup_sl")),
                    safe_float(data.get("tp")),
                    safe_float(data.get("qty")),
                    safe_float(data.get("exit_price")),
                    safe_float(data.get("pnl")),
                    json.dumps(data, ensure_ascii=False, default=str)
                ))
            conn.commit()
    except Exception as e:
        log.error(f"فشل حفظ الإشارة: {e}")

# ====================================================================
def create_client_with_retry(max_retries=2, delay=2):
    """Create Binance client safely.
    V3: Railway/Binance may block the hosting IP by region. The web app must not crash
    at startup; it should stay online and show the dashboard/log rejected signals.
    """
    if not API_KEY or not API_SECRET:
        log.warning("⚠️ Binance keys missing; running dashboard/webhook in broker-offline mode")
        return None
    for i in range(max_retries):
        try:
            c = Client(API_KEY, API_SECRET, testnet=USE_TESTNET)
            # force a lightweight request so restricted-region errors are detected early
            c.get_account()
            log.info("✅ اتصل ببايننس")
            return c
        except Exception as e:
            log.warning(f"⚠️ Binance connect attempt {i+1}: {e}")
            if i < max_retries - 1:
                time_module.sleep(delay)
    log.error("❌ Binance unavailable; keeping Flask alive instead of crashing")
    return None

client = create_client_with_retry()
BINANCE_OFFLINE_REASON = "Binance API unavailable/restricted from this Railway location" if client is None else ""

def ensure_binance_client():
    """Lazy reconnect. Returns True only when Binance is reachable."""
    global client, BINANCE_OFFLINE_REASON
    if client is not None:
        return True
    c = create_client_with_retry(max_retries=1, delay=0)
    if c is not None:
        client = c
        BINANCE_OFFLINE_REASON = ""
        return True
    BINANCE_OFFLINE_REASON = "Binance API unavailable/restricted from this Railway location"
    return False

state_lock = threading.Lock()

# ====================================================================
# الحالة
# ====================================================================
states = {}
processed_signals = []
signal_queue = queue.Queue()
symbol_info_cache = {}
_initialized = False

def fresh_state(symbol):
    return {
        "in_trade": False, "pending": False, "symbol": symbol,
        "entry_price": None, "backup_sl": None, "initial_sl": None, "current_sl": None, "tp_price": None,
        "bot_qty": 0.0, "buy_order_id": None, "backup_sl_order_id": None,
        "be_active": False, "last_action": None, "last_action_time": None,
        "last_error": None, "open_time": None,
    }

def get_state(symbol):
    if symbol not in states:
        states[symbol] = fresh_state(symbol)
    return states[symbol]

def clean_symbol(raw):
    if not raw:
        return raw
    s = str(raw).upper().strip()
    if ":" in s:
        s = s.split(":")[-1]
    s = s.replace(".P", "").replace("PERP", "")
    return s

def reset_symbol(symbol):
    with state_lock:
        states[symbol] = fresh_state(symbol)
        delete_state(symbol)

def log_action(symbol, action, details=""):
    s = get_state(symbol)
    s["last_action"] = action
    s["last_action_time"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    save_state(symbol, s)
    save_action_event(symbol, action, details)
    log.info(f"[{symbol}] ACTION: {action} | {details}")

def is_duplicate(data):
    sig = f"{data.get('action')}_{data.get('symbol')}_{data.get('entry','')}_{data.get('backup_sl','')}_{data.get('exit_price','')}"
    if sig in processed_signals[-20:]:
        return True
    processed_signals.append(sig)
    if len(processed_signals) > 100:
        processed_signals.pop(0)
    return False

# ====================================================================
# دوال السيمبول
# ====================================================================
def get_symbol_filters(symbol):
    if symbol in symbol_info_cache:
        return symbol_info_cache[symbol]
    if not ensure_binance_client():
        return {}
    try:
        info = client.get_symbol_info(symbol)
        f = {}
        for flt in info["filters"]:
            if flt["filterType"] == "LOT_SIZE":
                f["step_size"] = flt["stepSize"]
                f["min_qty"]   = float(flt["minQty"])
            if flt["filterType"] == "PRICE_FILTER":
                f["tick_size"] = flt["tickSize"]
            if flt["filterType"] in ("MIN_NOTIONAL", "NOTIONAL"):
                f["min_notional"] = float(flt.get("minNotional", 0))
        symbol_info_cache[symbol] = f
        return f
    except BinanceAPIException as e:
        log.error(f"Symbol info error {symbol}: {e}")
        return {}

def round_step(value, step):
    if not step:
        return value
    d_v = Decimal(str(value)); d_s = Decimal(str(step))
    return float((d_v / d_s).quantize(Decimal("1"), rounding=ROUND_DOWN) * d_s)

def fmt_qty(symbol, qty):
    step = get_symbol_filters(symbol).get("step_size", "0.00000001")
    r = round_step(qty, step)
    return f"{r:.8f}".rstrip("0").rstrip(".")

def fmt_price(symbol, price):
    tick = get_symbol_filters(symbol).get("tick_size", "0.01")
    r = round_step(price, tick)
    dec = len(str(tick).split(".")[1].rstrip("0")) if "." in str(tick) else 2
    return f"{r:.{max(dec,2)}f}"

def cancel_all_and_verify(symbol, max_retries=3):
    for attempt in range(max_retries):
        try:
            orders = client.get_open_orders(symbol=symbol)
            if not orders:
                return True
            for o in orders:
                try:
                    client.cancel_order(symbol=symbol, orderId=o["orderId"])
                except BinanceAPIException as e:
                    if e.code == -2011:
                        continue
                    raise
            time_module.sleep(0.3)
        except BinanceAPIException as e:
            log.error(f"[{symbol}] إلغاء {attempt+1}: {e}")
            time_module.sleep(0.5)
    try:
        return len(client.get_open_orders(symbol=symbol)) == 0
    except BinanceAPIException:
        return False

def place_backup_sl(symbol, qty, sl_price):
    try:
        qty_str = fmt_qty(symbol, qty)
        sl_str  = fmt_price(symbol, sl_price)
        order = client.create_order(
            symbol=symbol, side="SELL", type="STOP_LOSS_LIMIT",
            quantity=qty_str, stopPrice=sl_str, price=sl_str,
            timeInForce="GTC"
        )
        log.info(f"[{symbol}] SL @ {sl_str}")
        return order["orderId"]
    except BinanceAPIException as e:
        if e.code == -2010 and "trigger immediately" in str(e):
            try:
                qty_str = fmt_qty(symbol, qty)
                sell = client.order_market_sell(symbol=symbol, quantity=qty_str)
                s = get_state(symbol)
                save_trade(symbol, s.get("entry_price"), sl_price, "SL_MARKET", qty, 0,
                           sl=s.get("initial_sl") or s.get("backup_sl"),
                           current_sl=s.get("backup_sl"),
                           tp=s.get("tp_price"),
                           open_time=s.get("open_time"))
                reset_symbol(symbol)
                return "MARKET_SOLD"
            except BinanceAPIException as e2:
                log.error(f"[{symbol}] فشل البيع: {e2}")
                return None
        log.error(f"[{symbol}] فشل SL: {e}")
        get_state(symbol)["last_error"] = str(e)
        return None

def place_oco_exit(symbol, qty, sl_price, tp_price):
    """Place Binance broker-side OCO exit: TP limit + SL stop-limit.
    This is the main protection layer. If it fails, the caller must close the position.
    V21: يستخدم الكمية المتاحة فعلياً (يعالج partial fill) ويتحقق من MIN_NOTIONAL.
    """
    try:
        # V21 partial-fill guard: لا نحط OCO على كمية أكبر من المتاح فعلاً
        available = get_available_balance(symbol)
        if available > 0:
            qty = min(float(qty), available)
        # V21 MIN_NOTIONAL guard: لو القيمة أقل من الحد، OCO راح يُرفض
        filters = get_symbol_filters(symbol)
        min_notional = filters.get("min_notional", 0)
        if min_notional and tp_price and float(qty) * float(tp_price) < min_notional:
            log.error(f"[{symbol}] OCO تحت MIN_NOTIONAL ({float(qty)*float(tp_price):.4f} < {min_notional})")
            return None
        qty_str = fmt_qty(symbol, qty)
        tp_str  = fmt_price(symbol, tp_price)
        stop_str = fmt_price(symbol, sl_price)
        stop_limit = float(sl_price) * (1.0 - (OCO_STOP_LIMIT_BUFFER_PCT / 100.0))
        stop_limit_str = fmt_price(symbol, stop_limit)
        order = client.create_oco_order(
            symbol=symbol,
            side="SELL",
            quantity=qty_str,
            price=tp_str,
            stopPrice=stop_str,
            stopLimitPrice=stop_limit_str,
            stopLimitTimeInForce="GTC"
        )
        oid = order.get("orderListId") or order.get("listClientOrderId") or "OCO"
        log.info(f"[{symbol}] OCO TP={tp_str} SL={stop_str}/{stop_limit_str} qty={qty_str}")
        return oid
    except Exception as e:
        log.error(f"[{symbol}] فشل OCO: {e}")
        get_state(symbol)["last_error"] = str(e)
        return None

def place_broker_exit_protection(symbol, qty, sl_price, tp_price):
    """Preferred: Binance OCO. Optional fallback to stop-only if explicitly configured."""
    if BROKER_PROTECTION_MODE in ("OCO", "BROKER", "FULL"):
        oid = place_oco_exit(symbol, qty, sl_price, tp_price)
        if oid:
            return oid
        if REQUIRE_BROKER_PROTECTION:
            return None
    return place_backup_sl(symbol, qty, sl_price)

def emergency_close_unprotected(symbol, qty, reason="NO_BROKER_PROTECTION"):
    """Fail-safe: if broker TP/SL could not be placed after entry, close immediately."""
    try:
        available = get_available_balance(symbol)
        bot_qty = float(qty or 0)
        sell_qty = min(bot_qty, available) if bot_qty > 0 else 0.0
        if sell_qty <= 0:
            reset_symbol(symbol)
            return False
        qty_str = fmt_qty(symbol, sell_qty)
        current = get_current_price(symbol) or 0.0
        client.order_market_sell(symbol=symbol, quantity=qty_str)
        st = get_state(symbol)
        entry = float(st.get("entry_price") or current or 0)
        pnl = (current - entry) * sell_qty if current and entry else 0.0
        save_trade(symbol, entry, current, reason, sell_qty, pnl,
                   sl=st.get("initial_sl") or st.get("backup_sl"),
                   current_sl=st.get("current_sl") or st.get("backup_sl"),
                   tp=st.get("tp_price"), open_time=st.get("open_time"),
                   trade_quality="ProtectionFail")
        reset_symbol(symbol)
        log_action(symbol, reason, f"closed unprotected qty={qty_str}")
        return True
    except Exception as e:
        log.error(f"[{symbol}] emergency close failed: {e}")
        get_state(symbol)["last_error"] = str(e)
        save_state(symbol, get_state(symbol))
        return False

def verify_filled(symbol, order_id, timeout=5):
    start = time_module.time()
    while time_module.time() - start < timeout:
        try:
            o = client.get_order(symbol=symbol, orderId=order_id)
            if o["status"] == "FILLED":
                return True, float(o["executedQty"])
            if o["status"] in ("CANCELED", "REJECTED", "EXPIRED"):
                return False, 0.0
            time_module.sleep(0.3)
        except BinanceAPIException:
            time_module.sleep(0.3)
    try:
        o = client.get_order(symbol=symbol, orderId=order_id)
        return o["status"] == "FILLED", float(o["executedQty"])
    except BinanceAPIException:
        return False, 0.0

def get_available_balance(symbol):
    base_asset = symbol.replace("USDT","").replace("BUSD","").replace("FDUSD","")
    try:
        bal = client.get_asset_balance(asset=base_asset)
        return float(bal["free"]) if bal else 0.0
    except BinanceAPIException:
        return 0.0

def get_verified_bot_order_fill_qty_binance(symbol, order_id):
    """Return quantity proven to belong to the bot's own pending order.
    Never infers a fill from free balance alone, because that can be old/manual holdings.
    """
    if not order_id:
        return 0.0
    qty = 0.0
    try:
        o = client.get_order(symbol=symbol, orderId=order_id)
        qty = max(qty, safe_float(o.get("executedQty"), 0.0) or 0.0)
    except Exception:
        pass
    try:
        trades = client.get_my_trades(symbol=symbol, orderId=order_id)
        qty = max(qty, sum((safe_float(t.get("qty"), 0.0) or 0.0) for t in trades))
    except Exception:
        pass
    return qty

# ====================================================================
# PRICE CACHE (V21) - نداء واحد لكل العملات بدل نداء لكل عملة
# يقلل استهلاك Rate Limit بنسبة ~90% ويمنع حظر بايننس
# ====================================================================
_price_cache = {}
_price_cache_ts = 0.0
_price_cache_lock = threading.Lock()
PRICE_CACHE_TTL = env_float("PRICE_CACHE_TTL", 0.8)  # ثانية

def _refresh_price_cache():
    global _price_cache, _price_cache_ts
    if not ensure_binance_client():
        return
    try:
        tickers = client.get_all_tickers()
        with _price_cache_lock:
            _price_cache = {t["symbol"]: float(t["price"]) for t in tickers}
            _price_cache_ts = time_module.time()
    except Exception as e:
        log.error(f"price cache refresh failed: {e}")

def get_current_price(symbol, force=False):
    """يرجع السعر من الكاش. ينعش الكاش مرة كل PRICE_CACHE_TTL ثانية فقط."""
    global _price_cache_ts
    now = time_module.time()
    if force or (now - _price_cache_ts) > PRICE_CACHE_TTL or symbol not in _price_cache:
        _refresh_price_cache()
    with _price_cache_lock:
        val = _price_cache.get(symbol)
    if val is not None:
        return val
    # fallback مباشر لو الكاش ما فيه العملة
    try:
        ticker = client.get_symbol_ticker(symbol=symbol)
        return float(ticker["price"])
    except Exception:
        return None

def backtest_mirror_check(symbol, entry, sl, tp, mode="market"):
    """Return (ok, reason, current).
    mode="market": price must be close to TradingView entry now.
    mode="pending": allow current below entry because broker stop order waits at entry; reject only if already too far above entry.
    """
    if not BACKTEST_MIRROR_MODE:
        return True, "mirror off", None
    try:
        entry = float(entry); sl = float(sl); tp = float(tp)
        if entry <= 0:
            return False, "mirror: invalid entry", None
        current = get_current_price(symbol)
        if current is None or current <= 0:
            if MIRROR_REJECT_IF_NO_PRICE:
                return False, "mirror: no live price", current
            return True, "mirror: no price but allowed", current

        if REJECT_IF_PRICE_BEYOND_SL_TP:
            if sl and current <= float(sl):
                return False, f"mirror: live {current:.8f} already <= SL {float(sl):.8f}", current
            if tp and current >= float(tp):
                return False, f"mirror: live {current:.8f} already >= TP {float(tp):.8f}", current

        if mode == "pending" and current < entry:
            return True, f"mirror ok pending live={current:.8f} below entry={entry:.8f}", current

        dev_pct = abs(current - entry) / entry * 100.0
        if dev_pct > MAX_ENTRY_DEVIATION_PCT:
            return False, f"mirror: deviation {dev_pct:.4f}% > max {MAX_ENTRY_DEVIATION_PCT:.4f}% live={current:.8f} tv={entry:.8f}", current
        return True, f"mirror ok dev={dev_pct:.4f}% live={current:.8f} tv={entry:.8f}", current
    except Exception as e:
        return False, f"mirror error: {e}", None

def net_profit_filter_check(entry, sl, tp, qty):
    """Return (ok, reason). Buy-only quality filter before sending broker orders."""
    if not NET_FILTER_ENABLED:
        return True, ""
    try:
        entry = float(entry); sl = float(sl); tp = float(tp); qty = float(qty)
        if entry <= 0 or qty <= 0:
            return False, "net_filter: entry/qty invalid"
        if sl >= entry:
            return False, "net_filter: SL above/at entry"
        if tp <= entry:
            return False, "net_filter: TP below/at entry"
        risk_pct = (entry - sl) / entry * 100.0
        if risk_pct < MIN_RISK_PCT:
            return False, f"net_filter: risk {risk_pct:.3f}% < min {MIN_RISK_PCT:.3f}%"

        entry_value = entry * qty
        tp_value = tp * qty
        sl_value = sl * qty
        fee_tp = (abs(entry_value) + abs(tp_value)) * FEE_RATE_PER_SIDE
        fee_sl = (abs(entry_value) + abs(sl_value)) * FEE_RATE_PER_SIDE
        slip_cost = abs(entry_value) * (SLIPPAGE_PCT_RT / 100.0)
        fixed_half = FIXED_COST_RT / 2.0

        gross_profit = (tp - entry) * qty
        gross_loss = (entry - sl) * qty
        expected_net_profit = gross_profit - fee_tp - slip_cost - fixed_half
        expected_net_loss = gross_loss + fee_sl + slip_cost + fixed_half
        if expected_net_profit <= 0:
            return False, f"net_filter: expected net TP <= 0 ({expected_net_profit:.6f})"
        if expected_net_loss <= 0:
            return False, "net_filter: expected net loss invalid"
        net_rr = expected_net_profit / expected_net_loss
        if net_rr < MIN_NET_RR:
            return False, f"net_filter: netRR {net_rr:.2f} < min {MIN_NET_RR:.2f}"
        return True, f"net_filter ok risk={risk_pct:.3f}% netRR={net_rr:.2f}"
    except Exception as e:
        return False, f"net_filter error: {e}"


def do_market_sell(symbol, bot_qty, reason, entry_price, exit_price, pnl):
    cancel_all_and_verify(symbol)
    available = get_available_balance(symbol)
    if bot_qty <= 0:
        get_state(symbol)["last_error"] = "bot_qty <= 0; refusing to sell free balance"
        save_state(symbol, get_state(symbol))
        return {"status": "error", "reason": "bot_qty_invalid_no_balance_sell"}
    sell_qty = min(bot_qty, available)
    if sell_qty <= 0:
        reset_symbol(symbol)
        return {"status": "warning", "reason": "no qty"}
    qty_str = fmt_qty(symbol, sell_qty)
    sell = client.order_market_sell(symbol=symbol, quantity=qty_str)
    s = get_state(symbol)
    save_trade(symbol, entry_price, exit_price, reason, sell_qty, pnl,
               sl=s.get("initial_sl") or s.get("backup_sl"),
               current_sl=s.get("backup_sl"),
               tp=s.get("tp_price"),
               open_time=s.get("open_time"))
    reset_symbol(symbol)
    log_action(symbol, "EXIT", f"reason={reason} sold={qty_str}")
    return {"status": "ok", "sold": qty_str}

# ====================================================================
# معالجات الإشارات
# ====================================================================
def handle_entry(data):
    # FINAL PENDING-ONLY RULE:
    # Even if TradingView sends action=ENTRY from strategy.order.alert_message,
    # we do NOT open market. We treat it as a request to place a waiting entry order.
    return handle_pending_entry(data)

def handle_pending_entry(data):
    symbol = data["symbol"]
    entry  = float(data["entry"])
    sl     = float(data["backup_sl"])
    tp     = float(data["tp"])
    qty    = float(data["qty"])
    s = get_state(symbol)
    if s["in_trade"] or s["pending"]:
        return {"status": "ignored", "reason": "already in trade or pending"}

    ok_filter, filter_reason = net_profit_filter_check(entry, sl, tp, qty)
    if not ok_filter:
        return {"status": "rejected", "reason": filter_reason}

    ok_mirror, mirror_reason, mirror_price = backtest_mirror_check(symbol, entry, sl, tp, mode="pending")
    if not ok_mirror:
        return {"status": "rejected", "reason": mirror_reason}

    filters = get_symbol_filters(symbol)
    if qty < filters.get("min_qty", 0):
        return {"status": "error", "reason": "الكمية أقل من الحد"}

    try:
        current_price = get_current_price(symbol)
        if current_price is None or current_price <= 0:
            return {"status": "rejected", "reason": "pending_only: no live price"}

        qty_str = fmt_qty(symbol, qty)
        stop_str = fmt_price(symbol, entry)
        limit_price = entry * (1.0 + ENTRY_LIMIT_BUFFER_PCT / 100.0)
        limit_str = fmt_price(symbol, limit_price)

        # Pending-only with near-entry tolerance:
        # 1) live < entry  -> BUY STOP-LIMIT waiting for breakout at entry.
        # 2) live >= entry but still close -> LIMIT BUY at entry, waiting for a pullback/retest.
        # 3) live too far above entry -> reject; no market chasing.
        dev_pct_now = abs(current_price - entry) / entry * 100.0 if entry else 999.0
        if REJECT_IF_ENTRY_ALREADY_PASSED and current_price >= entry:
            if dev_pct_now <= NEAR_ENTRY_TOLERANCE_PCT:
                order = client.create_order(
                    symbol=symbol,
                    side="BUY",
                    type="LIMIT",
                    quantity=qty_str,
                    price=stop_str,
                    timeInForce="GTC"
                )
                entry_order_type = "LIMIT_PENDING_NEAR_ENTRY"
                log_name = "PENDING_ONLY_LIMIT_NEAR_ENTRY"
                method = "pending_only_limit_near_entry"
                log_details = f"entry={entry} live={current_price} dev={dev_pct_now:.4f}% tol={NEAR_ENTRY_TOLERANCE_PCT:.4f}%"
            else:
                return {"status": "rejected", "reason": f"pending_only: live {current_price:.8f} already above entry {entry:.8f} by {dev_pct_now:.4f}% > tolerance {NEAR_ENTRY_TOLERANCE_PCT:.4f}%; no market fallback"}
        else:
            order = client.create_order(
                symbol=symbol,
                side="BUY",
                type="STOP_LOSS_LIMIT",
                quantity=qty_str,
                stopPrice=stop_str,
                price=limit_str,
                timeInForce="GTC"
            )
            entry_order_type = "STOP_LOSS_LIMIT_PENDING_ONLY"
            log_name = "PENDING_ONLY_BUY_STOP"
            method = "pending_only_buy_stop"
            log_details = f"entry={entry} limit={limit_str} live={current_price}"

        with state_lock:
            s.update({
                "pending": True, "in_trade": False,
                "entry_price": entry, "backup_sl": sl, "initial_sl": sl, "current_sl": sl, "tp_price": tp,
                "bot_qty": float(qty_str), "buy_order_id": order["orderId"],
                "entry_order_type": entry_order_type,
                "open_time": datetime.utcnow().isoformat(),
            })
            save_state(symbol, s)
        log_action(symbol, log_name, log_details)
        return {"status": "ok", "method": method, "order_id": order.get("orderId")}
    except BinanceAPIException as e:
        s["last_error"] = str(e)
        save_state(symbol, s)
        return {"status": "error", "message": str(e)}

def handle_entry_filled(data):
    # TradingView order-fill alerts are not broker fills. Never use them to market buy.
    # If there is already pending/in_trade, it will be ignored by handle_pending_entry().
    return handle_pending_entry(data)



def _sell_verified_pending_fill_binance(symbol, filled_qty, reason, exit_price, pnl):
    """Sell only the quantity verified from the bot's own pending entry order.
    Never uses free balance alone, because that could sell old/manual holdings.
    """
    s = get_state(symbol)
    try:
        filled_qty = float(filled_qty or 0)
        if filled_qty <= 0:
            s["last_error"] = "verified filled qty <= 0"
            save_state(symbol, s)
            return {"status": "pending_fill_qty_invalid"}
        available = get_available_balance(symbol)
        sell_qty = min(filled_qty, available) if available is not None else filled_qty
        if sell_qty <= 0:
            with state_lock:
                s["last_error"] = "verified fill but no available balance to sell"
                s["needs_recheck"] = True
                s["recheck_since"] = datetime.utcnow().isoformat()
                save_state(symbol, s)
            log_action(symbol, "PENDING_FILL_NO_BALANCE_KEEP_STATE", f"verified_qty={filled_qty}")
            return {"status": "pending_fill_no_balance_keep_state"}
        qty_str = fmt_qty(symbol, sell_qty)
        client.order_market_sell(symbol=symbol, quantity=qty_str)
        entry = safe_float(s.get("entry_price"), safe_float(exit_price, 0.0))
        px = safe_float(exit_price, get_current_price(symbol) or entry)
        real_pnl = (px - entry) * sell_qty if entry and px else safe_float(pnl, 0.0)
        save_trade(symbol, entry, px, reason or "EXIT_PENDING_FILLED", sell_qty, real_pnl,
                   sl=s.get("initial_sl") or s.get("backup_sl"),
                   current_sl=s.get("current_sl") or s.get("backup_sl"),
                   tp=s.get("tp_price"), open_time=s.get("open_time"),
                   trade_quality="PendingRaceClosed")
        reset_symbol(symbol)
        log_action(symbol, "PENDING_RACE_FILLED_CLOSED", f"sold_verified_qty={qty_str}")
        return {"status": "ok", "note": "sold verified pending fill", "sold": qty_str}
    except Exception as e:
        s["last_error"] = str(e)
        save_state(symbol, s)
        log.error(f"[{symbol}] sell verified pending fill failed: {e}")
        return {"status": "error", "message": str(e)}


def _safe_cancel_pending_entry_binance(symbol, reason="CANCEL_PENDING", exit_price=None, pnl=0.0, close_if_filled=True):
    """V24 full pending-race guard.
    Handles every race window: before cancel, during cancel, and after cancel.
    If status is unknown, it keeps the bot state instead of resetting blindly.
    """
    s = get_state(symbol)
    order_id = s.get("buy_order_id")
    if not order_id:
        reset_symbol(symbol)
        log_action(symbol, "PENDING_CANCEL_NO_ORDER_ID_RESET", reason)
        return {"status": "cancelled_pending", "reason": "no_order_id"}

    def _read_order():
        return client.get_order(symbol=symbol, orderId=order_id)

    try:
        order = _read_order()
    except Exception as e:
        with state_lock:
            s["last_error"] = f"pending status unknown before cancel: {e}"
            s["needs_recheck"] = True
            s["recheck_since"] = datetime.utcnow().isoformat()
            save_state(symbol, s)
        log_action(symbol, "PENDING_STATUS_UNKNOWN_KEEP_STATE", str(e))
        return {"status": "pending_status_unknown_keep_state", "reason": str(e)}

    status = str(order.get("status", "")).upper()
    filled_qty = safe_float(order.get("executedQty"), 0.0) or 0.0

    # If any fill is already confirmed, cancel remaining and close verified fill.
    if status in ("FILLED", "PARTIALLY_FILLED") or filled_qty > 0:
        if status in ("NEW", "PARTIALLY_FILLED"):
            try:
                resp = client.cancel_order(symbol=symbol, orderId=order_id)
                filled_qty = max(filled_qty, safe_float(resp.get("executedQty"), 0.0) or 0.0)
            except Exception as ce:
                log.warning(f"[{symbol}] pending filled/partial cancel remaining warning: {ce}")
            try:
                order2 = _read_order()
                filled_qty = max(filled_qty, safe_float(order2.get("executedQty"), 0.0) or 0.0)
            except Exception:
                pass
        if close_if_filled:
            log_action(symbol, "PENDING_WAS_FILLED_ON_CANCEL", f"status={status} qty={filled_qty}")
            return _sell_verified_pending_fill_binance(symbol, filled_qty, reason, exit_price, pnl)

    if status in ("CANCELED", "REJECTED", "EXPIRED"):
        reset_symbol(symbol)
        log_action(symbol, "PENDING_ALREADY_CLOSED_RESET", f"status={status} reason={reason}")
        return {"status": "cancelled_pending", "reason": f"already_{status.lower()}"}

    # Main cancel attempt for unfilled NEW order.
    try:
        resp = client.cancel_order(symbol=symbol, orderId=order_id)
        filled_qty = max(filled_qty, safe_float(resp.get("executedQty"), 0.0) or 0.0)
    except Exception as ce:
        log.warning(f"[{symbol}] pending cancel warning; rechecking order: {ce}")

    # Always re-check after cancel attempt. This closes the V23 race gap.
    try:
        order_after = _read_order()
        status_after = str(order_after.get("status", "")).upper()
        filled_qty = max(filled_qty, safe_float(order_after.get("executedQty"), 0.0) or 0.0)
        if filled_qty > 0 or status_after in ("FILLED", "PARTIALLY_FILLED"):
            log_action(symbol, "PENDING_FILLED_DURING_CANCEL", f"status={status_after} qty={filled_qty}")
            return _sell_verified_pending_fill_binance(symbol, filled_qty, reason, exit_price, pnl)
        if status_after in ("CANCELED", "REJECTED", "EXPIRED"):
            reset_symbol(symbol)
            log_action(symbol, "EXIT_CANCELLED_PENDING_ONLY", f"status={status_after}; reason={reason}")
            return {"status": "cancelled_pending", "reason": "exit_received_while_pending_no_sell"}
        # Still open; keep tracking instead of reset.
        with state_lock:
            s["last_error"] = f"pending order still {status_after} after cancel attempt"
            s["needs_recheck"] = True
            s["recheck_since"] = datetime.utcnow().isoformat()
            save_state(symbol, s)
        log_action(symbol, "PENDING_CANCEL_STILL_OPEN_KEEP_STATE", f"status={status_after}")
        return {"status": "pending_cancel_still_open_keep_state", "reason": status_after}
    except Exception as e:
        with state_lock:
            s["last_error"] = f"pending post-cancel status unknown: {e}"
            s["needs_recheck"] = True
            s["recheck_since"] = datetime.utcnow().isoformat()
            save_state(symbol, s)
        log_action(symbol, "PENDING_POST_CANCEL_UNKNOWN_KEEP_STATE", str(e))
        return {"status": "pending_post_cancel_unknown_keep_state", "reason": str(e)}


def handle_exit(data):
    symbol = data.get("symbol")
    reason = data.get("exit_reason", "?")
    exit_price = float(data.get("exit_price", 0))
    pnl = float(data.get("pnl", 0))
    s = get_state(symbol)

    # V24: if EXIT arrives while pending, cancel bot-owned entry safely.
    # If the entry filled during the race window, close only the verified filled qty.
    if s.get("pending") and not s.get("in_trade"):
        return _safe_cancel_pending_entry_binance(symbol, reason=reason, exit_price=exit_price, pnl=pnl, close_if_filled=True)

    if not s.get("in_trade"):
        return {"status": "ignored"}
    try:
        return do_market_sell(symbol, s["bot_qty"], reason, s.get("entry_price"), exit_price, pnl)
    except BinanceAPIException as e:
        s["last_error"] = str(e)
        return {"status": "error", "message": str(e)}

def handle_update_backup_sl(data):
    symbol = data.get("symbol")
    new_sl = float(data["backup_sl"])
    s = get_state(symbol)
    if not s["in_trade"] and not s["pending"]:
        return {"status": "ignored"}
    if s["pending"]:
        with state_lock:
            s["backup_sl"] = new_sl
            s["current_sl"] = new_sl
            # If the order is still pending, no position exists yet; keep initial_sl aligned with the pending SL.
            s["initial_sl"] = new_sl
            save_state(symbol, s)
        log_action(symbol, "UPDATE_BACKUP_SL_PENDING", f"SL={new_sl}")
        return {"status": "ok"}
    if not cancel_all_and_verify(symbol):
        return {"status": "error", "reason": "فشل الإلغاء"}
    sl_id = place_broker_exit_protection(symbol, s["bot_qty"], new_sl, s.get("tp_price"))
    if not sl_id:
        emergency_close_unprotected(symbol, s["bot_qty"], "NO_BROKER_PROTECTION_AFTER_SL_UPDATE")
        return {"status": "error", "reason": "broker protection failed after SL update - closed"}
    with state_lock:
        s["backup_sl"] = new_sl
        s["current_sl"] = new_sl
        s["be_active"] = True
        s["backup_sl_order_id"] = sl_id
        save_state(symbol, s)
    log_action(symbol, "UPDATE_BACKUP_SL", f"SL={new_sl}")
    return {"status": "ok"}


def handle_cancel_pending(data):
    symbol = data.get("symbol")
    if not symbol:
        return {"status": "ignored"}
    s = get_state(symbol)
    # V24: cancel pending safely. If it filled during cancel, close only verified filled qty.
    if s.get("pending") and s.get("buy_order_id"):
        return _safe_cancel_pending_entry_binance(symbol, reason=data.get("reason", "CANCEL_PENDING"), exit_price=get_current_price(symbol) or s.get("entry_price"), pnl=0.0, close_if_filled=True)
    reset_symbol(symbol)
    log_action(symbol, "CANCEL_PENDING", "no pending order id; reset state")
    return {"status": "ok"}

def handle_add_on(data):
    # V22 SAFETY: add-on orders are disabled because they were market buys.
    # Pending-only system must never add exposure with market entry.
    symbol = data.get("symbol")
    log_action(symbol, "ADD_ON_REJECTED", "add-on disabled: no market buys allowed")
    return {"status": "rejected", "reason": "add_on_disabled_pending_only_no_market_buy"}


def force_market_exit(symbol, reason, exit_price=None):
    """Independent protection guard.
    Closes an open long position at market if price crosses Current SL or TP,
    even if TradingView did not send EXIT or Binance stop-limit did not fill.
    """
    s = get_state(symbol)
    if not s.get("in_trade"):
        return {"status": "ignored"}
    try:
        current = exit_price if exit_price is not None else get_current_price(symbol)
        if current is None:
            return {"status": "ignored", "reason": "no_price"}
        qty = float(s.get("bot_qty") or 0)
        available = get_available_balance(symbol)
        if qty <= 0:
            s["last_error"] = "guard qty <= 0; refusing to sell free balance"
            save_state(symbol, s)
            return {"status": "error", "reason": "guard_qty_invalid_no_balance_sell"}
        sell_qty = min(qty, available)
        if sell_qty <= 0:
            reset_symbol(symbol)
            log_action(symbol, "BROKER_FLAT_RESET", "no balance during guard")
            return {"status": "warning", "reason": "no_qty"}
        return do_market_sell(
            symbol,
            sell_qty,
            reason,
            float(s.get("entry_price") or current),
            current,
            (current - float(s.get("entry_price") or current)) * sell_qty,
        )
    except BinanceAPIException as e:
        s["last_error"] = str(e)
        save_state(symbol, s)
        log.error(f"[{symbol}] force exit failed: {e}")
        return {"status": "error", "message": str(e)}
    except Exception as e:
        s["last_error"] = str(e)
        save_state(symbol, s)
        log.error(f"[{symbol}] force exit failed: {e}")
        return {"status": "error", "message": str(e)}


def broker_reconcile_once():
    """V20 broker reconcile.
    Checks Binance open orders and base balance so the dashboard cannot stay OPEN forever.
    If an active trade has no broker-side protective orders, the bot either closes it or re-protects it.
    """
    if not BROKER_RECONCILE_ENABLED:
        return

    # ============================================================
    # V26: owner-locked stuck-pending recovery
    # V25 كان يحسم الحالة العالقة عبر free balance كحارس أخير.
    # هذا خطر لأنه ممكن يحمي/يبيع رصيد قديم أو يدوي.
    # V26 لا يعتبر أي كمية ملك البوت إلا إذا ثبتت من orderId/trades الخاص بأمر البوت.
    # إذا API لا يعطي تأكيد، تبقى الحالة في SAFE_QUARANTINE إلى أن تنحل أو يعمل المستخدم reset يدوي.
    # ============================================================
    for symbol, st in list(states.items()):
        if not st.get("needs_recheck"):
            continue
        if st.get("in_trade"):
            with state_lock:
                s = get_state(symbol)
                s["needs_recheck"] = False
                s["manual_review"] = False
                save_state(symbol, s)
            continue
        try:
            order_id = st.get("buy_order_id")
            if not order_id:
                reset_symbol(symbol)
                log_action(symbol, "RECHECK_RESOLVED_NO_ORDER", "cleared stuck state without order id")
                continue

            resolved = False
            status = "UNKNOWN"
            filled_qty = get_verified_bot_order_fill_qty_binance(symbol, order_id)
            try:
                o = client.get_order(symbol=symbol, orderId=order_id)
                status = str(o.get("status", "")).upper()
                filled_qty = max(filled_qty, safe_float(o.get("executedQty"), 0.0) or 0.0)
            except Exception as ge:
                log.warning(f"[{symbol}] owner-locked recheck order status unknown: {ge}")

            if filled_qty > 0 or status in ("FILLED", "PARTIALLY_FILLED"):
                try:
                    if status in ("NEW", "PARTIALLY_FILLED"):
                        client.cancel_order(symbol=symbol, orderId=order_id)
                except Exception:
                    pass
                use_qty = filled_qty
                sl = safe_float(st.get("current_sl") or st.get("backup_sl") or st.get("initial_sl"))
                tp = safe_float(st.get("tp_price"))
                oid = place_broker_exit_protection(symbol, use_qty, sl, tp) if (sl and tp) else None
                with state_lock:
                    s = get_state(symbol)
                    s.update({
                        "in_trade": True, "pending": False, "needs_recheck": False,
                        "manual_review": False, "bot_qty": use_qty,
                        "backup_sl_order_id": oid,
                    })
                    save_state(symbol, s)
                if not oid:
                    emergency_close_unprotected(symbol, use_qty, "RECHECK_FILLED_NO_PROTECTION")
                log_action(symbol, "RECHECK_RESOLVED_FILLED_OWNER_LOCKED", f"qty={use_qty} protected={bool(oid)}")
                resolved = True
            elif status in ("CANCELED", "REJECTED", "EXPIRED"):
                reset_symbol(symbol)
                log_action(symbol, "RECHECK_RESOLVED_CLOSED", f"status={status}")
                resolved = True

            if not resolved:
                with state_lock:
                    s = get_state(symbol)
                    s["needs_recheck"] = True
                    s["manual_review"] = True
                    s["last_error"] = f"SAFE_QUARANTINE: order {order_id} status={status}; not using balance-only recovery"
                    s["recheck_since"] = s.get("recheck_since") or datetime.utcnow().isoformat()
                    save_state(symbol, s)
                log_action(symbol, "RECHECK_SAFE_QUARANTINE", f"order_id={order_id} status={status}; no balance-only action")
        except Exception as e:
            log.error(f"[{symbol}] owner-locked recheck error: {e}")

    for symbol, st in list(states.items()):
        if not st.get("in_trade"):
            continue
        try:
            qty = safe_float(st.get("bot_qty"), 0.0)
            entry = safe_float(st.get("entry_price"))
            sl = safe_float(st.get("current_sl") or st.get("backup_sl") or st.get("initial_sl"))
            tp = safe_float(st.get("tp_price"))
            current = get_current_price(symbol)

            try:
                open_orders = client.get_open_orders(symbol=symbol)
            except Exception:
                open_orders = []

            # Binance spot OCO locks the base asset. When OCO fills, open orders normally disappear.
            # If no open order remains, decide whether this is flat/closed or naked/unprotected.
            if len(open_orders) != 0 and st.get("unprotected_seen_ts"):
                with state_lock:
                    s = get_state(symbol)
                    s.pop("unprotected_seen_ts", None)
                    save_state(symbol, s)
            if len(open_orders) == 0:
                free_base = get_available_balance(symbol)

                # If there is free base qty, the position may be naked. Handle immediately.
                if qty > 0 and free_base >= qty * 0.50:
                    if RECONCILE_UNPROTECTED_ACTION == "REPROTECT" and sl and tp:
                        oid = place_broker_exit_protection(symbol, min(qty, free_base), sl, tp)
                        if oid:
                            with state_lock:
                                s = get_state(symbol)
                                s["backup_sl_order_id"] = oid
                                save_state(symbol, s)
                            log_action(symbol, "RECONCILE_REPROTECT", f"qty={min(qty, free_base)} sl={sl} tp={tp}")
                            continue
                    emergency_close_unprotected(symbol, min(qty, free_base), "RECONCILE_UNPROTECTED_CLOSE")
                    continue

                # No free base and no open orders: assume broker-side exit already flattened it.
                # Save a reconciliation close so the dashboard resets instead of showing stale OPEN.
                exit_price = current or tp or sl or entry or 0.0
                pnl = (exit_price - (entry or exit_price)) * (qty or 0.0)
                reason = "BROKER_FLAT_RECONCILE"
                if tp and exit_price >= tp:
                    reason = "TP_RECONCILE"
                elif sl and exit_price <= sl:
                    reason = "SL_RECONCILE"
                save_trade(symbol, entry or exit_price, exit_price, reason, qty or 0.0, pnl,
                           sl=st.get("initial_sl") or st.get("backup_sl"),
                           current_sl=st.get("current_sl") or st.get("backup_sl"),
                           tp=st.get("tp_price"), open_time=st.get("open_time"),
                           trade_quality="BrokerReconcile")
                reset_symbol(symbol)
                log_action(symbol, reason, f"no open orders; reset stale state price={exit_price}")

        except Exception as e:
            log.error(f"[{symbol}] broker reconcile error: {e}")

def protection_guard_once():
    """Checks all active trades independently from TradingView alerts.

    V18 adds bot-side breakeven. This moves the broker-side SL without waiting
    for TradingView to send UPDATE_BACKUP_SL, which can be late on Renko bars.
    """
    # V21: ننعش الكاش مرة وحدة في بداية الدورة لكل العملات
    _refresh_price_cache()
    for symbol, st in list(states.items()):
        if not st.get("in_trade"):
            continue
        try:
            current = get_current_price(symbol)
            if current is None:
                continue

            entry = safe_float(st.get("entry_price"))
            initial_sl = safe_float(st.get("initial_sl") or st.get("backup_sl"))
            current_sl = safe_float(st.get("current_sl") or st.get("backup_sl"))
            tp = safe_float(st.get("tp_price"))
            qty = safe_float(st.get("bot_qty"), 0.0)

            # 0) Bot-side breakeven, before normal guard checks.
            if (BOT_BE_ENABLED and BOT_BE_UPDATE_BROKER and entry and initial_sl and qty > 0):
                risk = entry - initial_sl
                if risk > 0:
                    trigger_price = entry + risk * BOT_BE_TRIGGER_R
                    lock_price = entry + risk * BOT_BE_LOCK_R
                    if tp is not None:
                        lock_price = min(lock_price, float(tp) - 1e-12)
                    already_moved = current_sl is not None and current_sl >= lock_price - 1e-12
                    if current >= trigger_price and lock_price > (current_sl or initial_sl) and not already_moved:
                        if not cancel_all_and_verify(symbol):
                            emergency_close_unprotected(symbol, qty, "BE_CANCEL_FAILED")
                            continue
                        sl_id = place_broker_exit_protection(symbol, qty, lock_price, tp)
                        if not sl_id:
                            emergency_close_unprotected(symbol, qty, "BE_PROTECTION_FAILED")
                            continue
                        with state_lock:
                            s = get_state(symbol)
                            s["backup_sl"] = lock_price
                            s["current_sl"] = lock_price
                            s["be_active"] = True
                            s["backup_sl_order_id"] = sl_id
                            save_state(symbol, s)
                        log_action(symbol, "BOT_BE_UPDATE", f"trigger={BOT_BE_TRIGGER_R}R lock={BOT_BE_LOCK_R}R SL={lock_price}")
                        current_sl = lock_price

            sl = current_sl
            if sl is not None and current <= float(sl):
                force_market_exit(symbol, "GUARD_SL", current)
            elif tp is not None and current >= float(tp):
                force_market_exit(symbol, "GUARD_TP", current)
        except Exception as e:
            log.error(f"[{symbol}] protection guard error: {e}")

# ====================================================================
# مراقب الأوردرات
# ====================================================================
def monitor_pending_orders():
    while True:
        try:
            time_module.sleep(MONITOR_INTERVAL_SEC)

            # 1) Pending order monitor
            for symbol in [s for s, st in list(states.items()) if st.get("pending")]:
                s = get_state(symbol)
                if not s.get("pending") or not s.get("buy_order_id"):
                    continue
                try:
                    # V28: expire stale pending broker orders if TradingView cancel was missed.
                    if PENDING_MAX_AGE_MIN and PENDING_MAX_AGE_MIN > 0:
                        opened = safe_dt(s.get("open_time"))
                        if opened and (datetime.utcnow() - opened).total_seconds() > PENDING_MAX_AGE_MIN * 60:
                            _safe_cancel_pending_entry_binance(symbol, reason="BOT_PENDING_EXPIRED", exit_price=get_current_price(symbol) or s.get("entry_price"), pnl=0.0, close_if_filled=True)
                            continue
                    order = client.get_order(symbol=symbol, orderId=s["buy_order_id"])
                    status = order.get("status", "")
                    executed_qty = safe_float(order.get("executedQty"), 0.0) or 0.0
                    if status in ("FILLED", "PARTIALLY_FILLED") or executed_qty > 0:
                        # V24: protect partial fills too. Cancel any remaining quantity first.
                        if status == "PARTIALLY_FILLED":
                            try:
                                resp = client.cancel_order(symbol=symbol, orderId=s["buy_order_id"])
                                executed_qty = max(executed_qty, safe_float(resp.get("executedQty"), 0.0) or 0.0)
                            except Exception as ce:
                                log.warning(f"[{symbol}] partial fill cancel remaining warning: {ce}")
                            try:
                                order2 = client.get_order(symbol=symbol, orderId=s["buy_order_id"])
                                executed_qty = max(executed_qty, safe_float(order2.get("executedQty"), 0.0) or 0.0)
                            except Exception:
                                pass
                        actual_qty = executed_qty if executed_qty > 0 else float(order.get("executedQty", s["bot_qty"]))
                        sl_id = place_broker_exit_protection(symbol, actual_qty, s["backup_sl"], s.get("tp_price"))
                        if not sl_id:
                            emergency_close_unprotected(symbol, actual_qty, "NO_BROKER_PROTECTION_AFTER_PENDING_FILL")
                            continue
                        with state_lock:
                            s.update({
                                "in_trade": True, "pending": False,
                                "bot_qty": actual_qty, "backup_sl_order_id": sl_id,
                                "open_time": datetime.utcnow().isoformat(),
                            })
                            save_state(symbol, s)
                        log_action(symbol, "PENDING_FILLED_DETECTED", f"status={status} qty={actual_qty}")
                    elif status in ("CANCELED", "REJECTED", "EXPIRED"):
                        reset_symbol(symbol)
                except BinanceAPIException as e:
                    log.error(f"[{symbol}] pending monitor: {e}")

            # 2) Broker reconcile + independent broker protection guard for active trades
            broker_reconcile_once()
            protection_guard_once()

        except Exception as e:
            log.error(f"monitor_pending_orders: {e}")

# ====================================================================
# ULTRA FAST WEBHOOK QUEUE
# ====================================================================
def process_signal_task(data, action):
    try:
        log.info(f"معالجة في الخلفية: {json.dumps(data)}")
        # V3: if Binance blocks Railway IP, do not crash and do not attempt unsafe orders.
        if not ensure_binance_client():
            r = {"status": "rejected", "reason": BINANCE_OFFLINE_REASON}
            log_signal_event(data.get("symbol"), action, r["status"], r["reason"], data)
            return
        if action == "ENTRY":
            r = handle_entry(data)
        elif action in ("PLACE_BUY_STOP", "PENDING_ENTRY", "BUY_STOP"):
            r = handle_pending_entry(data)
        elif action == "ENTRY_FILLED":
            r = handle_entry_filled(data)
        elif action == "EXIT":
            r = handle_exit(data)
        elif action == "UPDATE_BACKUP_SL":
            r = handle_update_backup_sl(data)
        elif action == "CANCEL_PENDING":
            r = handle_cancel_pending(data)
        elif action == "ADD_ON":
            r = handle_add_on(data)
        else:
            r = {"status": "غير معروف", "action": action}
        log_signal_event(
            data.get("symbol"), action,
            str(r.get("status", "")),
            str(r.get("reason") or r.get("message") or r.get("action") or ""),
            data
        )
    except Exception as e:
        log.error(f"فشل معالجة إشارة الخلفية: {e}", exc_info=True)
        try:
            log_signal_event(data.get("symbol"), action, "error", str(e), data)
        except Exception:
            pass

def process_raw_signal(raw):
    # Parse and process TradingView payload completely in the background.
    # /webhook returns 200 OK before JSON parsing, DB writes, duplicate checks,
    # or broker API calls. This is the fastest ACK path for TradingView.
    data = None
    try:
        raw = (raw or "").strip()
        for line in reversed(raw.splitlines()):
            line = line.strip()
            if line.startswith("{"):
                try:
                    data = json.loads(line)
                    break
                except Exception:
                    continue
        if not data:
            log_signal_event("", "BAD_JSON", "bad_json", "JSON خاطئ", {"raw": raw[:2000]})
            return
        if not data.get("symbol"):
            log_signal_event("", str(data.get("action", "")).upper(), "bad_symbol", "لا يوجد رمز", data)
            return
        data["symbol"] = clean_symbol(data["symbol"])
        action = str(data.get("action", "")).upper()
        if is_duplicate(data):
            log_signal_event(data.get("symbol"), action, "duplicate", "duplicate ignored", data)
            return
        process_signal_task(data, action)
    except Exception as e:
        log.error(f"Raw signal worker parse/process error: {e}", exc_info=True)
        try:
            log_signal_event((data or {}).get("symbol"), str((data or {}).get("action", "ERROR")).upper(), "error", str(e), data or {"raw": str(raw)[:2000]})
        except Exception:
            pass

def signal_worker():
    while True:
        try:
            raw = signal_queue.get()
            process_raw_signal(raw)
            signal_queue.task_done()
        except Exception as e:
            log.error(f"Signal worker error: {e}", exc_info=True)
            time_module.sleep(1)

# ====================================================================
# Webhook - ULTRA FAST ACK
# ====================================================================
@app.route("/webhook", methods=["POST"])
def webhook():
    secret = request.headers.get("X-Webhook-Secret") or request.args.get("secret")
    if secret != WEBHOOK_SECRET:
        return "unauthorized", 401

    # أسرع مسار ممكن:
    # نقرأ النص، نرميه في queue، ونرجع OK فوراً.
    # لا JSON parsing، لا database، لا Binance/Alpaca، لا logging قبل الرد.
    try:
        raw = request.get_data(as_text=True, cache=False)
        signal_queue.put_nowait(raw)
        return "OK", 200, {"Content-Type": "text/plain"}
    except Exception:
        log.exception("Ultra-fast webhook enqueue failed")
        return "OK", 200, {"Content-Type": "text/plain"}

# ====================================================================
# Reset
# ====================================================================
@app.route("/reset/<symbol>", methods=["GET", "POST"])
def reset_symbol_route(symbol):
    secret = request.args.get("secret")
    if secret != WEBHOOK_SECRET:
        return jsonify({"error": "غير مصرح"}), 401
    symbol = clean_symbol(symbol)
    if symbol in states:
        cancel_all_and_verify(symbol)
    reset_symbol(symbol)
    return jsonify({"status": "ok", "symbol": symbol})

# ====================================================================
# الداشبورد التحليلي الشامل
# ====================================================================
def trade_dt(t, key):
    return safe_dt(t.get(key))


def _reason_key(r):
    return str(r or "").upper()

def _is_tp_reason(r):
    return _reason_key(r) in ("TP", "GUARD_TP", "TP_RECONCILE")

def _is_be_reason(r):
    return _reason_key(r) in ("BE", "SL_MARKET", "BE_MARKET")

def _is_sl_reason(r):
    return _reason_key(r) in (
        "SL", "GUARD_SL", "SL_RECONCILE", "RECONCILE_UNPROTECTED_CLOSE",
        "NO_BROKER_PROTECTION", "BE_PROTECTION_FAILED", "BE_CANCEL_FAILED",
        "PROTECTION_FAILED", "EXECUTION_ERROR"
    )

def reason_ar(r):
    if _is_tp_reason(r): return "✅ تيك بروفت"
    if _is_be_reason(r): return "➡️ بريك ايفن"
    if _is_sl_reason(r): return "❌ ستوب/حارس"
    if _reason_key(r) == "CANCEL_PENDING": return "🚫 إلغاء انتظار"
    return r or "—"

def calc_trade_metrics(trades):
    total = len(trades)
    total_pnl = sum(float(t.get("pnl") or 0) for t in trades)
    wins = [t for t in trades if float(t.get("pnl") or 0) > 0]
    losses = [t for t in trades if float(t.get("pnl") or 0) < 0]
    tp = [t for t in trades if _is_tp_reason(t.get("exit_reason"))]
    be = [t for t in trades if _is_be_reason(t.get("exit_reason"))]
    sl = [t for t in trades if _is_sl_reason(t.get("exit_reason"))]
    gross_profit = sum(float(t.get("pnl") or 0) for t in wins)
    gross_loss = abs(sum(float(t.get("pnl") or 0) for t in losses))
    rr_values = [float(t.get("rr_actual")) for t in trades if t.get("rr_actual") is not None]
    durations = [int(t.get("duration_min")) for t in trades if t.get("duration_min") is not None]
    return {
        "total": total,
        "total_pnl": total_pnl,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": (len(wins) / total * 100.0) if total else None,
        "profit_factor": (gross_profit / gross_loss) if gross_loss > 0 else None,
        "tp": len(tp), "be": len(be), "sl": len(sl),
        "avg_win": (gross_profit / len(wins)) if wins else None,
        "avg_loss": (-gross_loss / len(losses)) if losses else None,
        "avg_rr": (sum(rr_values) / len(rr_values)) if rr_values else None,
        "avg_duration": (sum(durations) / len(durations)) if durations else None,
        "best": max([float(t.get("pnl") or 0) for t in trades], default=None),
        "worst": min([float(t.get("pnl") or 0) for t in trades], default=None),
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
    }

def stat_card(title, value, sub="", cls=""):
    return f'''<div class="stat"><div class="stat-val {cls}">{value}</div><div class="stat-lbl">{title}</div><div class="stat-sub">{sub}</div></div>'''

def metric_row(label, value, cls=""):
    return f'''<div class="row"><span class="label">{label}</span><span class="val {cls}">{value}</span></div>'''

@app.route("/", methods=["GET"])
def dashboard():
    mode_txt = '🧪 Testnet' if USE_TESTNET else '🔴 LIVE'
    now = app_now()
    trades = load_trades(300)
    signals = load_signal_events(20)

    today = [t for t in trades if trade_dt(t, "close_time") and to_app_time(trade_dt(t, "close_time")) and to_app_time(trade_dt(t, "close_time")).date() == now.date()]
    m_all = calc_trade_metrics(trades)
    m_today = calc_trade_metrics(today)

    active_symbols = [sym for sym, st in states.items() if st.get("in_trade") or st.get("pending")]
    active_count = len([sym for sym in active_symbols if states[sym].get("in_trade")])
    pending_count = len([sym for sym in active_symbols if states[sym].get("pending")])
    pf_value = "∞" if m_all["profit_factor"] is None and m_all["gross_profit"] > 0 else fmt_num(m_all["profit_factor"], 2)

    top_stats = "".join([
        stat_card("Net P&L", fmt_money(m_all["total_pnl"]), f"Today {fmt_money(m_today['total_pnl'])}", "green" if m_all["total_pnl"] >= 0 else "red"),
        stat_card("Trades", str(m_all["total"]), f"Today {m_today['total']}", ""),
        stat_card("Win Rate", pct(m_all["win_rate"], 1), f"W {m_all['wins']} / L {m_all['losses']}", ""),
        stat_card("PF", pf_value, "Profit factor", ""),
        stat_card("TP/BE/SL", f"{m_all['tp']}/{m_all['be']}/{m_all['sl']}", "خروج الصفقات", ""),
        stat_card("Avg R", fmt_num(m_all["avg_rr"], 2), "based on initial SL", ""),
        stat_card("Open/Pending", f"{active_count}/{pending_count}", "نشط / انتظار", "yellow" if active_count or pending_count else ""),
    ])

    active_rows = ""
    for sym in active_symbols:
        st = states[sym]
        entry = safe_float(st.get("entry_price")); initial_sl = safe_float(st.get("initial_sl") or st.get("backup_sl")); current_sl = safe_float(st.get("current_sl") or st.get("backup_sl")); tp = safe_float(st.get("tp_price")); qty = safe_float(st.get("bot_qty"), 0.0)
        current = get_current_price(sym)
        risk_cash = (entry - initial_sl) * qty if entry is not None and initial_sl is not None and qty else None
        live_pnl = (current - entry) * qty if current is not None and entry is not None and qty else None
        live_r = live_pnl / risk_cash if live_pnl is not None and risk_cash and risk_cash > 0 else None
        status = "OPEN" if st.get("in_trade") else "PENDING"
        cls = "green" if st.get("in_trade") else "yellow"
        active_rows += f'''<tr><td><b>{escape(sym)}</b></td><td class="{cls}">{status}</td><td>{fmt_num(current,8)}</td><td>{fmt_num(entry,8)}</td><td class="red">{fmt_num(initial_sl,8)}</td><td class="yellow">{fmt_num(current_sl,8)}</td><td class="green">{fmt_num(tp,8)}</td><td>{fmt_num(qty,8)}</td><td class="{'green' if (live_pnl or 0)>=0 else 'red'}">{fmt_money(live_pnl)}</td><td>{fmt_num(live_r,2)}R</td><td>{escape(str(st.get('last_action') or '—'))}</td></tr>'''
    if not active_rows:
        active_rows = '<tr><td colspan="11" class="empty">لا توجد صفقات نشطة الآن</td></tr>'

    by_symbol = {}
    for t in trades:
        by_symbol.setdefault(t.get("symbol") or "?", []).append(t)
    symbol_rows = ""
    for sym, rows_t in sorted(by_symbol.items(), key=lambda kv: calc_trade_metrics(kv[1])["total_pnl"], reverse=True)[:10]:
        m = calc_trade_metrics(rows_t); cls = "green" if m["total_pnl"] >= 0 else "red"
        symbol_rows += f'''<tr><td><b>{escape(str(sym))}</b></td><td>{m['total']}</td><td class="{cls}">{fmt_money(m['total_pnl'])}</td><td>{pct(m['win_rate'],1)}</td><td>{fmt_num(m['profit_factor'],2) if m['profit_factor'] is not None else '∞'}</td><td>{m['tp']}/{m['be']}/{m['sl']}</td><td>{fmt_num(m['avg_rr'],2)}</td></tr>'''
    if not symbol_rows:
        symbol_rows = '<tr><td colspan="7" class="empty">لا توجد صفقات مغلقة بعد</td></tr>'

    trade_rows = ""
    for t in trades[:25]:
        pnl_val = safe_float(t.get("pnl"), 0.0); cls = "green" if pnl_val >= 0 else "red"; ct = to_app_time(trade_dt(t, "close_time")); ct_str = ct.strftime("%m-%d %H:%M") if ct else "—"
        init_sl = t.get('initial_sl_price') if t.get('initial_sl_price') is not None else t.get('sl_price')
        cur_sl = t.get('current_sl_price') if t.get('current_sl_price') is not None else t.get('sl_price')
        trade_rows += f'''<tr><td>{ct_str}</td><td><b>{escape(str(t.get('symbol') or '—'))}</b></td><td>{reason_ar(t.get('exit_reason'))}</td><td>{fmt_num(t.get('entry_price'),8)}</td><td>{fmt_num(t.get('exit_price'),8)}</td><td class="red">{fmt_num(init_sl,8)}</td><td class="yellow">{fmt_num(cur_sl,8)}</td><td class="green">{fmt_num(t.get('tp_price'),8)}</td><td class="{cls}">{fmt_money(pnl_val)}</td><td>{fmt_num(t.get('rr_actual'),2)}R</td><td>{t.get('duration_min') or '—'}د</td></tr>'''
    if not trade_rows:
        trade_rows = '<tr><td colspan="11" class="empty">لا توجد صفقات مغلقة بعد</td></tr>'

    signal_rows = ""
    for s in signals[:12]:
        status = str(s.get("status") or ""); cls = "green" if status in ("ok","success") else "yellow" if status in ("ignored","مكرر","duplicate") else "red" if status == "error" else ""; rt = to_app_time(safe_dt(s.get("received_at"))); rt_str = rt.strftime("%H:%M:%S") if rt else "—"
        signal_rows += f'''<tr><td>{rt_str}</td><td><b>{escape(str(s.get('symbol') or '—'))}</b></td><td>{escape(str(s.get('action') or '—'))}</td><td class="{cls}">{escape(status or '—')}</td><td>{fmt_num(s.get('entry_price'),8)}</td><td>{fmt_num(s.get('qty'),8)}</td><td>{escape(str(s.get('reason') or '—'))}</td></tr>'''
    if not signal_rows:
        signal_rows = '<tr><td colspan="7" class="empty">لا توجد إشارات بعد</td></tr>'

    html_page = f'''<!DOCTYPE html><html dir="rtl" lang="ar"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta http-equiv="refresh" content="10">
<title>Renko Bot V10 Fast</title><style>
*{{box-sizing:border-box;margin:0;padding:0}}body{{background:#080910;color:#e6e6ee;font-family:Arial,Tahoma,sans-serif;padding:14px;font-size:12px}}h1{{color:#00ff88;font-size:18px;margin-bottom:4px}}h2{{color:#9ea0ff;font-size:13px;margin:6px 0 10px}}.sub{{color:#888;font-size:11px;margin-bottom:12px}}.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:8px;margin-bottom:12px}}.stat{{background:#0d0e17;border:1px solid #24263a;border-radius:8px;padding:9px;text-align:center;min-height:62px}}.stat-val{{font-size:15px;font-weight:800;color:#fff}}.stat-lbl{{font-size:10px;color:#aaa;margin-top:3px}}.stat-sub{{font-size:9px;color:#666;margin-top:3px}}.card{{background:#12131d;border:1px solid #24263a;border-radius:9px;padding:10px;margin-bottom:10px}}.table-wrap{{overflow-x:auto;border-radius:7px;border:1px solid #24263a}}table{{width:100%;border-collapse:collapse;font-size:11px;min-width:760px}}th{{background:#1a1c2b;color:#9ea0ff;padding:7px;text-align:right;white-space:nowrap}}td{{padding:6px 7px;border-bottom:1px solid #202235;white-space:nowrap}}.green{{color:#00ff88!important}}.red{{color:#ff4d6d!important}}.yellow{{color:#ffd166!important}}.empty{{color:#777;text-align:center;padding:14px}}.footer{{color:#555;text-align:center;font-size:10px;margin-top:10px}}
</style></head><body>
<h1>⚡ HULL BOT · Binance Dashboard</h1><div class="sub">{mode_txt} · Binance Spot · تحديث كل 10 ثواني · توقيت الإمارات · مختصر للتحليل السريع</div><div class="grid">{top_stats}</div>
<div class="card"><h2>الصفقات النشطة</h2><div class="table-wrap"><table><tr><th>عملة</th><th>حالة</th><th>السعر</th><th>دخول</th><th>Initial SL</th><th>Current SL</th><th>TP</th><th>Qty</th><th>Live P&L</th><th>Live R</th><th>آخر إجراء</th></tr>{active_rows}</table></div></div>
<div class="card"><h2>الأداء حسب العملة</h2><div class="table-wrap"><table><tr><th>عملة</th><th>Trades</th><th>Net</th><th>Win%</th><th>PF</th><th>TP/BE/SL</th><th>Avg R</th></tr>{symbol_rows}</table></div></div>
<div class="card"><h2>آخر الصفقات</h2><div class="table-wrap"><table><tr><th>وقت</th><th>عملة</th><th>نتيجة</th><th>دخول</th><th>خروج</th><th>Initial SL</th><th>Current SL</th><th>TP</th><th>P&L</th><th>R</th><th>مدة</th></tr>{trade_rows}</table></div></div>
<div class="card"><h2>آخر إشارات Webhook</h2><div class="table-wrap"><table><tr><th>وقت</th><th>عملة</th><th>Action</th><th>Status</th><th>Entry</th><th>Qty</th><th>Reason</th></tr>{signal_rows}</table></div></div>
<p class="footer">V10 Fast ACK · UAE Time · R محسوب من Initial SL للصفقات الجديدة فقط</p></body></html>'''
    return html_page

# ====================================================================
# التشغيل
# ====================================================================
def startup():
    global _initialized
    if _initialized:
        return
    _initialized = True
    init_db()
    recovered = load_all_states()
    if recovered:
        states.update(recovered)
        log.info(f"✅ استعادة {len(recovered)} حالة")
    log.info(f"🔧 MODE: {'TESTNET' if USE_TESTNET else 'LIVE'}")
    monitor_thread = threading.Thread(target=monitor_pending_orders, daemon=True)
    monitor_thread.start()
    signal_thread = threading.Thread(target=signal_worker, daemon=True)
    signal_thread.start()

# Start workers during Gunicorn boot, not on the first TradingView webhook.
startup()

@app.before_request
def before_first_request():
    # Already started during boot; this remains only as a safety no-op.
    startup()

if __name__ == "__main__":
    startup()
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
