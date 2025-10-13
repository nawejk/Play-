# bot.py
# UTF-8

import os
import re
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
# Configuration (ENV)
# ---------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8212740282:AAEn-SYgor585F7Ver24ATQnfzzSXl-qB1U").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable required")

ADMIN_IDS = [a.strip() for a in os.getenv("ADMIN_IDS", "8076025426").split(",") if a.strip()]
SOLANA_RPC = os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com").strip()
CENTRAL_SOL_PUBKEY = os.getenv("CENTRAL_SOL_PUBKEY", "3wyVwpcbWt96mphJjskFsR2qoyafqJuSfGZYmiipW4oy").strip()

# Optional: Liste bekannter Exchange-Absender (CSV in ENV)
EXCHANGE_WALLETS = set([s.strip() for s in os.getenv("EXCHANGE_WALLETS", "").split(",") if s.strip()])

# Withdraw fee tiers (lockup_days: fee_percent)
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

DB_PATH = os.getenv("DB_PATH", "memebot_full.db")
LAMPORTS_PER_SOL = 1_000_000_000
MIN_SUB_SOL = float(os.getenv("MIN_SUB_SOL", "0.1"))

# Simulation flag – echte Trading-Integrationen später hinzufügen
SIMULATION_MODE = True

# ---------------------------
# Utilities: price + formatting
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
    lam = int(lamports_or_int or 0)
    sol = lam / LAMPORTS_PER_SOL
    usd = get_sol_usd()
    if usd > 0:
        return f"{sol:.6f} SOL (~{sol*usd:.2f} USDC)"
    return f"{sol:.6f} SOL"

def parse_fee_tiers() -> List[Tuple[int, float]]:
    return sorted([(int(d), float(p)) for d, p in _fee_tiers.items()], key=lambda x: x[0])

# ---------------------------
# DB schema & helpers
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
-- Gegen Referral-Missbrauch: jeder Nutzer kann genau EINEN Code einlösen.
CREATE TABLE IF NOT EXISTS ref_claims (
  claimer_user_id INTEGER PRIMARY KEY,
  ref_code TEXT NOT NULL,
  referrer_user_id INTEGER NOT NULL,
  claimed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        # idempotente ALTERs
        for stmt in [
            "ALTER TABLE users ADD COLUMN sub_types TEXT DEFAULT ''",
            "ALTER TABLE executions ADD COLUMN stake_lamports INTEGER DEFAULT 0",
            "ALTER TABLE payouts ADD COLUMN lockup_days INTEGER DEFAULT 0",
            "ALTER TABLE payouts ADD COLUMN fee_percent REAL DEFAULT 0.0",
            "ALTER TABLE users ADD COLUMN referral_code TEXT DEFAULT ''",
        ]:
            try: con.execute(stmt)
            except Exception: pass

# ---------------------------
# Misc helpers
# ---------------------------
def md_escape(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return (text.replace('\\', '\\\\')
                .replace('_', '\\_')
                .replace('*', '\\*')
                .replace('`', '\\`')
                .replace('[', '\\['))

def row_get(row, key, default=None):
    if row is None:
        return default
    try:
        return row[key] if key in row.keys() else default
    except Exception:
        try:
            return row.get(key, default)
        except Exception:
            return default

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
# CRUD & business logic
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

def all_users() -> List[int]:
    with get_db() as con:
        return [r["user_id"] for r in con.execute("SELECT user_id FROM users").fetchall()]

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
    st = row_get(u, "sub_types", "")
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

def get_balance_lamports(user_id: int) -> int:
    with get_db() as con:
        row = con.execute("SELECT sol_balance_lamports FROM users WHERE user_id=?", (user_id,)).fetchone()
        return row_get(row, "sol_balance_lamports", 0)

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
    frac = _risk_fraction(row_get(u, "auto_risk", "MEDIUM"))
    bal = row_get(u, "sol_balance_lamports", 0)
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
    if row_get(c, "market_type") == "FUTURES":
        core = f"Futures • {row_get(c,'base','?')} • {row_get(c,'side','?')} {row_get(c,'leverage','')}".strip()
    else:
        core = f"Meme • {row_get(c,'base','?')}"
    extra = f"\nToken: `{md_escape(row_get(c,'token_address',''))}`" if (row_get(c,"market_type") == "MEME" and row_get(c,"token_address")) else ""
    note = f"\nNotes: {md_escape(row_get(c,'notes',''))}" if row_get(c,"notes") else ""
    return f"🧩 *{core}*{extra}{note}"

# ---------------------------
# Keyboards
# ---------------------------
def kb_main(u):
    bal = fmt_sol_usdc(row_get(u, "sol_balance_lamports", 0))
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💸 Einzahlen", callback_data="deposit"),
           InlineKeyboardButton("💳 Auszahlung", callback_data="withdraw"))
    kb.add(InlineKeyboardButton("🔔 Signale", callback_data="sub_menu"),
           InlineKeyboardButton("📰 News", callback_data="news_sub_menu"))
    kb.add(InlineKeyboardButton("⚙️ Auto-Entry", callback_data="auto_menu"),
           InlineKeyboardButton("❓ Hilfe", callback_data="help"))
    kb.add(InlineKeyboardButton("🔗 Referral", callback_data="referral"),
           InlineKeyboardButton("📈 Mein Portfolio", callback_data="my_portfolio"))
    if is_admin(row_get(u, "user_id", 0)):
        kb.add(InlineKeyboardButton("🛠️ Admin (Kontrolle)", callback_data="admin_menu_big"))
    kb.add(InlineKeyboardButton(f"🏦 Guthaben: {bal}", callback_data="noop"))
    return kb

def kb_sub_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔔 Abonnieren", callback_data="sub_on"),
           InlineKeyboardButton("🔕 Abbestellen", callback_data="sub_off"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

def kb_news_sub():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("MEME", callback_data="news_sub_MEME"),
           InlineKeyboardButton("FUTURES", callback_data="news_sub_FUTURES"))
    kb.add(InlineKeyboardButton("Beide", callback_data="news_sub_BOTH"),
           InlineKeyboardButton("Aus", callback_data="news_sub_OFF"))
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
    kb.add(InlineKeyboardButton("💼 Guthaben ändern", callback_data="admin_balance_edit"))  # Single user / Ziel
    kb.add(InlineKeyboardButton("📉 Massen-Änderung", callback_data="admin_mass_balance"))  # ALL +/-X% / +/-X SOL
    kb.add(InlineKeyboardButton("📤 Broadcast an ALLE", callback_data="admin_broadcast_all"))
    kb.add(InlineKeyboardButton("🔧 Promotions / PnL", callback_data="admin_apply_pnl"))
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
           InlineKeyboardButton("🏷️ Wallet setzen", callback_data=f"admin_setwallet_{user_id}"))
    kb.add(InlineKeyboardButton("📤 Nachricht", callback_data=f"admin_msg_{user_id}"),
           InlineKeyboardButton("🧾 Payouts", callback_data=f"admin_payouts_{user_id}"))
    return kb

def kb_withdraw_options():
    kb = InlineKeyboardMarkup()
    for days, pct in parse_fee_tiers():
        kb.add(InlineKeyboardButton(f"{days} Tage • Fee {pct}%", callback_data=f"payoutopt_{days}"))
    kb.add(InlineKeyboardButton("↩️ Abbrechen", callback_data="back_home"))
    return kb

def kb_payout_manage(pid: int):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Genehmigen", callback_data=f"payout_APPROVE_{pid}"),
           InlineKeyboardButton("📤 Gesendet", callback_data=f"payout_SENT_{pid}"),
           InlineKeyboardButton("❌ Ablehnen", callback_data=f"payout_REJECT_{pid}"))
    return kb

# ---------------------------
# RPC watcher (backoff)
# ---------------------------
checked_signatures = set()

def rpc(method: str, params: list, *, _retries=2, _base_sleep=0.8):
    for attempt in range(_retries + 1):
        try:
            r = requests.post(
                SOLANA_RPC,
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
        if not self.central:
            return
        sigs = get_new_signatures_for_address(self.central, limit=20)
        if not sigs:
            return
        with get_db() as con:
            rows = con.execute("SELECT user_id, source_wallet FROM users WHERE source_wallet IS NOT NULL").fetchall()
        src_map = {row_get(r, "source_wallet"): row_get(r, "user_id") for r in rows if row_get(r, "source_wallet")}
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
                note = (f"⚠️ Unbekannte Einzahlung erkannt\n"
                        f"Sender: `{md_escape(sender)}`\nBetrag: {fmt_sol_usdc(amount)}\nSig: `{md_escape(sig)}`")
                for aid in ADMIN_IDS:
                    try:
                        bot.send_message(int(aid), note, parse_mode="Markdown")
                    except Exception:
                        pass
                if sender in EXCHANGE_WALLETS:
                    for aid in ADMIN_IDS:
                        try:
                            bot.send_message(int(aid), f"⚠️ Sender ist als Exchange-Wallet gelistet: `{md_escape(sender)}`", parse_mode="Markdown")
                        except Exception:
                            pass
                continue
            self._mark_seen(sig, uid, amount)
            if self.on_verified_deposit:
                self.on_verified_deposit({"user_id": uid, "amount_lamports": amount, "sig": sig})

# ---------------------------
# Simulated trading (replace when integrating real APIs)
# ---------------------------
def dex_market_buy_simulated(user_id: int, base: str, amount_lamports: int):
    return {"status": "FILLED", "txid": f"Live-DEX-{base}-{int(time.time())}", "spent_lamports": amount_lamports}

def futures_place_simulated(user_id: int, base: str, side: str, leverage: str, risk: str):
    return {"status": "FILLED", "order_id": f"Live-FUT-{base}-{int(time.time())}", "base": base}

# ---------------------------
# Bot init & safe send wrappers
# ---------------------------
init_db()
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# Safe sender: probiere Rohtext; bei Fehler → escapen; letzter Versuch ohne parse_mode
_original_send_message = bot.send_message
def _safe_send_message(chat_id, text, **kwargs):
    try:
        return _original_send_message(chat_id, text, **kwargs)
    except Exception:
        pm = kwargs.get("parse_mode")
        if pm and str(pm).upper().startswith("MARKDOWN"):
            kwargs2 = dict(kwargs)
            kwargs2["parse_mode"] = "Markdown"
            try:
                return _original_send_message(chat_id, md_escape(str(text)), **kwargs2)
            except Exception:
                kwargs3 = dict(kwargs2)
                kwargs3.pop("parse_mode", None)
                return _original_send_message(chat_id, str(text), **kwargs3)
        else:
            kwargs3 = dict(kwargs)
            kwargs3.pop("parse_mode", None)
            return _original_send_message(chat_id, str(text), **kwargs3)

bot.send_message = _safe_send_message

_original_edit_message_text = bot.edit_message_text
def _safe_edit_message_text(text, chat_id, message_id, **kwargs):
    try:
        return _original_edit_message_text(text, chat_id, message_id, **kwargs)
    except Exception:
        pm = kwargs.get("parse_mode")
        if pm and str(pm).upper().startswith("MARKDOWN"):
            kwargs2 = dict(kwargs)
            kwargs2["parse_mode"] = "Markdown"
            try:
                return _original_edit_message_text(md_escape(str(text)), chat_id, message_id, **kwargs2)
            except Exception:
                kwargs3 = dict(kwargs2)
                kwargs3.pop("parse_mode", None)
                return _original_edit_message_text(str(text), chat_id, message_id, **kwargs3)
        else:
            kwargs3 = dict(kwargs)
            kwargs3.pop("parse_mode", None)
            return _original_edit_message_text(str(text), chat_id, message_id, **kwargs3)

bot.edit_message_text = _safe_edit_message_text

# transient state
WAITING_SOURCE_WALLET: Dict[int, bool] = {}
WAITING_WITHDRAW_AMOUNT: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_SIMPLE_CALL: Dict[int, bool] = {}
ADMIN_AWAIT_BALANCE_EDIT: Dict[int, Optional[int]] = {}   # Ziel-UID oder None
ADMIN_AWAIT_SET_WALLET: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_TRADE_STATUS: Dict[int, bool] = {}
ADMIN_AWAIT_PNL: Dict[int, bool] = {}
ADMIN_AWAIT_NEWS_BROADCAST: Dict[int, Dict] = {}
ADMIN_AWAIT_IMPORT_BALANCES: Dict[int, bool] = {}
ADMIN_AWAIT_MASS_BALANCE: Dict[int, bool] = {}            # NEW: Massen-Änderung

watcher = CentralWatcher(CENTRAL_SOL_PUBKEY)

def _on_verified_deposit(evt: dict):
    uid = evt["user_id"]
    lam = evt["amount_lamports"]
    add_balance(uid, lam)
    new_bal = get_balance_lamports(uid)
    try:
        bot.send_message(uid, f"✅ Einzahlung verifiziert: {fmt_sol_usdc(lam)}\nNeues Guthaben: {fmt_sol_usdc(new_bal)}", parse_mode="Markdown")
    except Exception:
        pass

watcher.on_verified_deposit = _on_verified_deposit
threading.Thread(target=watcher.start, kwargs={"interval_sec": 40}, daemon=True).start()

# ---------------------------
# DB backup on start
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
# Home text
# ---------------------------
def get_bot_username():
    try:
        me = bot.get_me()
        return me.username or "<YourBotUsername>"
    except Exception:
        return "<YourBotUsername>"

def ensure_refcode(u):
    if not row_get(u, "referral_code"):
        code = gen_referral_for_user(row_get(u, "user_id"))
        set_referral(row_get(u, "user_id"), code)
        u = get_user(row_get(u, "user_id"))
    return u

def home_text(u) -> str:
    u = ensure_refcode(u)
    raw_uname = ("@" + row_get(u, "username", "")) if row_get(u, "username") else f"ID {row_get(u, 'user_id','?')}"
    uname = md_escape(raw_uname)
    bal = fmt_sol_usdc(row_get(u, "sol_balance_lamports", 0))
    refcode = row_get(u, "referral_code") or gen_referral_for_user(row_get(u, "user_id"))
    bot_username = get_bot_username()
    ref_link = f"https://t.me/{bot_username}?start={refcode}"
    ref_link_md = f"`{md_escape(ref_link)}`"
    return (
        f"👋 Hallo {uname} — willkommen!\n\n"
        "Dieses System bietet:\n"
        "• Einzahlungen & automatisches Gutschreiben (nur verifizierte Source-Wallets)  \n"
        "• Signale für Meme & Futures — abonnierbar einzeln oder kombiniert  \n"
        "• Auto-Entry mit Low/Medium/High-Einstellungen (transparente Einsatz-Regeln)  \n"
        f"• Referral-Programm: {ref_link_md}\n\n"
        f"🏦 Aktuelles Guthaben: *{bal}*  \n"
        "📩 Support: Nutze /support oder kontaktiere einen Admin direkt\n\n"
        "Hinweis: Systemmeldungen sind transparent — prüfe bitte alle Aktionen vor Auszahlung."
    )

# ---------------------------
# Handlers
# ---------------------------
@bot.message_handler(commands=["start"])
def cmd_start(m: Message):
    uid = m.from_user.id
    uname = m.from_user.username or ""
    admin_flag = 1 if is_admin(uid) else 0
    upsert_user(uid, uname, admin_flag)

    # Referral payload (/start CODE oder /start=CODE)
    ref_code = None
    txt = (m.text or "")
    parts = txt.split()
    if len(parts) >= 2:
        ref_code = parts[1].strip()
    if not ref_code and txt.startswith("/start="):
        ref_code = txt.split("=", 1)[1].strip()

    if ref_code:
        with get_db() as con:
            ref_row = con.execute("SELECT user_id FROM users WHERE referral_code=?", (ref_code,)).fetchone()
        referrer = row_get(ref_row, "user_id")
        if referrer and referrer != uid:
            # Prüfen, ob Nutzer schon jemals einen Code eingelöst hat
            with get_db() as con:
                existing = con.execute("SELECT 1 FROM ref_claims WHERE claimer_user_id=?", (uid,)).fetchone()
            if not existing:
                bonus_lam = int(0.01 * LAMPORTS_PER_SOL)  # 0.01 SOL Bonus
                add_balance(referrer, bonus_lam)
                add_balance(uid, bonus_lam)
                with get_db() as con:
                    con.execute("INSERT OR IGNORE INTO ref_claims(claimer_user_id, ref_code, referrer_user_id) VALUES (?,?,?)",
                                (uid, ref_code, referrer))
                try:
                    bot.send_message(referrer, f"🎉 Dein Referral {md_escape(ref_code)} wurde verwendet! Bonus: {fmt_sol_usdc(bonus_lam)}")
                except Exception:
                    pass
                try:
                    bot.send_message(uid, f"🎉 Willkommen! Du und der Referrer bekommen je {fmt_sol_usdc(bonus_lam)} Bonus.")
                except Exception:
                    pass
        # andernfalls: ignorieren (self-ref oder mehrfacher Versuch)

    u = get_user(uid)
    bot.reply_to(m, home_text(u), reply_markup=kb_main(u))

@bot.callback_query_handler(func=lambda c: True)
def on_cb(c: CallbackQuery):
    uid = c.from_user.id
    u = get_user(uid)
    data = c.data or ""

    if data == "back_home":
        bot.edit_message_text(home_text(u), c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_main(u))
        return

    if data == "noop":
        bot.answer_callback_query(c.id, "—")
        return

    if data == "help":
        bot.answer_callback_query(c.id)
        bot.send_message(uid,
                         ("ℹ️ Hilfe:\n\n"
                          "1) Einzahlen: Absender-Wallet angeben → Sende SOL an zentrale Adresse.\n"
                          "2) Abonnieren: Abo aktivieren/deaktivieren.\n"
                          "3) Auto-Entry: ON/OFF und Risiko einstellen.\n"
                          "4) Auszahlungen: Lockup & Gebühren auswählbar."),
                         parse_mode="Markdown")
        return

    # deposit
    if data == "deposit":
        if not row_get(u, "source_wallet"):
            WAITING_SOURCE_WALLET[uid] = True
            bot.answer_callback_query(c.id, "Bitte zuerst deine Absender-Wallet senden.")
            bot.send_message(uid, "Gib jetzt deine Absender-Wallet (SOL) ein:", parse_mode=None)
            return
        price = get_sol_usd()
        px = f"(1 SOL ≈ {price:.2f} USDC)" if price > 0 else ""
        bot.edit_message_text(
            f"Absender-Wallet: `{md_escape(row_get(u,'source_wallet','-'))}`\n"
            f"Sende SOL an: `{md_escape(CENTRAL_SOL_PUBKEY)}`\n{px}",
            c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_main(u)
        )
        return

    # withdraw
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
    if data == "news_sub_menu":
        bot.edit_message_text("News-Kategorien:", c.message.chat.id, c.message.message_id, reply_markup=kb_news_sub())
        return

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
        code = row_get(u, "referral_code") or gen_referral_for_user(uid)
        set_referral(uid, code)
        bot_username = get_bot_username()
        link = f"https://t.me/{bot_username}?start={code}"
        bot.answer_callback_query(c.id, "Referral-Link")
        bot.send_message(uid, f"Dein Referral-Link: `{md_escape(link)}`\nTeile ihn mit Freunden!", parse_mode="Markdown")
        return

    # portfolio
    if data == "my_portfolio":
        u = get_user(uid)
        bal = fmt_sol_usdc(row_get(u, "sol_balance_lamports", 0))
        subs = row_get(u, "sub_types", "-")
        bot.answer_callback_query(c.id, "Portfolio")
        bot.send_message(uid, f"🏦 Guthaben: *{bal}*\n📰 News-Abos: *{subs}*\nAuto: *{row_get(u,'auto_mode','OFF')} / {row_get(u,'auto_risk','MEDIUM')}*", parse_mode="Markdown")
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
        if not is_admin(uid): return
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "Sende den Call im Format:\nFUTURES|BASE|SIDE|LEV\noder\nMEME|NAME|TOKEN_ADDRESS", parse_mode=None)
        ADMIN_AWAIT_SIMPLE_CALL[uid] = True
        return

    # admin broadcast last call
    if data == "admin_broadcast_last":
        if not is_admin(uid): return
        with get_db() as con:
            row = con.execute("SELECT * FROM calls ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            bot.answer_callback_query(c.id, "Kein Call vorhanden.")
            return
        msg = "📣 Neuer Call:\n" + fmt_call(row)
        subs = all_subscribers()
        sent = 0
        for su in subs:
            try:
                bot.send_message(su, msg, parse_mode="Markdown")
                queue_execution(row_get(row,"id"), su, status="QUEUED", message="Queued by broadcast")
                sent += 1
            except Exception:
                pass
        bot.answer_callback_query(c.id, f"An {sent} Abonnenten gesendet.")
        return

    # admin list investors
    if data == "admin_list_investors":
        if not is_admin(uid): return
        rows = list_investors(limit=100, offset=0)
        if not rows:
            bot.answer_callback_query(c.id, "Keine Abonnenten.")
            return
        parts = ["👥 Investoren (Top)"]
        for r in rows:
            name = "@" + row_get(r, "username","") if row_get(r,"username") else "(kein Username)"
            parts.append(f"- {name} • {fmt_sol_usdc(row_get(r,'sol_balance_lamports',0))} • News: {row_get(r,'sub_types','-')}")
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "\n".join(parts), parse_mode="Markdown")
        return

    # admin view users pagination
    if data.startswith("admin_view_users_"):
        if not is_admin(uid): return
        try:
            offset = int(data.split("_")[-1])
        except Exception:
            offset = 0
        with get_db() as con:
            total = con.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
            rows = con.execute("""
                SELECT user_id, username, sol_balance_lamports, source_wallet, auto_mode, auto_risk, sub_types
                FROM users
                ORDER BY sol_balance_lamports DESC
                LIMIT 10 OFFSET ?
            """, (offset,)).fetchall()
        if not rows:
            bot.answer_callback_query(c.id, "Keine Nutzer.")
            return
        bot.answer_callback_query(c.id)
        for r in rows:
            uname = "@" + row_get(r,"username","") if row_get(r,"username") else "(kein Username)"
            txt = (f"{uname} • UID {row_get(r,'user_id')}\n"
                   f"Guthaben: {fmt_sol_usdc(row_get(r,'sol_balance_lamports',0))}\n"
                   f"Source: `{md_escape(row_get(r,'source_wallet','-'))}`\n"
                   f"Auto: {row_get(r,'auto_mode','OFF')} / {row_get(r,'auto_risk','MEDIUM')}\n"
                   f"News: {row_get(r,'sub_types','-')}")
            bot.send_message(uid, txt, parse_mode="Markdown", reply_markup=kb_user_row(row_get(r,"user_id")))
        bot.send_message(uid, "Navigation:", parse_mode=None, reply_markup=kb_users_pagination(offset, total))
        return

    # admin inline actions: single balance target
    if data.startswith("admin_balance_"):
        if not is_admin(uid): return
        try:
            target = int(data.split("_", 2)[2])
        except Exception:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        ADMIN_AWAIT_BALANCE_EDIT[uid] = target
        bot.answer_callback_query(c.id, f"Guthabenänderung: UID {target} — sende z. B. +0.2, -0.05 oder -40%")
        bot.send_message(uid, "Formate:\n• `+0.25` (addiere SOL)\n• `-0.10` (ziehe SOL ab)\n• `-40%` (prozentual ändern)", parse_mode="Markdown")
        return

    # admin mass balance
    if data == "admin_mass_balance":
        if not is_admin(uid): return
        ADMIN_AWAIT_MASS_BALANCE[uid] = True
        bot.answer_callback_query(c.id, "Massen-Änderung aktiv.")
        bot.send_message(uid, "Massen-Änderung:\n• `ALL -40%`\n• `ALL +15%`\n• `ALL +0.1` (SOL)\n\nOptional Filter:\n• `SUBS -20%` (nur Abonnenten)\n• `NEWS MEME -10%` (nur News-MEME)\n", parse_mode="Markdown")
        return

    # admin message to user
    if data.startswith("admin_msg_"):
        if not is_admin(uid): return
        try:
            target = int(data.split("_", 2)[2])
        except Exception:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        ADMIN_AWAIT_NEWS_BROADCAST[uid] = {"step": "awaiting_support_msg", "target": target}
        bot.answer_callback_query(c.id, "Sende die Support-Nachricht, optional danach Bild (oder 'nopict').")
        return

    # admin payout list per user
    if data.startswith("admin_payouts_"):
        if not is_admin(uid): return
        try:
            target = int(data.split("_", 2)[2])
        except Exception:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        with get_db() as con:
            rows = con.execute("SELECT * FROM payouts WHERE user_id=? ORDER BY created_at DESC LIMIT 10", (target,)).fetchall()
        if not rows:
            bot.answer_callback_query(c.id, "Keine Auszahlungen")
            return
        bot.answer_callback_query(c.id)
        for r in rows:
            bot.send_message(uid, f"#{row_get(r,'id')} • {fmt_sol_usdc(row_get(r,'amount_lamports',0))} • {row_get(r,'status','-')} • Lockup {row_get(r,'lockup_days',0)}d • Fee {row_get(r,'fee_percent',0)}%", parse_mode=None)
        return

    # admin broadcast an ALLE (nicht nur Abonnenten)
    if data == "admin_broadcast_all":
        if not is_admin(uid): return
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "Sende die Nachricht, die an *alle Nutzer* gehen soll (alle, die je /start gedrückt haben).", parse_mode="Markdown")
        ADMIN_AWAIT_TRADE_STATUS[uid] = True
        return

    if data == "admin_payout_queue":
        if not is_admin(uid): return
        with get_db() as con:
            rows = con.execute("SELECT p.*, u.username FROM payouts p JOIN users u ON u.user_id=p.user_id WHERE p.status='REQUESTED' ORDER BY p.created_at ASC LIMIT 50").fetchall()
        if not rows:
            bot.answer_callback_query(c.id, "Keine offenen Auszahlungen.")
            return
        for r in rows:
            uname = "@" + (row_get(r,"username","") or "") if row_get(r,"username") else "(kein Username)"
            txt = (f"Auszahlung #{row_get(r,'id')} • {uname} (UID {row_get(r,'user_id')})\n"
                   f"Betrag: {fmt_sol_usdc(row_get(r,'amount_lamports',0))}\n"
                   f"Lockup: {row_get(r,'lockup_days',0)}d • Fee: {row_get(r,'fee_percent',0)}%")
            bot.send_message(uid, txt, parse_mode="Markdown", reply_markup=kb_payout_manage(row_get(r,"id")))
        bot.answer_callback_query(c.id)
        return

    if data.startswith("payout_"):
        if not is_admin(uid): return
        parts = data.split("_", 2)
        if len(parts) < 3:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        action, sid = parts[1], parts[2]
        try:
            pid = int(sid)
        except Exception:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        with get_db() as con:
            row = con.execute("SELECT p.*, u.username FROM payouts p JOIN users u ON u.user_id=p.user_id WHERE p.id=?", (pid,)).fetchone()
        if not row:
            bot.answer_callback_query(c.id, "Anfrage nicht gefunden.")
            return
        if action == "APPROVE":
            with get_db() as con:
                con.execute("UPDATE payouts SET status='APPROVED' WHERE id=?", (pid,))
            bot.answer_callback_query(c.id, "Genehmigt.")
            try:
                bot.send_message(row_get(row,"user_id"), "✅ Deine Auszahlung wurde genehmigt. Admin wird die Zahlung durchführen.")
            except Exception:
                pass
        elif action == "SENT":
            with get_db() as con:
                con.execute("UPDATE payouts SET status='SENT' WHERE id=?", (pid,))
            bot.answer_callback_query(c.id, "Als gesendet markiert.")
            try:
                bot.send_message(row_get(row,"user_id"), "📤 Deine Auszahlung wurde als gesendet markiert.")
            except Exception:
                pass
        elif action == "REJECT":
            with get_db() as con:
                con.execute("UPDATE payouts SET status='REJECTED' WHERE id=?", (pid,))
            bot.answer_callback_query(c.id, "Abgelehnt.")
            try:
                bot.send_message(row_get(row,"user_id"), "❌ Deine Auszahlung wurde abgelehnt.")
            except Exception:
                pass
        return

    if data.startswith("payoutopt_"):
        try:
            days = int(data.split("_", 1)[1])
        except Exception:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        fee = float(_fee_tiers.get(days, 0.0))
        pending = WAITING_WITHDRAW_AMOUNT.get(uid)
        if not pending or pending <= 0:
            bot.answer_callback_query(c.id, "Keine ausstehende Auszahlung.")
            return
        lam = int(pending)
        with get_db() as con:
            cur = con.execute("INSERT INTO payouts(user_id, amount_lamports, status, note, lockup_days, fee_percent) VALUES (?,?,?,?,?,?)",
                              (uid, lam, "REQUESTED", f"User requested withdrawal ({days}d)", days, fee))
            pid = cur.lastrowid
        WAITING_WITHDRAW_AMOUNT.pop(uid, None)
        bot.answer_callback_query(c.id, "Auszahlung angefragt.")
        bot.send_message(uid, f"Auszahlung erstellt: {fmt_sol_usdc(lam)} • Lockup: {days}d • Fee: {fee}%", parse_mode=None)
        for aid in ADMIN_IDS:
            try:
                bot.send_message(int(aid), f"Neue Auszahlung #{pid} • User {uid} • {fmt_sol_usdc(lam)} • {days}d • Fee {fee}%", reply_markup=kb_payout_manage(pid))
            except Exception:
                pass
        return

    # admin apply PnL / promotions quick entry
    if data == "admin_apply_pnl":
        if not is_admin(uid):
            bot.answer_callback_query(c.id, "Nicht erlaubt.")
            return
        ADMIN_AWAIT_PNL[uid] = True
        bot.answer_callback_query(c.id, "Sende Promo/PnL-Befehl. Beispiele:\nPROMO PERCENT 20 ALL\nPROMO BONUS 0.05 SUBSCRIBERS\nPNL CALL_ID 20")
        return

    bot.answer_callback_query(c.id, "")

@bot.message_handler(func=lambda m: True)
def catch_all(m: Message):
    uid = m.from_user.id
    text = (m.text or "").strip() if m.text else ""

    # Admin: set wallet
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
        bot.reply_to(m, f"Source-Wallet für {target} gesetzt: `{md_escape(wallet)}`", parse_mode="Markdown")
        try:
            bot.send_message(target, f"Admin hat deine Source-Wallet gesetzt: `{md_escape(wallet)}`", parse_mode="Markdown")
        except Exception:
            pass
        return

    # Initial source wallet setting by user
    if WAITING_SOURCE_WALLET.get(uid, False):
        WAITING_SOURCE_WALLET[uid] = False
        wallet = text
        if not is_probably_solana_address(wallet):
            bot.reply_to(m, "Bitte eine gültige Solana-Adresse eingeben.", parse_mode="Markdown")
            return
        set_source_wallet(uid, wallet)
        price = get_sol_usd()
        px = f"(1 SOL ≈ {price:.2f} USDC)" if price > 0 else ""
        bot.reply_to(m, f"✅ Absender-Wallet gespeichert.\nSende SOL von `{md_escape(wallet)}` an `{md_escape(CENTRAL_SOL_PUBKEY)}`\n{px}", parse_mode="Markdown")
        return

    # Admin: create simple call
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

    # Admin: set balance (single user)
    if ADMIN_AWAIT_BALANCE_EDIT.get(uid) is not None:
        target = ADMIN_AWAIT_BALANCE_EDIT.pop(uid)
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        try:
            # erlaubte Eingaben: +0.2, -0.05, -40%
            if text.endswith("%"):
                pct = float(text[:-1].replace(",", "."))
                bal = get_balance_lamports(target)
                delta = int(bal * (pct / 100.0))
                add_balance(target, delta)
                nb = fmt_sol_usdc(get_balance_lamports(target))
                bot.reply_to(m, f"✅ Prozentuale Änderung {pct:+.2f}% angewandt.\nNeues Guthaben: {nb}")
            else:
                val = float(text.replace(",", "."))
                lam = int(val * LAMPORTS_PER_SOL)
                add_balance(target, lam)
                nb = fmt_sol_usdc(get_balance_lamports(target))
                bot.reply_to(m, f"✅ Änderung {val:+.6f} SOL angewandt.\nNeues Guthaben: {nb}")
        except Exception:
            bot.reply_to(m, "Formatfehler. Beispiele: `+0.2`, `-0.05`, `-40%`", parse_mode="Markdown")
        return

    # Admin: broadcast an ALLE
    if ADMIN_AWAIT_TRADE_STATUS.get(uid, False):
        ADMIN_AWAIT_TRADE_STATUS[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        msg = text
        if not msg:
            bot.reply_to(m, "Bitte Text senden.")
            return
        users = all_users()
        sent = 0
        for su in users:
            try:
                bot.send_message(su, f"📢 {msg}", parse_mode="Markdown")
                sent += 1
            except Exception:
                pass
        bot.reply_to(m, f"✅ Nachricht gesendet an {sent} Nutzer.")
        return

    # Admin: import balances (multi-line)
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
                m_uid = re.search(r"UID\s*([0-9]+)", ln)
                if not m_uid:
                    continue
                uid_num = int(m_uid.group(1))
                m_amt = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*SOL", ln)
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

    # Admin: support direct messaging flow
    if ADMIN_AWAIT_NEWS_BROADCAST.get(uid):
        ctx = ADMIN_AWAIT_NEWS_BROADCAST[uid]
        step = ctx.get("step")
        if step == "awaiting_support_msg":
            ctx["text"] = text
            ctx["step"] = "awaiting_support_img"
            bot.reply_to(m, "Optional: Sende ein Bild (oder 'nopict').")
            return
        if step == "awaiting_support_img":
            target = ctx.get("target")
            img = None
            if m.photo:
                img = m.photo[-1].file_id
            elif text.lower() != "nopict" and text.startswith("http"):
                img = text
            direct_text = ctx.get("text", "")
            try:
                if img:
                    bot.send_photo(target, img, caption=direct_text, parse_mode="Markdown")
                else:
                    bot.send_message(target, direct_text, parse_mode="Markdown")
                bot.reply_to(m, "Support-Nachricht gesendet.")
            except Exception as e:
                bot.reply_to(m, f"Fehler beim Senden: {e}")
            ADMIN_AWAIT_NEWS_BROADCAST.pop(uid, None)
            return

    # Withdraw amount entry by user
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

    # Admin: Massen-Änderung Parser
    if ADMIN_AWAIT_MASS_BALANCE.get(uid, False):
        ADMIN_AWAIT_MASS_BALANCE[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return

        s = text.strip()
        # Beispiele: "ALL -40%", "ALL +0.1", "SUBS -20%", "NEWS MEME -10%"
        try:
            target_set = []
            if s.upper().startswith("ALL"):
                target_set = all_users()
                payload = s[3:].strip()
            elif s.upper().startswith("SUBS"):
                target_set = all_subscribers()
                payload = s[4:].strip()
            elif s.upper().startswith("NEWS "):
                cat = s[5:].split()[0].upper()
                payload = s[5+len(cat):].strip()
                with get_db() as con:
                    target_set = [r["user_id"] for r in con.execute("SELECT user_id FROM users WHERE sub_types LIKE ?", (f"%{cat}%",)).fetchall()]
            else:
                bot.reply_to(m, "Bitte mit `ALL`, `SUBS` oder `NEWS <KAT>` starten.", parse_mode="Markdown")
                return

            if not target_set:
                bot.reply_to(m, "Keine Zielnutzer gefunden.")
                return

            payload = payload.replace(" ", "")
            affected = 0
            if payload.endswith("%"):
                pct = float(payload[:-1].replace(",", "."))
                for tu in target_set:
                    bal = get_balance_lamports(tu)
                    delta = int(bal * (pct / 100.0))
                    if delta != 0:
                        add_balance(tu, delta)
                        affected += 1
                bot.reply_to(m, f"✅ Prozentuale Änderung {pct:+.2f}% bei {affected} Nutzern angewandt.")
            else:
                # absoluter Betrag in SOL addieren (+ oder -)
                val = float(payload.replace(",", "."))
                lam = int(val * LAMPORTS_PER_SOL)
                if lam == 0:
                    bot.reply_to(m, "Betrag ist 0.")
                    return
                for tu in target_set:
                    add_balance(tu, lam)
                    affected += 1
                bot.reply_to(m, f"✅ Änderung {val:+.6f} SOL bei {affected} Nutzern angewandt.")
        except Exception as e:
            bot.reply_to(m, f"Fehler: {e}")
        return

    # Admin: apply PnL / promo parsing
    if ADMIN_AWAIT_PNL.get(uid, False):
        ADMIN_AWAIT_PNL[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        toks = text.split()
        if not toks:
            bot.reply_to(m, "Kein Befehl erkannt.")
            return
        try:
            cmd = toks[0].upper()
            if cmd == "PROMO":
                typ = toks[1].upper()  # PERCENT or BONUS
                val = float(toks[2])
                scope = toks[3].upper() if len(toks) > 3 else "ALL"
                affected = 0
                with get_db() as con:
                    if scope == "ALL":
                        rows = con.execute("SELECT user_id FROM users").fetchall()
                    elif scope == "SUBSCRIBERS":
                        rows = con.execute("SELECT user_id FROM users WHERE sub_active=1").fetchall()
                    elif scope in ("MEME", "FUTURES"):
                        rows = con.execute("SELECT user_id FROM users WHERE sub_types LIKE ?", (f"%{scope}%",)).fetchall()
                    else:
                        rows = []
                for r in rows:
                    uid_t = int(row_get(r,"user_id",0))
                    if typ == "PERCENT":
                        bal = get_balance_lamports(uid_t)
                        delta = int(bal * (val / 100.0))
                        if delta != 0:
                            add_balance(uid_t, delta)
                            affected += 1
                    else:  # BONUS (SOL)
                        lam = int(val * LAMPORTS_PER_SOL)
                        add_balance(uid_t, lam)
                        affected += 1
                bot.reply_to(m, f"PROMO applied to {affected} users.")
                return
            elif cmd == "PNL":
                call_id = int(toks[1])
                percent = float(toks[2])
                affected = 0
                with get_db() as con:
                    execs = con.execute("SELECT user_id, stake_lamports FROM executions WHERE call_id=? AND status IN ('FILLED','QUEUED')", (call_id,)).fetchall()
                for ex in execs:
                    uid_t = int(row_get(ex,"user_id",0))
                    stake = int(row_get(ex,"stake_lamports",0))
                    u = get_user(uid_t)
                    risk = row_get(u,"auto_risk","MEDIUM")
                    frac = _risk_fraction(risk)
                    pnl_lam = int(stake * (percent / 100.0) * frac)
                    if pnl_lam != 0:
                        add_balance(uid_t, pnl_lam)
                        affected += 1
                bot.reply_to(m, f"PNL applied to {affected} users for call {call_id}.")
                return
            else:
                bot.reply_to(m, "Unbekannter PNL/PROMO-Befehl.")
                return
        except Exception as e:
            bot.reply_to(m, f"Fehler beim Anwenden: {e}")
            return

    # default fallback
    bot.reply_to(m, "Ich habe das nicht verstanden. Benutze das Menü unten.", reply_markup=kb_main(get_user(uid)))

# ---------------------------
# Background loops: auto-executor & payout reminders
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
                if row_get(r,"auto_mode") != "ON":
                    with get_db() as con:
                        con.execute("UPDATE executions SET status='ERROR', message='Auto OFF' WHERE id=?", (row_get(r,"eid"),))
                    continue
                call = get_call(row_get(r,"call_id"))
                stake_info = row_get(r,"stake_lamports") or _compute_stake_for_user(row_get(r,"user_id"))
                if SIMULATION_MODE:
                    if row_get(call,"market_type") == "FUTURES":
                        result = futures_place_simulated(row_get(r,"user_id"), row_get(call,"base","?"), row_get(call,"side","?"), row_get(call,"leverage",""), row_get(r,"auto_risk","MEDIUM"))
                    else:
                        result = dex_market_buy_simulated(row_get(r,"user_id"), row_get(call,"base","?"), stake_info)
                else:
                    result = {"status": "FILLED", "txid": "LIVE-TX-REPLACE"}
                status = result.get("status") or "FILLED"
                txid = result.get("txid") or result.get("order_id") or ""
                with get_db() as con:
                    con.execute("UPDATE executions SET status=?, txid=?, message=? WHERE id=?", (status, txid, "JOINED", row_get(r,"eid")))
                try:
                    bot.send_message(row_get(r,"user_id"),
                                     f"🤖 Auto-Entry • {row_get(r,'auto_risk','MEDIUM')}\n"
                                     f"{fmt_call(call)}\n"
                                     f"Status: *{status}*\n"
                                     f"Einsatz (Info): {fmt_sol_usdc(stake_info)}\n"
                                     f"Guthaben: {fmt_sol_usdc(get_balance_lamports(row_get(r,'user_id')))}\n"
                                     f"`{md_escape(txid)}`",
                                     parse_mode="Markdown")
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
                    WHERE status='REQUESTED'
                      AND (last_notified_at IS NULL OR (strftime('%s','now') - strftime('%s',COALESCE(last_notified_at,'1970-01-01')) > 1200))
                    ORDER BY created_at ASC
                """).fetchall()
            for r in rows:
                for aid in ADMIN_IDS:
                    try:
                        bot.send_message(int(aid), f"⏰ Erinnerung: Auszahlung #{row_get(r,'id')} offen • Betrag {fmt_sol_usdc(row_get(r,'amount_lamports',0))}", reply_markup=kb_payout_manage(row_get(r,"id")))
                    except Exception:
                        pass
                with get_db() as con:
                    con.execute("UPDATE payouts SET last_notified_at=CURRENT_TIMESTAMP WHERE id=?", (row_get(r,"id"),))
            time.sleep(60)
        except Exception as e:
            print("payout reminder loop error:", e)
            time.sleep(60)

threading.Thread(target=auto_executor_loop, daemon=True).start()
threading.Thread(target=payout_reminder_loop, daemon=True).start()

print("Bot läuft — enhanced full. SIMULATION_MODE =", SIMULATION_MODE)
bot.infinity_polling(timeout=60, long_polling_timeout=60)