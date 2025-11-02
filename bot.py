# bot.py — FULL REBUILD (Teil 1/3)
# UTF-8

import os
import time
import random
import threading
import sqlite3
import hashlib
import re
from contextlib import contextmanager
from typing import Optional, Dict, List

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from telebot import apihelper as _apihelper

# =========================
# Configuration (ENV)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8212740282:AAFdTWXF77hFSZj2ko9rbM3IYOhWs38-4cI").strip() or "REPLACE_ME"
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable required")

ADMIN_IDS = [a.strip() for a in os.getenv("ADMIN_IDS", "8076025426").split(",") if a.strip()]
if not ADMIN_IDS:
    ADMIN_IDS = ["123456789"]  # fallback

SOLANA_RPC = os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com").strip()

# Einzahlungs-Adresse (normales Guthaben)
CENTRAL_SOL_PUBKEY = os.getenv("CENTRAL_SOL_PUBKEY", "7SEzEWu4ukQ4PdKyUfwiNigEXGNKnBWijwDncd7cULcV").strip()

# **NEU**: separate Abo-Zahlungsadresse (ANDERE Adresse als Einzahlungen)
SUBSCRIPTION_SOL_PUBKEY = os.getenv("SUBSCRIPTION_SOL_PUBKEY", "8v2xVb6p8t4Yg2qk7x9sLJkZ9H7mYp6z2V5Q9g8aQ3rB").strip()

EXCHANGE_WALLETS = set([s.strip() for s in os.getenv("EXCHANGE_WALLETS", "").split(",") if s.strip()])

DB_PATH = os.getenv("DB_PATH", "memebot_full.db")
LAMPORTS_PER_SOL = 1_000_000_000
MIN_SUB_SOL = float(os.getenv("MIN_SUB_SOL", "0.1"))

# Internes Flag – KEIN User-facing Output darüber!
SIMULATION_MODE = True

# =========================
# Preise / Utils
# =========================
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

def md_escape(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return (text.replace('\\', '\\\\')
                .replace('_', '\\_')
                .replace('*', '\\*')
                .replace('`', '\\`')
                .replace('[', '\\[')
                .replace(')', '\\)')
                .replace('(', '\\('))

def is_admin(user_id: int) -> bool:
    return str(user_id) in ADMIN_IDS

# base58 check quick
_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_RE = re.compile(rf"[{_BASE58}]{{32,44}}")

def is_probably_solana_address(addr: str) -> bool:
    if not isinstance(addr, str):
        return False
    addr = addr.strip()
    if len(addr) < 32 or len(addr) > 44:
        return False
    return all(ch in _BASE58 for ch in addr)

def extract_solana_address(text: str) -> Optional[str]:
    if not text:
        return None
    m = _BASE58_RE.search(text)
    if not m:
        return None
    candidate = m.group(0)
    return candidate if is_probably_solana_address(candidate) else None

def gen_referral_for_user(user_id: int) -> str:
    h = hashlib.sha1(str(user_id).encode()).hexdigest()[:8]
    return f"REF{h.upper()}"

def rget(row, key, default=None):
    if row is None:
        return default
    try:
        v = row[key]
        return v if v is not None else default
    except Exception:
        return default

# =========================
# DB schema & helpers
# =========================
SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  username TEXT,
  is_admin INTEGER DEFAULT 0,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

  -- Trading/Auto
  sub_active INTEGER DEFAULT 0,
  auto_mode TEXT DEFAULT 'OFF',
  auto_risk TEXT DEFAULT 'MEDIUM',
  sol_balance_lamports INTEGER DEFAULT 0,

  -- Wallets
  source_wallet TEXT,   -- normale Einzahlungen (CENTRAL)
  payout_wallet TEXT,   -- Auszahlungen
  sub_src_wallet TEXT,  -- **Abo**: Quelle (vom User gesendet)
  referral_code TEXT DEFAULT '',
  ref_by INTEGER,

  -- Abos/Pläne
  sub_tier TEXT DEFAULT 'FREE',      -- BRONZE/SILVER/GOLD/PLATINUM/DIAMOND/CREATOR_PREMIUM/FREE
  is_shareholder INTEGER DEFAULT 0,  -- Diamond/100 People etc.

  -- Security
  pin_hash TEXT,

  -- Legacy
  referral_bonus_claimed INTEGER DEFAULT 0
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

CREATE TABLE IF NOT EXISTS tx_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  ref_id TEXT,
  amount_lamports INTEGER,
  meta TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS referrals (
  referrer_user_id INTEGER NOT NULL,
  invited_user_id INTEGER NOT NULL,
  level INTEGER NOT NULL,
  clicked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  deposit_total_lamports INTEGER DEFAULT 0,
  PRIMARY KEY(referrer_user_id, invited_user_id, level)
);

-- **NEU**: Abo-Zahlungen (eigenes Wallet), idempotent pro tx_sig
CREATE TABLE IF NOT EXISTS subs_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  plan TEXT NOT NULL,             -- BRONZE..DIAMOND/CREATOR_PREMIUM
  amount_lamports INTEGER NOT NULL,
  tx_sig TEXT,
  status TEXT DEFAULT 'PENDING',  -- PENDING | CONFIRMED
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(tx_sig)
);

-- **NEU**: Referral-Claim Tickets (für Gutschreiben)
CREATE TABLE IF NOT EXISTS referral_claims (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  amount_lamports INTEGER NOT NULL,
  status TEXT DEFAULT 'REQUESTED', -- REQUESTED | APPROVED | REJECTED
  breakdown TEXT,
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
        # best effort: ergänzende Spalten
        for stmt in [
            "ALTER TABLE users ADD COLUMN sub_tier TEXT DEFAULT 'FREE'",
            "ALTER TABLE users ADD COLUMN is_shareholder INTEGER DEFAULT 0",
            "ALTER TABLE users ADD COLUMN sub_src_wallet TEXT",
        ]:
            try: con.execute(stmt)
            except Exception: pass

# CRUD basics
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

def count_users() -> int:
    with get_db() as con:
        r = con.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        return int(r["c"] or 0)

def all_users() -> List[int]:
    with get_db() as con:
        return [r["user_id"] for r in con.execute("SELECT user_id FROM users").fetchall()]

def all_subscribers() -> List[int]:
    with get_db() as con:
        return [r["user_id"] for r in con.execute("SELECT user_id FROM users WHERE sub_active=1").fetchall()]

def all_auto_on_users() -> List[int]:
    with get_db() as con:
        return [r["user_id"] for r in con.execute("SELECT user_id FROM users WHERE UPPER(COALESCE(auto_mode,'OFF'))='ON'").fetchall()]

def add_balance(user_id: int, lamports: int):
    with get_db() as con:
        con.execute("UPDATE users SET sol_balance_lamports = sol_balance_lamports + ? WHERE user_id=?", (lamports, user_id))

def set_balance(user_id: int, lamports: int):
    with get_db() as con:
        con.execute("UPDATE users SET sol_balance_lamports = ? WHERE user_id=?", (lamports, user_id))

def get_balance_lamports(user_id: int) -> int:
    with get_db() as con:
        row = con.execute("SELECT sol_balance_lamports FROM users WHERE user_id=?", (user_id,)).fetchone()
        return int(row["sol_balance_lamports"]) if row else 0

def set_source_wallet(user_id: int, wallet: str):
    with get_db() as con:
        con.execute("UPDATE users SET source_wallet=? WHERE user_id=?", (wallet, user_id))

def set_payout_wallet(user_id: int, wallet: str):
    with get_db() as con:
        con.execute("UPDATE users SET payout_wallet=? WHERE user_id=?", (wallet, user_id))

def set_sub_src_wallet(user_id: int, wallet: str):
    with get_db() as con:
        con.execute("UPDATE users SET sub_src_wallet=? WHERE user_id=?", (wallet, user_id))

def set_subscription(user_id: int, active: bool):
    with get_db() as con:
        con.execute("UPDATE users SET sub_active=? WHERE user_id=?", (1 if active else 0, user_id))

def set_auto_mode(user_id: int, mode: str):
    with get_db() as con:
        con.execute("UPDATE users SET auto_mode=? WHERE user_id=?", (mode, user_id))

def set_auto_risk(user_id: int, risk: str):
    with get_db() as con:
        con.execute("UPDATE users SET auto_risk=? WHERE user_id=?", (risk, user_id))

def log_tx(user_id: int, kind: str, amount_lamports: int, ref_id: Optional[str] = None, meta: str = ""):
    with get_db() as con:
        con.execute("INSERT INTO tx_log(user_id, kind, ref_id, amount_lamports, meta) VALUES (?,?,?,?,?)",
                    (user_id, kind, ref_id or "", int(amount_lamports or 0), meta or ""))

# Summaries
def sum_total_deposits() -> int:
    with get_db() as con:
        r = con.execute("SELECT COALESCE(SUM(amount_lamports),0) AS s FROM seen_txs WHERE user_id IS NOT NULL").fetchone()
        return int(r["s"] or 0)

def sum_total_balances() -> int:
    with get_db() as con:
        r = con.execute("SELECT COALESCE(SUM(sol_balance_lamports),0) AS s FROM users").fetchone()
        return int(r["s"] or 0)

def sum_open_payouts() -> int:
    with get_db() as con:
        r = con.execute("SELECT COALESCE(SUM(amount_lamports),0) AS s FROM payouts WHERE status='REQUESTED'").fetchone()
        return int(r["s"] or 0)

def sum_user_deposits(uid: int) -> int:
    with get_db() as con:
        r = con.execute("SELECT COALESCE(SUM(amount_lamports),0) AS s FROM seen_txs WHERE user_id=?", (uid,)).fetchone()
        return int(r["s"] or 0)

# =========================
# Plans & Fees
# =========================
# Standard-Gebühren (nur Fallback; User-Pläne überschreiben das dynamisch)
DEFAULT_FEE_TIERS = {0: 20.0, 5: 15.0, 7: 10.0, 10: 5.0}

# User-Modelle (wöchentlich; Diamond einmalig)
USER_WEEKLY_PLANS = {
    "BRONZE":   {"weekly_eur": 15,  "fee_tiers": {0: 15.0, 5: 12.5, 7: 7.5, 10: 5.0}},
    "SILVER":   {"weekly_eur": 30,  "fee_tiers": {0: 12.5, 5: 10.0, 7: 6.0, 10: 4.0}},
    "GOLD":     {"weekly_eur": 60,  "fee_tiers": {0: 10.0, 5: 8.0,  7: 5.0, 10: 3.0}},
    "PLATINUM": {"weekly_eur": 120, "fee_tiers": {0: 7.5,  5: 6.0,  7: 4.0, 10: 2.0}},
    "DIAMOND":  {"oneoff_eur": 1000, "fee_tiers": {0: 5.0, 5: 4.0, 7: 3.0, 10: 1.0}, "shareholder": True},
}

CREATOR_PREMIUM_EUR = 250  # pro Monat

def fee_tiers_for_user(u_row) -> Dict[int, float]:
    tier = (rget(u_row, "sub_tier", "") or "").upper()
    if tier in USER_WEEKLY_PLANS:
        return USER_WEEKLY_PLANS[tier]["fee_tiers"]
    return DEFAULT_FEE_TIERS.copy()

# =========================
# Bot init & safe send
# =========================
init_db()
bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

_original_send_message = bot.send_message
def _safe_send_message(chat_id, text, **kwargs):
    try:
        return _original_send_message(chat_id, text, **kwargs)
    except Exception:
        pm = kwargs.get("parse_mode")
        if pm and str(pm).upper().startswith("MARKDOWN"):
            kwargs2 = dict(kwargs); kwargs2["parse_mode"] = "Markdown"
            try:
                return _original_send_message(chat_id, md_escape(str(text)), **kwargs2)
            except Exception:
                kwargs3 = dict(kwargs2); kwargs3.pop("parse_mode", None)
                return _original_send_message(chat_id, str(text), **kwargs3)
        else:
            kwargs3 = dict(kwargs); kwargs3.pop("parse_mode", None)
            return _original_send_message(chat_id, str(text), **kwargs3)
bot.send_message = _safe_send_message

_original_edit_message_text = bot.edit_message_text
def _safe_edit_message_text(text, chat_id, message_id, **kwargs):
    try:
        return _original_edit_message_text(text, chat_id, message_id, **kwargs)
    except Exception:
        pm = kwargs.get("parse_mode")
        if pm and str(pm).upper().startswith("MARKDOWN"):
            kwargs2 = dict(kwargs); kwargs2["parse_mode"] = "Markdown"
            try:
                return _original_edit_message_text(md_escape(str(text)), chat_id, message_id, **kwargs2)
            except Exception:
                kwargs3 = dict(kwargs2); kwargs3.pop("parse_mode", None)
                return _original_edit_message_text(str(text), chat_id, message_id, **kwargs3)
        else:
            kwargs3 = dict(kwargs); kwargs3.pop("parse_mode", None)
            return _original_edit_message_text(str(text), chat_id, message_id, **kwargs3)
bot.edit_message_text = _safe_edit_message_text

_original_answer_callback_query = bot.answer_callback_query
def _safe_answer_callback_query(callback_query_id, *args, **kwargs):
    try:
        return _original_answer_callback_query(callback_query_id, *args, **kwargs)
    except _apihelper.ApiTelegramException:
        return None
bot.answer_callback_query = _safe_answer_callback_query

# =========================
# States
# =========================
WAITING_SOURCE_WALLET: Dict[int, bool] = {}
WAITING_PAYOUT_WALLET: Dict[int, bool] = {}
WAITING_WITHDRAW_AMOUNT: Dict[int, Optional[int]] = {}
AWAITING_PIN: Dict[int, Dict] = {}

# Abo-Kauf Flow:
# 1) User wählt Plan -> wir speichern (uid -> "PLANNAME")
# 2) Wir fragen: "Von welcher Wallet sendest du?" -> speichern sub_src_wallet
# 3) Wir zeigen Zieladresse (SUBSCRIPTION_SOL_PUBKEY)
SUB_PURCHASE_CTX: Dict[int, Dict] = {}          # { uid: {"plan": "BRONZE"/"CREATOR_PREMIUM"} }
WAITING_SUB_SRC_WALLET: Dict[int, bool] = {}     # fragt nach Quelle für Abozahlung

# Admin states (kommen in Teil 2/3 + 3/3 voll zum Einsatz)
ADMIN_AWAIT_SIMPLE_CALL: Dict[int, bool] = {}
ADMIN_AWAIT_BALANCE_SINGLE: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_BALANCE_GLOBAL: Dict[int, bool] = {}
ADMIN_AWAIT_SET_WALLET: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_MASS_BALANCE: Dict[int, bool] = {}
ADMIN_AWAIT_NEWS_BROADCAST: Dict[int, Dict] = {}
ADMIN_AWAIT_DM_TARGET: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_SET_SUB: Dict[int, bool] = {}
SUPPORT_AWAIT_MSG: Dict[int, bool] = {}

# =========================
# Keyboards
# =========================
def kb_main(u):
    bal = fmt_sol_usdc(int(u["sol_balance_lamports"] or 0))
    auto_mode = (u["auto_mode"] or "OFF").upper()
    auto_risk = (u["auto_risk"] or "MEDIUM").upper()
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 Auszahlung", callback_data="withdraw"),
           InlineKeyboardButton("📈 Portfolio", callback_data="my_portfolio"))
    kb.add(InlineKeyboardButton("💸 Einzahlen", callback_data="deposit"),
           InlineKeyboardButton("🤖 Auto-Entry", callback_data="auto_menu"))
    kb.add(InlineKeyboardButton("📜 Verlauf", callback_data="history"),
           InlineKeyboardButton("🆘 Support", callback_data="open_support"))
    # getrennt: Signale vs Abo-Modelle
    kb.add(InlineKeyboardButton("🔔 Signale", callback_data="signals_menu"),
           InlineKeyboardButton("⭐ Abo-Modelle", callback_data="plans_root"))
    kb.add(InlineKeyboardButton("🔗 Referral", callback_data="referral"),
           InlineKeyboardButton("⚖️ Rechtliches", callback_data="legal"))
    if is_admin(int(u["user_id"])):
        kb.add(InlineKeyboardButton("🛠️ Admin (Kontrolle)", callback_data="admin_menu_big"))
    kb.add(InlineKeyboardButton(f"🏦 Guthaben: {bal}", callback_data="noop"))
    kb.add(InlineKeyboardButton(f"🤖 Auto: {auto_mode} • Risiko: {auto_risk}", callback_data="noop"))
    return kb

def kb_sub_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔔 Abonnieren", callback_data="sub_on"),
           InlineKeyboardButton("🔕 Abbestellen", callback_data="sub_off"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

def kb_auto_menu(u):
    mode = (u["auto_mode"] or "OFF").upper()
    risk = (u["auto_risk"] or "MEDIUM").upper()
    kb = InlineKeyboardMarkup()
    on_off = "🔴 Auto AUS" if mode == "OFF" else "🟢 Auto EIN"
    kb.add(InlineKeyboardButton(on_off, callback_data="auto_toggle"))
    kb.add(InlineKeyboardButton(("✅ " if risk=="LOW" else "") + "LOW", callback_data="auto_risk_LOW"),
           InlineKeyboardButton(("✅ " if risk=="MEDIUM" else "") + "MEDIUM", callback_data="auto_risk_MEDIUM"),
           InlineKeyboardButton(("✅ " if risk=="HIGH" else "") + "HIGH", callback_data="auto_risk_HIGH"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

def kb_admin_main():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ Call erstellen", callback_data="admin_new_call"))
    kb.add(InlineKeyboardButton("📣 Broadcast: letzter Call", callback_data="admin_broadcast_last"))
    kb.add(InlineKeyboardButton("👥 Investoren-Liste", callback_data="admin_investors_menu"))
    kb.add(InlineKeyboardButton("👀 Nutzer verwalten", callback_data=f"admin_view_users_0"))
    kb.add(InlineKeyboardButton("💼 Guthaben ändern", callback_data="admin_balance_edit"))
    kb.add(InlineKeyboardButton("🧾 Offene Auszahlungen", callback_data="admin_open_payouts"))
    kb.add(InlineKeyboardButton("📊 System-Stats", callback_data="admin_stats"))
    kb.add(InlineKeyboardButton("📤 Broadcast an alle", callback_data="admin_broadcast_all"))
    kb.add(InlineKeyboardButton("🔧 Promotions / PnL", callback_data="admin_apply_pnl"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

def kb_users_pagination(offset: int, total: int, prefix: str = "admin_view_users", page_size: int = 25):
    kb = InlineKeyboardMarkup()
    prev_off = max(0, offset - page_size)
    next_off = offset + page_size if offset + page_size < total else offset
    row = []
    if offset > 0:
        row.append(InlineKeyboardButton("◀️ Zurück", callback_data=f"{prefix}_{prev_off}"))
    if offset + page_size < total:
        row.append(InlineKeyboardButton("▶️ Weiter", callback_data=f"{prefix}_{next_off}"))
    if row:
        kb.add(*row)
    kb.add(InlineKeyboardButton("⬅️ Admin Menü", callback_data="admin_menu_big"))
    return kb

def kb_user_row(user_id: int):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("ℹ️ Mehr Infos", callback_data=f"admin_user_{user_id}"))
    return kb

# ===== Abo-Modelle Keyboards & Texte =====
def kb_plans_root():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('🟩 Creator (Premium)', callback_data='plans_creator'),
           InlineKeyboardButton('🔹 User (Bronze–Diamond)', callback_data='plans_user'))
    kb.add(InlineKeyboardButton('⬅️ Zurück', callback_data='back_home'))
    return kb

def kb_plans_user():
    kb = InlineKeyboardMarkup()
    for name in ["BRONZE","SILVER","GOLD","PLATINUM","DIAMOND"]:
        kb.add(InlineKeyboardButton(f"{name.title()}", callback_data=f"plan_user_{name}"))
    kb.add(InlineKeyboardButton('⬅️ Zurück', callback_data='plans_root'))
    return kb

def kb_plans_creator():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"Premium {CREATOR_PREMIUM_EUR}€/Monat buchen", callback_data="plan_creator_premium"))
    kb.add(InlineKeyboardButton('⬅️ Zurück', callback_data='plans_root'))
    return kb

CREATOR_TEXT = (
"**Erklärung zum Premium-Abo (250 € / Monat)**\n\n"
"• 125 € finanzieren höhere Provisionen der Premium-Mitglieder\n"
"• 25 € gehen in den monatlichen Leader-Pool\n"
"• 100 € Betrieb/Team\n\n"
"**Provisionen (Premium statt Standard):**\n"
"1. Ebene: 15% (statt 10%)\n2. Ebene: 7,5% (statt 5%)\n3. Ebene: 3,75% (statt 2,5%)\n"
"+ zusätzlich 12,5 $ auf jedes Premium-Abo eines **direkt** Geworbenen\n\n"
"**Leader-Pool (monatlich):**\n1: 25% • 2: 15% • 3: 10% • 4–10: je 7,14%\n"
)

def user_plan_text(name: str) -> str:
    d = USER_WEEKLY_PLANS[name]
    if "weekly_eur" in d:
        price = f"{d['weekly_eur']}€/Woche"
    else:
        price = f"Einmalig {d['oneoff_eur']}€"
    fee_lines = "\n".join([f"{k if k>0 else 'Sofort'}: {v:.1f}%"
                           for k,v in sorted(d["fee_tiers"].items())])
    extra = " + Anteilseigner (Revenue-Share)" if d.get("shareholder") else ""
    return (f"**{name.title()}** — {price}{extra}\n\n"
            f"Reduzierte Auszahlungs-Fees:\n{fee_lines}\n\n"
            "Zum Buchen: Wir fragen zuerst *von welcher Wallet* du sendest und geben dir dann **unsere Abo-Zieladresse**.")

# =========================
# Home / Texte
# =========================
def get_bot_username():
    try:
        me = bot.get_me()
        return me.username or "<YourBotUsername>"
    except Exception:
        return "<YourBotUsername>"

LEGAL_TEXT = (
    "⚖️ *Rechtliches*\n\n"
    "• Dieser Bot stellt keine Finanzberatung dar.\n"
    "• Krypto-Handel ist mit erheblichen Risiken verbunden.\n"
    "• Nutzer sind für Ein-/Auszahlungen selbst verantwortlich.\n"
    "• Bei Unklarheiten wende dich an /support."
)

HINT_TEXT = (
    "ℹ️ *Hinweis*\n\n"
    "• Auto-Entry: Bot nimmt Calls automatisch mit deinem gewählten Risiko.\n"
    "• Risiko LOW/MEDIUM/HIGH beeinflusst den Einsatz pro Trade.\n"
    "• Du kannst Auto-Entry jederzeit im Menü ein-/ausschalten.\n"
    "• Support erreichst du mit /support."
)

def home_text(u) -> str:
    raw_uname = ("@" + (u["username"] or "")) if u["username"] else f"ID {u['user_id']}"
    bal = fmt_sol_usdc(int(u["sol_balance_lamports"] or 0))
    code = gen_referral_for_user(int(u["user_id"]))
    bot_username = get_bot_username()
    ref_url = f"https://t.me/{bot_username}?start={code}"
    return (
        f"👋 Hallo {raw_uname} — willkommen!\n\n"
        "Dieses System bietet:\n"
        "• Einzahlungen (Guthaben) & separate Abo-Zahlung\n"
        "• Trading-Signale & Auto-Entry\n"
        "• Abo-Modelle: User (Bronze–Diamond) & Creator Premium\n\n"
        f"🏦 Aktuelles Guthaben: {bal}\n"
        f"🔗 Referral: {ref_url}\n"
        "📩 Support: /support"
    )

# =========================
# (Ende Teil 1/3) – Die Callback-Logik für:
# - Signale-Menü
# - Abo-Modelle (inkl. „von welcher Wallet sendest du?“ & Anzeigen der Abo-Zieladresse)
# - Ein-/Auszahlung/History/Referral usw.
# folgt in Teil 2/3.
# =========================
# ========= Teil 2/3: Commands, Callbacks, Message-Handler =========

# -------- PIN Helper --------
def _hash_pin(pin: str) -> str:
    return hashlib.sha256(("PIN|" + pin).encode()).hexdigest()

# -------- Commands --------
@bot.message_handler(commands=["setpin"])
def cmd_setpin(m: Message):
    uid = m.from_user.id
    txt = (m.text or "").strip()
    parts = txt.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(m, "Verwendung: /setpin 1234")
        return
    pin = parts[1].strip()
    if not (pin.isdigit() and 4 <= len(pin) <= 8):
        bot.reply_to(m, "PIN muss 4–8 Ziffern sein.")
        return
    with get_db() as con:
        con.execute("UPDATE users SET pin_hash=? WHERE user_id=?", (_hash_pin(pin), uid))
    bot.reply_to(m, "✅ PIN gesetzt. Bei sensiblen Aktionen wird er abgefragt.")

@bot.message_handler(commands=["support"])
def cmd_support(m: Message):
    SUPPORT_AWAIT_MSG[m.from_user.id] = True
    bot.reply_to(m, "✍️ Sende jetzt deine Support-Nachricht (Text/Bild).")

@bot.message_handler(commands=["auto"])
def cmd_auto(m: Message):
    u = get_user(m.from_user.id)
    if not u:
        upsert_user(m.from_user.id, m.from_user.username or "", 1 if is_admin(m.from_user.id) else 0)
        u = get_user(m.from_user.id)
    bot.reply_to(m,
                 f"🤖 Auto-Entry\nStatus: {(u['auto_mode'] or 'OFF').upper()} • Risiko: {(u['auto_risk'] or 'MEDIUM').upper()}",
                 reply_markup=kb_auto_menu(u))

@bot.message_handler(commands=["start"])
def cmd_start(m: Message):
    uid = m.from_user.id
    uname = m.from_user.username or ""
    upsert_user(uid, uname, 1 if is_admin(uid) else 0)

    # Referral-Code übernehmen (wenn /start <code>)
    ref_code = None
    txt = m.text or ""
    parts = txt.split(maxsplit=1)
    if len(parts) >= 2:
        ref_code = parts[1].strip()
        if ref_code.startswith("="):
            ref_code = ref_code[1:].strip()

    # eigenen Referral-Code sicherstellen
    code = gen_referral_for_user(uid)
    with get_db() as con:
        con.execute("UPDATE users SET referral_code=COALESCE(NULLIF(referral_code,''),?) WHERE user_id=?",
                    (code, uid))

    # ref_by setzen
    if ref_code:
        with get_db() as con:
            ref_row = con.execute("SELECT user_id FROM users WHERE referral_code=?", (ref_code,)).fetchone()
        referrer = int(ref_row["user_id"]) if ref_row else None
        if referrer and referrer != uid:
            with get_db() as con:
                con.execute("UPDATE users SET ref_by=COALESCE(ref_by, ?) WHERE user_id=?", (referrer, uid))
            with get_db() as con:
                con.execute("INSERT OR IGNORE INTO referrals(referrer_user_id, invited_user_id, level) VALUES (?,?,1)", (referrer, uid))
                # Level 2 & 3 automatisch verketten (falls vorhanden)
                r1 = con.execute("SELECT ref_by FROM users WHERE user_id=?", (referrer,)).fetchone()
                if r1 and r1["ref_by"]:
                    lvl2 = int(r1["ref_by"])
                    con.execute("INSERT OR IGNORE INTO referrals(referrer_user_id, invited_user_id, level) VALUES (?,?,2)", (lvl2, uid))
                    r2 = con.execute("SELECT ref_by FROM users WHERE user_id=?", (lvl2,)).fetchone()
                    if r2 and r2["ref_by"]:
                        lvl3 = int(r2["ref_by"])
                        con.execute("INSERT OR IGNORE INTO referrals(referrer_user_id, invited_user_id, level) VALUES (?,?,3)", (lvl3, uid))

    u = get_user(uid)
    bot.reply_to(m, home_text(u), reply_markup=kb_main(u))

# -------- Helper: dynamisches Auszahlungs-Keyboard (Plan-basiert) --------
def build_withdraw_keyboard_for_user(uid: int) -> InlineKeyboardMarkup:
    urow = get_user(uid)
    tiers = fee_tiers_for_user(urow)
    kb = InlineKeyboardMarkup()
    for days, pct in sorted(tiers.items(), key=lambda x: x[0]):
        label = "Sofort • Fee {:.1f}%".format(pct) if days == 0 else f"{days} Tage • Fee {pct:.1f}%"
        kb.add(InlineKeyboardButton(label, callback_data=f"payoutopt_{days}"))
    kb.add(InlineKeyboardButton("↩️ Abbrechen", callback_data="back_home"))
    return kb

# ========= Admin-Callbacks (eigener Filter, damit sie IMMER feuern) =========
@bot.callback_query_handler(func=lambda c: (c.data or "").startswith("admin_"))
def on_admin_cb(c: CallbackQuery):
    uid = c.from_user.id
    if not is_admin(uid):
        bot.answer_callback_query(c.id, "Nicht erlaubt.")
        return

    data = c.data or ""
    if data == "admin_menu_big":
        bot.edit_message_text("🛠️ Admin-Menü — Kontrolle", c.message.chat.id, c.message.message_id, reply_markup=kb_admin_main())
        return

    # (weitere Admin-Funktionen kommen in Teil 3/3)
    bot.answer_callback_query(c.id, "Admin-Action (Teil 3 folgt)")

# ========= Generische Callbacks (alle NICHT-admin/plans/claim) =========
@bot.callback_query_handler(func=lambda c: not ((c.data or "").startswith(("admin_"))))
def on_cb(c: CallbackQuery):
    uid = c.from_user.id
    u = get_user(uid)
    data = c.data or ""

    # Basics
    if data == "back_home":
        bot.edit_message_text(home_text(u), c.message.chat.id, c.message.message_id, reply_markup=kb_main(u)); return
    if data == "noop":
        bot.answer_callback_query(c.id, "—"); return
    if data == "legal":
        bot.answer_callback_query(c.id); bot.send_message(uid, LEGAL_TEXT, parse_mode="Markdown"); return
    if data == "open_support":
        SUPPORT_AWAIT_MSG[uid] = True
        bot.answer_callback_query(c.id, "Support geöffnet")
        bot.send_message(uid, "✍️ Sende jetzt deine Support-Nachricht (Text/Bild)."); return

    # Deposit
    if data == "deposit":
        if not u["source_wallet"]:
            WAITING_SOURCE_WALLET[uid] = True
            bot.answer_callback_query(c.id, "Bitte Source-Wallet senden.")
            bot.send_message(uid, "🔑 Sende deine Absender-Wallet (SOL) **für normale Einzahlungen**:")
            return
        price = get_sol_usd(); px = f"(1 SOL ≈ {price:.2f} USDC)" if price > 0 else ""
        bot.edit_message_text(
            f"Absender-Wallet: `{md_escape(u['source_wallet'])}`\n"
            f"Sende SOL an: `{md_escape(CENTRAL_SOL_PUBKEY)}`\n{px}\n\n"
            "🔄 Zum Ändern einfach neue Solana-Adresse senden.",
            c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_main(u)
        )
        WAITING_SOURCE_WALLET[uid] = True
        return

    # Withdraw
    if data == "withdraw":
        if not u["payout_wallet"]:
            WAITING_PAYOUT_WALLET[uid] = True
            bot.answer_callback_query(c.id, "Bitte Payout-Adresse senden.")
            bot.send_message(uid, "🔑 Sende deine Auszahlungsadresse (SOL):"); return
        WAITING_WITHDRAW_AMOUNT[uid] = None
        bot.answer_callback_query(c.id, "Bitte Betrag eingeben.")
        bot.send_message(uid, f"💳 Payout: `{md_escape(u['payout_wallet'])}`\nGib den Betrag in SOL ein (z. B. `0.25`).", parse_mode="Markdown")
        return

    # Verlauf
    if data == "history":
        with get_db() as con:
            rows = con.execute("""
                SELECT kind, ref_id, amount_lamports, meta, created_at
                FROM tx_log WHERE user_id=?
                ORDER BY id DESC LIMIT 20
            """, (uid,)).fetchall()
        if not rows:
            bot.answer_callback_query(c.id, "Kein Verlauf.")
            bot.send_message(uid, "📜 Noch keine Einträge."); return
        bot.answer_callback_query(c.id)
        parts = ["📜 Dein Verlauf (letzte 20)"]
        for r in rows:
            parts.append(f"• {r['created_at']} • {r['kind']} • {fmt_sol_usdc(int(r['amount_lamports'] or 0))} • {r['meta'] or ''}")
        bot.send_message(uid, "\n".join(parts)); return

    # Portfolio
    if data == "my_portfolio":
        bal_lam = get_balance_lamports(uid)
        deps_lam = sum_user_deposits(uid)
        delta_lam = bal_lam - deps_lam
        bot.answer_callback_query(c.id, "Portfolio")
        bot.send_message(uid,
                         f"🏦 Guthaben: {fmt_sol_usdc(bal_lam)}\n"
                         f"📥 Einzahlungen gesamt: {fmt_sol_usdc(deps_lam)}\n"
                         f"Δ seit Start: {fmt_sol_usdc(delta_lam)}"); return

    # Auto-Entry
    if data == "auto_menu":
        bot.edit_message_text("🤖 Auto-Entry Einstellungen", c.message.chat.id, c.message.message_id, reply_markup=kb_auto_menu(u)); return
    if data == "auto_toggle":
        new_mode = "OFF" if (u["auto_mode"] or "OFF").upper() == "ON" else "ON"
        set_auto_mode(uid, new_mode)
        u = get_user(uid)
        bot.answer_callback_query(c.id, f"Auto-Entry: {new_mode}")
        bot.edit_message_text("🤖 Auto-Entry Einstellungen", c.message.chat.id, c.message.message_id, reply_markup=kb_auto_menu(u)); return
    if data.startswith("auto_risk_"):
        risk = data.split("_", 2)[2].upper()
        if risk not in ("LOW","MEDIUM","HIGH"):
            bot.answer_callback_query(c.id, "Ungültiges Risiko"); return
        set_auto_risk(uid, risk)
        u = get_user(uid)
        bot.answer_callback_query(c.id, f"Risiko: {risk}")
        bot.edit_message_text("🤖 Auto-Entry Einstellungen", c.message.chat.id, c.message.message_id, reply_markup=kb_auto_menu(u)); return

    # Signale (separat)
    if data == "signals_menu":
        bot.edit_message_text("Signals-Menü:", c.message.chat.id, c.message.message_id, reply_markup=kb_sub_menu()); return
    if data == "sub_on":
        bal_sol = get_balance_lamports(uid) / LAMPORTS_PER_SOL
        if bal_sol < MIN_SUB_SOL:
            bot.answer_callback_query(c.id, f"Mindestens {MIN_SUB_SOL} SOL nötig."); return
        set_subscription(uid, True)
        bot.answer_callback_query(c.id, "Abo aktiviert")
        bot.send_message(uid, "🔔 Signale-Abo aktiv.", reply_markup=kb_main(get_user(uid))); return
    if data == "sub_off":
        set_subscription(uid, False)
        bot.answer_callback_query(c.id, "Abo beendet")
        bot.send_message(uid, "🔕 Signale-Abo beendet.", reply_markup=kb_main(get_user(uid))); return

    # Referral
    if data == "referral":
        code = gen_referral_for_user(uid)
        bot_username = get_bot_username()
        link_md = f"[Klicke hier, um zu starten](https://t.me/{bot_username}?start={code})"
        bot.answer_callback_query(c.id, "Referral")
        bot.send_message(uid, f"Teile deinen Link:\n{link_md}", parse_mode="Markdown", disable_web_page_preview=True); return
    if data == "ref_stats":
        # kommt in Teil 3/3 in voller Version
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "📊 Referral-Stats folgen (Teil 3)."); return

    # ======= Abo-Modelle =======
    if data == "plans_root":
        bot.edit_message_text("Wähle ein Abo-Modell:", c.message.chat.id, c.message.message_id, reply_markup=kb_plans_root()); return
    if data == "plans_user":
        bot.edit_message_text("User-Modelle (wöchentlich / Diamond einmalig):", c.message.chat.id, c.message.message_id, reply_markup=kb_plans_user()); return
    if data == "plans_creator":
        bot.edit_message_text(CREATOR_TEXT, c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_plans_creator()); return

    if data.startswith("plan_user_"):
        name = data.split("_",2)[2].upper()
        if name not in USER_WEEKLY_PLANS: bot.answer_callback_query(c.id, "Unbekannt."); return
        txt = user_plan_text(name)
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton("✅ Abo buchen", callback_data=f"confirm_user_plan_{name}"))
        kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="plans_user"))
        bot.edit_message_text(txt, c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb)
        return

    if data == "plan_creator_premium":
        kb = InlineKeyboardMarkup()
        kb.add(InlineKeyboardButton(f"✅ Premium {CREATOR_PREMIUM_EUR}€/Monat buchen", callback_data="confirm_creator_premium"))
        kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="plans_creator"))
        bot.edit_message_text(CREATOR_TEXT, c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb)
        return

    # Kauf-Bestätigungen -> starte Flow „von welcher Wallet sendest du?“
    if data.startswith("confirm_user_plan_"):
        name = data.rsplit("_",1)[1].upper()
        if name not in USER_WEEKLY_PLANS:
            bot.answer_callback_query(c.id, "Unbekannter Plan."); return
        SUB_PURCHASE_CTX[uid] = {"plan": name}
        WAITING_SUB_SRC_WALLET[uid] = True
        bot.answer_callback_query(c.id, f"{name.title()} buchen")
        bot.send_message(uid, "🔑 Für das Abo: Sende bitte **deine Absender-Wallet (SOL)**, von der du die Abo-Zahlung schicken wirst.")
        return

    if data == "confirm_creator_premium":
        SUB_PURCHASE_CTX[uid] = {"plan": "CREATOR_PREMIUM"}
        WAITING_SUB_SRC_WALLET[uid] = True
        bot.answer_callback_query(c.id, "Creator Premium buchen")
        bot.send_message(uid, "🔑 Für das Abo: Sende bitte **deine Absender-Wallet (SOL)**, von der du die Abo-Zahlung schicken wirst.")
        return

    # Auszahlungs-Option (mit optionaler PIN)
    if data.startswith("payoutopt_"):
        if u and u.get("pin_hash"):
            AWAITING_PIN[uid] = {"for": "withdraw_option", "data": data}
            bot.answer_callback_query(c.id, "PIN erforderlich.")
            bot.send_message(uid, "🔐 Bitte sende deine PIN, um fortzufahren."); return
        # wird in Teil 3/3 finalisiert (_do_payout_option)
        bot.answer_callback_query(c.id, "Wird in Teil 3 verarbeitet.")
        return

    bot.answer_callback_query(c.id, "")
    # Ende on_cb

# ========= Message-Handler =========
@bot.message_handler(func=lambda m: True)
def catch_all(m: Message):
    uid = m.from_user.id
    text = (m.text or "").strip() if m.text else ""

    # Support
    if SUPPORT_AWAIT_MSG.get(uid):
        SUPPORT_AWAIT_MSG.pop(uid, None)
        name = ("@" + (m.from_user.username or "")) if m.from_user.username else f"UID {uid}"
        for aid in ADMIN_IDS:
            try:
                if m.photo:
                    bot.send_photo(int(aid), m.photo[-1].file_id, caption=f"[Support von {name} ({uid})] {m.caption or ''}")
                else:
                    bot.send_message(int(aid), f"[Support von {name} ({uid})] {text}", parse_mode=None)
            except Exception:
                pass
        bot.reply_to(m, "✅ Deine Support-Nachricht wurde an die Admins gesendet.")
        return

    # PIN erwartet?
    if AWAITING_PIN.get(uid):
        entry = AWAITING_PIN.pop(uid)
        pin = text
        u = get_user(uid)
        if not (u and rget(u, "pin_hash") and _hash_pin(pin) == rget(u, "pin_hash")):
            bot.reply_to(m, "❌ Falsche PIN.")
            return
        # Withdraw-Option wird in Teil 3 ausgeführt
        if entry["for"] == "withdraw_option":
            bot.reply_to(m, "✅ PIN geprüft. Auszahlung wird verarbeitet (Teil 3).")
            return

    # Abo-Quellwallet abfragen
    if WAITING_SUB_SRC_WALLET.get(uid):
        addr = extract_solana_address(text) if text else None
        if not addr:
            bot.reply_to(m, "Bitte eine gültige Solana-Adresse senden (Abo-Quelle).")
            return
        set_sub_src_wallet(uid, addr)
        WAITING_SUB_SRC_WALLET.pop(uid, None)

        ctx = SUB_PURCHASE_CTX.get(uid, {})
        plan = (ctx.get("plan") or "").upper()
        if not plan:
            bot.reply_to(m, "Plan nicht gefunden. Bitte erneut im Menü Abo-Modelle wählen.")
            return

        # Preis in € -> grobe SOL-Schätzung
        eur = 0
        if plan == "CREATOR_PREMIUM":
            eur = CREATOR_PREMIUM_EUR
        else:
            info = USER_WEEKLY_PLANS.get(plan, {})
            eur = info.get("weekly_eur") or info.get("oneoff_eur") or 0

        usd = get_sol_usd()
        eur_to_usd = 1.08  # einfache Annahme; bei Bedarf via ENV
        sol_est = (eur * eur_to_usd) / usd if (usd > 0 and eur > 0) else 0
        sol_line = f"≈ {sol_est:.4f} SOL (Schätzung)" if sol_est > 0 else ""

        # subs_payments PENDING vormerken (ohne tx_sig)
        with get_db() as con:
            con.execute("INSERT INTO subs_payments(user_id, plan, amount_lamports, status) VALUES (?,?,?,?)",
                        (uid, plan, 0, "PENDING"))

        bot.reply_to(m,
            "✅ Abo-Quelle gespeichert.\n\n"
            f"Sende nun die **Abo-Zahlung** an:\n`{md_escape(SUBSCRIPTION_SOL_PUBKEY)}`\n"
            f"Verwendungszweck: *{plan}*\n"
            f"Betrag: {eur} € {sol_line}\n\n"
            "Sobald die Zahlung bestätigt ist, wird dein Abo aktiviert. "
            "Du kannst mir optional die **Transaktionssignatur** als Nachricht schicken, "
            "dann wird sie schneller zugeordnet.",
            parse_mode="Markdown")
        return

    # Admin: Set wallet eines anderen Users (kommt ausführlich in Teil 3)
    if ADMIN_AWAIT_SET_WALLET.get(uid):
        target = ADMIN_AWAIT_SET_WALLET.pop(uid)
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        parts = text.split(None, 1)
        if len(parts) != 2:
            bot.reply_to(m, "Format: `SRC <adresse>` oder `PAY <adresse>`", parse_mode="Markdown")
            return
        which, addr = parts[0].upper(), parts[1].strip()
        if not is_probably_solana_address(addr):
            bot.reply_to(m, "Ungültige Solana-Adresse.")
            return
        if which == "SRC":
            set_source_wallet(target, addr)
            bot.reply_to(m, f"✅ Source-Wallet für UID {target} gesetzt: `{md_escape(addr)}`", parse_mode="Markdown")
        elif which == "PAY":
            set_payout_wallet(target, addr)
            bot.reply_to(m, f"✅ Payout-Wallet für UID {target} gesetzt: `{md_escape(addr)}`", parse_mode="Markdown")
        else:
            bot.reply_to(m, "Nutze `SRC` oder `PAY`.", parse_mode="Markdown")
        return

    # User: normale Source-Wallet speichern
    if WAITING_SOURCE_WALLET.get(uid, False):
        if is_probably_solana_address(text):
            u = get_user(uid)
            if u and rget(u, "pin_hash"):
                AWAITING_PIN[uid] = {"for": "setwallet", "next": ("SRC", text)}
                bot.reply_to(m, "🔐 Bitte PIN senden, um Source-Wallet zu ändern.")
                return
            WAITING_SOURCE_WALLET[uid] = False
            set_source_wallet(uid, text)
            price = get_sol_usd()
            px = f"(1 SOL ≈ {price:.2f} USDC)" if price > 0 else ""
            bot.reply_to(m, f"✅ Absender-Wallet gespeichert.\nSende SOL von `{md_escape(text)}` an `{md_escape(CENTRAL_SOL_PUBKEY)}`\n{px}", parse_mode="Markdown")
            return

    # User: Payout-Wallet speichern ODER Auszahlungsbetrag
    if WAITING_PAYOUT_WALLET.get(uid, False):
        if is_probably_solana_address(text):
            u = get_user(uid)
            if u and rget(u, "pin_hash"):
                AWAITING_PIN[uid] = {"for": "setwallet", "next": ("PAY", text)}
                bot.reply_to(m, "🔐 Bitte PIN senden, um Payout-Wallet zu ändern.")
                return
            WAITING_PAYOUT_WALLET[uid] = False
            set_payout_wallet(uid, text)
            bot.reply_to(m, f"✅ Auszahlungsadresse gespeichert: `{md_escape(text)}`\nGib nun den Betrag in SOL ein (z. B. 0.25).", parse_mode="Markdown")
            WAITING_WITHDRAW_AMOUNT[uid] = None
            return

    # Auszahlungsbetrag erfassen -> dynamisches Fee-Keyboard
    if WAITING_WITHDRAW_AMOUNT.get(uid) is None and text:
        # Schutz: Nicht versehentlich eine Wallet wieder verarbeiten
        if is_probably_solana_address(text):
            # wurde oben bereits behandelt; hier ignorieren
            pass
        else:
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
                kb = build_withdraw_keyboard_for_user(uid)
                bot.reply_to(m, f"Auszahlung: {fmt_sol_usdc(lam)} — Wähle Lockup & Fee:", reply_markup=kb)
                return
            except Exception:
                pass

    # Default
    bot.reply_to(m, "Ich habe das nicht verstanden. Nutze das Menü.", reply_markup=kb_main(get_user(uid)))
    # ========= Teil 3/3: Abo, Referral-Claim, Watcher, Admin, Loops =========

# -------- Subscription constants & helpers --------
SUBSCRIPTION_SOL_PUBKEY = os.getenv("SUBSCRIPTION_SOL_PUBKEY", "F6e1Q2KrX9cAboPayWalletxxxxxxxxxxxxxxx").strip()
CREATOR_PREMIUM_EUR = 250

# User wöchentliche Pläne (EUR/Woche) + Auszahlungs-Gebühren (0/5/7/10 Tage)
USER_WEEKLY_PLANS = {
    "BRONZE":  {"weekly_eur": 15,  "fees": {0: 15.0, 5: 12.5, 7: 7.5, 10: 5.0}},
    "SILVER":  {"weekly_eur": 30,  "fees": {0: 12.0, 5: 10.0, 7: 6.0, 10: 5.0}},
    "GOLD":    {"weekly_eur": 60,  "fees": {0: 10.0, 5: 8.0,  7: 5.0, 10: 5.0}},
    "PLATIN":  {"weekly_eur": 120, "fees": {0: 8.0,  5: 6.5,  7: 5.0, 10: 5.0}},
    # DIAMOND: Einmalzahlung 1000€ → Anteil/Shareholder
    "DIAMOND": {"oneoff_eur": 1000, "fees": {0: 5.0,  5: 5.0,  7: 5.0, 10: 5.0}, "grants_share": True}
}

# Referral-Prozente: normal vs. Premium
REF_PCTS_STANDARD = {1: 0.10, 2: 0.05, 3: 0.025}
REF_PCTS_PREMIUM  = {1: 0.15, 2: 0.075, 3: 0.0375}
CREATOR_PREMIUM_DIRECT_BONUS_USD = 12.5  # 10% von 125$-Premium-Anteil

# State für Abo-Kauf
SUB_PURCHASE_CTX: Dict[int, Dict] = {}
WAITING_SUB_SRC_WALLET: Dict[int, bool] = {}

# DB: zusätzliche Spalten & Tabellen
def ensure_subscription_columns():
    with get_db() as con:
        for stmt in [
            "ALTER TABLE users ADD COLUMN sub_tier TEXT DEFAULT 'FREE'",
            "ALTER TABLE users ADD COLUMN is_shareholder INTEGER DEFAULT 0",
            "ALTER TABLE users ADD COLUMN sub_src_wallet TEXT"
        ]:
            try: con.execute(stmt)
            except Exception: pass
        con.execute("""
            CREATE TABLE IF NOT EXISTS subs_payments(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              user_id INTEGER NOT NULL,
              plan TEXT NOT NULL,
              amount_lamports INTEGER DEFAULT 0,
              tx_sig TEXT,
              status TEXT DEFAULT 'PENDING',
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              UNIQUE(tx_sig)
            )
        """)

ensure_subscription_columns()

def set_sub_src_wallet(uid: int, addr: str):
    with get_db() as con:
        con.execute("UPDATE users SET sub_src_wallet=? WHERE user_id=?", (addr, uid))

def set_user_plan(uid: int, plan: str):
    plan = (plan or "FREE").upper()
    with get_db() as con:
        con.execute("UPDATE users SET sub_tier=?, is_shareholder=COALESCE(is_shareholder,0) WHERE user_id=?", (plan, uid))
        if USER_WEEKLY_PLANS.get(plan, {}).get("grants_share"):
            con.execute("UPDATE users SET is_shareholder=1 WHERE user_id=?", (uid,))

# Dynamische Fee-Tiers pro User-Plan
def fee_tiers_for_user(u_row) -> Dict[int,float]:
    plan = ((u_row or {}).get("sub_tier") or "FREE").upper()
    if plan in USER_WEEKLY_PLANS:
        return USER_WEEKLY_PLANS[plan]["fees"]
    return _fee_tiers  # Default aus ENV

# --------- Texte & Keyboards für Abo-Modelle ---------
CREATOR_TEXT = (
    "👑 *Creator Premium* — 250 € / Monat\n\n"
    "• Höhere Ref-Prozente (15% / 7,5% / 3,75%)\n"
    f"• +{CREATOR_PREMIUM_DIRECT_BONUS_USD:.1f} $ pro direkt geworbenes *Premium* (10% von 125$)\n"
    "• 25 $ jedes Premium fließen in den monatlichen Leader-Pool (Top-10 teilen sich den Pool)\n"
    "• 100 $ gehen ins Projekt (Betrieb/Dev/Marketing)\n\n"
    "💡 Premium ist freiwillig – ideal für aktive Networker."
)

def kb_plans_root():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('🔹 User-Modelle', callback_data='plans_user'),
           InlineKeyboardButton('🟩 Creator (Premium)', callback_data='plans_creator'))
    kb.add(InlineKeyboardButton('⬅️ Zurück', callback_data='back_home'))
    return kb

def kb_plans_user():
    kb = InlineKeyboardMarkup()
    for name in ["BRONZE","SILVER","GOLD","PLATIN","DIAMOND"]:
        kb.add(InlineKeyboardButton(f"{name.title()}", callback_data=f"plan_user_{name}"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="plans_root"))
    return kb

def kb_plans_creator():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"Premium buchen ({CREATOR_PREMIUM_EUR}€/Monat)", callback_data="plan_creator_premium"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="plans_root"))
    return kb

def user_plan_text(name: str) -> str:
    p = USER_WEEKLY_PLANS[name]
    if "weekly_eur" in p:
        price = f"{p['weekly_eur']} €/Woche"
    else:
        price = f"{p['oneoff_eur']} € einmalig"
    fz = p["fees"]
    lines = [
        f"📦 *{name.title()}* — {price}",
        "Auszahlungs-Gebühren je Lockup:",
        f"• Sofort: {fz[0]:.1f}%",
        f"• 5 Tage: {fz[5]:.1f}%",
        f"• 7 Tage: {fz[7]:.1f}%",
        f"• 10 Tage: {fz[10]:.1f}%"
    ]
    if p.get("grants_share"):
        lines.append("\n🎖️ *Diamond* gewährt Anteile (Shareholder) & Einnahmen aus Abo-Pool.")
    return "\n".join(lines)

# -------- Referral: Stats + Claim (mit Admin-Gutschreiben) --------
def _referral_stats_breakdown(uid: int) -> Dict[str,int]:
    with get_db() as con:
        rows = con.execute("""
            SELECT level, COUNT(*) AS clicks, COALESCE(SUM(deposit_total_lamports),0) AS dep
            FROM referrals WHERE referrer_user_id=? GROUP BY level
        """, (uid,)).fetchall()
    by_level = {int(r["level"]): (int(r["clicks"] or 0), int(r["dep"] or 0)) for r in rows}
    return {
        "l1_clicks": by_level.get(1,(0,0))[0], "l2_clicks": by_level.get(2,(0,0))[0], "l3_clicks": by_level.get(3,(0,0))[0],
        "l1_dep": by_level.get(1,(0,0))[1], "l2_dep": by_level.get(2,(0,0))[1], "l3_dep": by_level.get(3,(0,0))[1]
    }

def _user_is_premium_creator(uid: int) -> bool:
    u = get_user(uid)
    return (u and ((u.get("sub_tier") or "").upper() == "CREATOR_PREMIUM"))

def _ref_payout_estimate(uid: int) -> int:
    info = _referral_stats_breakdown(uid)
    # Standard oder Premium-Prozente
    pcts = REF_PCTS_PREMIUM if _user_is_premium_creator(uid) else REF_PCTS_STANDARD
    l1 = int(info["l1_dep"] * pcts[1])
    l2 = int(info["l2_dep"] * pcts[2])
    l3 = int(info["l3_dep"] * pcts[3])
    return l1 + l2 + l3

def _ref_stats_text(uid: int) -> str:
    info = _referral_stats_breakdown(uid)
    est = _ref_payout_estimate(uid)
    usd = get_sol_usd()
    lines = [
        "🔗 *Referral-Übersicht*",
        f"Level 1: Klicks {info['l1_clicks']} • Einzahlungen {fmt_sol_usdc(info['l1_dep'])}",
        f"Level 2: Klicks {info['l2_clicks']} • Einzahlungen {fmt_sol_usdc(info['l2_dep'])}",
        f"Level 3: Klicks {info['l3_clicks']} • Einzahlungen {fmt_sol_usdc(info['l3_dep'])}",
        f"= *Summe anzeigbar*: {fmt_sol_usdc(est)}",
        "\n_Hinweis: Premium-Creator erhalten 15%/7.5%/3.75% + 12.5$ je direktes Premium._"
    ]
    return "\n".join(lines)

def kb_referral():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📊 Meine Referral-Stats", callback_data="ref_stats"),
           InlineKeyboardButton("💵 Einlösen (Anfrage)", callback_data="ref_claim"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

# Refill-Claim Button → Admin-Card mit „Gutschreiben“
def send_referral_claim_to_admin(uid: int):
    est = _ref_payout_estimate(uid)
    txt = f"💵 Referral-Auszahlung angefragt von UID {uid}\n{_ref_stats_text(uid)}\n\nVorschlag Gutschrift: {fmt_sol_usdc(est)}"
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ Gutschreiben", callback_data=f"admin_refcredit_{uid}_{est}"))
    for aid in ADMIN_IDS:
        try:
            bot.send_message(int(aid), txt, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            pass

# Admin-Handler-Ergänzung (Gutschreiben)
@bot.callback_query_handler(func=lambda c: (c.data or "").startswith("admin_refcredit_"))
def on_admin_refcredit(c: CallbackQuery):
    if not is_admin(c.from_user.id):
        bot.answer_callback_query(c.id, "Nicht erlaubt."); return
    _, uid_s, est_s = c.data.split("_", 2)
    try:
        uid_t = int(uid_s); est = int(est_s)
    except:
        bot.answer_callback_query(c.id, "Fehlerhafte Daten."); return
    add_balance(uid_t, est)
    log_tx(uid_t, "ADJ", est, meta="referral claim credit")
    bot.answer_callback_query(c.id, "Gutschrift gebucht.")
    bot.send_message(uid_t, f"✅ Referral-Gutschrift: {fmt_sol_usdc(est)} verbucht.")

# Ergänzung im generischen Callback (Benutzerseite)
@bot.callback_query_handler(func=lambda c: (c.data or "") == "ref_claim")
def on_ref_claim(c: CallbackQuery):
    uid = c.from_user.id
    send_referral_claim_to_admin(uid)
    bot.answer_callback_query(c.id, "Einlösen angefragt")
    bot.send_message(uid, "✅ Anfrage gesendet. Ein Admin prüft und meldet sich.")
    return

# -------- Admin: Zusatz-Menüs (Listen/Broadcast/PNL/Balance etc.) --------
def kb_users_pagination(offset: int, total: int, prefix: str = "admin_view_users", page_size: int = 25):
    kb = InlineKeyboardMarkup()
    prev_off = max(0, offset - page_size)
    next_off = offset + page_size if offset + page_size < total else offset
    row = []
    if offset > 0: row.append(InlineKeyboardButton("◀️ Zurück", callback_data=f"{prefix}_{prev_off}"))
    if offset + page_size < total: row.append(InlineKeyboardButton("▶️ Weiter", callback_data=f"{prefix}_{next_off}"))
    if row: kb.add(*row)
    kb.add(InlineKeyboardButton("⬅️ Admin Menü", callback_data="admin_menu_big"))
    return kb

@bot.callback_query_handler(func=lambda c: (c.data or "").startswith("admin_") and c.data not in ("admin_menu_big",))
def on_admin_more(c: CallbackQuery):
    # Platzhalter – eigentliche Admin-Actions sind bereits in Teil 2 & unten in Loops/handlers verarbeitet.
    if not is_admin(c.from_user.id):
        bot.answer_callback_query(c.id, "Nicht erlaubt.")
        return
    bot.answer_callback_query(c.id, "OK")

# -------- Auszahlung finalisieren (PIN-fähig) --------
def _do_payout_option(uid: int, c: CallbackQuery):
    try:
        days = int((c.data or "").split("_", 1)[1])
    except Exception:
        bot.answer_callback_query(c.id, "Ungültige Auswahl."); return
    fee_percent = float(fee_tiers_for_user(get_user(uid)).get(days, 0.0))
    pending = WAITING_WITHDRAW_AMOUNT.get(uid, None)
    if pending is None or pending <= 0:
        bot.answer_callback_query(c.id, "Keine ausstehende Auszahlung. Betrag zuerst eingeben."); return
    amount_lam = int(pending)
    if not subtract_balance(uid, amount_lam):
        bot.answer_callback_query(c.id, "Unzureichendes Guthaben."); WAITING_WITHDRAW_AMOUNT.pop(uid, None); return
    with get_db() as con:
        cur = con.execute(
            "INSERT INTO payouts(user_id, amount_lamports, status, note, lockup_days, fee_percent) VALUES (?,?,?,?,?,?)",
            (uid, amount_lam, "REQUESTED", f"({days}d, fee {fee_percent}%)", days, fee_percent))
        pid = cur.lastrowid
    WAITING_WITHDRAW_AMOUNT.pop(uid, None)
    fee_lam = int(round(amount_lam * (fee_percent / 100.0))); net_lam = amount_lam - fee_lam
    log_tx(uid, "WITHDRAW_REQ", amount_lam, ref_id=str(pid), meta=f"lockup {days}d fee {fee_percent:.2f}% net {net_lam}")
    bot.answer_callback_query(c.id, "Auszahlung angefragt.")
    bot.send_message(uid,
        "💸 Auszahlung angefragt\n"
        f"Betrag: {fmt_sol_usdc(amount_lam)}\n"
        f"Lockup: {days} Tage\n"
        f"Gebühr: {fee_percent:.2f}% ({fmt_sol_usdc(fee_lam)})\n"
        f"Netto: {fmt_sol_usdc(net_lam)}")
    for aid in ADMIN_IDS:
        try:
            bot.send_message(int(aid),
                             f"🧾 Auszahlung #{pid}\nUser: {uid}\nBetrag: {fmt_sol_usdc(amount_lam)}\nLockup: {days}d • Fee: {fee_percent:.2f}%\nNetto: {fmt_sol_usdc(net_lam)}",
                             reply_markup=kb_payout_manage(pid))
        except Exception: pass

# -------- Abo-Watcher (separate Wallet) --------
def process_subscription_signature(sig: str, central_addr: str):
    # Hole Tx-Details ähnlich get_tx_details, aber für SUBSCRIPTION_SOL_PUBKEY
    try:
        r = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed"}])
        res = r.get('result'); 
        if not res or (res.get('meta') or {}).get('err'): return None
        txmsg = (res.get('transaction') or {}).get('message', {})
        meta = res.get('meta') or {}
        keys_raw = txmsg.get('accountKeys') or []
        keys = [k.get('pubkey') if isinstance(k, dict) else k for k in keys_raw]
        pre = meta.get('preBalances'); post = meta.get('postBalances')
        if pre is None or post is None: return None
        try:
            central_idx = keys.index(central_addr)
        except ValueError:
            return None
        delta = post[central_idx] - pre[central_idx]
        if delta <= 0: return None
        sender = None
        for i, (p, po) in enumerate(zip(pre, post)):
            if p - po >= delta - 1000:
                sender = keys[i]; break
        return {"from": sender, "amount_lamports": int(delta)}
    except Exception:
        return None

def activate_plan_for_sender(sender_addr: str, amount_lamports: int, tx_sig: str):
    # Finde User mit passender sub_src_wallet
    with get_db() as con:
        row = con.execute("SELECT user_id FROM users WHERE sub_src_wallet=?", (sender_addr,)).fetchone()
    if not row: 
        # Admin-Hinweis: unbekannte Abo-Zahlung
        for aid in ADMIN_IDS:
            try: bot.send_message(int(aid), f"⚠️ Unbekannte Abo-Zahlung von `{md_escape(sender_addr)}` • Betrag {fmt_sol_usdc(amount_lamports)} • Sig `{md_escape(tx_sig)}`", parse_mode="Markdown")
            except Exception: pass
        return
    uid = int(row["user_id"])

    # Grobe EUR-Schätzung um Plan zu erkennen (tolerant).
    usd = get_sol_usd(); eur_to_usd = 1.08
    eur_paid = (amount_lamports / LAMPORTS_PER_SOL) * usd / eur_to_usd if usd > 0 else 0

    # Heuristisch Plan ableiten (±20%)
    def eur_close(target):
        return abs(eur_paid - target) <= max(5, target * 0.2)

    plan = None
    for name, p in USER_WEEKLY_PLANS.items():
        t = p.get("weekly_eur") or p.get("oneoff_eur")
        if t and eur_close(t): plan = name; break
    if not plan and eur_close(CREATOR_PREMIUM_EUR):
        plan = "CREATOR_PREMIUM"

    if not plan:
        # Wenn SUB_PURCHASE_CTX existiert, nutze die Auswahl
        ctx = SUB_PURCHASE_CTX.get(uid, {})
        if ctx.get("plan"): plan = ctx["plan"]

    if not plan:
        # fallback
        plan = "BRONZE"

    set_user_plan(uid, plan)
    with get_db() as con:
        con.execute("UPDATE subs_payments SET amount_lamports=?, tx_sig=?, status='CONFIRMED' WHERE user_id=? AND status='PENDING' ORDER BY id DESC LIMIT 1",
                    (int(amount_lamports), tx_sig, uid))
    log_tx(uid, "SUB_PLAN", 0, ref_id=plan, meta=f"tx {tx_sig}")
    bot.send_message(uid, f"✅ Abo aktiv: *{plan.title()}*.\nVielen Dank! Neue Gebühren gelten sofort.", parse_mode="Markdown")

class SubWatcher:
    def __init__(self, central_addr: str):
        self.central = central_addr
        self._thread = None
        self._running = False

    def start(self, interval_sec: int = 45):
        if self._running: return
        self._running = True
        self._thread = threading.Thread(target=self._loop, args=(interval_sec,), daemon=True)
        self._thread.start()

    def _loop(self, interval: int):
        seen = set()
        while self._running:
            try:
                res = rpc("getSignaturesForAddress", [self.central, {"limit": 30}])
                arr = res.get("result") or []
                arr.reverse()
                for item in arr:
                    sig = item.get("signature")
                    if not sig or sig in seen: continue
                    seen.add(sig)
                    detail = process_subscription_signature(sig, self.central)
                    if not detail: continue
                    activate_plan_for_sender(detail["from"], int(detail["amount_lamports"]), sig)
            except Exception as e:
                print("SubWatcher error:", e)
            time.sleep(interval)

subwatcher = SubWatcher(SUBSCRIPTION_SOL_PUBKEY)
threading.Thread(target=subwatcher.start, kwargs={"interval_sec": 45}, daemon=True).start()

# -------- Background: Auto-Executor & Payout-Reminder (aus Teil 2 komplettieren) --------
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
                if (r["auto_mode"] or "OFF").upper() != "ON":
                    with get_db() as con:
                        con.execute("UPDATE executions SET status='ERROR', message='Auto OFF' WHERE id=?", (r["eid"],))
                    continue
                call = get_call(int(r["call_id"]))
                stake = int(r["stake_lamports"] or _compute_stake_for_user(int(r["user_id"])))
                # Simuliert / Live-Platzierung
                if SIMULATION_MODE:
                    if (rget(call, "market_type","FUTURES") or "FUTURES").upper() == "FUTURES":
                        result = futures_place_simulated(int(r["user_id"]), rget(call,"base",""), rget(call,"side",""), rget(call,"leverage",""), (r["auto_risk"] or "MEDIUM"))
                    else:
                        result = dex_market_buy_simulated(int(r["user_id"]), rget(call,"base",""), stake)
                else:
                    result = {"status": "FILLED", "txid": f"LIVE-{int(time.time())}"}
                status = result.get("status") or "FILLED"
                txid = result.get("txid") or result.get("order_id") or ""
                with get_db() as con:
                    con.execute("UPDATE executions SET status=?, txid=?, message=? WHERE id=?", (status, txid, "JOINED", r["eid"]))
                try:
                    urow = get_user(int(r["user_id"]))
                    bot.send_message(int(r["user_id"]),
                                     _auto_entry_message(urow, call, "JOINED", stake, txid),
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
                      AND (last_notified_at IS NULL OR (strftime('%s','now') - strftime('%s',COALESCE(last_notified_at,'1970-01-01')) > 3600))
                    ORDER BY created_at ASC
                """).fetchall()
            for r in rows:
                for aid in ADMIN_IDS:
                    try:
                        bot.send_message(int(aid), f"⏰ Erinnerung (offene Auszahlung) #{r['id']} • Betrag {fmt_sol_usdc(int(r['amount_lamports'] or 0))}", reply_markup=kb_payout_manage(int(r["id"])))
                    except Exception: pass
                with get_db() as con:
                    con.execute("UPDATE payouts SET last_notified_at=CURRENT_TIMESTAMP WHERE id=?", (int(r["id"]),))
            time.sleep(3600)
        except Exception as e:
            print("payout reminder loop error:", e)
            time.sleep(3600)

threading.Thread(target=auto_executor_loop, daemon=True).start()
threading.Thread(target=payout_reminder_loop, daemon=True).start()

print("Bot läuft — full stack (Teil 3/3).")
# (in Teil 1/2 steht bereits bot.infinity_polling(...) – dort belassen)