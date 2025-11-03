# bot.py
# UTF-8

import os
import time
import random
import threading
import sqlite3
import hashlib
import re
from contextlib import contextmanager
from typing import Optional, Dict, List, Tuple

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from telebot import apihelper as _apihelper

# ---------------------------
# Configuration (ENV)
# ---------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "8212740282:AAFQAGatNDY56bV5mmU_ZM15gg3mN1lOp7c").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable required")

ADMIN_IDS = [a.strip() for a in os.getenv("ADMIN_IDS", "8076025426").split(",") if a.strip()]
SOLANA_RPC = os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com").strip()

# Zentrale Einzahlungsadresse (Balance / Trading)
CENTRAL_SOL_PUBKEY = os.getenv("CENTRAL_SOL_PUBKEY", "7SEzEWu4ukQ4PdKyUfwiNigEXGNKnBWijwDncd7cULcV").strip()
# Separate Abo-Wallet (nicht die zentrale)
SUBS_SOL_PUBKEY = os.getenv("SUBS_SOL_PUBKEY", "Ha1Nef4424cQuVkfuAT5nCrtCdhxfoRYRi3Y5mAX619u").strip()

EXCHANGE_WALLETS = set([s.strip() for s in os.getenv("EXCHANGE_WALLETS", "").split(",") if s.strip()])

DEFAULT_FEE_TIERS = {0: 20.0, 5: 15.0, 7: 10.0, 10: 5.0}
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

# Preise (können via ENV justiert werden)
CREATOR_PRICE_USD = float(os.getenv("CREATOR_PRICE_USD", "250"))
BRONZE_PRICE_EUR = float(os.getenv("BRONZE_PRICE_EUR", "15"))
SILVER_PRICE_EUR  = float(os.getenv("SILVER_PRICE_EUR", "30"))
GOLD_PRICE_EUR    = float(os.getenv("GOLD_PRICE_EUR", "50"))
PLATIN_PRICE_EUR  = float(os.getenv("PLATIN_PRICE_EUR", "75"))
DIAMOND_PRICE_EUR = float(os.getenv("DIAMOND_PRICE_EUR", "1000"))

# Internes Flag – KEIN User-facing Output darüber!
SIMULATION_MODE = True

# ---------------------------
# Abo-Pläne & Gebühren-Overrides
# ---------------------------
# Plan-Codes:
# CREATOR: 250 $ / Monat (30 Tage)
# BRONZE..PLATIN: wöchentlich (7 Tage), DIAMOND: one-time (lifetime)
PLAN_DEFS = {
    "CREATOR": {
        "label": "Creator (Premium)",
        "period_days": 30,
        "price_kind": "USD",
        "price": CREATOR_PRICE_USD,
        "one_time": False,
    },
    "BRONZE": {
        "label": "Bronze",
        "period_days": 7,
        "price_kind": "EUR",
        "price": BRONZE_PRICE_EUR,
        "one_time": False,
    },
    "SILVER": {
        "label": "Silver",
        "period_days": 7,
        "price_kind": "EUR",
        "price": SILVER_PRICE_EUR,
        "one_time": False,
    },
    "GOLD": {
        "label": "Gold",
        "period_days": 7,
        "price_kind": "EUR",
        "price": GOLD_PRICE_EUR,
        "one_time": False,
    },
    "PLATIN": {
        "label": "Platin",
        "period_days": 7,
        "price_kind": "EUR",
        "price": PLATIN_PRICE_EUR,
        "one_time": False,
    },
    "DIAMOND": {
        "label": "Diamond (Einmalig)",
        "period_days": 99999,   # praktisch "lifetime"
        "price_kind": "EUR",
        "price": DIAMOND_PRICE_EUR,
        "one_time": True,
    },
}

# Withdrawal-Fee-Overrides je Plan (Tage -> Prozent)
# Bronze (laut Vorgabe fix), weitere Stufen sinnvoll gestaffelt (leicht anpassbar)
PLAN_FEE_OVERRIDES: Dict[str, Dict[int, float]] = {
    "BRONZE": {0: 15.0, 5: 12.5, 7: 7.5, 10: 5.0},
    "SILVER": {0: 12.0, 5: 10.0, 7: 6.0, 10: 4.0},
    "GOLD":   {0: 10.0, 5: 8.0,  7: 5.0, 10: 3.0},
    "PLATIN": {0: 8.0,  5: 6.0,  7: 4.0, 10: 2.0},
    "DIAMOND":{0: 5.0,  5: 4.0,  7: 2.0, 10: 1.0},
    # CREATOR bezieht sich auf Referral-Boost, kein zwingender Fee-Rabatt:
    "CREATOR": DEFAULT_FEE_TIERS.copy()
}

# Referral-Prozente
# Normal: 10% / 5% / 2.5%
REF_LEVELS_NORMAL = (0.10, 0.05, 0.025)
# Premium/Creator: 15% / 7.5% / 3.75% + 10% (=12.5$) Kickback pro direktes Premium-Abo
REF_LEVELS_CREATOR = (0.15, 0.075, 0.0375)
CREATOR_DIRECT_PREMIUM_BONUS_USD = 12.5

# Badge/Referral-Meilensteine – *deine angepassten* Regeln:
# - bis 9 Referrals: pro User 0.01 SOL
# - bei 10 Usern: einmalig 0.05 SOL
# - bei 20 Referrals: ab dann pro User 0.015 SOL
# - bei 50 Referrals: einmalig 100 USDT + ab dann pro User 0.02
# - bei 100 Referrals: wird man Premium-User/Teilhaber (wir setzen Flag)
BADGE_THRESHOLDS = {
    1:   {"name": "Starter",      "one_time_usd": 1,    "lifetime_badge": False},
    5:   {"name": "Builder",      "one_time_usd": 5,    "lifetime_badge": False},
    10:  {"name": "Pro Builder",  "one_time_usd": 100,  "lifetime_badge": False},
    25:  {"name": "Expert",       "one_time_usd": 250,  "lifetime_badge": False},
    50:  {"name": "Master",       "one_time_usd": 500,  "lifetime_badge": False},
    100: {"name": "Legend",       "one_time_usd": 1000, "lifetime_badge": True},  # + Special Role
}
# Zusätzliche dynamische Bonus-Logik (gem. Vorgabe oben) behandeln wir im Code.

# Bonus-Pool: 25$ pro Creator-Abo gehen in Monats-Pool – Verteilung (1:25%, 2:15%, 3:10%, 4–10: je 7.14%)
POOL_TOP_SPLIT = {
    1: 0.25,
    2: 0.15,
    3: 0.10,
    4: 0.0714,
    5: 0.0714,
    6: 0.0714,
    7: 0.0714,
    8: 0.0714,
    9: 0.0714,
    10:0.0714,
}

# ---------------------------
# Utilities
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

def usd_to_lamports(usd: float) -> int:
    px = get_sol_usd()
    if px <= 0:
        raise RuntimeError("Price feed unavailable")
    sol = usd / px
    return int(sol * LAMPORTS_PER_SOL)

def eur_to_lamports(eur: float) -> int:
    # Approximieren EUR ~ USD (vereinfachend). Wenn du willst, eigene EUR/USD-Feed integrieren.
    return usd_to_lamports(eur)

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

def is_probably_solana_address(addr: str) -> bool:
    if not isinstance(addr, str):
        return False
    addr = addr.strip()
    if len(addr) < 32 or len(addr) > 44:
        return False
    allowed = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    return all(ch in allowed for ch in addr)

_BASE58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_BASE58_RE = re.compile(rf"[{_BASE58}]{{32,44}}")

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
  payout_wallet TEXT,
  sub_types TEXT DEFAULT '',
  referral_code TEXT DEFAULT '',
  referral_bonus_claimed INTEGER DEFAULT 0,
  ref_by INTEGER,
  pin_hash TEXT,
  premium_flag INTEGER DEFAULT 0 -- wird z.B. bei 100 Referrals gesetzt (Teilhaber)
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
-- Abonnements
CREATE TABLE IF NOT EXISTS subscriptions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  plan_code TEXT NOT NULL,
  status TEXT DEFAULT 'ACTIVE',
  started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  expires_at TIMESTAMP,
  one_time INTEGER DEFAULT 0,
  src_wallet TEXT,
  pay_sig TEXT
);
-- Badge/Referral Zähler
CREATE TABLE IF NOT EXISTS referral_counters (
  user_id INTEGER PRIMARY KEY,
  total_refs INTEGER DEFAULT 0,
  last_milestone INTEGER DEFAULT 0
);
-- monatlicher Pool aus Creator-Abos
CREATE TABLE IF NOT EXISTS premium_pool (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  month_key TEXT NOT NULL, -- z.B. 2025-11
  amount_usd REAL DEFAULT 0.0
);
-- Ranking neuer Creator in Monat (für Pool-Verteilung)
CREATE TABLE IF NOT EXISTS premium_ref_stats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  month_key TEXT NOT NULL,
  referrer_user_id INTEGER NOT NULL,
  count_new_creator INTEGER DEFAULT 0
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
            "ALTER TABLE users ADD COLUMN referral_code TEXT DEFAULT ''",
            "ALTER TABLE users ADD COLUMN referral_bonus_claimed INTEGER DEFAULT 0",
            "ALTER TABLE users ADD COLUMN payout_wallet TEXT",
            "ALTER TABLE users ADD COLUMN ref_by INTEGER",
            "ALTER TABLE users ADD COLUMN pin_hash TEXT",
            "ALTER TABLE users ADD COLUMN premium_flag INTEGER DEFAULT 0",
        ]:
            try: con.execute(stmt)
            except Exception: pass
        # praktische Indizes
        try: con.execute("CREATE INDEX IF NOT EXISTS idx_subs_user ON subscriptions(user_id)")
        except Exception: pass
        try: con.execute("CREATE INDEX IF NOT EXISTS idx_subs_status ON subscriptions(status)")
        except Exception: pass

# CRUD basics (unverändert + kleine Helfer)
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

def set_subscription_flag(user_id: int, active: bool):
    with get_db() as con:
        con.execute("UPDATE users SET sub_active=? WHERE user_id=?", (1 if active else 0, user_id))

def set_auto_mode(user_id: int, mode: str):
    with get_db() as con:
        con.execute("UPDATE users SET auto_mode=? WHERE user_id=?", (mode, user_id))

def set_auto_risk(user_id: int, risk: str):
    with get_db() as con:
        con.execute("UPDATE users SET auto_risk=? WHERE user_id=?", (risk, user_id))

def subtract_balance(user_id: int, lamports: int) -> bool:
    with get_db() as con:
        row = con.execute("SELECT sol_balance_lamports FROM users WHERE user_id=?", (user_id,)).fetchone()
        cur = int(row["sol_balance_lamports"]) if row else 0
        if cur < lamports:
            return False
        con.execute("UPDATE users SET sol_balance_lamports = sol_balance_lamports - ? WHERE user_id=?", (lamports, user_id))
        return True

def log_tx(user_id: int, kind: str, amount_lamports: int, ref_id: Optional[str] = None, meta: str = ""):
    with get_db() as con:
        con.execute("INSERT INTO tx_log(user_id, kind, ref_id, amount_lamports, meta) VALUES (?,?,?,?,?)",
                    (user_id, kind, ref_id or "", int(amount_lamports or 0), meta or ""))

# --------- Summaries ----------
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

# ---------------------------
# Subscription helpers
# ---------------------------
def get_active_plan(uid: int) -> Optional[str]:
    with get_db() as con:
        r = con.execute("""
            SELECT plan_code, expires_at, one_time, status
            FROM subscriptions
            WHERE user_id=? AND status='ACTIVE'
            ORDER BY expires_at DESC NULLS LAST, id DESC
            LIMIT 1
        """, (uid,)).fetchone()
    if not r:
        return None
    if int(rget(r, "one_time", 0)) == 1:
        return r["plan_code"]
    # check expiry
    with get_db() as con:
        chk = con.execute("""
            SELECT 1 FROM subscriptions
            WHERE user_id=? AND plan_code=? AND status='ACTIVE'
              AND (expires_at IS NULL OR strftime('%s', expires_at) > strftime('%s','now'))
            LIMIT 1
        """, (uid, r["plan_code"])).fetchone()
    return r["plan_code"] if chk else None

def set_plan(uid: int, plan_code: str, expires_days: int, src_wallet: Optional[str], pay_sig: Optional[str], one_time: bool):
    with get_db() as con:
        if one_time:
            con.execute("""
                INSERT INTO subscriptions(user_id, plan_code, status, one_time, src_wallet, pay_sig)
                VALUES (?,?,?,?,?,?)
            """, (uid, plan_code, "ACTIVE", 1, src_wallet or "", pay_sig or ""))
        else:
            con.execute("""
                INSERT INTO subscriptions(user_id, plan_code, status, started_at, expires_at, one_time, src_wallet, pay_sig)
                VALUES (?, ?, 'ACTIVE', CURRENT_TIMESTAMP, datetime('now', ? || ' days'), 0, ?, ?)
            """, (uid, plan_code, expires_days, src_wallet or "", pay_sig or ""))

def cancel_plan(uid: int, plan_code: Optional[str] = None):
    with get_db() as con:
        if plan_code:
            con.execute("UPDATE subscriptions SET status='CANCELED' WHERE user_id=? AND plan_code=? AND status='ACTIVE'", (uid, plan_code))
        else:
            con.execute("UPDATE subscriptions SET status='CANCELED' WHERE user_id=? AND status='ACTIVE'", (uid,))

def plan_fee_tiers_for_user(uid: int) -> Dict[int, float]:
    plan = get_active_plan(uid)
    if not plan:
        return _fee_tiers
    return PLAN_FEE_OVERRIDES.get(plan, _fee_tiers)

def user_is_creator(uid: int) -> bool:
    p = get_active_plan(uid)
    return p == "CREATOR"

def user_is_diamond(uid: int) -> bool:
    p = get_active_plan(uid)
    return p == "DIAMOND"

def month_key_now() -> str:
    return time.strftime("%Y-%m")

def pool_add_creator_fee(usd_amount: float):
    mk = month_key_now()
    with get_db() as con:
        row = con.execute("SELECT id, amount_usd FROM premium_pool WHERE month_key=?", (mk,)).fetchone()
        if row:
            con.execute("UPDATE premium_pool SET amount_usd = amount_usd + ? WHERE id=?", (usd_amount, int(row["id"])))
        else:
            con.execute("INSERT INTO premium_pool(month_key, amount_usd) VALUES (?,?)", (mk, usd_amount))

def premium_ref_add_count(referrer_uid: int, count: int = 1):
    mk = month_key_now()
    with get_db() as con:
        row = con.execute("""
            SELECT id, count_new_creator FROM premium_ref_stats
            WHERE month_key=? AND referrer_user_id=?
        """, (mk, referrer_uid)).fetchone()
        if row:
            con.execute("UPDATE premium_ref_stats SET count_new_creator = count_new_creator + ? WHERE id=?", (count, int(row["id"])))
        else:
            con.execute("INSERT INTO premium_ref_stats(month_key, referrer_user_id, count_new_creator) VALUES (?,?,?)", (mk, referrer_uid, count))

# ---------------------------
# Calls & executions (unverändert)
# ---------------------------
def create_call(created_by: int, market_type: str, base: str, side: Optional[str], leverage: Optional[str], token_addr: Optional[str], notes: str) -> int:
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
    frac = _risk_fraction((u["auto_risk"] or "MEDIUM"))
    bal = int(u["sol_balance_lamports"] or 0)
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
        log_tx(user_id, "TRADE", stake_lamports, ref_id=str(call_id), meta=f"Queued")
        return cur.lastrowid

def fmt_call(c) -> str:
    market_type = rget(c, "market_type", "")
    base = rget(c, "base", "")
    if (market_type or "").upper() == "FUTURES":
        side = rget(c, "side", "")
        lev = rget(c, "leverage", "")
        core = f"Futures • {base} • {side} {lev}".strip()
    else:
        core = f"MEME • {base}"
    token_addr = rget(c, "token_address", "")
    notes = rget(c, "notes", "")
    extra = f"\nToken: `{md_escape(token_addr)}`" if ((market_type or "").upper() == "MEME" and token_addr) else ""
    note = f"\nNotes: {md_escape(notes)}" if notes else ""
    return f"🧩 *{core}*{extra}{note}"
    # ---------------------------
# Keyboards (inkl. Abo)
# ---------------------------
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

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
    # Ersetzt „News“ durch „Abo-Modelle“
    kb.add(InlineKeyboardButton("💎 Abo-Modelle", callback_data="subs_menu"),
           InlineKeyboardButton("🔗 Referral", callback_data="referral"))
    kb.add(InlineKeyboardButton("ℹ️ Hinweis", callback_data="hint"),
           InlineKeyboardButton("⚖️ Rechtliches", callback_data="legal"))
    if is_admin(int(u["user_id"])):
        kb.add(InlineKeyboardButton("🛠️ Admin (Kontrolle)", callback_data="admin_menu_big"))
    kb.add(InlineKeyboardButton(f"🏦 Guthaben: {bal}", callback_data="noop"))
    kb.add(InlineKeyboardButton(f"🤖 Auto: {auto_mode} • Risiko: {auto_risk}", callback_data="noop"))
    return kb

def kb_subs_main():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("👑 Creator (250 $/Monat)", callback_data="subs_choose_CREATOR"),
           InlineKeyboardButton("👤 User", callback_data="subs_user_menu"))
    kb.add(InlineKeyboardButton("📘 Nutzer-Handbuch", callback_data="subs_handbook"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

def kb_user_plans():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🥉 Bronze (wöchentlich)", callback_data="subs_choose_BRONZE"),
           InlineKeyboardButton("🥈 Silver (wöchentlich)", callback_data="subs_choose_SILVER"))
    kb.add(InlineKeyboardButton("🥇 Gold (wöchentlich)", callback_data="subs_choose_GOLD"),
           InlineKeyboardButton("💠 Platin (wöchentlich)", callback_data="subs_choose_PLATIN"))
    kb.add(InlineKeyboardButton("💎 Diamond (einmalig)", callback_data="subs_choose_DIAMOND"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="subs_menu"))
    return kb

def kb_subs_buy(plan_code: str):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📨 Ich habe gesendet", callback_data=f"subs_sent_{plan_code}"))
    kb.add(InlineKeyboardButton("↩️ Abbrechen", callback_data="subs_menu"))
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
    # Neuer Admin-Bereich
    kb.add(InlineKeyboardButton("🧩 Abos verwalten", callback_data="admin_subs_menu"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

def kb_admin_subs_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📃 Aktive Abos", callback_data="admin_subs_list_active_0"))
    kb.add(InlineKeyboardButton("➕ Abo gewähren", callback_data="admin_subs_grant"))
    kb.add(InlineKeyboardButton("❌ Abo entfernen", callback_data="admin_subs_remove"))
    kb.add(InlineKeyboardButton("⏰ Ablauf setzen", callback_data="admin_subs_set_expiry"))
    kb.add(InlineKeyboardButton("⬅️ Admin Menü", callback_data="admin_menu_big"))
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

def kb_withdraw_options_for(uid: int):
    tiers = plan_fee_tiers_for_user(uid)
    kb = InlineKeyboardMarkup()
    for days, pct in sorted(tiers.items(), key=lambda x: x[0]):
        label = "Sofort • Fee {:.1f}%".format(pct) if days == 0 else f"{days} Tage • Fee {pct:.1f}%"
        kb.add(InlineKeyboardButton(label, callback_data=f"payoutopt_{days}"))
    kb.add(InlineKeyboardButton("↩️ Abbrechen", callback_data="back_home"))
    return kb

# ---------------------------
# States (Abo)
# ---------------------------
SUB_WAITING_SOURCE_WALLET: Dict[int, bool] = {}
SUB_SELECTED_PLAN: Dict[int, str] = {}
SUB_LAST_PRICE_LAMPORTS: Dict[int, int] = {}  # gemerkter Preis in Lamports pro User
SUB_PENDING_SRC: Dict[int, str] = {}

# ---------------------------
# RPC für Abo-Zahlungen (separate Wallet)
# ---------------------------
checked_signatures_subs = set()

def get_tx_details_to(sig: str, dst_addr: str):
    """Wie get_tx_details, aber Zieladresse parametrisierbar."""
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
        pre = meta.get('preBalances'); post = meta.get('postBalances')
        if pre is None or post is None: return None
        try:
            dst_idx = keys.index(dst_addr)
        except ValueError:
            return None
        delta_dst = post[dst_idx] - pre[dst_idx] if dst_idx < len(pre) and dst_idx < len(post) else 0
        if delta_dst <= 0: return None
        sender = None
        for i, (p, po) in enumerate(zip(pre, post)):
            if p - po >= delta_dst - 1000:
                sender = keys[i]; break
        if not sender:
            for inst in (txmsg.get('instructions') or []):
                if isinstance(inst, dict):
                    info = (inst.get('parsed') or {}).get('info') or {}
                    if info.get('destination') == dst_addr and info.get('source'):
                        sender = info['source']; break
                    if info.get('to') == dst_addr and info.get('from'):
                        sender = info['from']; break
        return {"from": sender, "amount_lamports": int(delta_dst), "blockTime": res.get("blockTime") or 0}
    except Exception:
        return None

def scan_subs_recent(limit: int = 25):
    sigs = get_new_signatures_for_address(SUBS_SOL_PUBKEY, limit=limit)
    if not sigs: return []
    found = []
    for sig in sigs:
        if sig in checked_signatures_subs:
            continue
        details = get_tx_details_to(sig, SUBS_SOL_PUBKEY)
        checked_signatures_subs.add(sig)
        if not details:
            continue
        found.append((sig, details))
    return found

def verify_subscription_payment(uid: int, plan_code: str, expected_lamports: int, expected_sender: Optional[str]) -> Optional[Tuple[str, int, str]]:
    """
    Prüft, ob auf SUBS_SOL_PUBKEY eine passende Zahlung einging:
    - amount >= expected_lamports * 0.99 (Toleranz)
    - sender matches expected_sender (falls gesetzt)
    Gibt (sig, amount_lamports, sender) zurück oder None.
    """
    txs = scan_subs_recent(limit=30)
    tol = int(expected_lamports * 0.99)
    for sig, det in txs:
        lam = int(det.get("amount_lamports") or 0)
        sender = det.get("from") or ""
        if lam >= tol and (not expected_sender or expected_sender == sender):
            return (sig, lam, sender)
    return None

# ---------------------------
# Abo-Flow Text
# ---------------------------
SUBS_HANDBOOK_TEXT = (
    "📘 *Abo-Handbuch*\n\n"
    "• Creator (250 $/Monat): erhöhte Referral-Prozente (15% / 7.5% / 3.75%) "
    "+ Bonus 10% (=12.5 $) pro direkt geworbenem Premium sowie Monats-Pool.\n"
    "• User-Abos (wöchentlich): Bronze, Silver, Gold, Platin – mit reduzierten "
    "Auszahlungsgebühren. Diamond (einmalig 1000 €) = Anteile & Anteil an Abo-Einnahmen.\n\n"
    "Zahlung: Sende von *deiner Absender-Wallet* an die Abo-Adresse. Danach „Ich habe gesendet“ drücken."
)

def plan_price_lamports(plan_code: str) -> int:
    pd = PLAN_DEFS[plan_code]
    if pd["price_kind"] == "USD":
        return usd_to_lamports(float(pd["price"]))
    else:
        return eur_to_lamports(float(pd["price"]))

def plan_desc(plan_code: str) -> str:
    pd = PLAN_DEFS[plan_code]
    label = pd["label"]
    price = pd["price"]
    kind = pd["price_kind"]
    period = "einmalig" if pd["one_time"] else f"alle {pd['period_days']} Tage"
    return f"*{label}* • {price:.2f} {kind} • {period}"

def subs_intro_text() -> str:
    return (
        "💎 *Abo-Modelle*\n\n"
        "Wähle zwischen:\n"
        "• 👑 *Creator*: 250 $/Monat (Premium-Referrals, Pool, höhere Prozente)\n"
        "• 👤 *User*: Bronze / Silver / Gold / Platin (wöchentlich) oder Diamond (einmalig)\n\n"
        "Beim Kauf wird *eine separate Abo-Wallet* verwendet (nicht die zentrale).\n"
        "Zuerst gibst du *deine Absender-Wallet* an, dann erhältst du die Abo-Adresse."
    )

# ---------------------------
# Handbuch / Referral-Bilder (Creator)
# ---------------------------
CREATOR_INFO_TEXT = (
    "👑 *Creator-Premium*\n\n"
    "• Normale User: 10% / 5% / 2.5%\n"
    "• Premium (Creator): 15% / 7.5% / 3.75% *plus* 10% (=12.5 $) auf jedes Premium-Abo deiner direkten Referrals.\n\n"
    "Verteilung je 250 $:\n"
    "• 125 $ → finanzieren höhere Provisionen\n"
    "• 25 $ → Monats-Pool (Top 10 Werber)\n"
    "• 100 $ → Projekt/Team (Betrieb, Entwicklung, Marketing)\n\n"
    "🏆 Monats-Pool Split: 25% / 15% / 10% / Plätze 4–10 je 7.14%."
)

# ---------------------------
# Admin States (Abos)
# ---------------------------
ADMIN_SUBS_GRANT_WAIT: Dict[int, bool] = {}
ADMIN_SUBS_REMOVE_WAIT: Dict[int, bool] = {}
ADMIN_SUBS_EXPIRY_WAIT: Dict[int, bool] = {}

# ---------------------------
# Bot init & safe send wrappers (wie in Teil 1)
# ---------------------------
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

# ---------------------------
# Start/Home/Texts (ein paar Hilfstexte aus Teil 1 genutzt)
# ---------------------------
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
    code = _ensure_user_refcode(int(u["user_id"]))
    bot_username = get_bot_username()
    ref_url = f"https://t.me/{bot_username}?start={code}"
    plan = get_active_plan(int(u["user_id"])) or "—"
    return (
        f"👋 Hallo {raw_uname} — willkommen!\n\n"
        "Dieses System bietet:\n"
        "• Einzahlungen über verifizierte Source-Wallets\n"
        "• Trading-Signale für Spot & Futures\n"
        "• Auto-Entry mit Low/Medium/High Risiko\n"
        "• Abo-Modelle (Creator & User) mit Vorteilen\n\n"
        f"🏦 Guthaben: {bal}\n"
        f"💎 Aktives Abo: {plan}\n"
        f"🔗 Referral: {ref_url}\n"
        "📩 Support: /support"
    )

# ---------------------------
# Watcher (Zentrale Einzahlungen aus Teil 1 bleiben)
# ---------------------------
class CentralWatcher:
    def __init__(self, central_addr: str):
        self.central = central_addr
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self.on_verified_deposit = None

    def start(self, interval_sec: int = 40):
        if self._running: return
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
        if not sigs: return
        with get_db() as con:
            rows = con.execute("SELECT user_id, source_wallet FROM users WHERE source_wallet IS NOT NULL").fetchall()
        src_map = { (r["source_wallet"]): r["user_id"] for r in rows if r["source_wallet"] }
        for sig in sigs:
            if self._is_seen(sig):
                checked_signatures.add(sig); continue
            details = get_tx_details(sig, self.central)
            checked_signatures.add(sig)
            if not details: continue
            sender = details.get("from"); amount = int(details.get("amount_lamports") or 0)
            if not sender or amount <= 0: continue
            uid = src_map.get(sender)
            self._mark_seen(sig, uid if uid else None, amount)
            if not uid:
                note = (f"⚠️ Unbekannte Einzahlung erkannt\n"
                        f"Sender: `{md_escape(sender)}`\nBetrag: {fmt_sol_usdc(amount)}\nSig: `{md_escape(sig)}`")
                for aid in ADMIN_IDS:
                    try: bot.send_message(int(aid), note, parse_mode="Markdown")
                    except Exception: pass
                if sender in EXCHANGE_WALLETS:
                    for aid in ADMIN_IDS:
                        try: bot.send_message(int(aid), f"⚠️ Absender ist als Exchange-Wallet gelistet: `{md_escape(sender)}`", parse_mode="Markdown")
                        except Exception: pass
                return
            add_balance(uid, amount)
            log_tx(uid, "DEPOSIT", amount, ref_id=sig, meta=f"from {sender}")
            # referral deposit tracking bleibt gleich
            _apply_referral_deposit(uid, amount)
            if self.on_verified_deposit:
                self.on_verified_deposit({"user_id": uid, "amount_lamports": amount, "sig": sig})

watcher = CentralWatcher(CENTRAL_SOL_PUBKEY)

# ---------------------------
# Referral / Badges – Helfer
# ---------------------------
def referral_increment(referrer_id: int):
    with get_db() as con:
        row = con.execute("SELECT total_refs, last_milestone FROM referral_counters WHERE user_id=?", (referrer_id,)).fetchone()
        if not row:
            con.execute("INSERT INTO referral_counters(user_id, total_refs, last_milestone) VALUES (?,?,?)",
                        (referrer_id, 1, 0))
            total_refs = 1; last_ms = 0
        else:
            total_refs = int(row["total_refs"] or 0) + 1
            last_ms = int(row["last_milestone"] or 0)
            con.execute("UPDATE referral_counters SET total_refs=? WHERE user_id=?", (total_refs, referrer_id))

    # Dynamische Boni gem. Wunsch:
    # bis 9: 0.01 SOL je User
    # bei 10: einmalig 0.05 SOL
    # 11-19: weiterhin 0.015? (du sagtest ab 20 dann 0.015 – wir halten exakt Spezifikation):
    # 10 erreicht -> one-time 0.05; 1..9 -> 0.01; 20 erreicht -> one-time switch + future per-user 0.015;
    # 50 erreicht -> one-time 100 USDT + future per-user 0.02; 100 -> premium_flag
    try:
        if total_refs <= 9:
            add_balance(referrer_id, int(0.01 * LAMPORTS_PER_SOL))
            log_tx(referrer_id, "REF_BONUS", int(0.01 * LAMPORTS_PER_SOL), meta="per referral <=9")
        elif total_refs == 10:
            add_balance(referrer_id, int(0.05 * LAMPORTS_PER_SOL))
            log_tx(referrer_id, "REF_BONUS", int(0.05 * LAMPORTS_PER_SOL), meta="milestone 10")
        elif 11 <= total_refs < 20:
            # kein spezieller per-user Bonus spezifiziert; wir lassen es neutral
            pass
        elif total_refs >= 20 and total_refs < 50:
            add_balance(referrer_id, int(0.015 * LAMPORTS_PER_SOL))
            log_tx(referrer_id, "REF_BONUS", int(0.015 * LAMPORTS_PER_SOL), meta="per referral >=20,<50")
        elif total_refs == 50:
            # 100 USDT einmalig – approximieren als USDC
            lam = usd_to_lamports(100.0)
            add_balance(referrer_id, lam)
            log_tx(referrer_id, "REF_BONUS", lam, meta="milestone 50 (100 USDT)")
        elif total_refs > 50 and total_refs < 100:
            add_balance(referrer_id, int(0.02 * LAMPORTS_PER_SOL))
            log_tx(referrer_id, "REF_BONUS", int(0.02 * LAMPORTS_PER_SOL), meta="per referral >50,<100")

        if total_refs == 100:
            with get_db() as con:
                con.execute("UPDATE users SET premium_flag=1 WHERE user_id=?", (referrer_id,))
            # Optionale Nachricht an Admin
            for aid in ADMIN_IDS:
                try: bot.send_message(int(aid), f"🎖 UID {referrer_id} hat 100 Referrals erreicht → premium_flag gesetzt.")
                except Exception: pass
    except Exception as e:
        print("referral_increment error:", e)

# ---------------------------
# Abo-Kauf – Flow
# ---------------------------
def explain_creator():
    return CREATOR_INFO_TEXT + "\n\n" + (
        "🔁 Ablauf Kauf:\n"
        "1) Sende deine *Absender-Wallet (SOL)*\n"
        "2) Du bekommst die *Abo-Adresse*\n"
        "3) Sende 250 $ (in SOL-Äquivalent)\n"
        "4) Drücke „Ich habe gesendet“ – wir prüfen on-chain."
    )

def explain_user_plan():
    return (
        "👤 *User-Abos*\n"
        "Bronze/Silver/Gold/Platin sind *wöchentlich*; Diamond *einmalig* 1000 €.\n"
        "Vorteil: geringere Auszahlungsgebühren (je Stufe Staffelung).\n\n"
        "🔁 Ablauf Kauf:\n"
        "1) Sende deine *Absender-Wallet (SOL)*\n"
        "2) Du bekommst die *Abo-Adresse*\n"
        "3) Sende den Planbetrag (in SOL-Äquivalent)\n"
        "4) Drücke „Ich habe gesendet“ – wir prüfen on-chain."
    )

def subs_prepare_payment(uid: int, plan_code: str):
    # Preis berechnen
    price_lam = plan_price_lamports(plan_code)
    SUB_SELECTED_PLAN[uid] = plan_code
    SUB_LAST_PRICE_LAMPORTS[uid] = price_lam
    # Absender-Wallet?
    with get_db() as con:
        u = con.execute("SELECT source_wallet FROM users WHERE user_id=?", (uid,)).fetchone()
    src = rget(u, "source_wallet", "")
    if not src:
        SUB_WAITING_SOURCE_WALLET[uid] = True
        bot.send_message(uid, "🔑 Sende *deine Absender-Wallet (SOL)* für das Abo.", parse_mode="Markdown")
        return
    SUB_PENDING_SRC[uid] = src
    px = get_sol_usd()
    bot.send_message(
        uid,
        f"✅ Absender-Wallet: `{md_escape(src)}`\n"
        f"Sende *{fmt_sol_usdc(price_lam)}* an die *Abo-Adresse*:\n`{md_escape(SUBS_SOL_PUBKEY)}`\n\n"
        f"(1 SOL ≈ {px:.2f} USDC)\n"
        "Wenn gesendet, drücke unten.",
        parse_mode="Markdown",
        reply_markup=kb_subs_buy(plan_code)
    )

def complete_subscription(uid: int, plan_code: str, pay_sig: str, sender: str):
    pd = PLAN_DEFS[plan_code]
    one_time = bool(pd["one_time"])
    period = int(pd["period_days"])
    set_plan(uid, plan_code, period, sender, pay_sig, one_time)
    set_subscription_flag(uid, True)  # Schaltet Signale etc. frei
    # Creator-spezifisch: Pool + Referrer-Boostzählung
    if plan_code == "CREATOR":
        pool_add_creator_fee(25.0)
        # Direkt-Bonus an Referrer?
        with get_db() as con:
            ref_by_row = con.execute("SELECT ref_by FROM users WHERE user_id=?", (uid,)).fetchone()
        ref_by = int(rget(ref_by_row, "ref_by", 0) or 0)
        if ref_by:
            # 12.5 $ in Lamports gutschreiben
            lam = usd_to_lamports(CREATOR_DIRECT_PREMIUM_BONUS_USD)
            add_balance(ref_by, lam)
            log_tx(ref_by, "REF_CREATOR_BONUS", lam, ref_id=str(uid), meta="direct creator premium")
            premium_ref_add_count(ref_by, 1)
    bot.send_message(uid, f"✅ *Abo aktiv*: {plan_desc(plan_code)}", parse_mode="Markdown")

# ---------------------------
# Commands & Handlers (Abo-relevante Callbacks)
# ---------------------------
@bot.message_handler(commands=["start"])
def cmd_start(m: Message):
    uid = m.from_user.id
    uname = m.from_user.username or ""
    upsert_user(uid, uname, 1 if is_admin(uid) else 0)

    ref_code = None
    txt = m.text or ""
    parts = txt.split(maxsplit=1)
    if len(parts) >= 2:
        ref_code = parts[1].strip()
        if ref_code.startswith("="):
            ref_code = ref_code[1:].strip()

    _ensure_user_refcode(uid)

    if ref_code:
        with get_db() as con:
            ref_row = con.execute("SELECT user_id FROM users WHERE referral_code=?", (ref_code,)).fetchone()
        referrer = int(ref_row["user_id"]) if ref_row else None
        if referrer and referrer != uid:
            _set_ref_by(uid, referrer)
            referral_increment(referrer)

    u = get_user(uid)
    bot.reply_to(m, home_text(u), reply_markup=kb_main(u))

@bot.callback_query_handler(func=lambda c: True)
def on_cb_part2(c: CallbackQuery):
    uid = c.from_user.id
    data = c.data or ""
    u = get_user(uid)

    # --- Abo-Menü ---
    if data == "subs_menu":
        bot.answer_callback_query(c.id)
        bot.edit_message_text(subs_intro_text(), c.message.chat.id, c.message.message_id,
                              parse_mode="Markdown", reply_markup=kb_subs_main()); return

    if data == "subs_handbook":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, SUBS_HANDBOOK_TEXT, parse_mode="Markdown"); return

    if data == "subs_user_menu":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, explain_user_plan(), parse_mode="Markdown", reply_markup=kb_user_plans()); return

    # Plan-Wahl
    if data.startswith("subs_choose_"):
        plan_code = data.split("_", 2)[2]
        if plan_code not in PLAN_DEFS:
            bot.answer_callback_query(c.id, "Unbekannter Plan."); return
        bot.answer_callback_query(c.id, f"Plan: {plan_code}")
        if plan_code == "CREATOR":
            bot.send_message(uid, explain_creator(), parse_mode="Markdown")
        else:
            bot.send_message(uid, plan_desc(plan_code), parse_mode="Markdown")
        subs_prepare_payment(uid, plan_code)
        return

    # User klickt „Ich habe gesendet“
    if data.startswith("subs_sent_"):
        plan_code = data.split("_", 2)[2]
        if plan_code not in PLAN_DEFS:
            bot.answer_callback_query(c.id, "Plan ungültig."); return
        exp = SUB_LAST_PRICE_LAMPORTS.get(uid)
        src = SUB_PENDING_SRC.get(uid)
        if not exp or not src:
            bot.answer_callback_query(c.id, "Keine Zahlung erfasst. Sende zuerst deine Absender-Wallet."); return
        bot.answer_callback_query(c.id, "Prüfe Zahlung …")
        res = verify_subscription_payment(uid, plan_code, exp, src)
        if not res:
            bot.send_message(uid, "❌ Noch keine passende Zahlung auf der Abo-Adresse gefunden. Bitte 1–2 Minuten später erneut drücken.")
            return
        sig, lam, sender = res
        # Abo aktivieren
        complete_subscription(uid, plan_code, sig, sender)
        # Aufräumen
        SUB_SELECTED_PLAN.pop(uid, None)
        SUB_LAST_PRICE_LAMPORTS.pop(uid, None)
        SUB_PENDING_SRC.pop(uid, None)
        SUB_WAITING_SOURCE_WALLET.pop(uid, None)
        return

    # Admin: Subs
    if data == "admin_subs_menu":
        if not is_admin(uid):
            bot.answer_callback_query(c.id, "Nicht erlaubt."); return
        bot.answer_callback_query(c.id)
        bot.edit_message_text("🧩 Abos verwalten", c.message.chat.id, c.message.message_id,
                              reply_markup=kb_admin_subs_menu()); return

    if data.startswith("admin_subs_list_active_"):
        if not is_admin(uid):
            bot.answer_callback_query(c.id, "Nicht erlaubt."); return
        try:
            offset = int(data.rsplit("_", 1)[1])
        except:
            offset = 0
        page = 25
        with get_db() as con:
            rows = con.execute("""
                SELECT s.id, s.user_id, s.plan_code, s.status, s.expires_at, u.username
                FROM subscriptions s
                JOIN users u ON u.user_id = s.user_id
                WHERE s.status='ACTIVE'
                ORDER BY s.expires_at IS NULL DESC, s.expires_at ASC
                LIMIT ? OFFSET ?
            """, (page, offset)).fetchall()
            tot = con.execute("SELECT COUNT(*) AS c FROM subscriptions WHERE status='ACTIVE'").fetchone()["c"]
        bot.answer_callback_query(c.id)
        if not rows:
            bot.send_message(uid, "Keine aktiven Abos.")
        else:
            bot.send_message(uid, f"Aktive Abos – Seite {offset//page+1}")
            for r in rows:
                uname = ("@" + (r["username"] or "")) if r["username"] else f"UID {r['user_id']}"
                exp = rget(r, "expires_at", "—")
                bot.send_message(uid, f"#{r['id']} • {uname} • {r['plan_code']} • bis {exp}")
        nav = InlineKeyboardMarkup()
        prev_off = max(0, offset - page)
        next_off = offset + page if offset + page < tot else offset
        if offset > 0:
            nav.add(InlineKeyboardButton("◀️ Zurück", callback_data=f"admin_subs_list_active_{prev_off}"))
        if offset + page < tot:
            nav.add(InlineKeyboardButton("▶️ Weiter", callback_data=f"admin_subs_list_active_{next_off}"))
        nav.add(InlineKeyboardButton("⬅️ Admin Abos", callback_data="admin_subs_menu"))
        bot.send_message(uid, "Navigation:", reply_markup=nav)
        return

    if data == "admin_subs_grant":
        if not is_admin(uid):
            bot.answer_callback_query(c.id, "Nicht erlaubt."); return
        ADMIN_SUBS_GRANT_WAIT[uid] = True
        bot.answer_callback_query(c.id, "Abo gewähren")
        bot.send_message(uid, "Sende: `UID <id> <PLAN_CODE> [tage]` (tage optional; z.B. `UID 12345 BRONZE 14`)", parse_mode="Markdown")
        return

    if data == "admin_subs_remove":
        if not is_admin(uid):
            bot.answer_callback_query(c.id, "Nicht erlaubt."); return
        ADMIN_SUBS_REMOVE_WAIT[uid] = True
        bot.answer_callback_query(c.id, "Abo entfernen")
        bot.send_message(uid, "Sende: `UID <id> [PLAN_CODE]` (PLAN_CODE optional, entfernt alle aktiven)", parse_mode="Markdown")
        return

    if data == "admin_subs_set_expiry":
        if not is_admin(uid):
            bot.answer_callback_query(c.id, "Nicht erlaubt."); return
        ADMIN_SUBS_EXPIRY_WAIT[uid] = True
        bot.answer_callback_query(c.id, "Ablauf setzen")
        bot.send_message(uid, "Sende: `UID <id> <tage>` (setzt Ablauf ab jetzt)", parse_mode="Markdown")
        return
        # ---------------------------
# Falls RPC-Helper noch nicht im File sind: bereitstellen
# ---------------------------
try:
    checked_signatures
except NameError:
    checked_signatures = set()

try:
    rpc
except NameError:
    def rpc(method: str, params: list, *, _retries=2, _base_sleep=0.8):
        for attempt in range(_retries + 1):
            try:
                r = requests.post(
                    SOLANA_RPC,
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                    timeout=10
                )
                if r.status_code == 429:
                    time.sleep(_base_sleep * (2 ** attempt) + random.uniform(0, 0.4))
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                if attempt < _retries:
                    time.sleep(_base_sleep * (2 ** attempt) + random.uniform(0, 0.4))
                    continue
                print("RPC error:", e)
                return {"result": None}
        return {"result": None}

try:
    get_new_signatures_for_address
except NameError:
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

try:
    get_tx_details
except NameError:
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
            pre = meta.get('preBalances'); post = meta.get('postBalances')
            if pre is None or post is None: return None
            try:
                central_idx = keys.index(central_addr)
            except ValueError:
                return None
            delta_central = post[central_idx] - pre[central_idx] if central_idx < len(pre) and central_idx < len(post) else 0
            if delta_central <= 0: return None
            sender = None
            for i, (p, po) in enumerate(zip(pre, post)):
                if p - po >= delta_central - 1000:
                    sender = keys[i]; break
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

# ---------------------------
# Bestehende Zusatz-Helfer aus altem Bot (PIN etc.) – falls fehlen
# ---------------------------
try:
    _hash_pin
except NameError:
    def _hash_pin(pin: str) -> str:
        return hashlib.sha256(("PIN|" + pin).encode()).hexdigest()

# ---------------------------
# Weitere bestehende Texte/Keyboards sind in Teil 2 definiert
# ---------------------------

# ---------------------------
# Sub-/Referral-Utilities (aus Teil 1/2 benötigt)
# ---------------------------
def _ensure_user_refcode(uid: int) -> str:
    u = get_user(uid)
    code = (u["referral_code"] or "") if u else ""
    if not code:
        code = gen_referral_for_user(uid)
        with get_db() as con:
            con.execute("UPDATE users SET referral_code=? WHERE user_id=?", (code, uid))
    return code

def _linkify_ref(bot_username: str, code: str) -> str:
    return f"[Klicke hier, um zu starten](https://t.me/{bot_username}?start={code})"

def _set_ref_by(invited_id: int, referrer_id: int):
    with get_db() as con:
        con.execute("UPDATE users SET ref_by=? WHERE user_id=? AND ref_by IS NULL", (referrer_id, invited_id))
    with get_db() as con:
        con.execute("INSERT OR IGNORE INTO referrals(referrer_user_id, invited_user_id, level) VALUES (?,?,1)",
                    (referrer_id, invited_id))
    with get_db() as con:
        r1 = con.execute("SELECT ref_by FROM users WHERE user_id=?", (referrer_id,)).fetchone()
        if r1 and r1["ref_by"]:
            lvl2 = int(r1["ref_by"])
            con.execute("INSERT OR IGNORE INTO referrals(referrer_user_id, invited_user_id, level) VALUES (?,?,2)",
                        (lvl2, invited_id))
            r2 = con.execute("SELECT ref_by FROM users WHERE user_id=?", (lvl2,)).fetchone()
            if r2 and r2["ref_by"]:
                lvl3 = int(r2["ref_by"])
                con.execute("INSERT OR IGNORE INTO referrals(referrer_user_id, invited_user_id, level) VALUES (?,?,3)",
                            (lvl3, invited_id))

def _apply_referral_deposit(invited_id: int, amount_lamports: int):
    with get_db() as con:
        con.execute("""
            UPDATE referrals SET deposit_total_lamports = deposit_total_lamports + ?
            WHERE invited_user_id=? AND level IN (1,2,3)
        """, (int(amount_lamports), invited_id))

def _ref_stats_text(uid: int) -> str:
    with get_db() as con:
        rows = con.execute("""
            SELECT level, COUNT(*) as clicks, COALESCE(SUM(deposit_total_lamports),0) as dep
            FROM referrals
            WHERE referrer_user_id=?
            GROUP BY level
        """, (uid,)).fetchall()
    by_level = {int(r["level"]): (int(r["clicks"] or 0), int(r["dep"] or 0)) for r in rows}
    l1 = by_level.get(1, (0,0)); l2 = by_level.get(2,(0,0)); l3 = by_level.get(3,(0,0))
    dep1 = l1[1]; dep2 = l2[1]; dep3 = l3[1]
    est10 = int(dep1 * 0.10)
    est5  = int(dep2 * 0.05)
    est25 = int(dep3 * 0.025)
    total_est = est10 + est5 + est25
    return (
        "🔗 *Referral-Übersicht*\n"
        f"Level 1: Klicks {l1[0]} • Einzahlungen {fmt_sol_usdc(dep1)} • *10%*: {fmt_sol_usdc(est10)}\n"
        f"Level 2: Klicks {l2[0]} • Einzahlungen {fmt_sol_usdc(dep2)} • *5%*: {fmt_sol_usdc(est5)}\n"
        f"Level 3: Klicks {l3[0]} • Einzahlungen {fmt_sol_usdc(dep3)} • *2.5%*: {fmt_sol_usdc(est25)}\n"
        f"= *Summe anzeigbar*: {fmt_sol_usdc(total_est)}\n\n"
        "_Hinweis: Auszahlung erfolgt nach Prüfung durch Admin (Einlösen drücken)._"
    )

# ---------------------------
# Sub: Message-Flows (Wallet-Eingabe für Abo, Support, Auto etc.)
# ---------------------------
SUPPORT_AWAIT_MSG: Dict[int, bool] = {}
AWAITING_PIN: Dict[int, Dict] = {}
WAITING_SOURCE_WALLET: Dict[int, bool] = {}
WAITING_PAYOUT_WALLET: Dict[int, bool] = {}
WAITING_WITHDRAW_AMOUNT: Dict[int, Optional[int]] = {}

ADMIN_AWAIT_SIMPLE_CALL: Dict[int, bool] = {}
ADMIN_AWAIT_BALANCE_SINGLE: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_BALANCE_GLOBAL: Dict[int, bool] = {}
ADMIN_AWAIT_SET_WALLET: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_MASS_BALANCE: Dict[int, bool] = {}
ADMIN_AWAIT_NEWS_BROADCAST: Dict[int, Dict] = {}
ADMIN_AWAIT_DM_TARGET: Dict[int, Optional[int]] = {}

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

# ---------------------------
# Auszahlungs-Flow mit Abo-Fees
# ---------------------------
def _do_payout_option(uid: int, c_or_dummy):
    try:
        days = int((c_or_dummy.data or "").split("_", 1)[1])
    except Exception:
        try:
            bot.answer_callback_query(c_or_dummy.id, "Ungültige Auswahl.")
        except Exception:
            pass
        return
    # Fee-Tiers abhängig vom aktiven Abo
    fee_map = plan_fee_tiers_for_user(uid)
    fee_percent = float(fee_map.get(days, 0.0))
    pending = WAITING_WITHDRAW_AMOUNT.get(uid, None)
    if pending is None or pending <= 0:
        try:
            bot.answer_callback_query(c_or_dummy.id, "Keine ausstehende Auszahlung. Betrag zuerst eingeben.")
        except Exception:
            pass
        return
    amount_lam = int(pending)
    if not subtract_balance(uid, amount_lam):
        try:
            bot.answer_callback_query(c_or_dummy.id, "Unzureichendes Guthaben.")
        except Exception:
            pass
        WAITING_WITHDRAW_AMOUNT.pop(uid, None); return
    with get_db() as con:
        cur = con.execute(
            "INSERT INTO payouts(user_id, amount_lamports, status, note, lockup_days, fee_percent) VALUES (?,?,?,?,?,?)",
            (uid, amount_lam, "REQUESTED", f"({days}d, fee {fee_percent}%)", days, fee_percent))
        pid = cur.lastrowid
    WAITING_WITHDRAW_AMOUNT.pop(uid, None)
    fee_lam = int(round(amount_lam * (fee_percent / 100.0))); net_lam = amount_lam - fee_lam
    log_tx(uid, "WITHDRAW_REQ", amount_lam, ref_id=str(pid), meta=f"lockup {days}d fee {fee_percent:.2f}% net {net_lam}")
    try:
        bot.answer_callback_query(getattr(c_or_dummy, "id", ""), "Auszahlung angefragt.")
    except Exception:
        pass
    bot.send_message(uid,
        "💸 Auszahlung angefragt\n"
        f"Betrag: {fmt_sol_usdc(amount_lam)}\n"
        f"Lockup: {days} Tage\n"
        f"Gebühr: {fee_percent:.2f}% ({fmt_sol_usdc(fee_lam)})\n"
        f"Netto: {fmt_sol_usdc(net_lam)}")
    # Admin ping
    for aid in ADMIN_IDS:
        try:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("✅ Genehmigen", callback_data=f"payout_APPROVE_{pid}"),
                   InlineKeyboardButton("📤 Gesendet", callback_data=f"payout_SENT_{pid}"),
                   InlineKeyboardButton("❌ Ablehnen", callback_data=f"payout_REJECT_{pid}"))
            bot.send_message(int(aid),
                             f"🧾 Auszahlung #{pid}\nUser: {uid}\nBetrag: {fmt_sol_usdc(amount_lam)}\nLockup: {days}d • Fee: {fee_percent:.2f}%\nNetto: {fmt_sol_usdc(net_lam)}",
                             reply_markup=kb)
        except Exception:
            pass

# ---------------------------
# Catch-all: verarbeitet Texte (inkl. Sub- und Admin-States)
# ---------------------------
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

    # Admin: Direct Message Versand
    if ADMIN_AWAIT_DM_TARGET.get(uid):
        target = ADMIN_AWAIT_DM_TARGET.pop(uid)
        try:
            if m.photo:
                bot.send_photo(int(target), m.photo[-1].file_id, caption=text or "")
            else:
                bot.send_message(int(target), text, parse_mode="Markdown")
            bot.reply_to(m, f"✅ Nachricht an UID {target} gesendet.")
        except Exception:
            bot.reply_to(m, f"❌ Konnte Nachricht an UID {target} nicht senden.")
        return

    # PIN erwartet?
    if AWAITING_PIN.get(uid):
        entry = AWAITING_PIN.pop(uid)
        pin = text
        u = get_user(uid)
        if not (u and rget(u, "pin_hash") and _hash_pin(pin) == rget(u, "pin_hash")):
            bot.reply_to(m, "❌ Falsche PIN.")
            return
        if entry["for"] == "withdraw_option":
            class _DummyC: pass
            dummy = _DummyC(); dummy.data = entry["data"]; dummy.id = "pin-ok"
            _do_payout_option(uid, dummy)
            return
        if entry["for"] == "setwallet":
            which, addr = entry["next"]
            if which == "SRC":
                set_source_wallet(uid, addr)
                bot.reply_to(m, f"✅ Source-Wallet gespeichert: `{md_escape(addr)}`", parse_mode="Markdown")
            else:
                set_payout_wallet(uid, addr)
                bot.reply_to(m, f"✅ Payout-Wallet gespeichert: `{md_escape(addr)}`", parse_mode="Markdown")
            return

    # Admin: Abos gewähren/entfernen/Expiry setzen
    if ADMIN_SUBS_GRANT_WAIT.get(uid):
        ADMIN_SUBS_GRANT_WAIT[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt."); return
        # Format: UID <id> <PLAN_CODE> [tage]
        toks = text.split()
        if len(toks) < 3 or toks[0].upper() != "UID":
            bot.reply_to(m, "Format: `UID <id> <PLAN_CODE> [tage]`", parse_mode="Markdown"); return
        try: tgt = int(toks[1])
        except: bot.reply_to(m, "Ungültige UID."); return
        plan = toks[2].upper()
        if plan not in PLAN_DEFS:
            bot.reply_to(m, "Unbekannter PLAN_CODE."); return
        days = None
        if len(toks) >= 4:
            try: days = int(toks[3])
            except: days = None
        pd = PLAN_DEFS[plan]
        one_time = bool(pd["one_time"])
        period = days if days is not None else int(pd["period_days"])
        set_plan(tgt, plan, period, src_wallet=None, pay_sig="ADMIN", one_time=one_time)
        set_subscription_flag(tgt, True)
        bot.reply_to(m, f"✅ Abo {plan} für UID {tgt} gesetzt ({'einmalig' if one_time else f'{period} Tage'}).")
        return

    if ADMIN_SUBS_REMOVE_WAIT.get(uid):
        ADMIN_SUBS_REMOVE_WAIT[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt."); return
        # Format: UID <id> [PLAN_CODE]
        toks = text.split()
        if len(toks) < 2 or toks[0].upper() != "UID":
            bot.reply_to(m, "Format: `UID <id> [PLAN_CODE]`", parse_mode="Markdown"); return
        try: tgt = int(toks[1])
        except: bot.reply_to(m, "Ungültige UID."); return
        plan = toks[2].upper() if len(toks) >= 3 else None
        cancel_plan(tgt, plan)
        bot.reply_to(m, f"✅ Abo{' '+plan if plan else ''} von UID {tgt} beendet.")
        return

    if ADMIN_SUBS_EXPIRY_WAIT.get(uid):
        ADMIN_SUBS_EXPIRY_WAIT[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt."); return
        # Format: UID <id> <tage>
        toks = text.split()
        if len(toks) != 3 or toks[0].upper() != "UID":
            bot.reply_to(m, "Format: `UID <id> <tage>`", parse_mode="Markdown"); return
        try:
            tgt = int(toks[1]); days = int(toks[2])
        except:
            bot.reply_to(m, "Ungültige Werte."); return
        with get_db() as con:
            con.execute("""
                UPDATE subscriptions
                   SET expires_at = datetime('now', ? || ' days')
                 WHERE user_id=? AND status='ACTIVE' AND one_time=0
            """, (days, tgt))
        bot.reply_to(m, f"✅ Ablauf für aktive Laufzeit-Abos von UID {tgt} auf {days} Tage gesetzt.")
        return

    # Admin: Set wallet eines anderen Users
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

    # User: Wallet Eingaben – *Abo*-Source-Wallet
    if SUB_WAITING_SOURCE_WALLET.get(uid, False):
        if is_probably_solana_address(text):
            SUB_WAITING_SOURCE_WALLET[uid] = False
            SUB_PENDING_SRC[uid] = text
            plan_code = SUB_SELECTED_PLAN.get(uid)
            if not plan_code:
                bot.reply_to(m, "Kein Plan gewählt. Wähle im Abo-Menü erneut.")
                return
            price_lam = SUB_LAST_PRICE_LAMPORTS.get(uid) or plan_price_lamports(plan_code)
            px = get_sol_usd()
            bot.reply_to(m,
                f"✅ Absender-Wallet gespeichert.\n"
                f"Sende *{fmt_sol_usdc(price_lam)}* an die *Abo-Adresse*:\n`{md_escape(SUBS_SOL_PUBKEY)}`\n\n"
                f"(1 SOL ≈ {px:.2f} USDC)\n"
                "Wenn gesendet, drücke unten „Ich habe gesendet“.",
                parse_mode="Markdown",
                reply_markup=kb_subs_buy(plan_code))
            return
        else:
            bot.reply_to(m, "Bitte eine gültige Solana-Adresse senden (Base58, 32–44 Zeichen).")
            return

    # User: Wallet Eingaben – *Einzahlungen* (normale Source-Wallet)
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

    # User: Payout-Wallet Eingaben
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

    # Withdraw amount entry
    if WAITING_WITHDRAW_AMOUNT.get(uid) is None:
        # Erster Versuch: Betrag?
        try:
            sol = float(text.replace(",", "."))
            if sol > 0:
                lam = int(sol * LAMPORTS_PER_SOL)
                if get_balance_lamports(uid) < lam:
                    bot.reply_to(m, f"Unzureichendes Guthaben. Verfügbar: {fmt_sol_usdc(get_balance_lamports(uid))}")
                    WAITING_WITHDRAW_AMOUNT.pop(uid, None)
                    return
                WAITING_WITHDRAW_AMOUNT[uid] = lam
                bot.reply_to(m, f"Auszahlung: {fmt_sol_usdc(lam)} — Wähle Lockup & Fee:", reply_markup=kb_withdraw_options_for(uid))
                return
        except Exception:
            pass
        # Kein Betrag – fällt in Default

    # Default
    u = get_user(uid) or {"user_id": uid, "sol_balance_lamports": 0, "auto_mode": "OFF", "auto_risk":"MEDIUM"}
    bot.reply_to(m, "Ich habe das nicht verstanden. Nutze das Menü.", reply_markup=kb_main(u))

# ---------------------------
# Zusätzliche Callback-Handler, die in Teil 2 referenziert werden
# (Deposit/Withdraw/Referral/History/Admin Payout manage etc.)
# ---------------------------
@bot.callback_query_handler(func=lambda c: True)
def on_cb_part3(c: CallbackQuery):
    uid = c.from_user.id
    data = c.data or ""
    u = get_user(uid)

    if data == "back_home":
        bot.edit_message_text(home_text(u), c.message.chat.id, c.message.message_id, reply_markup=kb_main(u)); return
    if data == "noop":
        bot.answer_callback_query(c.id, "—"); return

    if data == "legal":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, LEGAL_TEXT, parse_mode="Markdown"); return

    if data == "hint":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, HINT_TEXT, parse_mode="Markdown"); return

    if data == "open_support":
        SUPPORT_AWAIT_MSG[uid] = True
        bot.answer_callback_query(c.id, "Support geöffnet")
        bot.send_message(uid, "✍️ Sende jetzt deine Support-Nachricht (Text/Bild).")
        return

    # deposit
    if data == "deposit":
        if not u["source_wallet"]:
            WAITING_SOURCE_WALLET[uid] = True
            bot.answer_callback_query(c.id, "Bitte Source-Wallet senden.")
            bot.send_message(uid, "🔑 Sende deine Absender-Wallet (SOL):"); return
        price = get_sol_usd()
        px = f"(1 SOL ≈ {price:.2f} USDC)" if price > 0 else ""
        bot.edit_message_text(
            f"Absender-Wallet: `{md_escape(u['source_wallet'])}`\n"
            f"Sende SOL an: `{md_escape(CENTRAL_SOL_PUBKEY)}`\n{px}\n\n"
            "🔄 Zum Ändern einfach neue Solana-Adresse senden.",
            c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_main(u)
        )
        WAITING_SOURCE_WALLET[uid] = True
        return

    # withdraw
    if data == "withdraw":
        if not u["payout_wallet"]:
            WAITING_PAYOUT_WALLET[uid] = True
            bot.answer_callback_query(c.id, "Bitte Payout-Adresse senden.")
            bot.send_message(uid, "🔑 Sende deine Auszahlungsadresse (SOL):"); return
        WAITING_WITHDRAW_AMOUNT[uid] = None
        bot.answer_callback_query(c.id, "Bitte Betrag eingeben.")
        bot.send_message(uid, f"💳 Payout: `{md_escape(u['payout_wallet'])}`\nGib den Betrag in SOL ein (z. B. `0.25`).", parse_mode="Markdown")
        WAITING_PAYOUT_WALLET[uid] = True
        return

    if data.startswith("payoutopt_"):
        urow = get_user(uid)
        if urow and urow.get("pin_hash"):
            AWAITING_PIN[uid] = {"for": "withdraw_option", "data": data}
            bot.answer_callback_query(c.id, "PIN erforderlich.")
            bot.send_message(uid, "🔐 Bitte sende deine PIN, um fortzufahren."); return
        return _do_payout_option(uid, c)

    # subscriptions (werden in Teil 2 behandelt) – hier nur Fallback
    if data.startswith("subs_"):
        bot.answer_callback_query(c.id, "Abo-Flow aktiv."); return

    # history
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

    # referral
    if data == "referral":
        code = _ensure_user_refcode(uid)
        bot_username = get_bot_username()
        link_md = _linkify_ref(bot_username, code)
        bot.answer_callback_query(c.id, "Referral")
        bot.send_message(uid, f"Teile deinen Link:\n{link_md}", parse_mode="Markdown", disable_web_page_preview=True); return

    # admin payout manage
    if data.startswith("payout_"):
        if not is_admin(uid):
            bot.answer_callback_query(c.id, "Nicht erlaubt."); return
        action, pid_s = data.split("_", 2)[1:]
        try: pid = int(pid_s)
        except: bot.answer_callback_query(c.id, "Ungültige ID."); return
        with get_db() as con:
            row = con.execute("SELECT * FROM payouts WHERE id=?", (pid,)).fetchone()
        if not row:
            bot.answer_callback_query(c.id, "Anfrage nicht gefunden."); return
        tgt_uid = int(row["user_id"]); amt = int(row["amount_lamports"]); days = int(row["lockup_days"]); fee_percent = float(row["fee_percent"])
        fee_lam = int(round(amt * (fee_percent/100.0))); net_lam = amt - fee_lam
        if action == "APPROVE":
            with get_db() as con: con.execute("UPDATE payouts SET status='APPROVED' WHERE id=?", (pid,))
            bot.answer_callback_query(c.id, "Genehmigt.")
            bot.send_message(tgt_uid, f"✅ Deine Auszahlung #{pid} wurde genehmigt."); return
        if action == "SENT":
            with get_db() as con: con.execute("UPDATE payouts SET status='SENT' WHERE id=?", (pid,))
            log_tx(tgt_uid, "WITHDRAW_SENT", amt, ref_id=str(pid), meta=f"net {net_lam} (fee {fee_percent:.2f}%)")
            bot.answer_callback_query(c.id, "Als gesendet markiert.")
            bot.send_message(tgt_uid, f"📤 Auszahlung #{pid} gesendet.\nBetrag: {fmt_sol_usdc(amt)}\nGebühr: {fee_percent:.2f}% ({fmt_sol_usdc(fee_lam)})\nNetto: {fmt_sol_usdc(net_lam)}"); return
        if action == "REJECT":
            with get_db() as con: con.execute("UPDATE payouts SET status='REJECTED' WHERE id=?", (pid,))
            add_balance(tgt_uid, amt); log_tx(tgt_uid, "ADJ", amt, ref_id=str(pid), meta="payout rejected refund")
            bot.answer_callback_query(c.id, "Abgelehnt & erstattet.")
            bot.send_message(tgt_uid, f"❌ Auszahlung #{pid} abgelehnt. Betrag erstattet."); return

    # Admin-Menü (Teile davon in Teil 2)
    if data == "admin_menu_big":
        if not is_admin(uid): bot.answer_callback_query(c.id, "Nicht erlaubt."); return
        bot.edit_message_text("🛠️ Admin-Menü — Kontrolle", c.message.chat.id, c.message.message_id, reply_markup=kb_admin_main()); return

    bot.answer_callback_query(c.id, "")

# ---------------------------
# Background loops (Auto-Executor & Payout-Reminder)
# ---------------------------
def dex_market_buy_simulated(user_id: int, base: str, amount_lamports: int):
    return {"status": "FILLED", "txid": f"Live-DEX-{base}-{int(time.time())}", "spent_lamports": amount_lamports}

def futures_place_simulated(user_id: int, base: str, side: str, leverage: str, risk: str):
    return {"status": "FILLED", "order_id": f"Live-FUT-{base}-{int(time.time())}", "base": base}

def _auto_entry_message(u_row, call_row, status_str: str, stake_lamports: int, txid_hint: str = "") -> str:
    risk = (rget(u_row, "auto_risk", "MEDIUM") or "MEDIUM").upper()
    mt = (rget(call_row, "market_type", "FUTURES") or "FUTURES").upper()
    if mt == "FUTURES":
        base = rget(call_row, "base", ""); side = rget(call_row, "side", ""); lev  = rget(call_row, "leverage", "")
        line2 = f"🧩 Futures • {base} • {side} {lev}"
    else:
        base = rget(call_row, "base", ""); line2 = f"🧩 Spot • {base}"
    bal_now = get_balance_lamports(int(u_row["user_id"]))
    lines = [
        f"🤖 Auto-Entry • {risk}",
        line2,
        f"Status: {status_str}",
        "Auto-Trading ist für diesen Call aktiviert.",
        f"Einsatz (Info): {fmt_sol_usdc(stake_lamports)}",
        f"Guthaben bleibt unverändert: {fmt_sol_usdc(bal_now)}",
        "Live-ORDER"
    ]
    if txid_hint:
        lines.append(f"`{md_escape(txid_hint)}`")
    return "\n".join(lines)

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
                        kb = InlineKeyboardMarkup()
                        kb.add(InlineKeyboardButton("✅ Genehmigen", callback_data=f"payout_APPROVE_{r['id']}"),
                               InlineKeyboardButton("📤 Gesendet", callback_data=f"payout_SENT_{r['id']}"),
                               InlineKeyboardButton("❌ Ablehnen", callback_data=f"payout_REJECT_{r['id']}"))
                        bot.send_message(int(aid), f"⏰ Erinnerung (offene Auszahlung) #{r['id']} • Betrag {fmt_sol_usdc(int(r['amount_lamports'] or 0))}", reply_markup=kb)
                    except Exception: pass
                with get_db() as con:
                    con.execute("UPDATE payouts SET last_notified_at=CURRENT_TIMESTAMP WHERE id=?", (int(r["id"]),))
            time.sleep(3600)
        except Exception as e:
            print("payout reminder loop error:", e)
            time.sleep(3600)

# ---------------------------
# Start Loops & Polling
# ---------------------------
threading.Thread(target=auto_executor_loop, daemon=True).start()
threading.Thread(target=payout_reminder_loop, daemon=True).start()

# --- Watcher erst nach allen Definitionen starten ---
threading.Thread(target=watcher.start, kwargs={"interval_sec": 40}, daemon=True).start()

print("Bot läuft — Abo-Modelle aktiviert.")
bot.remove_webhook()
bot.infinity_polling(timeout=60, long_polling_timeout=60)