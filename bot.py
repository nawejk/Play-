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
from typing import Optional, Dict, List

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from telebot import apihelper as _apihelper

# =========================
# Configuration (ENV)
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "8212740282:AAFdTWXF77hFSZj2ko9rbM3IYOhWs38-4cI").strip()
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable required")

# Admin-IDs kommasepariert, z. B. "111,222"
ADMIN_IDS = [a.strip() for a in os.getenv("ADMIN_IDS", "8076025426").split(",") if a.strip()]

# Solana Settings
SOLANA_RPC = os.getenv("SOLANA_RPC", "https://api.mainnet-beta.solana.com").strip()
CENTRAL_SOL_PUBKEY = os.getenv("CENTRAL_SOL_PUBKEY", "7SEzEWu4ukQ4PdKyUfwiNigEXGNKnBWijwDncd7cULcVa").strip()
EXCHANGE_WALLETS = set([s.strip() for s in os.getenv("EXCHANGE_WALLETS", "").split(",") if s.strip()])

# Withdrawal Fee-Tiers (LockupDays:Fee%)
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

# Internes Flag – KEIN User-facing Output darüber!
SIMULATION_MODE = True

# =========================
# Utilities
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
    """Sicherer Getter für sqlite3.Row und dicts."""
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

  -- Subscription / Roles
  sub_active INTEGER DEFAULT 0,         -- legacy simple flag
  sub_tier TEXT DEFAULT 'FREE',         -- 'FREE' | 'PREMIUM' | 'CREATOR'
  is_shareholder INTEGER DEFAULT 0,     -- optional flag for special perks

  -- Auto-Entry
  auto_mode TEXT DEFAULT 'OFF',         -- 'ON' | 'OFF'
  auto_risk TEXT DEFAULT 'MEDIUM',      -- 'LOW' | 'MEDIUM' | 'HIGH'

  -- Balances & Wallets
  sol_balance_lamports INTEGER DEFAULT 0,
  source_wallet TEXT,
  payout_wallet TEXT,

  -- Referral
  sub_types TEXT DEFAULT '',
  referral_code TEXT DEFAULT '',
  referral_bonus_claimed INTEGER DEFAULT 0,
  ref_by INTEGER,

  -- PIN
  pin_hash TEXT
);

-- Einzahlungen, die wir gesehen haben (für Guthaben/Refill + Referral-Earnings)
CREATE TABLE IF NOT EXISTS seen_txs (
  sig TEXT PRIMARY KEY,
  user_id INTEGER,
  amount_lamports INTEGER,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Admin Calls (Futures/Meme) & Auto-Entry Executions
CREATE TABLE IF NOT EXISTS calls (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_by INTEGER NOT NULL,
  market_type TEXT NOT NULL, -- 'FUTURES' | 'MEME'
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

-- Auszahlungs-Anfragen
CREATE TABLE IF NOT EXISTS payouts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  amount_lamports INTEGER NOT NULL,
  status TEXT DEFAULT 'REQUESTED', -- REQUESTED | APPROVED | SENT | REJECTED
  note TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_notified_at TIMESTAMP,
  lockup_days INTEGER DEFAULT 0,
  fee_percent REAL DEFAULT 0.0
);

-- Transaktionslog (ADJ, DEPOSIT, WITHDRAW_REQ/SENT, TRADE, PNL, REFERRAL_CREDIT, REFERRAL_PAYOUT)
CREATE TABLE IF NOT EXISTS tx_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  kind TEXT NOT NULL,
  ref_id TEXT,
  amount_lamports INTEGER,
  meta TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Referral-Klicks & Refill-Aggregate (3 Ebenen)
CREATE TABLE IF NOT EXISTS referrals (
  referrer_user_id INTEGER NOT NULL,
  invited_user_id INTEGER NOT NULL,
  level INTEGER NOT NULL, -- 1,2,3
  clicked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  deposit_total_lamports INTEGER DEFAULT 0,
  PRIMARY KEY(referrer_user_id, invited_user_id, level)
);

-- Referral-Claims (Creator/User fordert Auszahlung seiner verdienten Prozente)
CREATE TABLE IF NOT EXISTS referral_claims (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  claimant_user_id INTEGER NOT NULL,
  total_eligible_lamports INTEGER NOT NULL,  -- Summe der aktuell berechtigten Vergütung
  total_claimed_lamports INTEGER NOT NULL,   -- was davon ausgezahlt/als Guthaben verbucht wird
  status TEXT DEFAULT 'REQUESTED',           -- REQUESTED | APPROVED | CREDITED | REJECTED
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Subscription Payments Historie (optional für Pläne)
CREATE TABLE IF NOT EXISTS subs_payments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  plan TEXT NOT NULL,                        -- 'PREMIUM' etc.
  amount_lamports INTEGER NOT NULL,
  tx_sig TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(tx_sig)
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
        # Backfills (idempotent)
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
            "ALTER TABLE users ADD COLUMN sub_tier TEXT DEFAULT 'FREE'",
            "ALTER TABLE users ADD COLUMN is_shareholder INTEGER DEFAULT 0",
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
        return [r["user_id"] for r in con.execute("SELECT user_id FROM users WHERE sub_active=1 OR sub_tier!='FREE'").fetchall()]

def all_auto_on_users() -> List[int]:
    with get_db() as con:
        return [r["user_id"] for r in con.execute("SELECT user_id FROM users WHERE UPPER(COALESCE(auto_mode,'OFF'))='ON'").fetchall()]

def add_balance(user_id: int, lamports: int):
    with get_db() as con:
        con.execute("UPDATE users SET sol_balance_lamports = sol_balance_lamports + ? WHERE user_id=?", (int(lamports or 0), user_id))

def set_balance(user_id: int, lamports: int):
    with get_db() as con:
        con.execute("UPDATE users SET sol_balance_lamports = ? WHERE user_id=?", (int(lamports or 0), user_id))

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

def set_subscription(user_id: int, active: bool, tier: str = None):
    with get_db() as con:
        con.execute("UPDATE users SET sub_active=?, sub_tier=COALESCE(?, sub_tier) WHERE user_id=?",
                    (1 if active else 0, tier, user_id))

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

# =========================
# Calls & executions
# =========================
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

# =========================
# Referral helpers (Levels: 10% / 5% / 0.5%)
# =========================
REF_LVL1 = 0.10
REF_LVL2 = 0.05
REF_LVL3 = 0.005  # 0,5%

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

def _ref_earnings(uid: int) -> Dict[str, int]:
    """Berechnet, wie viel der Referrer aktuell theoretisch verdient hat (Levels 1..3)."""
    with get_db() as con:
        rows = con.execute("""
            SELECT level, COALESCE(SUM(deposit_total_lamports),0) AS dep
            FROM referrals
            WHERE referrer_user_id=?
            GROUP BY level
        """, (uid,)).fetchall()
    by_level = {int(r["level"]): int(r["dep"] or 0) for r in rows}
    l1 = by_level.get(1, 0); l2 = by_level.get(2, 0); l3 = by_level.get(3, 0)
    e1 = int(l1 * REF_LVL1)
    e2 = int(l2 * REF_LVL2)
    e3 = int(l3 * REF_LVL3)
    return {"l1_dep": l1, "l2_dep": l2, "l3_dep": l3, "e1": e1, "e2": e2, "e3": e3, "total": e1 + e2 + e3}

def _ref_stats_text(uid: int) -> str:
    e = _ref_earnings(uid)
    total_est = e["total"]
    return (
        "🔗 *Refill-Stats*\n"
        f"Level 1: Einzahlungen {fmt_sol_usdc(e['l1_dep'])} • *10%* ⇒ {fmt_sol_usdc(e['e1'])}\n"
        f"Level 2: Einzahlungen {fmt_sol_usdc(e['l2_dep'])} • *5%* ⇒ {fmt_sol_usdc(e['e2'])}\n"
        f"Level 3: Einzahlungen {fmt_sol_usdc(e['l3_dep'])} • *0,5%* ⇒ {fmt_sol_usdc(e['e3'])}\n"
        f"= *Summe anzeigbar*: {fmt_sol_usdc(total_est)}\n\n"
        "_Drücke „Einlösen“, damit ein Admin prüft und dir dein Anteil als Guthaben gutschreibt._"
    )

# =========================
# Texte (Rechtliches / Handbuch / Hinweise)
# =========================
LEGAL_TEXT = (
    "⚖️ *Rechtliches*\n\n"
    "• Dieser Bot stellt keine Finanzberatung dar.\n"
    "• Krypto-Handel ist mit erheblichen Risiken verbunden.\n"
    "• Nutzer sind für Ein-/Auszahlungen selbst verantwortlich.\n"
    "• Bei Unklarheiten wende dich an /support."
)

HANDBUCH_TEXT = (
    "📘 *Handbuch*\n\n"
    "• *Einzahlen*: Lege deine Absender-Wallet fest, sende SOL an die zentrale Wallet.\n"
    "• *Auto-Entry*: Bot nimmt Calls automatisch – Risiko LOW/MEDIUM/HIGH bestimmt Einsatz.\n"
    "• *Abo (User/Premium/Creator)*: Premium erhält Vorteile (z. B. höhere PnL-Multiplikatoren).\n"
    "• *Referral*: 10% / 5% / 0,5% deiner Downlines auf deren Refill (Einzahlungen), einlösbar.\n"
    "• *Auszahlen*: Betrag wählen, Lockup/Fees auswählen; Admin bestätigt Auszahlung.\n"
)

HINT_TEXT = (
    "ℹ️ *Hinweis*\n\n"
    "• Auto-Entry: Bot nimmt Calls automatisch mit deinem gewählten Risiko.\n"
    "• LOW/MEDIUM/HIGH beeinflusst den Einsatz pro Trade.\n"
    "• Du kannst Auto-Entry jederzeit im Menü ein-/ausschalten.\n"
    "• Support erreichst du mit /support."
)

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
    kb.add(InlineKeyboardButton("🔔 Abos/Signale", callback_data="sub_menu"),
           InlineKeyboardButton("🔗 Referral", callback_data="referral"))
    kb.add(InlineKeyboardButton("📘 Handbuch", callback_data="handbuch"),
           InlineKeyboardButton("⚖️ Rechtliches", callback_data="legal"))
    if is_admin(int(u["user_id"])):
        kb.add(InlineKeyboardButton("🛠️ Admin (Kontrolle)", callback_data="admin_menu_big"))
    kb.add(InlineKeyboardButton(f"🏦 Guthaben: {bal}", callback_data="noop"))
    kb.add(InlineKeyboardButton(f"🤖 Auto: {auto_mode} • Risiko: {auto_risk}", callback_data="noop"))
    return kb

def kb_sub_menu(u=None):
    tier = (rget(u, "sub_tier", "FREE") if u else "FREE").upper()
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔔 USER (Free)", callback_data="sub_set_FREE"),
           InlineKeyboardButton("🟩 PREMIUM", callback_data="sub_set_PREMIUM"))
    kb.add(InlineKeyboardButton("🛠️ CREATOR", callback_data="sub_set_CREATOR"))
    kb.add(InlineKeyboardButton(f"Status: {tier}", callback_data="noop"))
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

def kb_withdraw_options():
    kb = InlineKeyboardMarkup()
    for days, pct in sorted(_fee_tiers.items(), key=lambda x: x[0]):
        label = "Sofort • Fee 20%" if days == 0 else f"{days} Tage • Fee {pct}%"
        kb.add(InlineKeyboardButton(label, callback_data=f"payoutopt_{days}"))
    kb.add(InlineKeyboardButton("↩️ Abbrechen", callback_data="back_home"))
    return kb

def kb_referral():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📊 Meine Refill-Stats", callback_data="ref_stats"),
           InlineKeyboardButton("💵 Einlösen (Anfrage)", callback_data="ref_claim"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

def kb_payout_manage(pid: int):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Genehmigen", callback_data=f"payout_APPROVE_{pid}"),
           InlineKeyboardButton("📤 Gesendet", callback_data=f"payout_SENT_{pid}"),
           InlineKeyboardButton("❌ Ablehnen", callback_data=f"payout_REJECT_{pid}"))
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
    kb.add(InlineKeyboardButton("🎁 Promo/PNL", callback_data="admin_apply_pnl"))
    kb.add(InlineKeyboardButton("🏷️ Abo-Übersicht", callback_data="admin_subs_menu"))
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

def kb_investors_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🏆 Top 50", callback_data="admin_list_investors_top50"))
    kb.add(InlineKeyboardButton("📚 Alle (mit Seiten)", callback_data="admin_list_investors_all_0"))
    kb.add(InlineKeyboardButton("⬅️ Admin Menü", callback_data="admin_menu_big"))
    return kb

def kb_claim_admin(claim_id: int, claimant_uid: int, add_amount_lamports: int):
    """Admin-Buttons bei Referral-Claim: Guthaben hinzufügen etc."""
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ Guthaben gutschreiben", callback_data=f"claim_credit_{claim_id}_{claimant_uid}_{add_amount_lamports}"))
    kb.add(InlineKeyboardButton("❌ Ablehnen", callback_data=f"claim_reject_{claim_id}"))
    return kb
    # ========== Init & Bot Setup ==========
init_db()

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)  # Parse-Mode setzen wir gezielt

# Safe send wrappers (Markdown-Fallbacks)
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

# ========== States ==========
WAITING_SOURCE_WALLET: Dict[int, bool] = {}
WAITING_PAYOUT_WALLET: Dict[int, bool] = {}
WAITING_WITHDRAW_AMOUNT: Dict[int, Optional[int]] = {}
AWAITING_PIN: Dict[int, Dict] = {}

ADMIN_AWAIT_SIMPLE_CALL: Dict[int, bool] = {}
ADMIN_AWAIT_BALANCE_SINGLE: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_BALANCE_GLOBAL: Dict[int, bool] = {}
ADMIN_AWAIT_SET_WALLET: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_MASS_BALANCE: Dict[int, bool] = {}
ADMIN_AWAIT_NEWS_BROADCAST: Dict[int, Dict] = {}
ADMIN_AWAIT_DM_TARGET: Dict[int, Optional[int]] = {}
ADMIN_AWAIT_SET_SUB: Dict[int, bool] = {}
SUPPORT_AWAIT_MSG: Dict[int, bool] = {}

# ========== Bot-Helfer ==========
def get_bot_username():
    try:
        me = bot.get_me()
        return me.username or "<YourBotUsername>"
    except Exception:
        return "<YourBotUsername>"

def home_text(u) -> str:
    raw_uname = ("@" + (u["username"] or "")) if u["username"] else f"ID {u['user_id']}"
    bal = fmt_sol_usdc(int(u["sol_balance_lamports"] or 0))
    code = _ensure_user_refcode(int(u["user_id"]))
    bot_username = get_bot_username()
    ref_url = f"https://t.me/{bot_username}?start={code}"
    tier = (u["sub_tier"] or "FREE").upper()
    return (
        f"👋 Hallo {raw_uname} — willkommen!\n\n"
        "Dieses System bietet:\n"
        "• Einzahlungen über verifizierte Source-Wallets\n"
        "• Trading-Signale & Auto-Entry (LOW/MEDIUM/HIGH)\n"
        "• Abo-Modelle: USER / PREMIUM / CREATOR\n"
        "• Referral mit Ebenen 10% / 5% / 0,5%\n\n"
        f"🏦 Aktuelles Guthaben: {bal}\n"
        f"📦 Abo: {tier}\n"
        f"🔗 Referral: {ref_url}\n"
        "📘 Handbuch: /help  •  Support: /support"
    )

def _hash_pin(pin: str) -> str:
    return hashlib.sha256(("PIN|" + pin).encode()).hexdigest()

# ========== RPC / Watcher ==========
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
                time.sleep(_base_sleep * (2 ** attempt) + random.uniform(0, 0.4))
                continue
            r.raise_for_status()
            return r.json()
        except requests.RequestException:
            if attempt < _retries:
                time.sleep(_base_sleep * (2 ** attempt) + random.uniform(0, 0.4))
                continue
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
    except Exception:
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
    except Exception:
        return None

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
            if self.on_verified_deposit:
                self.on_verified_deposit({"user_id": uid, "amount_lamports": amount, "sig": sig})
            _apply_referral_deposit(uid, amount)

watcher = CentralWatcher(CENTRAL_SOL_PUBKEY)

def _on_verified_deposit(evt: dict):
    uid = evt["user_id"]; lam = evt["amount_lamports"]
    try:
        bot.send_message(uid, f"✅ Einzahlung verifiziert: {fmt_sol_usdc(lam)}\nNeues Guthaben: {fmt_sol_usdc(get_balance_lamports(uid))}", parse_mode="Markdown")
    except Exception:
        pass

watcher.on_verified_deposit = _on_verified_deposit
threading.Thread(target=watcher.start, kwargs={"interval_sec": 40}, daemon=True).start()

# ========== Auto-Entry Join Nachricht ==========
def _auto_entry_message(u_row, call_row, status_str: str, stake_lamports: int, txid_hint: str = "") -> str:
    risk = (rget(u_row, "auto_risk", "MEDIUM") or "MEDIUM").upper()
    mt = (rget(call_row, "market_type", "FUTURES") or "FUTURES").upper()
    if mt == "FUTURES":
        base = rget(call_row, "base", "")
        side = rget(call_row, "side", "")
        lev  = rget(call_row, "leverage", "")
        line2 = f"🧩 Futures • {base} • {side} {lev}"
    else:
        base = rget(call_row, "base", "")
        line2 = f"🧩 Spot • {base}"
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

# ========== Commands ==========
@bot.message_handler(commands=["help"])
def cmd_help(m: Message):
    bot.reply_to(m, HANDBUCH_TEXT, parse_mode="Markdown")

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

# ========== /start ==========
@bot.message_handler(commands=["start"])
def cmd_start(m: Message):
    uid = m.from_user.id
    uname = m.from_user.username or ""
    upsert_user(uid, uname, 1 if is_admin(uid) else 0)

    # Referral-Startparameter
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

    u = get_user(uid)
    bot.reply_to(m, home_text(u), reply_markup=kb_main(u))

# ========== Callback Handler ==========
@bot.callback_query_handler(func=lambda c: True)
def on_cb(c: CallbackQuery):
    uid = c.from_user.id
    u = get_user(uid)
    data = c.data or ""

    if data == "back_home":
        bot.edit_message_text(home_text(u), c.message.chat.id, c.message.message_id, reply_markup=kb_main(u)); return
    if data == "noop":
        bot.answer_callback_query(c.id, "—"); return

    if data == "legal":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, LEGAL_TEXT, parse_mode="Markdown"); return

    if data == "handbuch":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, HANDBUCH_TEXT, parse_mode="Markdown"); return

    if data == "open_support":
        SUPPORT_AWAIT_MSG[uid] = True
        bot.answer_callback_query(c.id, "Support geöffnet")
        bot.send_message(uid, "✍️ Sende jetzt deine Support-Nachricht (Text/Bild).")
        return

    # ===== Einzahlen =====
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

    # ===== Auszahlen =====
    if data == "withdraw":
        if not u["payout_wallet"]:
            WAITING_PAYOUT_WALLET[uid] = True
            bot.answer_callback_query(c.id, "Bitte Payout-Adresse senden.")
            bot.send_message(uid, "🔑 Sende deine Auszahlungsadresse (SOL):"); return
        WAITING_WITHDRAW_AMOUNT[uid] = None
        bot.answer_callback_query(c.id, "Bitte Betrag eingeben.")
        bot.send_message(uid, f"💳 Payout: `{md_escape(u['payout_wallet'])}`\nGib den Betrag in SOL ein (z. B. `0.25`).", parse_mode="Markdown")
        return

    # ===== Abos / Signale =====
    if data == "sub_menu":
        bot.edit_message_text("Abo-/Signal-Menü:", c.message.chat.id, c.message.message_id, reply_markup=kb_sub_menu(u)); return

    if data.startswith("sub_set_"):
        tier = data.split("_", 2)[2].upper()
        if tier not in ("FREE","PREMIUM","CREATOR"):
            bot.answer_callback_query(c.id, "Ungültiger Plan.")
            return
        set_subscription(uid, active=(tier!="FREE"), tier=tier)
        bot.answer_callback_query(c.id, f"Abo: {tier}")
        bot.send_message(uid, f"📦 Abo gesetzt: {tier}", reply_markup=kb_main(get_user(uid)))
        return

    # ===== Auto-Entry =====
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

    # ===== Verlauf / Portfolio =====
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

    if data == "my_portfolio":
        bal_lam = get_balance_lamports(uid)
        deps_lam = sum_user_deposits(uid)
        delta_lam = bal_lam - deps_lam
        bot.answer_callback_query(c.id, "Portfolio")
        bot.send_message(uid,
                         f"🏦 Guthaben: {fmt_sol_usdc(bal_lam)}\n"
                         f"📥 Einzahlungen gesamt: {fmt_sol_usdc(deps_lam)}\n"
                         f"Δ seit Start: {fmt_sol_usdc(delta_lam)}"); return

    # ===== Referral =====
    if data == "referral":
        code = _ensure_user_refcode(uid)
        bot_username = get_bot_username()
        link_md = _linkify_ref(bot_username, code)
        bot.answer_callback_query(c.id, "Referral")
        bot.send_message(uid, f"Teile deinen Link:\n{link_md}", parse_mode="Markdown", disable_web_page_preview=True, reply_markup=kb_referral()); return

    if data == "ref_stats":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, _ref_stats_text(uid), parse_mode="Markdown"); return

    if data == "ref_claim":
        # Summiere aktuelle Anspruchssumme:
        e = _ref_earnings(uid)
        total_est = int(e["total"])
        if total_est <= 0:
            bot.answer_callback_query(c.id, "Derzeit nichts einlösbar.")
            return
        # Claim anlegen
        with get_db() as con:
            cur = con.execute(
                "INSERT INTO referral_claims(claimant_user_id, total_eligible_lamports, total_claimed_lamports, status) VALUES (?,?,?,?)",
                (uid, total_est, total_est, "REQUESTED")
            )
            claim_id = cur.lastrowid

        # Admin benachrichtigen inkl. Button "Guthaben gutschreiben"
        for aid in ADMIN_IDS:
            try:
                bot.send_message(
                    int(aid),
                    "💵 Referral-Einlösen angefragt\n"
                    f"User: {uid}\n"
                    f"Level 1: {fmt_sol_usdc(e['e1'])} • Level 2: {fmt_sol_usdc(e['e2'])} • Level 3: {fmt_sol_usdc(e['e3'])}\n"
                    f"Summe: {fmt_sol_usdc(total_est)}",
                    reply_markup=kb_claim_admin(claim_id, uid, total_est)
                )
            except Exception:
                pass

        bot.answer_callback_query(c.id, "Einlösen angefragt")
        bot.send_message(uid, "✅ Anfrage gesendet. Ein Admin prüft und schreibt dir dein Anteil als Guthaben gut.")
        return

    # ===== Auszahlung – Option gewählt (inkl. PIN) =====
    if data.startswith("payoutopt_"):
        u = get_user(uid)
        if u and rget(u, "pin_hash"):
            AWAITING_PIN[uid] = {"for": "withdraw_option", "data": data}
            bot.answer_callback_query(c.id, "PIN erforderlich.")
            bot.send_message(uid, "🔐 Bitte sende deine PIN, um fortzufahren.")
            return
        _do_payout_option(uid, c)
        return

    bot.answer_callback_query(c.id, "")

# ===== Auszahlungshilfe =====
def _do_payout_option(uid: int, c: CallbackQuery):
    try:
        days = int((c.data or "").split("_", 1)[1])
    except Exception:
        bot.answer_callback_query(c.id, "Ungültige Auswahl."); return
    fee_percent = float(_fee_tiers.get(days, 0.0))
    pending = WAITING_WITHDRAW_AMOUNT.get(uid, None)
    if pending is None or pending <= 0:
        bot.answer_callback_query(c.id, "Keine ausstehende Auszahlung. Betrag zuerst eingeben."); return
    amount_lam = int(pending)
    if not subtract_balance(uid, amount_lam):
        bot.answer_callback_query(c.id, "Unzureichendes Guthaben.")
        WAITING_WITHDRAW_AMOUNT.pop(uid, None)
        return
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

# ========== Messages ==========
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

    # Admin: Set Subscription per Text (optional)
    if ADMIN_AWAIT_SET_SUB.get(uid):
        ADMIN_AWAIT_SET_SUB[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, 'Nicht erlaubt.')
            return
        try:
            txt = (m.text or '').strip()
            if not txt.upper().startswith('UID '):
                bot.reply_to(m, 'Format: `UID <id> <PLAN>`', parse_mode='Markdown')
                return
            _, id_s, plan = txt.split(None, 2)
            uid_t = int(id_s)
            plan = plan.strip().upper()
            if plan not in ("FREE","PREMIUM","CREATOR"):
                bot.reply_to(m, 'Plan muss FREE/PREMIUM/CREATOR sein.')
                return
            set_subscription(uid_t, active=(plan!="FREE"), tier=plan)
            bot.reply_to(m, f'✅ Abo gesetzt: UID {uid_t} → {plan}')
        except Exception as e:
            bot.reply_to(m, f'Fehler: {e}')
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
            class _DummyC:
                pass
            dummy = _DummyC()
            dummy.data = entry["data"]
            dummy.message = m
            dummy.id = "pin-ok"
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

    # User: Wallet Eingaben
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
        if is_probably_solana_address(text):
            u = get_user(uid)
            if u and rget(u, "pin_hash"):
                AWAITING_PIN[uid] = {"for": "setwallet", "next": ("PAY", text)}
                bot.reply_to(m, "🔐 Bitte PIN senden, um Payout-Wallet zu ändern.")
                return
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
            return
        except Exception:
            pass

    # Default
    bot.reply_to(m, "Ich habe das nicht verstanden. Nutze das Menü.", reply_markup=kb_main(get_user(uid)))
    # ========== Admin: Callback-Aktionen ==========
@bot.callback_query_handler(func=lambda c: (c.data or "").startswith(("admin_", "payout_", "claim_")))
def on_admin_cb(c: CallbackQuery):
    uid = c.from_user.id
    if not is_admin(uid):
        bot.answer_callback_query(c.id, "Nicht erlaubt.")
        return

    data = c.data or ""

    # Hauptmenü
    if data == "admin_menu_big":
        bot.edit_message_text("🛠️ Admin-Menü — Kontrolle", c.message.chat.id, c.message.message_id, reply_markup=kb_admin_main())
        return

    # Neuen Call anfordern
    if data == "admin_new_call":
        ADMIN_AWAIT_SIMPLE_CALL[uid] = True
        bot.answer_callback_query(c.id, "Call erstellen")
        bot.send_message(uid, "Sende den Call:\n• FUTURES|BASE|SIDE|LEV|OPTIONALE_NOTES\n• MEME|NAME|TOKEN_ADDRESS|OPTIONALE_NOTES")
        return

    # Broadcast: letzter Call (inkl. Auto-Entry)
    if data == "admin_broadcast_last":
        with get_db() as con:
            row = con.execute("SELECT * FROM calls ORDER BY id DESC LIMIT 1").fetchone()
        if not row:
            bot.answer_callback_query(c.id, "Kein Call vorhanden.")
            return

        msg = "📣 Neuer Call:\n" + fmt_call(row)
        subs = all_subscribers()
        sent_announce = 0
        for su in subs:
            try:
                bot.send_message(su, msg, parse_mode="Markdown")
                sent_announce += 1
            except Exception:
                pass

        auto_users = all_auto_on_users()
        joined = 0
        for au in auto_users:
            try:
                stake = _compute_stake_for_user(au)
                if (rget(row, "market_type", "FUTURES") or "FUTURES").upper() == "FUTURES":
                    result = futures_place_simulated(
                        au,
                        rget(row, "base", ""),
                        rget(row, "side", ""),
                        rget(row, "leverage", ""),
                        rget(get_user(au), "auto_risk", "MEDIUM")
                    )
                else:
                    result = dex_market_buy_simulated(au, rget(row, "base", ""), stake)
                txid = result.get("order_id") or result.get("txid") or "LIVE"
                queue_execution(int(row["id"]), au, status="FILLED", message="FILLED", stake_lamports=stake)
                urow = get_user(au)
                bot.send_message(au, _auto_entry_message(urow, row, "JOINED", stake, txid_hint=txid), parse_mode="Markdown")
                joined += 1
            except Exception as e:
                print("Broadcast auto error:", e)
                pass

        bot.answer_callback_query(c.id, f"📣 Ankündigungen: {sent_announce} • Auto-Entry JOINED: {joined}")
        return

    # Investoren-Menü
    if data == "admin_investors_menu":
        bot.answer_callback_query(c.id)
        bot.edit_message_text("👥 Investoren — Auswahl", c.message.chat.id, c.message.message_id, reply_markup=kb_investors_menu())
        return

    # Top 50 nach Guthaben
    if data == "admin_list_investors_top50":
        with get_db() as con:
            rows = con.execute("""
                SELECT user_id, username, sol_balance_lamports
                FROM users
                ORDER BY sol_balance_lamports DESC
                LIMIT 50
            """).fetchall()
        bot.answer_callback_query(c.id)
        if not rows:
            bot.send_message(uid, "Keine Nutzer.")
            return
        bot.send_message(uid, "👥 Investoren — Top 50")
        for r in rows:
            uname = ("@" + (r["username"] or "")) if r["username"] else f"UID {r['user_id']}"
            bot.send_message(uid, f"{uname} • Guthaben {fmt_sol_usdc(int(r['sol_balance_lamports'] or 0))}",
                             reply_markup=kb_user_row(int(r["user_id"])))
        return

    # Alle Investoren (Seiten)
    if data.startswith("admin_list_investors_all_"):
        try:
            offset = int(data.rsplit("_", 1)[1])
        except:
            offset = 0
        page_size = 25
        total = count_users()
        with get_db() as con:
            rows = con.execute("""
                SELECT user_id, username, sol_balance_lamports
                FROM users
                ORDER BY sol_balance_lamports DESC
                LIMIT ? OFFSET ?
            """, (page_size, offset)).fetchall()
        bot.answer_callback_query(c.id)
        bot.send_message(uid, f"👥 Investoren — Alle (Seite {offset//page_size+1})")
        for r in rows:
            uname = ("@" + (r["username"] or "")) if r["username"] else f"UID {r['user_id']}"
            bot.send_message(uid, f"{uname} • Guthaben {fmt_sol_usdc(int(r['sol_balance_lamports'] or 0))}",
                             reply_markup=kb_user_row(int(r["user_id"])))
        kb = InlineKeyboardMarkup()
        prev_off = max(0, offset - page_size)
        next_off = offset + page_size if offset + page_size < total else offset
        if offset > 0:
            kb.add(InlineKeyboardButton("◀️ Zurück", callback_data=f"admin_list_investors_all_{prev_off}"))
        if offset + page_size < total:
            kb.add(InlineKeyboardButton("▶️ Weiter", callback_data=f"admin_list_investors_all_{next_off}"))
        kb.add(InlineKeyboardButton("⬅️ Admin Menü", callback_data="admin_menu_big"))
        bot.send_message(uid, "Navigation:", reply_markup=kb)
        return

    # User-Übersicht (Seiten)
    if data.startswith("admin_view_users_"):
        try:
            offset = int(data.rsplit("_", 1)[1])
        except:
            offset = 0
        page_size = 25
        total = count_users()
        with get_db() as con:
            rows = con.execute("""
                SELECT user_id, username, sol_balance_lamports, source_wallet, payout_wallet, sub_active, sub_tier
                FROM users
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            """, (page_size, offset)).fetchall()
        bot.answer_callback_query(c.id)
        bot.send_message(uid, f"👀 Nutzer verwalten (Seite {offset//page_size+1})")
        for r in rows:
            uname = ("@" + (r["username"] or "")) if r["username"] else f"UID {r['user_id']}"
            sub_flag = "🔔" if int(r["sub_active"] or 0) == 1 or (r["sub_tier"] or "FREE") != "FREE" else "🔕"
            bot.send_message(uid,
                f"{uname} • {sub_flag} {(r['sub_tier'] or 'FREE')}\n"
                f"Guthaben: {fmt_sol_usdc(int(r['sol_balance_lamports'] or 0))}\n"
                f"SRC: `{md_escape(r['source_wallet'] or '-')}`\nPAY: `{md_escape(r['payout_wallet'] or '-')}`",
                parse_mode="Markdown",
                reply_markup=kb_user_actions(int(r["user_id"])))
        bot.send_message(uid, "Navigation:", reply_markup=kb_users_pagination(offset, total))
        return

    # Einzel-User-Infos
    if data.startswith("admin_user_"):
        try:
            target = int(data.split("_", 2)[2])
        except Exception:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        tu = get_user(target)
        if not tu:
            bot.answer_callback_query(c.id, "User nicht gefunden.")
            return
        txt = (f"👤 {('@'+tu['username']) if tu['username'] else 'UID '+str(tu['user_id'])}\n"
               f"Abo: {(tu['sub_tier'] or 'FREE')}\n"
               f"Guthaben: {fmt_sol_usdc(int(tu['sol_balance_lamports'] or 0))}\n"
               f"Source: `{md_escape(tu['source_wallet'] or '-')}`\n"
               f"Payout: `{md_escape(tu['payout_wallet'] or '-')}`")
        bot.answer_callback_query(c.id)
        bot.send_message(uid, txt, parse_mode="Markdown", reply_markup=kb_user_actions(target))
        return

    # Offene Auszahlungen
    if data == "admin_open_payouts":
        with get_db() as con:
            rows = con.execute("SELECT * FROM payouts WHERE status='REQUESTED' ORDER BY created_at ASC LIMIT 100").fetchall()
        if not rows:
            bot.answer_callback_query(c.id, "Keine offenen Auszahlungen.")
            return
        bot.answer_callback_query(c.id)
        for r in rows:
            pid = int(r["id"]); uline = get_user(int(r["user_id"]))
            uname = ("@" + (uline["username"] or "")) if (uline and uline["username"]) else f"UID {r['user_id']}"
            bot.send_message(uid,
                             f"#{pid} • {uname}\nBetrag: {fmt_sol_usdc(int(r['amount_lamports'] or 0))}\n"
                             f"Lockup {int(r['lockup_days'] or 0)}d • Fee {float(r['fee_percent'] or 0):.2f}%",
                             reply_markup=kb_payout_manage(pid))
        return

    # Stats
    if data == "admin_stats":
        with get_db() as con:
            users_total = con.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
            subs_total = con.execute("SELECT COUNT(*) AS c FROM users WHERE sub_active=1 OR sub_tier!='FREE'").fetchone()["c"]
        deposits = sum_total_deposits(); balances = sum_total_balances(); open_p = sum_open_payouts()
        bot.answer_callback_query(c.id)
        bot.send_message(uid,
                         "📊 System-Stats\n"
                         f"👥 Nutzer gesamt: {users_total}\n"
                         f"🔔 Abos aktiv/nicht-FREE: {subs_total}\n"
                         f"📥 Einzahlungen gesamt: {fmt_sol_usdc(deposits)}\n"
                         f"🏦 Gesamtguthaben: {fmt_sol_usdc(balances)}\n"
                         f"🧾 Offene Auszahlungen: {fmt_sol_usdc(open_p)}")
        return

    # Broadcast an alle
    if data == "admin_broadcast_all":
        ADMIN_AWAIT_NEWS_BROADCAST[uid] = {"step": "await_text_to_all"}
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "Sende die Nachricht, die an **alle Nutzer** gehen soll.", parse_mode="Markdown")
        return

    # Promo / PNL / Massenbalance
    if data == "admin_apply_pnl":
        ADMIN_AWAIT_MASS_BALANCE[uid] = True
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "Sende z. B.: `ALL -40%` / `PROMO PERCENT 20 ALL` / `PNL <CALL_ID> 20`", parse_mode="Markdown")
        return

    # Guthaben ändern (globaler Modus)
    if data == "admin_balance_edit":
        ADMIN_AWAIT_BALANCE_GLOBAL[uid] = True
        bot.answer_callback_query(c.id, "Guthaben ändern")
        bot.send_message(uid, "Format:\n• `UID 12345 0.25`\n• `@username -40%`\n• `ALL -20%`", parse_mode="Markdown")
        return

    # Einzel: Guthaben ändern
    if data.startswith("admin_balance_"):
        try:
            target = int(data.split("_", 2)[2])
        except:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        ADMIN_AWAIT_BALANCE_SINGLE[uid] = target
        bot.answer_callback_query(c.id, f"Balance UID {target}")
        bot.send_message(uid, "Sende Zahl wie `0.25` (SOL) oder Prozent wie `-40%`.", parse_mode="Markdown")
        return

    # Wallet eines Users setzen
    if data.startswith("admin_setwallet_"):
        try:
            target = int(data.split("_", 2)[2])
        except:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        ADMIN_AWAIT_SET_WALLET[uid] = target
        bot.answer_callback_query(c.id, f"Wallet UID {target}")
        bot.send_message(uid, "Format: `SRC <adresse>` oder `PAY <adresse>`", parse_mode="Markdown")
        return

    # DM an einen User
    if data.startswith("admin_msg_"):
        try:
            target = int(data.split("_", 2)[2])
        except:
            bot.answer_callback_query(c.id, "Ungültig")
            return
        ADMIN_AWAIT_DM_TARGET[uid] = target
        bot.answer_callback_query(c.id, "Nachricht")
        bot.send_message(uid, f"Sende die Nachricht für UID {target} (Markdown erlaubt).")
        return

    # Auszahlung-Management
    if data.startswith("payout_"):
        _, action, pid_s = data.split("_", 2)
        try:
            pid = int(pid_s)
        except:
            bot.answer_callback_query(c.id, "Ungültige ID.")
            return
        with get_db() as con:
            row = con.execute("SELECT * FROM payouts WHERE id=?", (pid,)).fetchone()
        if not row:
            bot.answer_callback_query(c.id, "Anfrage nicht gefunden.")
            return
        tgt_uid = int(row["user_id"]); amt = int(row["amount_lamports"]); fee_percent = float(row["fee_percent"])
        fee_lam = int(round(amt * (fee_percent/100.0))); net_lam = amt - fee_lam
        if action == "APPROVE":
            with get_db() as con: con.execute("UPDATE payouts SET status='APPROVED' WHERE id=?", (pid,))
            bot.answer_callback_query(c.id, "Genehmigt.")
            bot.send_message(tgt_uid, f"✅ Deine Auszahlung #{pid} wurde genehmigt.")
            return
        if action == "SENT":
            with get_db() as con: con.execute("UPDATE payouts SET status='SENT' WHERE id=?", (pid,))
            log_tx(tgt_uid, "WITHDRAW_SENT", amt, ref_id=str(pid), meta=f"net {net_lam} (fee {fee_percent:.2f}%)")
            bot.answer_callback_query(c.id, "Als gesendet markiert.")
            bot.send_message(tgt_uid, f"📤 Auszahlung #{pid} gesendet.\nBetrag: {fmt_sol_usdc(amt)}\nGebühr: {fee_percent:.2f}% ({fmt_sol_usdc(fee_lam)})\nNetto: {fmt_sol_usdc(net_lam)}")
            return
        if action == "REJECT":
            with get_db() as con: con.execute("UPDATE payouts SET status='REJECTED' WHERE id=?", (pid,))
            add_balance(tgt_uid, amt); log_tx(tgt_uid, "ADJ", amt, ref_id=str(pid), meta="payout rejected refund")
            bot.answer_callback_query(c.id, "Abgelehnt & erstattet.")
            bot.send_message(tgt_uid, f"❌ Auszahlung #{pid} abgelehnt. Betrag erstattet.")
            return

    # Referral-Claim: Guthaben gutschreiben / ablehnen
    if data.startswith("claim_credit_"):
        # claim_credit_<claim_id>_<uid>_<lamports>
        _, _, cid_s, who_s, lam_s = data.split("_", 4)
        try:
            cid = int(cid_s); who = int(who_s); lam = int(lam_s)
        except:
            bot.answer_callback_query(c.id, "Ungültig.")
            return
        with get_db() as con:
            con.execute("UPDATE referral_claims SET status='APPROVED' WHERE id=?", (cid,))
        add_balance(who, lam)
        log_tx(who, "REFERRAL_PAYOUT", lam, ref_id=str(cid), meta="claim credited as balance")
        bot.answer_callback_query(c.id, "Guthaben gutgeschrieben.")
        bot.send_message(who, f"✅ Dein Referral-Claim #{cid} wurde gutgeschrieben: {fmt_sol_usdc(lam)}")
        return

    if data.startswith("claim_reject_"):
        _, _, cid_s = data.split("_", 2)
        try:
            cid = int(cid_s)
        except:
            bot.answer_callback_query(c.id, "Ungültig.")
            return
        with get_db() as con:
            con.execute("UPDATE referral_claims SET status='REJECTED' WHERE id=?", (cid,))
        bot.answer_callback_query(c.id, "Claim abgelehnt.")
        return


# ========== Admin: Texteingaben / States ergänzen ==========
@bot.message_handler(func=lambda m: True)
def admin_state_catch(m: Message):
    uid = m.from_user.id
    text = (m.text or "").strip() if m.text else ""

    # Wenn bereits vom allgemeinen Handler behandelt wurde, nicht doppelt arbeiten:
    # (Wir lassen hier nur die Admin-States laufen, die in Teil 2 noch nicht abgedeckt waren.)

    # Neuer Call
    if ADMIN_AWAIT_SIMPLE_CALL.get(uid, False):
        ADMIN_AWAIT_SIMPLE_CALL[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        parts = [p.strip() for p in (text or "").split("|")]
        if len(parts) < 2:
            bot.reply_to(m, "Formatfehler.")
            return
        t0 = parts[0].upper()
        if t0 == "FUTURES" and len(parts) >= 4:
            _, base, side, lev = parts[:4]
            notes = parts[4] if len(parts) >= 5 else ""
            cid = create_call(uid, "FUTURES", base.upper(), side.upper(), lev, None, notes)
            c = get_call(cid)
            bot.reply_to(m, "✅ Call gespeichert:\n" + fmt_call(c), parse_mode="Markdown")
        elif t0 == "MEME" and len(parts) >= 3:
            _, name_or_symbol, token_addr = parts[:3]
            notes = parts[3] if len(parts) >= 4 else ""
            cid = create_call(uid, "MEME", name_or_symbol.upper(), None, None, token_addr, notes)
            c = get_call(cid)
            bot.reply_to(m, "✅ Call gespeichert:\n" + fmt_call(c), parse_mode="Markdown")
        else:
            bot.reply_to(m, "Formatfehler.")
        return

    # Einzel-Balance ändern
    if ADMIN_AWAIT_BALANCE_SINGLE.get(uid) is not None:
        target = ADMIN_AWAIT_BALANCE_SINGLE.pop(uid)
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        try:
            txt = text.replace(" ", "")
            if txt.endswith("%"):
                pct = float(txt[:-1].replace(",", "."))
                old = get_balance_lamports(target)
                new = int(round(old * (1 + pct / 100.0)))
                set_balance(target, new)
                log_tx(target, "ADJ", new - old, meta=f"admin {pct:+.2f}%")
                bot.reply_to(m, f"✅ UID {target}: {fmt_sol_usdc(old)} → {fmt_sol_usdc(new)} ({pct:+.2f}%)")
            else:
                sol = float(text.replace(",", "."))
                lam = int(sol * LAMPORTS_PER_SOL)
                old = get_balance_lamports(target)
                set_balance(target, lam)
                log_tx(target, "ADJ", lam - old, meta="admin set")
                bot.reply_to(m, f"✅ Guthaben gesetzt: UID {target} {fmt_sol_usdc(lam)}")
        except Exception:
            bot.reply_to(m, "Bitte Zahl (z. B. `0.25`) oder Prozent (z. B. `-40%`) senden.")
        return

    # Global / Promo / PNL
    if ADMIN_AWAIT_BALANCE_GLOBAL.get(uid, False) or ADMIN_AWAIT_MASS_BALANCE.get(uid, False):
        is_mass = ADMIN_AWAIT_MASS_BALANCE.get(uid, False)
        ADMIN_AWAIT_BALANCE_GLOBAL[uid] = False
        ADMIN_AWAIT_MASS_BALANCE[uid] = False
        if not is_admin(uid):
            bot.reply_to(m, "Nicht erlaubt.")
            return
        cmd = text.strip()
        try:
            # ALL +/- %
            if cmd.upper().startswith("ALL"):
                parts = cmd.split()
                if len(parts) != 2 or not parts[1].endswith("%"):
                    bot.reply_to(m, "Format: `ALL -40%` / `ALL +25%`")
                    return
                pct = float(parts[1][:-1].replace(",", "."))
                ids = all_users()
                changed = 0
                for uid_t in ids:
                    old = get_balance_lamports(uid_t)
                    new = int(round(old * (1 + pct / 100.0)))
                    set_balance(uid_t, new)
                    log_tx(uid_t, "ADJ", new - old, meta=f"mass {pct:+.2f}%")
                    changed += 1
                bot.reply_to(m, f"✅ Massenänderung: {changed} Nutzer ({pct:+.2f}%).")
                return

            toks = cmd.split()
            verb = toks[0].upper()
            if verb == "PROMO":
                typ = toks[1].upper()
                val = float(toks[2])
                scope = toks[3].upper() if len(toks) > 3 else "ALL"
                with get_db() as con:
                    if scope == "ALL":
                        rows = con.execute("SELECT user_id FROM users").fetchall()
                    elif scope == "SUBSCRIBERS":
                        rows = con.execute("SELECT user_id FROM users WHERE sub_active=1 OR sub_tier!='FREE'").fetchall()
                    else:
                        rows = []
                affected = 0
                for r in rows:
                    uid_t = int(r["user_id"])
                    if typ == "PERCENT":
                        bal = get_balance_lamports(uid_t)
                        delta = int(bal * (val / 100.0))
                        add_balance(uid_t, delta)
                        log_tx(uid_t, "ADJ", delta, meta=f"promo {val:.2f}%")
                    else:
                        lam = int(val * LAMPORTS_PER_SOL)
                        add_balance(uid_t, lam)
                        log_tx(uid_t, "ADJ", lam, meta=f"promo {val} SOL")
                    affected += 1
                bot.reply_to(m, f"✅ PROMO auf {affected} Nutzer.")
                return

            if verb == "PNL":
                call_id = int(toks[1])
                percent = float(toks[2])
                affected = 0
                with get_db() as con:
                    execs = con.execute(
                        "SELECT user_id, stake_lamports FROM executions WHERE call_id=? AND status IN ('FILLED','QUEUED')",
                        (call_id,)
                    ).fetchall()
                for ex in execs:
                    uid_t = int(ex["user_id"])
                    stake = int(ex["stake_lamports"] or 0)
                    u2 = get_user(uid_t)
                    risk = (rget(u2, "auto_risk", "MEDIUM") or "MEDIUM")
                    frac = _risk_fraction(risk)
                    pnl_lam = int(stake * (percent / 100.0) * frac)
                    add_balance(uid_t, pnl_lam)
                    log_tx(uid_t, "PNL", pnl_lam, ref_id=str(call_id), meta=f"{percent:+.2f}% * {frac:.2f}")
                    affected += 1
                bot.reply_to(m, f"✅ PNL (Call {call_id}) auf {affected} Nutzer.")
                return

            # Einzel-Target: UID/Username
            toks = cmd.split()
            if len(toks) < 2:
                bot.reply_to(m, "Format: `UID 12345 0.25` oder `@username -40%`", parse_mode="Markdown")
                return
            target_id = None
            value_token = None
            if toks[0].upper() == "UID":
                if len(toks) < 3:
                    bot.reply_to(m, "Format: `UID 12345 0.25`", parse_mode="Markdown")
                    return
                try:
                    target_id = int(toks[1])
                except Exception:
                    bot.reply_to(m, "Ungültige UID.", parse_mode="Markdown")
                    return
                value_token = toks[2]
            elif toks[0].startswith("@"):
                username = toks[0][1:]
                with get_db() as con:
                    r = con.execute("SELECT user_id FROM users WHERE username=?", (username,)).fetchone()
                if not r:
                    bot.reply_to(m, "User nicht gefunden.", parse_mode="Markdown")
                    return
                target_id = int(r["user_id"])
                value_token = toks[1]
            else:
                bot.reply_to(m, "`UID <id>` oder `@username` zuerst.", parse_mode="Markdown")
                return

            t = value_token.replace(" ", "")
            if t.endswith("%"):
                pct = float(t[:-1].replace(",", "."))
                old = get_balance_lamports(target_id)
                new = int(round(old * (1 + pct / 100.0)))
                set_balance(target_id, new)
                log_tx(target_id, "ADJ", new - old, meta=f"admin {pct:+.2f}%")
                bot.reply_to(m, f"✅ UID {target_id}: {fmt_sol_usdc(old)} → {fmt_sol_usdc(new)} ({pct:+.2f}%)")
            else:
                sol = float(t.replace(",", "."))
                lam = int(sol * LAMPORTS_PER_SOL)
                old = get_balance_lamports(target_id)
                set_balance(target_id, lam)
                log_tx(target_id, "ADJ", lam - old, meta="admin set")
                bot.reply_to(m, f"✅ Guthaben gesetzt: UID {target_id} {fmt_sol_usdc(lam)}")
        except Exception as e:
            bot.reply_to(m, f"Fehler: {e}")
        return

    # Broadcast an alle
    if ADMIN_AWAIT_NEWS_BROADCAST.get(uid):
        ctx = ADMIN_AWAIT_NEWS_BROADCAST.pop(uid, None)
        if ctx and ctx.get("step") == "await_text_to_all":
            msg = text
            ids = all_users()
            sent = 0
            for t in ids:
                try:
                    if m.photo:
                        bot.send_photo(t, m.photo[-1].file_id, caption=msg)
                    else:
                        bot.send_message(t, msg, parse_mode="Markdown")
                    sent += 1
                except Exception:
                    pass
            bot.reply_to(m, f"✅ Broadcast an {sent} Nutzer gesendet.")
        return

    # DM an einzelnes Ziel
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


# ========== Background Loops ==========
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
                # Place order (no user-facing "simulation")
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

print("Bot läuft — enhanced.")
bot.infinity_polling(timeout=60, long_polling_timeout=60)