import os, time, random, json, sqlite3, threading, requests, hashlib
from typing import Dict, List, Optional
from telebot import TeleBot, types
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, CallbackQuery

# ---------------------------
# CONFIGURATION
# ---------------------------

BOT_TOKEN = "8212740282:AAFsokebNJhI0GpAEP0zUUhePFApJAT8aGk"
ADMIN_IDS = [7919108078]

DB_PATH = "data.db"
SOLANA_RPC = "https://api.mainnet-beta.solana.com"
CENTRAL_SOL_PUBKEY = "Ha1Nef4424cQuVkfuAT5nCrtCdhxfoRYRi3Y5mAX619u"  # zentrale Wallet für Abos & Einzahlungen

LAMPORTS_PER_SOL = 1000000000
EURUSD_RATE = 1.08

# Preis-Tabellen
PLAN_PRICES = {
    "BRONZE":  ("USD", 16.5, "7 Tage"),
    "SILVER":  ("USD", 33.0, "7 Tage"),
    "GOLD":    ("USD", 55.0, "7 Tage"),
    "PLATINUM":("USD", 110.0, "7 Tage"),
    "DIAMOND": ("USD", 1100.0, "einmalig"),
    "PREMIUM": ("USD", 250.0, "30 Tage")
}

# ---------------------------
# INIT BOT
# ---------------------------

bot = TeleBot(BOT_TOKEN, parse_mode="Markdown")

# ---------------------------
# DATABASE
# ---------------------------

def get_db():
    con = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000")
    return con

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    sol_balance_lamports INTEGER DEFAULT 0,
    source_wallet TEXT,
    payout_wallet TEXT,
    ref_by INTEGER,
    role TEXT DEFAULT 'USER',
    sub_tier TEXT DEFAULT 'FREE',
    is_shareholder INTEGER DEFAULT 0,
    sub_expires TIMESTAMP DEFAULT NULL,
    sub_active INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS referrals (
    referrer_user_id INTEGER,
    invited_user_id INTEGER,
    level INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(referrer_user_id, invited_user_id)
);

CREATE TABLE IF NOT EXISTS referral_milestones (
    referrer_user_id INTEGER NOT NULL,
    milestone TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(referrer_user_id, milestone)
);

CREATE TABLE IF NOT EXISTS txlog (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    type TEXT,
    lamports INTEGER,
    ref_id TEXT,
    meta TEXT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payouts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount_lamports INTEGER,
    status TEXT,
    note TEXT,
    lockup_days INTEGER,
    fee_percent REAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_notified_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS subs_payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    plan TEXT,
    amount_lamports INTEGER,
    tx_sig TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tx_sig)
);
"""

def init_db():
    with get_db() as con:
        con.executescript(SCHEMA)
        con.commit()

def rget(row, key, default=None):
    try:
        return row[key] if row and key in row.keys() else default
    except Exception:
        return default

# ---------------------------
# PRICE & RPC UTILITIES
# ---------------------------

def get_sol_usd() -> Optional[float]:
    try:
        r = requests.get("https://price.jup.ag/v4/price?ids=SOL", timeout=8)
        data = r.json()
        return float(data["data"]["SOL"]["price"])
    except Exception:
        return None

def usd_to_lamports(usd: float) -> int:
    px = get_sol_usd() or 0.0
    if px <= 0:
        return 0
    sol = usd / px
    return int(sol * LAMPORTS_PER_SOL)

def fmt_sol_usdc(lam: int) -> str:
    sol = lam / LAMPORTS_PER_SOL
    px = get_sol_usd()
    if px:
        usdc = sol * px
        return f"{sol:.4f} SOL (~{usdc:.2f} USDC)"
    return f"{sol:.4f} SOL"

# ---------------------------
# USER UTILITIES
# ---------------------------

def user_exists(uid: int) -> bool:
    with get_db() as con:
        row = con.execute("SELECT 1 FROM users WHERE user_id=?", (uid,)).fetchone()
        return bool(row)

def ensure_user(m: Message):
    with get_db() as con:
        if not user_exists(m.from_user.id):
            con.execute(
                "INSERT OR IGNORE INTO users(user_id, username) VALUES (?, ?)",
                (m.from_user.id, m.from_user.username)
            )
            con.commit()

def get_user(uid: int):
    with get_db() as con:
        r = con.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
        return r

def set_balance(uid: int, lamports: int):
    with get_db() as con:
        con.execute("UPDATE users SET sol_balance_lamports=? WHERE user_id=?", (lamports, uid))
        con.commit()

def add_balance(uid: int, delta: int):
    with get_db() as con:
        con.execute("UPDATE users SET sol_balance_lamports=sol_balance_lamports+? WHERE user_id=?", (delta, uid))
        con.commit()

def get_balance_lamports(uid: int) -> int:
    with get_db() as con:
        row = con.execute("SELECT sol_balance_lamports FROM users WHERE user_id=?", (uid,)).fetchone()
        return int(row["sol_balance_lamports"] or 0) if row else 0

# ---------------------------
# RPC + PAYMENT DETECTION
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

def get_confirmed_tx_amount(sig: str) -> Optional[int]:
    try:
        res = rpc("getTransaction", [sig, {"encoding": "jsonParsed"}])
        tx = res.get("result")
        if not tx:
            return None
        meta = tx.get("meta", {})
        pre = meta.get("preBalances", [])
        post = meta.get("postBalances", [])
        if len(pre) != len(post):
            return None
        diff = post[1] - pre[1] if len(post) > 1 else 0
        return diff
    except Exception:
        return None

def check_payment_received(min_lamports: int) -> Optional[Dict]:
    sigs = get_new_signatures_for_address(CENTRAL_SOL_PUBKEY, 25)
    for sig in sigs:
        if sig in checked_signatures:
            continue
        amt = get_confirmed_tx_amount(sig)
        if amt and amt >= min_lamports:
            checked_signatures.add(sig)
            return {"sig": sig, "lamports": amt}
    return None

# ---------------------------
# LOGGING
# ---------------------------

def log_tx(uid: int, typ: str, lam: int, ref_id: str = None, meta: str = None):
    with get_db() as con:
        con.execute(
            "INSERT INTO txlog(user_id,type,lamports,ref_id,meta) VALUES (?,?,?,?,?)",
            (uid, typ, lam, ref_id, meta)
        )
        con.commit()
        # ---------------------------
# FEES & ABO–PREISE / MENÜS
# ---------------------------

# Staffel je User-Abo-Stufe (für Auszahlungs-Gebühren)
FEE_TIERS_BY_TIER = {
    'FREE':     {0: 20.0, 5: 15.0, 7: 10.0, 10: 5.0},
    'BRONZE':   {0: 15.0, 5: 12.5, 7: 7.5, 10: 5.0},
    'SILVER':   {0: 12.0, 5: 10.0, 7: 6.0, 10: 4.0},
    'GOLD':     {0: 10.0, 5: 8.0,  7: 5.0, 10: 3.0},
    'PLATINUM': {0: 8.0,  5: 6.0,  7: 4.0, 10: 2.0},
    'DIAMOND':  {0: 5.0,  5: 4.0,  7: 2.5, 10: 1.5}
}

# Abo-Laufzeiten in Tagen
PLAN_DUR_DAYS = {
    "BRONZE": 7,
    "SILVER": 7,
    "GOLD": 7,
    "PLATINUM": 7,
    "DIAMOND": 36500,  # quasi „ewig“
    "PREMIUM": 30
}

# Pending-Maps für Zahlungen (Abo-Käufe)
SUB_PENDING = {}          # uid -> {"plan":..., "expect_lamports":..., "created":...}
SUB_GRACE = {}            # uid -> grace_end_ts

def md_escape(s: str) -> str:
    if not s:
        return ""
    # Markdown V1-escape
    for ch in ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        s = s.replace(ch, f"\\{ch}")
    return s

def get_fee_tiers_for_user(uid: int) -> Dict[int, float]:
    u = get_user(uid)
    tier = (rget(u, 'sub_tier', 'FREE') or 'FREE').upper()
    return FEE_TIERS_BY_TIER.get(tier, FEE_TIERS_BY_TIER['FREE'])

def kb_withdraw_options_user(uid: int) -> InlineKeyboardMarkup:
    tiers = get_fee_tiers_for_user(uid)
    kb = InlineKeyboardMarkup()
    for days, pct in sorted(tiers.items(), key=lambda x: x[0]):
        label = 'Sofort • Fee {:.2f}%'.format(pct) if days == 0 else f'{days} Tage • Fee {pct}%'
        kb.add(InlineKeyboardButton(label, callback_data=f'payoutopt_{days}'))
    kb.add(InlineKeyboardButton('↩️ Abbrechen', callback_data='back_home'))
    return kb

def _plan_price_display(plan: str) -> str:
    code, usd, period = PLAN_PRICES[plan]
    px = get_sol_usd()
    if px:
        sol = usd / px
        return f"{usd:.2f} USDC / {sol:.4f} SOL ({period})"
    return f"{usd:.2f} USDC ({period})"

def _plan_price_lamports(plan: str) -> int:
    _, usd, _ = PLAN_PRICES[plan]
    return usd_to_lamports(usd)

def kb_main(u=None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🏦 Guthaben", callback_data="my_portfolio"),
           InlineKeyboardButton("👥 Referral", callback_data="referral"))
    kb.add(InlineKeyboardButton("💸 Auszahlung", callback_data="withdraw"))
    kb.add(InlineKeyboardButton("⭐ Abo-Modelle", callback_data="sub_models"),
           InlineKeyboardButton("📰 News", callback_data="news"))
    kb.add(InlineKeyboardButton("⚙️ Auto", callback_data="auto_menu"),
           InlineKeyboardButton("🧾 Verlauf", callback_data="history"))
    kb.add(InlineKeyboardButton("🆘 Support", callback_data="support"))
    if u and int(rget(u, "user_id", 0)) in ADMIN_IDS:
        kb.add(InlineKeyboardButton("🛠️ Admin", callback_data="admin_menu_big"))
    return kb

def kb_role_menu() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton('🔹 Referral-Modell', callback_data='role_CREATOR'),
           InlineKeyboardButton('🟩 User-Modell', callback_data='role_USER'))
    kb.add(InlineKeyboardButton('⬅️ Zurück', callback_data='back_home'))
    return kb

def kb_plan_menu_user() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"🥉 Bronze • {_plan_price_display('BRONZE')}", callback_data="plan_BRONZE"))
    kb.add(InlineKeyboardButton(f"🥈 Silber • {_plan_price_display('SILVER')}", callback_data="plan_SILVER"))
    kb.add(InlineKeyboardButton(f"🥇 Gold • {_plan_price_display('GOLD')}", callback_data="plan_GOLD"))
    kb.add(InlineKeyboardButton(f"💎 Platin • {_plan_price_display('PLATINUM')}", callback_data="plan_PLATINUM"))
    kb.add(InlineKeyboardButton(f"🔶 Diamond • {_plan_price_display('DIAMOND')}", callback_data="plan_DIAMOND"))
    kb.add(InlineKeyboardButton('⬅️ Zurück', callback_data='sub_models'))
    return kb

def kb_plan_menu_creator() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton(f"💠 Premium • {_plan_price_display('PREMIUM')}", callback_data="plan_PREMIUM"))
    kb.add(InlineKeyboardButton('⬅️ Zurück', callback_data='sub_models'))
    return kb

def kb_buy_now(plan: str, expect_lamports: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Ich habe gesendet", callback_data=f"sub_paid_{plan}"))
    kb.add(InlineKeyboardButton("↩️ Abbrechen", callback_data="sub_models"))
    return kb

def kb_sub_info_buttons() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📘 Referral-Modell Info", callback_data="info_referral"),
           InlineKeyboardButton("📗 User-Modell Info", callback_data="info_user"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_home"))
    return kb

# ---------------------------
# START / BASICS
# ---------------------------

@bot.message_handler(commands=["start"])
def cmd_start(m: Message):
    ensure_user(m)
    u = get_user(m.from_user.id)
    bot.send_message(m.chat.id,
                     "Willkommen! Wähle eine Option:",
                     reply_markup=kb_main(u))

@bot.callback_query_handler(func=lambda c: True)
def on_cb(c: CallbackQuery):
    uid = c.from_user.id
    data = c.data or ""
    u = get_user(uid)

    # Navigation
    if data == "back_home":
        bot.answer_callback_query(c.id)
        bot.edit_message_text("Hauptmenü:", c.message.chat.id, c.message.message_id, reply_markup=kb_main(u))
        return

    if data == "news":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "📰 *News*\n\nHier erscheinen Creator-Updates & Infos.", parse_mode="Markdown")
        return

    # Abo-Modelle
    if data == "sub_models":
        bot.answer_callback_query(c.id)
        text = (
            "⭐ *Abo-Modelle*\n\n"
            "Wähle:\n"
            "• *Referral-Modell* – Premium (monatlich), höhere Provisionen\n"
            "• *User-Modell* – Bronze/Silber/Gold/Platin/Diamond (wöchentlich), günstigere Auszahlungs-Fees"
        )
        try:
            bot.edit_message_text(text, c.message.chat.id, c.message.message_id, parse_mode="Markdown", reply_markup=kb_role_menu())
        except Exception:
            bot.send_message(uid, text, parse_mode="Markdown", reply_markup=kb_role_menu())
        return

    if data == "info_referral":
        bot.answer_callback_query(c.id)
        txt = (
            "🎯 *Referral-Modell – Premium (250 $ / Monat)*\n\n"
            "• 125 $ → höhere Provisionen für Premium\n"
            "• 25 $ → Bonus-Pool (Top 10)\n"
            "• 100 $ → Projekt & Team\n\n"
            "Provisionen (Normal → Premium):\n"
            "1️⃣ 10% → 15%\n2️⃣ 5% → 7,5%\n3️⃣ 2,5% → 3,75%\n\n"
            "Zusatz: +10% (= 12,5 $) pro direkt geworbenem Premium."
        )
        bot.send_message(uid, txt, parse_mode="Markdown")
        return

    if data == "info_user":
        bot.answer_callback_query(c.id)
        txt = (
            "🛡️ *User-Abo – wöchentlich*\n\n"
            "• 🥉 Bronze – 15 €\n"
            "• 🥈 Silber – 30 €\n"
            "• 🥇 Gold – 50 €\n"
            "• 💎 Platin – 100 €\n"
            "• 🔶 Diamond – einmalig 1000 € (Teilhaber)\n\n"
            "Gebühren sinken je Stufe; exakte Staffel im Bot hinterlegt und bei Auszahlung angezeigt."
        )
        bot.send_message(uid, txt, parse_mode="Markdown")
        return

    # Rollenwahl = welcher Abo-Zweig
    if data.startswith("role_"):
        role = data.split("_", 1)[1].upper()
        with get_db() as con:
            con.execute("UPDATE users SET role=? WHERE user_id=?", (role, uid))
            con.commit()
        bot.answer_callback_query(c.id, f"Rolle: {role}")
        if role == "CREATOR":
            txt = "*Premium-Abo (monatlich)*\n\nWähle:"
            bot.send_message(uid, txt, parse_mode="Markdown", reply_markup=kb_plan_menu_creator())
        else:
            txt = "*User-Abo (wöchentlich)*\n\nWähle deine Stufe:"
            bot.send_message(uid, txt, parse_mode="Markdown", reply_markup=kb_plan_menu_user())
        return

    # Planwahl
    if data.startswith("plan_"):
        plan = data.split("_", 1)[1].upper()
        if plan not in PLAN_PRICES:
            bot.answer_callback_query(c.id, "Unbekannter Plan."); 
            return
        # Preise berechnen
        expect_lam = _plan_price_lamports(plan)
        price_text = _plan_price_display(plan)

        # Pending speichern (exakte Betragszuordnung)
        SUB_PENDING[uid] = {
            "plan": plan,
            "expect_lamports": expect_lam,
            "created": int(time.time())
        }
        bot.answer_callback_query(c.id, f"Abo: {plan}")
        msg = (
            f"🧾 *{plan}* aktivieren\n"
            f"Preis: *{price_text}*\n\n"
            f"Sende exakt diesen Betrag an:\n"
            f"`{md_escape(CENTRAL_SOL_PUBKEY)}`\n\n"
            "Nach Zahlung klicke auf *„Ich habe gesendet“* – die Bestätigung läuft automatisch (RPC)."
        )
        try:
            bot.edit_message_text(msg, c.message.chat.id, c.message.message_id,
                                  parse_mode="Markdown", reply_markup=kb_buy_now(plan, expect_lam))
        except Exception:
            bot.send_message(uid, msg, parse_mode="Markdown", reply_markup=kb_buy_now(plan, expect_lam))
        return

    # Nutzer klickt „Ich habe gesendet“
    if data.startswith("sub_paid_"):
        plan = data.split("_", 2)[2].upper()
        pend = SUB_PENDING.get(uid)
        if not pend or pend.get("plan") != plan:
            bot.answer_callback_query(c.id, "Keine ausstehende Zahlung erkannt.")
            return
        bot.answer_callback_query(c.id, "Prüfe Zahlung …")
        # Direkter Sofort-Check (ansonsten Hintergrund-Thread erledigt den Rest)
        found = check_payment_received(pend["expect_lamports"])
        if found:
            _activate_subscription(uid, plan, pend["expect_lamports"], found["sig"])
            SUB_PENDING.pop(uid, None)
            bot.send_message(uid, f"✅ *Abo {plan} aktiv* — Danke! (Tx: `{md_escape(found['sig'])}`)", parse_mode="Markdown")
        else:
            bot.send_message(uid, "⏳ Noch nichts gefunden. Ich prüfe weiter automatisch. Wenn du gesendet hast, kommt die Bestätigung in Kürze.")
        return

    # Hauptfunktionen (hier nur Platzhalter-Navigation, Logik folgt in Teil 3/4)
    if data == "my_portfolio":
        bal_lam = get_balance_lamports(uid)
        bot.answer_callback_query(c.id)
        bot.send_message(uid, f"🏦 Guthaben: {fmt_sol_usdc(bal_lam)}", reply_markup=kb_main(u))
        return

    if data == "referral":
        bot.answer_callback_query(c.id, "Referral")
        bot.send_message(uid,
                         "🔗 *Dein Referral-Link*\n\nTeile den Bot und verdiene Provisionen.\n(Details siehst du im Admin/Stats-Bereich.)",
                         parse_mode="Markdown")
        return

    if data == "withdraw":
        bot.answer_callback_query(c.id, "Auszahlung")
        bot.send_message(uid, "Gib den Betrag (in SOL) oder eine Zieladresse ein.\n(Auszahlung-Flow folgt gleich.)")
        return

    if data == "auto_menu":
        bot.answer_callback_query(c.id, "Auto")
        bot.send_message(uid, "Auto-Trading-Einstellungen (folgt).", reply_markup=kb_main(u))
        return

    if data == "history":
        bot.answer_callback_query(c.id, "Verlauf")
        bot.send_message(uid, "Letzte Transaktionen (folgt).", reply_markup=kb_main(u))
        return

    if data == "support":
        bot.answer_callback_query(c.id, "Support")
        bot.send_message(uid, "Sende deine Support-Nachricht (Text oder Bild).")
        return

    # Admin-Menü-Einstieg kommt in Teil 3
    if data == "admin_menu_big":
        if uid not in ADMIN_IDS:
            bot.answer_callback_query(c.id, "Nicht erlaubt.")
            return
        bot.answer_callback_query(c.id)
        bot.send_message(uid, "🛠️ Admin-Menü (Funktionen folgen gleich).")
        return

# ---------------------------
# SUBSCRIPTION CORE
# ---------------------------

def _activate_subscription(uid: int, plan: str, paid_lamports: int, tx_sig: str):
    days = PLAN_DUR_DAYS.get(plan, 30)
    now = int(time.time())
    with get_db() as con:
        # bestehendes Ablaufdatum berücksichtigen
        row = con.execute("SELECT sub_tier, sub_expires FROM users WHERE user_id=?", (uid,)).fetchone()
        cur_exp = 0
        if row and rget(row, "sub_expires"):
            try:
                cur_exp = int(rget(row, "sub_expires", 0) or 0)
            except Exception:
                cur_exp = 0
        new_exp = max(now, cur_exp) + days * 86400
        con.execute("UPDATE users SET sub_tier=?, sub_active=1, sub_expires=? WHERE user_id=?", (plan, new_exp, uid))
        con.execute("INSERT OR IGNORE INTO subs_payments(user_id, plan, amount_lamports, tx_sig) VALUES (?,?,?,?)",
                    (uid, plan, paid_lamports, tx_sig))
        # Diamond → Anteilseigner
        if plan == "DIAMOND":
            con.execute("UPDATE users SET is_shareholder=1 WHERE user_id=?", (uid,))
        con.commit()
    log_tx(uid, "SUB_PAID", paid_lamports, ref_id=tx_sig, meta=plan)
    # Grace löschen (falls lief)
    SUB_GRACE.pop(uid, None)

def _expire_and_grace_scheduler():
    while True:
        try:
            now = int(time.time())
            with get_db() as con:
                # Abos abgelaufen → 1 Tag Grace setzen, wenn noch nicht
                rows = con.execute(
                    "SELECT user_id, sub_tier, sub_expires, sub_active FROM users WHERE sub_active=1 AND sub_expires IS NOT NULL AND sub_expires < ?",
                    (now,)
                ).fetchall()
            for r in rows:
                uid = int(r["user_id"])
                if uid not in SUB_GRACE:
                    SUB_GRACE[uid] = now + 86400  # +1 Tag
                    try:
                        bot.send_message(uid,
                                         "⏰ Dein Abo ist abgelaufen. Du hast *24h Grace*, um zu verlängern – danach entfallen die Vorteile.",
                                         parse_mode="Markdown",
                                         reply_markup=kb_role_menu())
                    except Exception:
                        pass
            # Grace-Ende → Vorteile entziehen
            for uid, till in list(SUB_GRACE.items()):
                if now >= till:
                    with get_db() as con:
                        con.execute("UPDATE users SET sub_active=0 WHERE user_id=?", (uid,))
                        con.commit()
                    SUB_GRACE.pop(uid, None)
                    try:
                        bot.send_message(uid, "❌ Grace vorbei – Vorteile deaktiviert. Du kannst jederzeit neu buchen: ⭐ Abo-Modelle.", reply_markup=kb_role_menu())
                    except Exception:
                        pass
        except Exception:
            pass
        time.sleep(30)

def _subscription_payment_watcher():
    # Dieser Loop überprüft regelmäßig die zentrale Wallet auf neue Einzahlungen
    # und matcht sie gegen SUB_PENDING anhand exakter Beträge.
    while True:
        try:
            # Alle aktuellen Pending-Beträge einsammeln
            wants = [(uid, info["expect_lamports"], info["plan"]) for uid, info in SUB_PENDING.items()]
            if not wants:
                time.sleep(6)
                continue
            # neue Signaturen
            new = get_new_signatures_for_address(CENTRAL_SOL_PUBKEY, limit=25)
            for sig in new:
                if sig in checked_signatures:
                    continue
                amt = get_confirmed_tx_amount(sig)
                if not amt or amt <= 0:
                    continue
                # exakten Match suchen
                for uid, expect, plan in wants:
                    if amt == expect:
                        checked_signatures.add(sig)
                        _activate_subscription(uid, plan, amt, sig)
                        SUB_PENDING.pop(uid, None)
                        try:
                            bot.send_message(uid, f"✅ *Abo {plan} aktiv* — Danke! (Tx: `{md_escape(sig)}`)", parse_mode="Markdown")
                        except Exception:
                            pass
                        break
        except Exception:
            pass
        time.sleep(6)

# Scheduler starten (Abos & Zahlungserkennung)
threading.Thread(target=_expire_and_grace_scheduler, daemon=True).start()
threading.Thread(target=_subscription_payment_watcher, daemon=True).start()
# ---------------------------
# ADMIN KEYBOARDS & HELPERS
# ---------------------------

def kb_admin_main() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📦 Abo-Übersicht", callback_data="admin_subs_menu"),
           InlineKeyboardButton("👥 Nutzer", callback_data="admin_view_users_0"))
    kb.add(InlineKeyboardButton("💬 Broadcast", callback_data="admin_broadcast_all"),
           InlineKeyboardButton("🧮 Apply PnL/Promo", callback_data="admin_apply_pnl"))
    kb.add(InlineKeyboardButton("💸 Offene Auszahlungen", callback_data="admin_open_payouts"))
    kb.add(InlineKeyboardButton("📊 Stats", callback_data="admin_stats"))
    return kb

def kb_subs_admin_user_actions(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Set: BRONZE", callback_data=f"admin_setsub_{uid}_BRONZE"),
           InlineKeyboardButton("Set: SILVER",  callback_data=f"admin_setsub_{uid}_SILVER"))
    kb.add(InlineKeyboardButton("Set: GOLD",   callback_data=f"admin_setsub_{uid}_GOLD"),
           InlineKeyboardButton("Set: PLATIN", callback_data=f"admin_setsub_{uid}_PLATINUM"))
    kb.add(InlineKeyboardButton("Set: DIAMOND", callback_data=f"admin_setsub_{uid}_DIAMOND"))
    kb.add(InlineKeyboardButton("Set: PREMIUM", callback_data=f"admin_setsub_{uid}_PREMIUM"))
    kb.add(InlineKeyboardButton("❌ Abo deaktivieren", callback_data=f"admin_setsub_{uid}_OFF"))
    return kb

def kb_user_row(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔧 Aktionen", callback_data=f"admin_user_{uid}"),
           InlineKeyboardButton("💳 Balance", callback_data=f"admin_balance_{uid}"))
    kb.add(InlineKeyboardButton("👤 Abo", callback_data=f"admin_setsub_{uid}_ASK"),
           InlineKeyboardButton("✉️ DM",  callback_data=f"admin_msg_{uid}"))
    kb.add(InlineKeyboardButton("💼 Wallet", callback_data=f"admin_setwallet_{uid}"))
    return kb

def kb_user_actions(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💳 Balance ändern", callback_data=f"admin_balance_{uid}"))
    kb.add(InlineKeyboardButton("👤 Abo setzen", callback_data=f"admin_setsub_{uid}_ASK"))
    kb.add(InlineKeyboardButton("✉️ Direktnachricht", callback_data=f"admin_msg_{uid}"))
    kb.add(InlineKeyboardButton("💼 Wallet setzen", callback_data=f"admin_setwallet_{uid}"))
    return kb

def kb_users_pagination(offset: int, total: int, page_size: int = 25) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    prev_off = max(0, offset - page_size)
    next_off = offset + page_size if offset + page_size < total else offset
    if offset > 0:
        kb.add(InlineKeyboardButton("◀️ Zurück", callback_data=f"admin_view_users_{prev_off}"))
    if offset + page_size < total:
        kb.add(InlineKeyboardButton("▶️ Weiter", callback_data=f"admin_view_users_{next_off}"))
    kb.add(InlineKeyboardButton("⬅️ Admin Menü", callback_data="admin_menu_big"))
    return kb

def kb_payout_manage(pid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("✅ Genehmigen", callback_data=f"payout_APPROVE_{pid}"),
           InlineKeyboardButton("📤 Als gesendet", callback_data=f"payout_SENT_{pid}"))
    kb.add(InlineKeyboardButton("❌ Ablehnen", callback_data=f"payout_REJECT_{pid}"))
    return kb

# ---------------------------
# ADMIN CALLBACKS
# ---------------------------

@bot.callback_query_handler(func=lambda c: c.data == "admin_menu_big")
def cb_admin_menu(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id, "Nicht erlaubt.")
        return
    bot.answer_callback_query(c.id)
    try:
        bot.edit_message_text("🛠️ Admin-Menü — Kontrolle", c.message.chat.id, c.message.message_id,
                              reply_markup=kb_admin_main())
    except Exception:
        bot.send_message(uid, "🛠️ Admin-Menü — Kontrolle", reply_markup=kb_admin_main())

@bot.callback_query_handler(func=lambda c: c.data == "admin_subs_menu")
def cb_admin_subs_menu(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id, "Nicht erlaubt.")
        return
    with get_db() as con:
        rows = con.execute('SELECT sub_tier, COUNT(*) c FROM users GROUP BY sub_tier').fetchall()
    parts = ['📦 Abo-Übersicht']
    for r in rows:
        tier = r["sub_tier"] or "FREE"
        parts.append(f"• {tier}: {r['c']}")
    bot.answer_callback_query(c.id)
    bot.send_message(uid, '\n'.join(parts))
    with get_db() as con:
        subs = con.execute("""
            SELECT user_id, username, sub_tier, sub_expires, sub_active
            FROM users
            WHERE sub_tier IS NOT NULL AND sub_tier!='FREE'
            ORDER BY COALESCE(sub_expires, 0) DESC
            LIMIT 100
        """).fetchall()
    if subs:
        for urow in subs:
            uname = ('@'+(urow['username'] or '')) if (urow['username']) else f'UID {urow['user_id']}'
            exp = int(rget(urow, "sub_expires", 0) or 0)
            active = "🔔" if int(rget(urow, "sub_active", 0) or 0) == 1 else "🔕"
            line = f"{active} {uname} • {urow['sub_tier']} • läuft bis: {time.strftime('%Y-%m-%d %H:%M', time.localtime(exp)) if exp else '-'}"
            bot.send_message(uid, line, reply_markup=kb_subs_admin_user_actions(int(urow["user_id"])))

@bot.callback_query_handler(func=lambda c: c.data.startswith("admin_view_users_"))
def cb_admin_view_users(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id, "Nicht erlaubt.")
        return
    try:
        offset = int(c.data.rsplit("_", 1)[1])
    except:
        offset = 0
    page_size = 25
    total = count_users()
    with get_db() as con:
        rows = con.execute("""
            SELECT user_id, username, sol_balance_lamports, source_wallet, payout_wallet, sub_active
            FROM users
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        """, (page_size, offset)).fetchall()
    bot.answer_callback_query(c.id)
    bot.send_message(uid, f"👀 Nutzer verwalten (Seite {offset//page_size+1})")
    for r in rows:
        uname = ("@" + (r["username"] or "")) if r["username"] else f"UID {r['user_id']}"
        sub = "🔔" if int(rget(r, "sub_active", 0) or 0) == 1 else "🔕"
        bot.send_message(uid,
            f"{uname} • {sub}\n"
            f"Guthaben: {fmt_sol_usdc(int(r['sol_balance_lamports'] or 0))}\n"
            f"SRC: `{md_escape(r['source_wallet'] or '-')}`\nPAY: `{md_escape(r['payout_wallet'] or '-')}`",
            parse_mode="Markdown",
            reply_markup=kb_user_row(int(r["user_id"])))
    bot.send_message(uid, "Navigation:", reply_markup=kb_users_pagination(offset, total, page_size))

@bot.callback_query_handler(func=lambda c: c.data.startswith("admin_user_"))
def cb_admin_user(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id, "Nicht erlaubt.")
        return
    try:
        target = int(c.data.split("_", 2)[2])
    except Exception:
        bot.answer_callback_query(c.id, "Ungültig"); return
    tu = get_user(target)
    if not tu:
        bot.answer_callback_query(c.id, "User nicht gefunden."); return
    uname = ("@" + tu["username"]) if tu.get("username") else f"UID {tu['user_id']}"
    txt = (f"{uname}\n"
           f"Guthaben: {fmt_sol_usdc(int(tu.get('sol_balance_lamports') or 0))}\n"
           f"Source: `{md_escape(tu.get('source_wallet') or '-')}`\n"
           f"Payout: `{md_escape(tu.get('payout_wallet') or '-')}`")
    bot.answer_callback_query(c.id)
    bot.send_message(uid, txt, parse_mode="Markdown", reply_markup=kb_user_actions(target))

@bot.callback_query_handler(func=lambda c: c.data == "admin_open_payouts")
def cb_admin_open_payouts(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id, "Nicht erlaubt."); return
    with get_db() as con:
        rows = con.execute("SELECT * FROM payouts WHERE status='REQUESTED' ORDER BY created_at ASC LIMIT 100").fetchall()
    if not rows:
        bot.answer_callback_query(c.id, "Keine offenen Auszahlungen."); return
    bot.answer_callback_query(c.id)
    for r in rows:
        pid = int(r["id"])
        uline = get_user(int(r["user_id"]))
        uname = ("@" + (uline.get("username") or "")) if (uline and uline.get("username")) else f"UID {r['user_id']}"
        bot.send_message(uid,
                         f"#{pid} • {uname}\nBetrag: {fmt_sol_usdc(int(r['amount_lamports'] or 0))}\n"
                         f"Lockup {int(r['lockup_days'] or 0)}d • Fee {float(r['fee_percent'] or 0):.2f}%",
                         reply_markup=kb_payout_manage(pid))

@bot.callback_query_handler(func=lambda c: c.data == "admin_stats")
def cb_admin_stats(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id, "Nicht erlaubt.")
        return
    with get_db() as con:
        users_total = con.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        subs_total  = con.execute("SELECT COUNT(*) AS c FROM users WHERE sub_active=1").fetchone()["c"]
    deposits = sum_total_deposits()
    balances = sum_total_balances()
    open_p = sum_open_payouts()
    bot.answer_callback_query(c.id)
    bot.send_message(uid,
                     "📊 System-Stats\n"
                     f"👥 Nutzer gesamt: {users_total}\n"
                     f"🔔 Abos aktiv: {subs_total}\n"
                     f"📥 Einzahlungen gesamt: {fmt_sol_usdc(deposits)}\n"
                     f"🏦 Gesamtguthaben: {fmt_sol_usdc(balances)}\n"
                     f"🧾 Offene Auszahlungen: {fmt_sol_usdc(open_p)}")

@bot.callback_query_handler(func=lambda c: c.data == "admin_broadcast_all")
def cb_admin_broadcast(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id); return
    ADMIN_AWAIT_NEWS_BROADCAST[uid] = {"step": "await_text_to_all"}
    bot.answer_callback_query(c.id)
    bot.send_message(uid, "Sende die Nachricht, die an **alle Nutzer** gehen soll.", parse_mode="Markdown")

@bot.message_handler(func=lambda m: ADMIN_AWAIT_NEWS_BROADCAST.get(m.from_user.id) is not None)
def admin_broadcast_msg(m: Message):
    uid = m.from_user.id
    ctx = ADMIN_AWAIT_NEWS_BROADCAST.pop(uid, None)
    if not ctx or ctx.get("step") != "await_text_to_all":
        return
    msg = m.caption if m.photo else (m.text or "")
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

# Admin: Abo „Set“ (Buttons und Freitext)
ADMIN_AWAIT_SET_SUB = {}

@bot.callback_query_handler(func=lambda c: c.data.startswith("admin_setsub_"))
def cb_admin_setsub(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id, "Nicht erlaubt."); return
    _, _, rest = c.data.split("_", 2)  # admin_setsub_{uid}_{PLAN|ASK|OFF}
    try:
        target_s, plan = rest.split("_", 1)
        target = int(target_s)
    except Exception:
        bot.answer_callback_query(c.id, "Ungültig."); return
    if plan == "ASK":
        bot.answer_callback_query(c.id)
        bot.send_message(uid, f"Abo für UID {target} setzen:", reply_markup=kb_subs_admin_user_actions(target))
        return
    if plan == "OFF":
        with get_db() as con:
            con.execute("UPDATE users SET sub_active=0, sub_tier='FREE', sub_expires=NULL WHERE user_id=?", (target,))
            con.commit()
        bot.answer_callback_query(c.id, "Abo deaktiviert.")
        try:
            bot.send_message(target, "❌ Dein Abo wurde deaktiviert.")
        except Exception:
            pass
        return
    if plan not in PLAN_PRICES and plan not in ("BRONZE","SILVER","GOLD","PLATINUM","DIAMOND","PREMIUM"):
        bot.answer_callback_query(c.id, "Plan unbekannt."); return
    # direkte Aktivierung (ohne Zahlung)
    now = int(time.time())
    days = PLAN_DUR_DAYS.get(plan, 30)
    with get_db() as con:
        row = con.execute("SELECT sub_expires FROM users WHERE user_id=?", (target,)).fetchone()
        cur_exp = int(rget(row, "sub_expires", 0) or 0) if row else 0
        new_exp = max(now, cur_exp) + days * 86400
        con.execute("UPDATE users SET sub_tier=?, sub_active=1, sub_expires=? WHERE user_id=?", (plan, new_exp, target))
        if plan == "DIAMOND":
            con.execute("UPDATE users SET is_shareholder=1 WHERE user_id=?", (target,))
        con.commit()
    bot.answer_callback_query(c.id, f"{plan} aktiv.")
    try:
        bot.send_message(target, f"✅ Dein Abo *{plan}* wurde aktiviert (Admin).", parse_mode="Markdown")
    except Exception:
        pass

@bot.message_handler(func=lambda m: ADMIN_AWAIT_SET_SUB.get(m.from_user.id))
def admin_set_sub_text(m: Message):
    uid = m.from_user.id
    ADMIN_AWAIT_SET_SUB[uid] = False
    if uid not in ADMIN_IDS:
        bot.reply_to(m, 'Nicht erlaubt.'); return
    try:
        txt = (m.text or '').strip()
        if not txt.upper().startswith('UID '):
            bot.reply_to(m, 'Format: `UID <id> <PLAN>`', parse_mode='Markdown'); return
        _, id_s, plan = txt.split(None, 2)
        uid_t = int(id_s)
        plan = plan.strip().upper()
        now = int(time.time())
        days = PLAN_DUR_DAYS.get(plan, 30)
        with get_db() as con:
            r = con.execute("SELECT sub_expires FROM users WHERE user_id=?", (uid_t,)).fetchone()
            cur_exp = int(rget(r, "sub_expires", 0) or 0) if r else 0
            new_exp = max(now, cur_exp) + days * 86400
            con.execute('UPDATE users SET sub_tier=?, sub_active=1, sub_expires=? WHERE user_id=?',
                        (plan, new_exp, uid_t))
            if plan == 'DIAMOND':
                con.execute('UPDATE users SET is_shareholder=1 WHERE user_id=?', (uid_t,))
            con.commit()
        bot.reply_to(m, f'✅ Abo gesetzt: UID {uid_t} → {plan}')
        try:
            bot.send_message(uid_t, f"✅ Dein Abo *{plan}* wurde aktiviert (Admin).", parse_mode="Markdown")
        except Exception:
            pass
    except Exception as e:
        bot.reply_to(m, f'Fehler: {e}')

# ---------------------------
# CALLS (Erstellen & Broadcast)
# ---------------------------

ADMIN_AWAIT_SIMPLE_CALL = {}

@bot.callback_query_handler(func=lambda c: c.data == "admin_new_call")
def cb_admin_new_call(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS: return
    bot.answer_callback_query(c.id, "Call erstellen")
    ADMIN_AWAIT_SIMPLE_CALL[uid] = True
    bot.send_message(uid, "Sende den Call:\n• FUTURES|BASE|SIDE|LEV|OPTIONALE_NOTES\n• MEME|NAME|TOKEN_ADDRESS|OPTIONALE_NOTES")

@bot.message_handler(func=lambda m: ADMIN_AWAIT_SIMPLE_CALL.get(m.from_user.id, False))
def admin_simple_call(m: Message):
    uid = m.from_user.id
    ADMIN_AWAIT_SIMPLE_CALL[uid] = False
    if uid not in ADMIN_IDS:
        bot.reply_to(m, "Nicht erlaubt.")
        return
    parts = [p.strip() for p in (m.text or "").split("|")]
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

@bot.callback_query_handler(func=lambda c: c.data == "admin_broadcast_last")
def cb_admin_broadcast_last(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS: return
    with get_db() as con:
        row = con.execute("SELECT * FROM calls ORDER BY id DESC LIMIT 1").fetchone()
    if not row:
        bot.answer_callback_query(c.id, "Kein Call vorhanden."); return
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
            result = futures_place_simulated(
                au, rget(row, "base", ""), rget(row, "side", ""), rget(row, "leverage", ""), rget(get_user(au), "auto_risk", "MEDIUM")
            )
            txid = result.get("order_id") or result.get("txid") or "LIVE"
            queue_execution(int(row["id"]), au, status="FILLED", message="FILLED", stake_lamports=stake)
            urow = get_user(au)
            bot.send_message(au, _auto_entry_message(urow, row, "JOINED", stake, txid_hint=txid), parse_mode="Markdown")
            joined += 1
        except Exception as e:
            print("Broadcast auto error:", e)
            pass
    bot.answer_callback_query(c.id, f"📣 Ankündigungen: {sent_announce} • Auto-Entry JOINED: {joined}")

# ---------------------------
# AUSZAHLUNG (PIN & Fees)
# ---------------------------

AWAITING_PIN = {}  # uid -> {"for": "...", "data": ..., "next": (...)}
WAITING_SOURCE_WALLET = {}
WAITING_PAYOUT_WALLET = {}
WAITING_WITHDRAW_AMOUNT = {}

@bot.callback_query_handler(func=lambda c: c.data.startswith("payoutopt_"))
def cb_payout_option(c: CallbackQuery):
    uid = c.from_user.id
    u = get_user(uid)
    # PIN-Schutz, falls gesetzt
    if u and rget(u, "pin_hash"):
        AWAITING_PIN[uid] = {"for": "withdraw_option", "data": c.data}
        bot.answer_callback_query(c.id, "PIN erforderlich.")
        bot.send_message(uid, "🔐 Bitte sende deine PIN, um fortzufahren.")
        return
    _do_payout_option(uid, c)

def _do_payout_option(uid: int, c: CallbackQuery):
    try:
        days = int((c.data or "").split("_", 1)[1])
    except Exception:
        bot.answer_callback_query(c.id, "Ungültige Auswahl."); return
    tiers = get_fee_tiers_for_user(uid)
    fee_percent = float(tiers.get(days, 0.0))
    pending = WAITING_WITHDRAW_AMOUNT.get(uid, None)
    if pending is None or pending <= 0:
        bot.answer_callback_query(c.id, "Keine ausstehende Auszahlung. Betrag zuerst eingeben."); return
    amount_lam = int(pending)
    if not subtract_balance(uid, amount_lam):
        bot.answer_callback_query(c.id, "Unzureichendes Guthaben.")
        WAITING_WITHDRAW_AMOUNT.pop(uid, None); return
    with get_db() as con:
        cur = con.execute(
            "INSERT INTO payouts(user_id, amount_lamports, status, note, lockup_days, fee_percent) VALUES (?,?,?,?,?,?)",
            (uid, amount_lam, "REQUESTED", f"({days}d, fee {fee_percent}%)", days, fee_percent))
        pid = cur.lastrowid
        con.commit()
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

@bot.callback_query_handler(func=lambda c: c.data.startswith("payout_"))
def cb_admin_payout_action(c: CallbackQuery):
    uid = c.from_user.id
    if uid not in ADMIN_IDS:
        bot.answer_callback_query(c.id, "Nicht erlaubt."); return
    _, action, pid_s = c.data.split("_", 2)
    try:
        pid = int(pid_s)
    except:
        bot.answer_callback_query(c.id, "Ungültige ID."); return
    with get_db() as con:
        row = con.execute("SELECT * FROM payouts WHERE id=?", (pid,)).fetchone()
    if not row:
        bot.answer_callback_query(c.id, "Anfrage nicht gefunden."); return
    tgt_uid = int(row["user_id"]); amt = int(row["amount_lamports"])
    days = int(row["lockup_days"]); fee_percent = float(row["fee_percent"])
    fee_lam = int(round(amt * (fee_percent/100.0))); net_lam = amt - fee_lam
    if action == "APPROVE":
        with get_db() as con: 
            con.execute("UPDATE payouts SET status='APPROVED' WHERE id=?", (pid,))
            con.commit()
        bot.answer_callback_query(c.id, "Genehmigt.")
        bot.send_message(tgt_uid, f"✅ Deine Auszahlung #{pid} wurde genehmigt.")
        return
    if action == "SENT":
        with get_db() as con: 
            con.execute("UPDATE payouts SET status='SENT' WHERE id=?", (pid,))
            con.commit()
        log_tx(tgt_uid, "WITHDRAW_SENT", amt, ref_id=str(pid), meta=f"net {net_lam} (fee {fee_percent:.2f}%)")
        bot.answer_callback_query(c.id, "Als gesendet markiert.")
        bot.send_message(tgt_uid, f"📤 Auszahlung #{pid} gesendet.\nBetrag: {fmt_sol_usdc(amt)}\nGebühr: {fee_percent:.2f}% ({fmt_sol_usdc(fee_lam)})\nNetto: {fmt_sol_usdc(net_lam)}")
        return
    if action == "REJECT":
        with get_db() as con: 
            con.execute("UPDATE payouts SET status='REJECTED' WHERE id=?", (pid,))
            con.commit()
        add_balance(tgt_uid, amt); log_tx(tgt_uid, "ADJ", amt, ref_id=str(pid), meta="payout rejected refund")
        bot.answer_callback_query(c.id, "Abgelehnt & erstattet.")
        bot.send_message(tgt_uid, f"❌ Auszahlung #{pid} abgelehnt. Betrag erstattet.")
        return

# ---------------------------
# NACHRICHTEN-HANDLER für Support, PIN, Wallets, Withdraw-Betrag
# ---------------------------

ADMIN_AWAIT_DM_TARGET = {}
ADMIN_AWAIT_BALANCE_SINGLE = {}
ADMIN_AWAIT_BALANCE_GLOBAL = {}
ADMIN_AWAIT_MASS_BALANCE = {}
SUPPORT_AWAIT_MSG = {}

@bot.message_handler(func=lambda m: True)
def catch_all(m: Message):
    uid = m.from_user.id
    text = (m.text or "").strip() if m.text else ""

    # Support: Weiterleitung an Admins
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

    # Admin: DM weiterleiten
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

    # PIN-Abfrage
    if AWAITING_PIN.get(uid):
        entry = AWAITING_PIN.pop(uid)
        pin = text
        u = get_user(uid)
        ok = False
        if u and rget(u, "pin_hash"):
            ok = (_hash_pin(pin) == rget(u, "pin_hash"))
        if not ok:
            bot.reply_to(m, "❌ Falsche PIN.")
            return
        if entry["for"] == "withdraw_option":
            class _DummyC: pass
            dummy = _DummyC()
            dummy.data = entry["data"]; dummy.message = m; dummy.id = "pin-ok"
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
        if uid not in ADMIN_IDS:
            bot.reply_to(m, "Nicht erlaubt."); return
        parts = text.split(None, 1)
        if len(parts) != 2:
            bot.reply_to(m, "Format: `SRC <adresse>` oder `PAY <adresse>`", parse_mode="Markdown"); return
        which, addr = parts[0].upper(), parts[1].strip()
        if not is_probably_solana_address(addr):
            bot.reply_to(m, "Ungültige Solana-Adresse."); return
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
        # Falls versehentlich Adresse statt Zahl gesendet
        if is_probably_solana_address(text):
            u = get_user(uid)
            if u and rget(u, "pin_hash"):
                AWAITING_PIN[uid] = {"for": "setwallet", "next": ("PAY", text)}
                bot.reply_to(m, "🔐 Bitte PIN senden, um Payout-Wallet zu ändern.")
                return
        try:
            sol = float(text.replace(",", "."))
            if sol <= 0:
                bot.reply_to(m, "Betrag muss > 0 sein."); return
            lam = int(sol * LAMPORTS_PER_SOL)
            if get_balance_lamports(uid) < lam:
                bot.reply_to(m, f"Unzureichendes Guthaben. Verfügbar: {fmt_sol_usdc(get_balance_lamports(uid))}")
                WAITING_WITHDRAW_AMOUNT.pop(uid, None)
                return
            WAITING_WITHDRAW_AMOUNT[uid] = lam
            bot.reply_to(m, f"Auszahlung: {fmt_sol_usdc(lam)} — Wähle Lockup & Fee:", reply_markup=kb_withdraw_options_user(uid))
            return
        except Exception:
            pass

    # Default
    bot.reply_to(m, "Ich habe das nicht verstanden. Nutze das Menü.", reply_markup=kb_main(get_user(uid)))
    # ---------------------------
# REFERRALS & BONI
# ---------------------------

def gen_referral_for_user(user_id: int) -> str:
    h = hashlib.sha1(str(user_id).encode()).hexdigest()[:8]
    return f"REF{h.upper()}"

def find_user_by_refcode(code: str) -> Optional[int]:
    if not code or not code.upper().startswith("REF"):
        return None
    suffix = code[3:].lower()
    with get_db() as con:
        rows = con.execute("SELECT user_id FROM users").fetchall()
        for r in rows:
            if hashlib.sha1(str(r["user_id"]).encode()).hexdigest()[:8] == suffix:
                return int(r["user_id"])
    return None

def record_referral(ref_by: int, invited_id: int, level: int = 1):
    if not ref_by or ref_by == invited_id:
        return
    with get_db() as con:
        con.execute(
            "INSERT OR IGNORE INTO referrals(referrer_user_id, invited_user_id, level) VALUES (?,?,?)",
            (ref_by, invited_id, level)
        )
        con.commit()

def reward_referral_chain(invited_id: int, amount_usdc: float):
    """
    Verteile Provisionen an 3 Ebenen – basierend auf Abo (Premium/Normal)
    """
    with get_db() as con:
        chain = []
        # Ebene 1
        r1 = con.execute("SELECT ref_by FROM users WHERE user_id=?", (invited_id,)).fetchone()
        if not r1 or not rget(r1, "ref_by"): return
        lvl1 = int(rget(r1, "ref_by"))
        chain.append(lvl1)
        # Ebene 2
        r2 = con.execute("SELECT ref_by FROM users WHERE user_id=?", (lvl1,)).fetchone()
        if r2 and rget(r2, "ref_by"):
            lvl2 = int(rget(r2, "ref_by"))
            chain.append(lvl2)
        # Ebene 3
        if len(chain) >= 2:
            r3 = con.execute("SELECT ref_by FROM users WHERE user_id=?", (chain[1],)).fetchone()
            if r3 and rget(r3, "ref_by"):
                lvl3 = int(rget(r3, "ref_by"))
                chain.append(lvl3)
    perc_norm = [0.10, 0.05, 0.025]
    perc_prem = [0.15, 0.075, 0.0375]
    for lvl, refid in enumerate(chain, 1):
        u = get_user(refid)
        tier = rget(u, "sub_tier", "FREE")
        perc = perc_prem[lvl - 1] if tier == "PREMIUM" else perc_norm[lvl - 1]
        pay_usdc = amount_usdc * perc
        pay_lam = usd_to_lamports(pay_usdc)
        add_balance(refid, pay_lam)
        log_tx(refid, f"REF_LVL{lvl}", pay_lam, ref_id=f"invited:{invited_id}", meta=f"{pay_usdc:.2f} USDC")
        try:
            bot.send_message(refid, f"💰 Referral-Einnahme (Lvl {lvl}): +{fmt_sol_usdc(pay_lam)}", parse_mode="Markdown")
        except Exception:
            pass

# ---------------------------
# STATISTIK & SUMMEN
# ---------------------------

def count_users() -> int:
    with get_db() as con:
        r = con.execute("SELECT COUNT(*) c FROM users").fetchone()
        return int(r["c"])

def all_users() -> List[int]:
    with get_db() as con:
        rows = con.execute("SELECT user_id FROM users").fetchall()
    return [int(r["user_id"]) for r in rows]

def all_subscribers() -> List[int]:
    with get_db() as con:
        rows = con.execute("SELECT user_id FROM users WHERE sub_active=1").fetchall()
    return [int(r["user_id"]) for r in rows]

def sum_total_balances() -> int:
    with get_db() as con:
        r = con.execute("SELECT SUM(sol_balance_lamports) s FROM users").fetchone()
        return int(r["s"] or 0)

def sum_total_deposits() -> int:
    with get_db() as con:
        r = con.execute("SELECT SUM(lamports) s FROM txlog WHERE type='DEPOSIT'").fetchone()
        return int(r["s"] or 0)

def sum_open_payouts() -> int:
    with get_db() as con:
        r = con.execute("SELECT SUM(amount_lamports) s FROM payouts WHERE status='REQUESTED'").fetchone()
        return int(r["s"] or 0)

# ---------------------------
# WALLET SETZEN
# ---------------------------

def set_source_wallet(uid: int, addr: str):
    with get_db() as con:
        con.execute("UPDATE users SET source_wallet=? WHERE user_id=?", (addr, uid))
        con.commit()

def set_payout_wallet(uid: int, addr: str):
    with get_db() as con:
        con.execute("UPDATE users SET payout_wallet=? WHERE user_id=?", (addr, uid))
        con.commit()

def subtract_balance(uid: int, lam: int) -> bool:
    bal = get_balance_lamports(uid)
    if bal < lam: return False
    set_balance(uid, bal - lam)
    return True

def is_probably_solana_address(s: str) -> bool:
    if not s or len(s) < 32 or len(s) > 44: return False
    allowed = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    return all(ch in allowed for ch in s)

# ---------------------------
# PIN HANDLING
# ---------------------------

import hashlib
def _hash_pin(pin: str) -> str:
    return hashlib.sha256(pin.encode()).hexdigest()

# ---------------------------
# INIT & LAUNCH
# ---------------------------

def init_all():
    print("🔧 Initialisiere Datenbank ...")
    init_db()
    print("✅ Datenbank bereit.")
    print("🚀 Starte Background-Watcher ...")
    threading.Thread(target=_expire_and_grace_scheduler, daemon=True).start()
    threading.Thread(target=_subscription_payment_watcher, daemon=True).start()
    print("✅ Watcher aktiv.")
    print("🤖 Bot läuft ...")

if __name__ == "__main__":
    init_all()
    bot.infinity_polling(timeout=60, long_polling_timeout=40)
