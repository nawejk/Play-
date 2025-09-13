import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import sqlite3
import requests
import datetime
import threading
import time

# ==============================
# CONFIG
# ==============================
BOT_TOKEN = "8223196888:AAEXRex4OONwq1ZSANuB2NviAtnSxiKgnqk"

# Admins – du kannst entweder nur IDs ODER auch Usernames (ohne "@") pflegen
ADMIN_IDS = [7919108078]            # <- trage hier DEINE echte ID ein
ADMIN_USERNAMES = []                # z.B. ["Fux98"] (ohne @), optional

CENTRAL_WALLET = "3z7UW4WBBy8GJT7sA93snf3pWS64WENShZb4hKtFqtxk"

# Solana RPC Endpoint (kostenlos)
RPC_URL = "https://api.mainnet-beta.solana.com"

# Mindestbetrag in SOL (nur Hinweis im UI; NICHT technisch erzwungen)
MIN_DEPOSIT = 0.5

bot = telebot.TeleBot(BOT_TOKEN, parse_mode=None)

# ==============================
# DATABASE
# ==============================
def init_db():
    conn = sqlite3.connect("bot.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    wallet TEXT
                )""")
    c.execute("""CREATE TABLE IF NOT EXISTS deposits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount REAL,
                    tx_hash TEXT,
                    timestamp TEXT,
                    status TEXT
                )""")
    conn.commit()
    conn.close()

init_db()

# ==============================
# HELPERS
# ==============================
BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

def is_valid_solana_address(addr: str) -> bool:
    """Einfache Base58/Längen-Prüfung für Solana-Addrs (32-44 Zeichen üblich)."""
    if not isinstance(addr, str):
        return False
    addr = addr.strip()
    if not (32 <= len(addr) <= 44):
        return False
    for ch in addr:
        if ch not in BASE58_ALPHABET:
            return False
    return True

def get_user_wallet(user_id: int):
    conn = sqlite3.connect("bot.db")
    c = conn.cursor()
    c.execute("SELECT wallet FROM users WHERE user_id=?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None

def set_user_wallet(user_id: int, wallet: str):
    conn = sqlite3.connect("bot.db")
    c = conn.cursor()
    c.execute("UPDATE users SET wallet=? WHERE user_id=?", (wallet, user_id))
    conn.commit()
    conn.close()

def is_admin(user_id: int, username: str | None) -> bool:
    """Erkennt Admin via ID ODER Username (case-insensitive, ohne @)."""
    if user_id in ADMIN_IDS:
        return True
    if username:
        uname = username.lstrip("@").lower()
        for u in ADMIN_USERNAMES:
            if uname == u.lower():
                return True
    return False

def log_console(prefix, msg):
    print(f"[{prefix}] {msg}")

# ==============================
# KEYBOARDS
# ==============================
def main_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💸 Einzahlung tätigen", callback_data="deposit"))
    kb.add(InlineKeyboardButton("📊 Meine Einzahlungen", callback_data="my_deposits"))
    kb.add(InlineKeyboardButton("🔑 Meine Wallet", callback_data="my_wallet"))
    kb.add(InlineKeyboardButton("📅 Auszahlungstermin", callback_data="payout_info"))
    kb.add(InlineKeyboardButton("🛠 Support", callback_data="support"))
    return kb

def admin_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📋 Alle Investoren", callback_data="all_users"))
    kb.add(InlineKeyboardButton("📥 Neue Einzahlungen", callback_data="all_deposits"))
    kb.add(InlineKeyboardButton("📤 Offene Auszahlungen", callback_data="pending_payouts"))
    return kb

def my_wallet_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔁 Wallet ändern", callback_data="change_wallet"))
    kb.add(InlineKeyboardButton("⬅️ Zurück", callback_data="back_main"))
    return kb

def deposit_admin_buttons(deposit_id: int):
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ Auszahlung erledigt", callback_data=f"payout_done:{deposit_id}"),
        InlineKeyboardButton("❗ Problem", callback_data=f"payout_problem:{deposit_id}")
    )
    return kb

# ==============================
# COMMANDS
# ==============================
@bot.message_handler(commands=["start"])
def start(msg):
    user_id = msg.from_user.id
    username = msg.from_user.username

    # Debug: wer ist das?
    log_console("START", f"user_id={user_id}, username={username}")

    conn = sqlite3.connect("bot.db")
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, username, wallet) VALUES (?, ?, ?)",
              (user_id, username or "Unbekannt", None))
    conn.commit()
    conn.close()

    disclaimer = (
        "⚠️ *Wichtiger Hinweis*\n\n"
        "Investitionen erfolgen auf eigenes Risiko. 🚨\n"
        "- Es gibt keine Garantie für Gewinne.\n"
        "- Bei Liquidationen oder Verlusten gibt es keine Rückerstattung.\n"
        "- Wir achten darauf, nie mit 100% Risiko zu handeln und planen alle Trades verantwortungsvoll.\n\n"
        f"*Empfohlener Mindestbetrag für Einzahlungen: {MIN_DEPOSIT} SOL.*\n\n"
        "Mit Nutzung dieses Bots akzeptierst du diese Bedingungen."
    )
    bot.send_message(user_id, disclaimer, parse_mode="Markdown")
    bot.send_message(
        user_id,
        "👋 Willkommen bei unserem Investment-Bot!\n\n"
        "➡️ Bitte registriere zuerst deine eigene **Solana-Wallet-Adresse**, "
        "damit wir deine Einzahlungen zuordnen können.",
        reply_markup=main_menu(),
        parse_mode="Markdown"
    )
    # Direkt hilfreiche Info für Admin-Setup:
    bot.send_message(user_id, f"🆔 Deine ID: `{user_id}` | Username: `{username or '—'}`", parse_mode="Markdown")

@bot.message_handler(commands=["admin"])
def admin_panel(msg):
    uid = msg.from_user.id
    uname = msg.from_user.username
    if is_admin(uid, uname):
        bot.send_message(msg.chat.id, "⚙️ Admin-Menü", reply_markup=admin_menu())
    else:
        bot.reply_to(msg, "❌ Du bist kein Admin.")

@bot.message_handler(commands=["whoami"])
def whoami(msg):
    bot.reply_to(msg, f"🆔 Deine ID: {msg.from_user.id}\n👤 Username: @{msg.from_user.username}" if msg.from_user.username else f"🆔 Deine ID: {msg.from_user.id}\n👤 Username: —")

# ==============================
# CALLBACK HANDLER
# ==============================
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call: CallbackQuery):
    user_id = call.from_user.id
    username = call.from_user.username

    conn = sqlite3.connect("bot.db")
    c = conn.cursor()

    if call.data == "back_main":
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        bot.send_message(user_id, "🏠 Hauptmenü", reply_markup=main_menu())

    elif call.data == "deposit":
        # Erst prüfen, ob Wallet registriert ist
        w = get_user_wallet(user_id)
        if not w:
            bot.send_message(
                user_id,
                "🔑 Du hast noch keine Wallet registriert.\n"
                "Bitte sende mir jetzt deine **Solana-Wallet-Adresse**."
            )
            bot.register_next_step_handler(call.message, save_wallet_then_show_central_wallet)
        else:
            # Direkt zentrale Wallet anzeigen (Hinweis nur Info)
            bot.send_message(
                user_id,
                f"💸 Sende jetzt an unsere zentrale Wallet:\n\n`{CENTRAL_WALLET}`\n\n"
                f"⚠️ *Wichtig:* Nur Einzahlungen **von deiner registrierten Wallet** werden erkannt.\n"
                f"ℹ️ Empfohlener Mindestbetrag: **{MIN_DEPOSIT} SOL** (kleinere Beträge werden auch erkannt).",
                parse_mode="Markdown"
            )

    elif call.data == "my_deposits":
        c.execute("SELECT amount, timestamp, status FROM deposits WHERE user_id=? ORDER BY id DESC", (user_id,))
        rows = c.fetchall()
        if rows:
            text = "📊 *Deine Einzahlungen:*\n\n"
            for r in rows:
                text += f"- {r[0]} SOL | {r[1]} | Status: {r[2]}\n"
        else:
            text = "❌ Keine Einzahlungen gefunden."
        bot.send_message(user_id, text, parse_mode="Markdown")

    elif call.data == "my_wallet":
        w = get_user_wallet(user_id)
        if w:
            bot.send_message(user_id, f"🔑 Deine registrierte Wallet:\n`{w}`", parse_mode="Markdown", reply_markup=my_wallet_menu())
        else:
            bot.send_message(
                user_id,
                "🔑 Du hast noch keine Wallet registriert.\n"
                "Bitte sende mir jetzt deine **Solana-Wallet-Adresse**."
            )
            bot.register_next_step_handler(call.message, save_wallet)

    elif call.data == "change_wallet":
        bot.send_message(
            user_id,
            "✏️ Sende mir jetzt deine *neue* **Solana-Wallet-Adresse**.\n"
            "Hinweis: Nur gültige Solana-Adressen (Base58, 32–44 Zeichen) werden akzeptiert."
        )
        bot.register_next_step_handler(call.message, change_wallet_save)

    elif call.data == "payout_info":
        bot.send_message(
            user_id,
            "📅 Auszahlungen erfolgen manuell durch die Admins.\n"
            f"ℹ️ Empfohlener Mindestbetrag für Einzahlungen: **{MIN_DEPOSIT} SOL**.",
            parse_mode="Markdown"
        )

    elif call.data == "support":
        bot.send_message(user_id, "🛠 Support: @Fux98")

    # ----- ADMIN PANEL -----
    elif call.data == "all_users" and is_admin(user_id, username):
        c.execute("SELECT user_id, username, wallet FROM users ORDER BY user_id DESC")
        rows = c.fetchall()
        if not rows:
            bot.send_message(user_id, "Keine Investoren gefunden.")
        else:
            text = "📋 *Alle Investoren:*\n\n"
            for r in rows:
                text += f"👤 {r[1]} | ID: {r[0]} | Wallet: {r[2]}\n"
            bot.send_message(user_id, text, parse_mode="Markdown")

    elif call.data == "all_deposits" and is_admin(user_id, username):
        c.execute("SELECT id, user_id, amount, timestamp, status FROM deposits ORDER BY id DESC")
        rows = c.fetchall()
        if rows:
            for r in rows:
                text = (
                    f"💰 Einzahlung #{r[0]}\n"
                    f"👤 User: {r[1]}\n"
                    f"📥 Betrag: {r[2]} SOL\n"
                    f"⏰ {r[3]}\n"
                    f"Status: {r[4]}"
                )
                bot.send_message(user_id, text, reply_markup=deposit_admin_buttons(r[0]))
        else:
            bot.send_message(user_id, "❌ Keine Einzahlungen vorhanden.")

    elif call.data == "pending_payouts" and is_admin(user_id, username):
        c.execute("SELECT id, user_id, amount, timestamp FROM deposits WHERE status='Eingezahlt' ORDER BY id ASC")
        rows = c.fetchall()
        if rows:
            for r in rows:
                text = (
                    f"🕒 Offen #{r[0]}\n"
                    f"👤 User: {r[1]}\n"
                    f"📥 Betrag: {r[2]} SOL\n"
                    f"⏰ {r[3]}"
                )
                bot.send_message(user_id, text, reply_markup=deposit_admin_buttons(r[0]))
        else:
            bot.send_message(user_id, "✅ Keine offenen Auszahlungen.")

    elif call.data.startswith("payout_done") and is_admin(user_id, username):
        dep_id = call.data.split(":")[1]
        c.execute("UPDATE deposits SET status=? WHERE id=?", ("Ausgezahlt", dep_id))
        conn.commit()
        bot.send_message(user_id, f"✅ Auszahlung für Einzahlung #{dep_id} markiert.")

    elif call.data.startswith("payout_problem") and is_admin(user_id, username):
        dep_id = call.data.split(":")[1]
        c.execute("UPDATE deposits SET status=? WHERE id=?", ("Problem", dep_id))
        conn.commit()
        bot.send_message(user_id, f"❗ Problem bei Einzahlung #{dep_id} markiert.")

    else:
        # Nicht-Admin versucht Admin-Action
        if call.data in ("all_users", "all_deposits", "pending_payouts") or \
           call.data.startswith(("payout_done", "payout_problem")):
            bot.answer_callback_query(call.id, "❌ Keine Admin-Berechtigung.", show_alert=True)

    conn.close()

# ==============================
# WALLET SET/CHANGE
# ==============================
@bot.message_handler(func=lambda m: False)
def _placeholder(_):  # verhindert "falsche" Handler-Kollision
    pass

def save_wallet(msg):
    user_id = msg.from_user.id
    candidate = (msg.text or "").strip()

    if not is_valid_solana_address(candidate):
        bot.send_message(user_id, "❌ Ungültige Solana-Adresse. Bitte prüfe Base58-Schreibweise und Länge (32–44 Zeichen).")
        return

    set_user_wallet(user_id, candidate)
    bot.send_message(user_id, f"✅ Deine Wallet wurde gespeichert: `{candidate}`", parse_mode="Markdown")

def save_wallet_then_show_central_wallet(msg):
    """Speichert Wallet (falls gültig) und zeigt danach sofort die zentrale Wallet zum Senden an."""
    user_id = msg.from_user.id
    candidate = (msg.text or "").strip()
    if not is_valid_solana_address(candidate):
        bot.send_message(user_id, "❌ Ungültige Solana-Adresse. Bitte erneut senden (Base58, 32–44 Zeichen).")
        return
    set_user_wallet(user_id, candidate)
    bot.send_message(user_id, f"✅ Deine Wallet wurde gespeichert: `{candidate}`", parse_mode="Markdown")
    bot.send_message(
        user_id,
        f"💸 Sende jetzt an unsere zentrale Wallet:\n\n`{CENTRAL_WALLET}`\n\n"
        f"⚠️ Nur Einzahlungen von deiner **registrierten Wallet** werden erkannt.\n"
        f"ℹ️ Empfohlener Mindestbetrag: **{MIN_DEPOSIT} SOL** (kleinere Beträge werden auch erkannt).",
        parse_mode="Markdown"
    )

def change_wallet_save(msg):
    user_id = msg.from_user.id
    candidate = (msg.text or "").strip()

    if not is_valid_solana_address(candidate):
        bot.send_message(user_id, "❌ Ungültige Solana-Adresse. Bitte prüfe Base58-Schreibweise und Länge (32–44 Zeichen).")
        return

    set_user_wallet(user_id, candidate)
    bot.send_message(user_id, f"🔁 Deine Wallet wurde geändert zu:\n`{candidate}`", parse_mode="Markdown")

# ==============================
# TRANSACTION CHECK (Real)
# ==============================
def rpc_call(method, params):
    headers = {"Content-Type": "application/json"}
    data = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(RPC_URL, headers=headers, json=data, timeout=20)
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"])
    return j.get("result", None)

def get_signatures_for_address(address, limit=20, before=None):
    params = [address, {"limit": limit}]
    if before:
        params[1]["before"] = before
    return rpc_call("getSignaturesForAddress", params) or []

def get_transaction_detail(sig):
    return rpc_call("getTransaction", [sig, {"encoding": "jsonParsed"}]) or {}

def check_transactions_loop():
    """
    Erkennung:
    - Finde Index der CENTRAL_WALLET in accountKeys.
    - Betrag = (postBalances[idx] - preBalances[idx]) / 1e9 (ALLE Beträge speichern).
    - Sende-Index = Account mit größtem negativen Delta (vermutlich Sender).
    - Prüfe, ob Sender als User-Wallet registriert ist.
    - Speichere nur neue TXs (per 'seen' verhindert Doppelungen).
    """
    seen = set()
    last_before_sig = None  # optional für Paginierung

    while True:
        try:
            txs = get_signatures_for_address(CENTRAL_WALLET, limit=20, before=last_before_sig)
            if txs:
                last_before_sig = txs[-1]["signature"]

            for tx in txs:
                sig = tx["signature"]
                if sig in seen:
                    continue
                seen.add(sig)

                detail = get_transaction_detail(sig)
                if not detail:
                    continue

                meta = detail.get("meta")
                txmsg = detail.get("transaction", {}).get("message", {})
                if not meta or not txmsg:
                    continue

                pre = meta.get("preBalances") or []
                post = meta.get("postBalances") or []
                keys = txmsg.get("accountKeys") or []

                # Index der zentralen Wallet bestimmen
                target_idx = None
                for i, k in enumerate(keys):
                    kpub = k["pubkey"] if isinstance(k, dict) else k
                    if kpub == CENTRAL_WALLET:
                        target_idx = i
                        break
                if target_idx is None or target_idx >= len(pre) or target_idx >= len(post):
                    continue

                delta_lamports = post[target_idx] - pre[target_idx]
                if delta_lamports <= 0:
                    continue

                amount_sol = round(delta_lamports / 1e9, 9)

                # Sender heuristisch: größter negativer Delta
                sender_idx = None
                sender_abs = 0
                for i in range(min(len(pre), len(post))):
                    d = post[i] - pre[i]
                    if d < 0 and abs(d) > sender_abs:
                        sender_abs = abs(d)
                        sender_idx = i
                if sender_idx is None:
                    continue

                sender_key = keys[sender_idx]["pubkey"] if isinstance(keys[sender_idx], dict) else keys[sender_idx]

                # Prüfen, ob Sender registriert ist
                conn = sqlite3.connect("bot.db")
                c = conn.cursor()
                c.execute("SELECT user_id FROM users WHERE wallet=?", (sender_key,))
                user = c.fetchone()

                if user:
                    user_id = user[0]
                    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                    c.execute(
                        "INSERT INTO deposits (user_id, amount, tx_hash, timestamp, status) VALUES (?, ?, ?, ?, ?)",
                        (user_id, amount_sol, sig, timestamp, "Eingezahlt")
                    )
                    dep_id = c.lastrowid
                    conn.commit()
                    conn.close()

                    # Hinweis an Admin, falls unter empfohlenem Mindestbetrag
                    min_note = " ⚠️ *unter Mindestbetrag*" if amount_sol < MIN_DEPOSIT else ""

                    for admin in ADMIN_IDS:
                        try:
                            bot.send_message(
                                admin,
                                f"💰 *Neue Einzahlung erkannt!*{min_note}\n\n"
                                f"👤 User-ID: {user_id}\n"
                                f"📥 Betrag: {amount_sol} SOL\n"
                                f"🔑 Wallet (Absender): {sender_key}\n"
                                f"⏰ {timestamp}\n"
                                f"Tx: `{sig}`",
                                parse_mode="Markdown",
                                reply_markup=deposit_admin_buttons(dep_id)
                            )
                        except Exception as e:
                            print("Admin-Notify-Fehler:", e)
                else:
                    conn.close()

        except Exception as e:
            print("Fehler im TX-Loop:", e)

        time.sleep(30)

# Thread starten
threading.Thread(target=check_transactions_loop, daemon=True).start()

# ==============================
# START BOT
# ==============================
print("🤖 Bot läuft...")
bot.infinity_polling()