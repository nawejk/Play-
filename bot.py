import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import sqlite3
import requests
import datetime
import threading
import time
import html

# ==============================
# CONFIG
# ==============================
BOT_TOKEN = "8223196888:AAEXRex4OONwq1ZSANuB2NviAtnSxiKgnqk"
ADMIN_IDS = [7919108078]  # Admin-ID(s)
CENTRAL_WALLET = "3z7UW4WBBy8GJT7sA93snf3pWS64WENShZb4hKtFqtxk"

# Öffentlicher Solana RPC
RPC_URL = "https://api.mainnet-beta.solana.com"

# Hinweis-Mindestbetrag in SOL (nur Info)
MIN_DEPOSIT = 0.5

# Polling-Intervalle / Backoff
TX_POLL_SECONDS = 30
RATE_LIMIT_BACKOFF = 60  # bei 429

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

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
                    tx_hash TEXT UNIQUE,
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

def esc(s: str) -> str:
    return html.escape(s or "")

def is_valid_solana_address(addr: str) -> bool:
    if not isinstance(addr, str):
        return False
    addr = addr.strip()
    if not (32 <= len(addr) <= 44):
        return False
    return all(ch in BASE58_ALPHABET for ch in addr)

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

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# ==============================
# KEYBOARDS
# ==============================
def back_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("⬅️ Zurück zum Menü", callback_data="back_main"))
    return kb

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
    kb.add(InlineKeyboardButton("📥 Alle Einzahlungen", callback_data="all_deposits"))
    kb.add(InlineKeyboardButton("📤 Offene Auszahlungen", callback_data="pending_payouts"))
    kb.add(InlineKeyboardButton("⬅️ Zurück zum Menü", callback_data="back_main"))
    return kb

def my_wallet_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔁 Wallet ändern", callback_data="change_wallet"))
    kb.add(InlineKeyboardButton("⬅️ Zurück zum Menü", callback_data="back_main"))
    return kb

def deposit_admin_buttons(deposit_id: int):
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ Auszahlung erledigt", callback_data=f"payout_done:{deposit_id}"),
        InlineKeyboardButton("❗ Problem", callback_data=f"payout_problem:{deposit_id}")
    )
    kb.add(InlineKeyboardButton("⬅️ Zurück zum Menü", callback_data="back_main"))
    return kb

# ==============================
# COMMANDS
# ==============================
@bot.message_handler(commands=["start"])
def start(msg):
    user_id = msg.from_user.id
    username = msg.from_user.username or "Unbekannt"

    conn = sqlite3.connect("bot.db")
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, username, wallet) VALUES (?, ?, ?)",
              (user_id, username, None))
    conn.commit()
    conn.close()

    disclaimer = (
        "⚠️ <b>Wichtiger Hinweis</b>\n\n"
        "Investitionen erfolgen auf eigenes Risiko. 🚨\n"
        "- Es gibt keine Garantie für Gewinne.\n"
        "- Bei Liquidationen oder Verlusten gibt es keine Rückerstattung.\n"
        "- Wir achten darauf, nie mit 100% Risiko zu handeln und planen alle Trades verantwortungsvoll.\n\n"
        f"ℹ️  Mindestbetrag: <b>{MIN_DEPOSIT} SOL</b>.\n\n"
        "Mit Nutzung dieses Bots akzeptierst du diese Bedingungen."
    )
    bot.send_message(user_id, disclaimer)
    bot.send_message(
        user_id,
        "👋 Willkommen bei unserem Investment-Bot!\n\n"
        "➡️ Bitte registriere zuerst deine eigene <b>Solana-Wallet-Adresse</b>, "
        "damit wir deine Einzahlungen zuordnen können.",
        reply_markup=main_menu()
    )

@bot.message_handler(commands=["admin"])
def admin_panel(msg):
    if is_admin(msg.from_user.id):
        bot.send_message(msg.chat.id, "⚙️ Admin-Menü", reply_markup=admin_menu())
    else:
        bot.reply_to(msg, "❌ Du bist kein Admin.")

# ==============================
# CALLBACK HANDLER
# ==============================
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call: CallbackQuery):
    user_id = call.from_user.id

    if call.data == "back_main":
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        bot.send_message(user_id, "🏠 Hauptmenü", reply_markup=main_menu())
        return

    conn = sqlite3.connect("bot.db")
    c = conn.cursor()

    try:
        if call.data == "deposit":
            w = get_user_wallet(user_id)
            if not w:
                bot.send_message(
                    user_id,
                    "🔑 Du hast noch keine Wallet registriert.\n"
                    "Bitte sende mir jetzt deine <b>Solana-Wallet-Adresse</b>.",
                    reply_markup=back_menu()
                )
                bot.register_next_step_handler(call.message, save_wallet_then_show_central_wallet)
            else:
                bot.send_message(
                    user_id,
                    f"💸 Sende jetzt an unsere zentrale Wallet:\n\n<code>{esc(CENTRAL_WALLET)}</code>\n\n"
                    f"⚠️ <i>Nur Einzahlungen</i> <b>von deiner registrierten Wallet</b> werden erkannt.\n"
                    f"ℹ️  Mindestbetrag: <b>{MIN_DEPOSIT} SOL</b> .",
                    reply_markup=back_menu()
                )

        elif call.data == "my_deposits":
            c.execute("SELECT amount, timestamp, status FROM deposits WHERE user_id=? ORDER BY id DESC", (user_id,))
            rows = c.fetchall()
            if rows:
                text = "📊 <b>Deine Einzahlungen:</b>\n\n"
                for amount, ts, status in rows:
                    text += f"- {amount} SOL | {esc(ts)} | Status: {esc(status)}\n"
            else:
                text = "❌ Keine Einzahlungen gefunden."
            bot.send_message(user_id, text, reply_markup=back_menu())

        elif call.data == "my_wallet":
            w = get_user_wallet(user_id)
            if w:
                bot.send_message(
                    user_id,
                    f"🔑 Deine registrierte Wallet:\n<code>{esc(w)}</code>",
                    reply_markup=my_wallet_menu()
                )
            else:
                bot.send_message(
                    user_id,
                    "🔑 Du hast noch keine Wallet registriert.\n"
                    "Bitte sende mir jetzt deine <b>Solana-Wallet-Adresse</b>.",
                    reply_markup=back_menu()
                )
                bot.register_next_step_handler(call.message, save_wallet)

        elif call.data == "change_wallet":
            bot.send_message(
                user_id,
                "✏️ Sende mir jetzt deine <b>neue</b> Solana-Wallet-Adresse.\n"
                "Hinweis: Nur gültige Solana-Adressen (Base58, 32–44 Zeichen) werden akzeptiert.",
                reply_markup=back_menu()
            )
            bot.register_next_step_handler(call.message, change_wallet_save)

        elif call.data == "payout_info":
            bot.send_message(
                user_id,
                "📅 Auszahlungen erfolgen manuell durch die Admins.\n"
                f"ℹ️ Empfohlener Mindestbetrag: <b>{MIN_DEPOSIT} SOL</b>.",
                reply_markup=back_menu()
            )

        elif call.data == "support":
            bot.send_message(user_id, "🛠 Support: @Fux98", reply_markup=back_menu())

        # ----- ADMIN -----
        elif call.data == "all_users" and is_admin(user_id):
            c.execute("SELECT user_id, username, wallet FROM users ORDER BY user_id DESC")
            rows = c.fetchall()
            if not rows:
                bot.send_message(user_id, "Keine Investoren gefunden.", reply_markup=back_menu())
            else:
                lines = ["📋 <b>Alle Investoren:</b>\n"]
                for uid, uname, wallet in rows:
                    lines.append(f"👤 {esc(uname)} | ID: {uid} | Wallet: {esc(wallet or '-')}")
                bot.send_message(user_id, "\n".join(lines), reply_markup=back_menu())

        elif call.data == "all_deposits" and is_admin(user_id):
            c.execute("SELECT id, user_id, amount, timestamp, status FROM deposits ORDER BY id DESC")
            rows = c.fetchall()
            if rows:
                for dep_id, uid, amount, ts, status in rows:
                    text = (
                        f"💰 Einzahlung #{dep_id}\n"
                        f"👤 User: {uid}\n"
                        f"📥 Betrag: {amount} SOL\n"
                        f"⏰ {esc(ts)}\n"
                        f"Status: {esc(status)}"
                    )
                    bot.send_message(user_id, text, reply_markup=deposit_admin_buttons(dep_id))
            else:
                bot.send_message(user_id, "❌ Keine Einzahlungen vorhanden.", reply_markup=back_menu())

        elif call.data == "pending_payouts" and is_admin(user_id):
            c.execute("SELECT id, user_id, amount, timestamp FROM deposits WHERE status='Eingezahlt' ORDER BY id ASC")
            rows = c.fetchall()
            if rows:
                for dep_id, uid, amount, ts in rows:
                    text = (
                        f"🕒 Offen #{dep_id}\n"
                        f"👤 User: {uid}\n"
                        f"📥 Betrag: {amount} SOL\n"
                        f"⏰ {esc(ts)}"
                    )
                    bot.send_message(user_id, text, reply_markup=deposit_admin_buttons(dep_id))
            else:
                bot.send_message(user_id, "✅ Keine offenen Auszahlungen.", reply_markup=back_menu())

        elif call.data.startswith("payout_done") and is_admin(user_id):
            dep_id = call.data.split(":")[1]
            c.execute("UPDATE deposits SET status=? WHERE id=?", ("Ausgezahlt", dep_id))
            conn.commit()
            bot.send_message(user_id, f"✅ Auszahlung für Einzahlung #{dep_id} markiert.", reply_markup=back_menu())

        elif call.data.startswith("payout_problem") and is_admin(user_id):
            dep_id = call.data.split(":")[1]
            c.execute("UPDATE deposits SET status=? WHERE id=?", ("Problem", dep_id))
            conn.commit()
            bot.send_message(user_id, f"❗ Problem bei Einzahlung #{dep_id} markiert.", reply_markup=back_menu())

        else:
            # Nicht-Admin hat Admin-Action versucht
            if call.data in ("all_users", "all_deposits", "pending_payouts") or call.data.startswith(("payout_done", "payout_problem")):
                bot.answer_callback_query(call.id, "❌ Keine Admin-Berechtigung.", show_alert=True)

    finally:
        conn.close()

# ==============================
# WALLET SET/CHANGE
# ==============================
def save_wallet(msg):
    user_id = msg.from_user.id
    candidate = (msg.text or "").strip()
    if not is_valid_solana_address(candidate):
        bot.send_message(user_id, "❌ Ungültige Solana-Adresse. Bitte Base58 & Länge (32–44) prüfen.", reply_markup=back_menu())
        return
    set_user_wallet(user_id, candidate)
    bot.send_message(user_id, f"✅ Deine Wallet wurde gespeichert:\n<code>{esc(candidate)}</code>", reply_markup=back_menu())

def save_wallet_then_show_central_wallet(msg):
    user_id = msg.from_user.id
    candidate = (msg.text or "").strip()
    if not is_valid_solana_address(candidate):
        bot.send_message(user_id, "❌ Ungültige Solana-Adresse. Bitte erneut senden (Base58, 32–44).", reply_markup=back_menu())
        return
    set_user_wallet(user_id, candidate)
    bot.send_message(user_id, f"✅ Deine Wallet wurde gespeichert:\n<code>{esc(candidate)}</code>")
    bot.send_message(
        user_id,
        f"💸 Sende jetzt an unsere zentrale Wallet:\n\n<code>{esc(CENTRAL_WALLET)}</code>\n\n"
        f"⚠️ Nur Einzahlungen von deiner <b>registrierten Wallet</b> werden erkannt.\n"
        f"ℹ️ Empfohlener Mindestbetrag: <b>{MIN_DEPOSIT} SOL</b> (kleinere Beträge werden auch erkannt).",
        reply_markup=back_menu()
    )

def change_wallet_save(msg):
    user_id = msg.from_user.id
    candidate = (msg.text or "").strip()
    if not is_valid_solana_address(candidate):
        bot.send_message(user_id, "❌ Ungültige Solana-Adresse. Bitte Base58 & Länge (32–44) prüfen.", reply_markup=back_menu())
        return
    set_user_wallet(user_id, candidate)
    bot.send_message(user_id, f"🔁 Deine Wallet wurde geändert zu:\n<code>{esc(candidate)}</code>", reply_markup=back_menu())

# ==============================
# SOLANA RPC
# ==============================
def rpc_call(method, params):
    headers = {"Content-Type": "application/json"}
    data = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(RPC_URL, headers=headers, json=data, timeout=25)
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"])
    return j.get("result", None)

def get_signatures_for_address(address, limit=50, before=None):
    params = [address, {"limit": limit}]
    if before:
        params[1]["before"] = before
    return rpc_call("getSignaturesForAddress", params) or []

def get_transaction_detail(sig):
    return rpc_call("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed"}]) or {}

# ==============================
# TX LOOP (robust via parsed instructions)
# ==============================
def check_transactions_loop():
    """
    Erkennt Einzahlungen zuverlässig über System-Transfer-Instructions:
    - getSignaturesForAddress(CENTRAL_WALLET)
    - getTransaction(signature, jsonParsed)
    - Finde instruction mit type='transfer' & destination == CENTRAL_WALLET
    - source = info['source'], lamports = info['lamports']
    - Wenn source bei einem Nutzer registriert ist -> speichern & Admin benachrichtigen
    """
    seen = set()
    last_before_sig = None

    while True:
        try:
            try:
                txs = get_signatures_for_address(CENTRAL_WALLET, limit=50, before=last_before_sig)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 429:
                    print("Rate limit (429) – backoff …")
                    time.sleep(RATE_LIMIT_BACKOFF)
                    continue
                else:
                    raise
            except Exception as e:
                print("get_signatures error:", e)
                time.sleep(TX_POLL_SECONDS)
                continue

            if txs:
                last_before_sig = txs[-1]["signature"]

            for tx in txs:
                sig = tx["signature"]
                if sig in seen:
                    continue
                seen.add(sig)

                try:
                    detail = get_transaction_detail(sig)
                except requests.HTTPError as e:
                    if e.response is not None and e.response.status_code == 429:
                        print("Rate limit (429) @get_transaction – backoff …")
                        time.sleep(RATE_LIMIT_BACKOFF)
                        continue
                    else:
                        print("get_transaction error:", e)
                        continue
                except Exception as e:
                    print("get_transaction error:", e)
                    continue

                if not detail:
                    continue

                txmsg = detail.get("transaction", {}).get("message", {})
                meta = detail.get("meta", {})
                if not txmsg:
                    continue

                # Suche nach System-Transfer-Instructions (parsed)
                found_any = False
                # Top-level instructions
                all_instr = txmsg.get("instructions", []) or []
                # plus inner instructions (Liste von Blöcken)
                inner = detail.get("meta", {}).get("innerInstructions") or []
                for block in inner:
                    all_instr.extend(block.get("instructions", []) or [])

                for ins in all_instr:
                    parsed = ins.get("parsed")
                    program = ins.get("program")
                    if not parsed or program != "system":
                        continue
                    if parsed.get("type") != "transfer":
                        continue
                    info = parsed.get("info", {})
                    dest = info.get("destination")
                    src = info.get("source")
                    lamports = info.get("lamports")
                    if dest == CENTRAL_WALLET and src and lamports is not None:
                        amount_sol = round(lamports / 1e9, 9)

                        # Prüfen, ob src (= Absender) registriert ist
                        conn = sqlite3.connect("bot.db")
                        c = conn.cursor()
                        c.execute("SELECT user_id FROM users WHERE wallet=?", (src,))
                        user = c.fetchone()
                        if user:
                            user_id = user[0]
                            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                            try:
                                c.execute(
                                    "INSERT OR IGNORE INTO deposits (user_id, amount, tx_hash, timestamp, status) VALUES (?, ?, ?, ?, ?)",
                                    (user_id, amount_sol, sig, timestamp, "Eingezahlt")
                                )
                                conn.commit()
                                # hole die id (falls neu)
                                c.execute("SELECT id FROM deposits WHERE tx_hash=?", (sig,))
                                dep_id_row = c.fetchone()
                                dep_id = dep_id_row[0] if dep_id_row else None
                            finally:
                                conn.close()

                            # Admin benachrichtigen
                            if dep_id:
                                below_note = " ⚠️ <i>unter empfohlenem Mindestbetrag</i>" if amount_sol < MIN_DEPOSIT else ""
                                for admin in ADMIN_IDS:
                                    try:
                                        bot.send_message(
                                            admin,
                                            "💰 <b>Neue Einzahlung erkannt!</b>" + below_note + "\n\n"
                                            f"👤 User-ID: {user_id}\n"
                                            f"📥 Betrag: {amount_sol} SOL\n"
                                            f"🔑 Absender-Wallet: <code>{esc(src)}</code>\n"
                                            f"⏰ {esc(timestamp)}\n"
                                            f"Tx: <code>{esc(sig)}</code>",
                                            reply_markup=deposit_admin_buttons(dep_id)
                                        )
                                    except Exception as e:
                                        print("Admin-Notify-Fehler:", e)
                            found_any = True
                # Ende instructions
                if not found_any:
                    # Keine passende Transfer-Instruction (evtl. Stake o.ä.) – ignorieren
                    pass

        except Exception as e:
            print("Fehler im TX-Loop:", e)

        time.sleep(TX_POLL_SECONDS)

# Thread starten
threading.Thread(target=check_transactions_loop, daemon=True).start()

# ==============================
# START BOT
# ==============================
print("🤖 Bot läuft…")
bot.infinity_polling(timeout=60, long_polling_timeout=60)