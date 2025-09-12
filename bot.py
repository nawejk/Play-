import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import sqlite3
import requests
import datetime
import threading
import time

# ==============================
# CONFIG (füge selbst ein)
# ==============================
BOT_TOKEN = "8223196888:AAEXRex4OONwq1ZSANuB2NviAtnSxiKgnqk"
ADMIN_IDS = [7919108078, 987654321]   # Telegram-IDs der Admins
CENTRAL_WALLET = "3z7UW4WBBy8GJT7sA93snf3pWS64WENShZb4hKtFqtxk"

# Solana RPC Endpoint (kostenlos)
RPC_URL = "https://api.mainnet-beta.solana.com"

bot = telebot.TeleBot(BOT_TOKEN)

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
# KEYBOARDS
# ==============================
def main_menu():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💸 Einzahlung tätigen", callback_data="deposit"))
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

def deposit_admin_buttons(deposit_id):
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
    username = msg.from_user.username or "Unbekannt"

    conn = sqlite3.connect("bot.db")
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO users (user_id, username, wallet) VALUES (?, ?, ?)", 
              (user_id, username, None))
    conn.commit()
    conn.close()

    disclaimer = (
        "⚠️ *Wichtiger Hinweis*\n\n"
        "Investitionen erfolgen auf eigenes Risiko. 🚨\n"
        "- Es gibt keine Garantie für Gewinne.\n"
        "- Bei Liquidationen oder Verlusten gibt es keine Rückerstattung.\n"
        "- Wir achten darauf, nie mit 100% Risiko zu handeln und planen alle Trades verantwortungsvoll.\n\n"
        "Mit Nutzung dieses Bots akzeptierst du diese Bedingungen."
    )

    bot.send_message(user_id, disclaimer, parse_mode="Markdown")
    bot.send_message(user_id, 
        "👋 Willkommen bei unserem Investment-Bot!\n\n"
        "➡️ Bitte registriere zuerst deine eigene **Solana-Wallet-Adresse**, "
        "damit wir deine Einzahlungen zuordnen können.",
        reply_markup=main_menu(),
        parse_mode="Markdown"
    )

@bot.message_handler(commands=["admin"])
def admin_panel(msg):
    if msg.from_user.id in ADMIN_IDS:
        bot.send_message(msg.chat.id, "⚙️ Admin-Menü", reply_markup=admin_menu())

# ==============================
# CALLBACK HANDLER
# ==============================
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call: CallbackQuery):
    user_id = call.from_user.id
    conn = sqlite3.connect("bot.db")
    c = conn.cursor()

    if call.data == "deposit":
        bot.send_message(user_id,
            f"💸 Bitte sende deine Einzahlung an unsere zentrale Wallet:\n\n`{CENTRAL_WALLET}`",
            parse_mode="Markdown"
        )

    elif call.data == "my_deposits":
        c.execute("SELECT amount, timestamp, status FROM deposits WHERE user_id=?", (user_id,))
        rows = c.fetchall()
        if rows:
            text = "📊 *Deine Einzahlungen:*\n\n"
            for r in rows:
                text += f"- {r[0]} SOL | {r[1]} | Status: {r[2]}\n"
        else:
            text = "❌ Keine Einzahlungen gefunden."
        bot.send_message(user_id, text, parse_mode="Markdown")

    elif call.data == "my_wallet":
        c.execute("SELECT wallet FROM users WHERE user_id=?", (user_id,))
        wallet = c.fetchone()
        if wallet and wallet[0]:
            bot.send_message(user_id, f"🔑 Deine Wallet: `{wallet[0]}`", parse_mode="Markdown")
        else:
            bot.send_message(user_id, "🔑 Du hast noch keine Wallet registriert.\nBitte sende mir jetzt deine Solana-Wallet-Adresse.")
            bot.register_next_step_handler(call.message, save_wallet)

    elif call.data == "payout_info":
        bot.send_message(user_id, "📅 Deine Auszahlungen erfolgen jeweils *7 Tage nach Einzahlung*.\n\n"
                                  "Admins informieren dich, sobald deine Auszahlung erfolgt.",
                                  parse_mode="Markdown")

    elif call.data == "support":
        bot.send_message(user_id, "🛠 Support-Team: @nadjad_crpt")

    # ----- ADMIN PANEL -----
    elif call.data == "all_users" and user_id in ADMIN_IDS:
        c.execute("SELECT user_id, username, wallet FROM users")
        rows = c.fetchall()
        text = "📋 *Alle Investoren:*\n\n"
        for r in rows:
            text += f"👤 {r[1]} | ID: {r[0]} | Wallet: {r[2]}\n"
        bot.send_message(user_id, text, parse_mode="Markdown")

    elif call.data == "all_deposits" and user_id in ADMIN_IDS:
        c.execute("SELECT id, user_id, amount, timestamp, status FROM deposits")
        rows = c.fetchall()
        if rows:
            for r in rows:
                text = f"💰 Einzahlung #{r[0]}\n👤 User: {r[1]}\n📥 Betrag: {r[2]} SOL\n⏰ {r[3]}\nStatus: {r[4]}"
                bot.send_message(user_id, text, reply_markup=deposit_admin_buttons(r[0]))
        else:
            bot.send_message(user_id, "❌ Keine Einzahlungen vorhanden.")

    elif call.data.startswith("payout_done") and user_id in ADMIN_IDS:
        dep_id = call.data.split(":")[1]
        c.execute("UPDATE deposits SET status=? WHERE id=?", ("Ausgezahlt", dep_id))
        conn.commit()
        bot.send_message(user_id, f"✅ Auszahlung für Einzahlung #{dep_id} markiert.")

    elif call.data.startswith("payout_problem") and user_id in ADMIN_IDS:
        dep_id = call.data.split(":")[1]
        c.execute("UPDATE deposits SET status=? WHERE id=?", ("Problem", dep_id))
        conn.commit()
        bot.send_message(user_id, f"❗ Problem bei Einzahlung #{dep_id} markiert.")

    conn.close()

# ==============================
# WALLET REGISTRATION
# ==============================
def save_wallet(msg):
    user_id = msg.from_user.id
    wallet = msg.text.strip()

    conn = sqlite3.connect("bot.db")
    c = conn.cursor()
    c.execute("UPDATE users SET wallet=? WHERE user_id=?", (wallet, user_id))
    conn.commit()
    conn.close()

    bot.send_message(user_id, f"✅ Deine Wallet wurde gespeichert: `{wallet}`", parse_mode="Markdown")

# ==============================
# TRANSACTION CHECK (Real)
# ==============================
def get_transactions(address, limit=5):
    headers = {"Content-Type": "application/json"}
    data = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSignaturesForAddress",
        "params": [address, {"limit": limit}]
    }
    r = requests.post(RPC_URL, headers=headers, json=data)
    return r.json().get("result", [])

def get_transaction_detail(sig):
    headers = {"Content-Type": "application/json"}
    data = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [sig, {"encoding": "jsonParsed"}]
    }
    r = requests.post(RPC_URL, headers=headers, json=data)
    return r.json().get("result", {})

def check_transactions_loop():
    seen = set()
    while True:
        try:
            txs = get_transactions(CENTRAL_WALLET, limit=10)
            for tx in txs:
                sig = tx["signature"]
                if sig in seen:
                    continue
                seen.add(sig)
                detail = get_transaction_detail(sig)
                if not detail:
                    continue

                try:
                    # Betrag herausfinden
                    meta = detail["meta"]
                    pre = meta["preBalances"]
                    post = meta["postBalances"]
                    amount = abs((post[0] - pre[0]) / 1e9)  # in SOL

                    # Absender/Empfänger checken
                    accounts = detail["transaction"]["message"]["accountKeys"]
                    sender = accounts[0]["pubkey"]
                    receiver = accounts[1]["pubkey"]

                    if receiver == CENTRAL_WALLET:
                        # In DB speichern
                        conn = sqlite3.connect("bot.db")
                        c = conn.cursor()
                        c.execute("SELECT user_id FROM users WHERE wallet=?", (sender,))
                        user = c.fetchone()
                        if user:
                            user_id = user[0]
                            timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                            c.execute("INSERT INTO deposits (user_id, amount, tx_hash, timestamp, status) VALUES (?, ?, ?, ?, ?)",
                                      (user_id, amount, sig, timestamp, "Eingezahlt"))
                            conn.commit()
                            conn.close()

                            # Admin benachrichtigen
                            for admin in ADMIN_IDS:
                                bot.send_message(admin,
                                    f"💰 *Neue Einzahlung erkannt!*\n\n"
                                    f"👤 User-ID: {user_id}\n"
                                    f"📥 Betrag: {amount} SOL\n"
                                    f"🔑 Wallet: {sender}\n"
                                    f"⏰ {timestamp}\n"
                                    f"Tx: `{sig}`",
                                    parse_mode="Markdown",
                                    reply_markup=deposit_admin_buttons(sig)
                                )
                except Exception as e:
                    print("Fehler TX-Parsing:", e)

        except Exception as e:
            print("Fehler im Loop:", e)

        time.sleep(30)

threading.Thread(target=check_transactions_loop, daemon=True).start()

# ==============================
# START BOT
# ==============================
print("🤖 Bot läuft...")
bot.infinity_polling()