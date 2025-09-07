#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PulsePlay – Full Telegram Bot (buttons UI, markets, coinflip, manual signals, Solana deposits)
- Inline-Menü mit Kontostand
- Prediction Markets (YES/NO: +pct oder above:price), Auto-Lock/Settle
- Coinflip (verdeckte Win-Rate 30%, Payout 2x), Einsatz-Buttons
- Trader/Follower + manuelle Signale (MemeCoin, Futures, Lux) mit Wochenpreis
- Solana-Deposit-Verifizierung (USDC/SOL) gegen zentrale Adresse
- Admin-Panel als Buttons (kein /command nötig)
Technik:
  Python 3.10+ | pyTelegramBotAPI | SQLAlchemy 2 | APScheduler | requests

WICHTIG: Diese Datei nutzt fest eingetragene Defaults aus der Nutzer-Nachricht.
Du kannst sie später via Umgebungsvariablen überschreiben:
  ENV_BOT_TOKEN, ENV_ADMIN_IDS, ENV_CENTRAL_DEPOSIT_ADDRESS,
  ENV_SOLANA_RPC_URL, ENV_DEFAULT_FEE_PCT, ENV_COINFLIP_PROB, ENV_COINFLIP_MULT

Start:
  pip install -r requirements.txt
  python3 bot.py
"""
import os, json, threading, random
from datetime import datetime, timedelta, timezone
from typing import Optional, List

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from sqlalchemy import (create_engine, Column, Integer, String, DateTime, Boolean,
                        ForeignKey, Float, Text, UniqueConstraint, Index)
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session, relationship

# ========= CONFIG (mit Defaults aus der Nachricht) =========
BOT_TOKEN = os.getenv("ENV_BOT_TOKEN", "8200746289:AAGbzwf7sUHVHlLDb3foXbZpj9SVGnqLeNU")
ADMIN_IDS = [int(x) for x in os.getenv("ENV_ADMIN_IDS", "7919108078").split(",") if x.strip()]
CENTRAL_DEPOSIT_ADDRESS = os.getenv("ENV_CENTRAL_DEPOSIT_ADDRESS", "CKZEpwiVqAHLiSbdc8Ebf8xaQ2fofgPCNmzi4cV32M1s")
SOLANA_RPC_URL = os.getenv("ENV_SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
USDC_SOLANA_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

DEFAULT_FEE_PCT = float(os.getenv("ENV_DEFAULT_FEE_PCT", "0.04"))  # 4%
COINFLIP_WIN_PROB = float(os.getenv("ENV_COINFLIP_PROB", "0.30"))  # 30% Win (geheim)
COINFLIP_PAYOUT_MULT = float(os.getenv("ENV_COINFLIP_MULT", "2.0"))  # 2x Auszahlung bei Win
DEBUG = os.getenv("ENV_DEBUG", "1") == "1"

# ========= Utils =========
def esc(text: str) -> str:
    return (text or "").replace("<", "&lt;").replace(">", "&gt;")

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def dbg(msg: str):
    if DEBUG:
        print(f"[{datetime.utcnow().isoformat()}] {msg}")

def is_admin(tg_id: int) -> bool:
    return tg_id in ADMIN_IDS

# ========= Bot & DB =========
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

Base = declarative_base()
engine = create_engine("sqlite:///pulseplay_full.db", connect_args={"check_same_thread": False})
SessionLocal = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))

# ========= Datenbank-Modelle =========
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True, index=True)
    username = Column(String, default="")
    role = Column(String, default="user")  # user, trader, admin
    created_at = Column(DateTime(timezone=True), default=now_utc)

class Balance(Base):
    __tablename__ = "balances"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    currency = Column(String, default="USDC")
    amount = Column(Float, default=0.0)

class Market(Base):
    __tablename__ = "markets"
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    timeframe = Column(String)  # 1h,4h,24h
    condition = Column(String)  # +2% | above:123.45
    start_at = Column(DateTime(timezone=True))
    lock_at = Column(DateTime(timezone=True))
    settle_at = Column(DateTime(timezone=True))
    status = Column(String, default="open")  # open, locked, settled, canceled
    reference_price = Column(Float, default=0.0)
    settle_price = Column(Float, default=0.0)
    oracle_source = Column(String, default="coingecko")

class Bet(Base):
    __tablename__ = "bets"
    id = Column(Integer, primary_key=True)
    market_id = Column(Integer, ForeignKey("markets.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    side = Column(String)  # YES, NO
    stake = Column(Float, default=0.0)
    fee = Column(Float, default=0.0)
    payout = Column(Float, default=0.0)
    placed_at = Column(DateTime(timezone=True), default=now_utc)
    settled = Column(Boolean, default=False)

class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    type = Column(String)  # deposit, withdraw
    method = Column(String, default="solana")
    amount = Column(Float, default=0.0)
    tx = Column(String, default="")  # signature
    status = Column(String, default="pending")
    created_at = Column(DateTime(timezone=True), default=now_utc)
    meta = Column(Text, default="{}")  # sender/dest

class Trader(Base):
    __tablename__ = "traders"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, index=True)
    is_approved = Column(Boolean, default=False)
    weekly_price_usdc = Column(Float, default=19.0)

class Follow(Base):
    __tablename__ = "follows"
    id = Column(Integer, primary_key=True)
    follower_id = Column(Integer, ForeignKey("users.id"), index=True)
    trader_id = Column(Integer, ForeignKey("traders.id"), index=True)
    started_at = Column(DateTime(timezone=True), default=now_utc)
    __table_args__ = (UniqueConstraint("follower_id", "trader_id", name="uq_follow"),)

class ManualSignal(Base):
    __tablename__ = "manual_signals"
    id = Column(Integer, primary_key=True)
    trader_id = Column(Integer, ForeignKey("traders.id"), index=True)
    category = Column(String)  # memecoin, futures, lux
    text = Column(Text)
    created_at = Column(DateTime(timezone=True), default=now_utc)

Base.metadata.create_all(bind=engine)

# ========= DB-Helper =========
def get_session():
    return SessionLocal()

def ensure_user(message: Message) -> User:
    db = get_session()
    try:
        tid = str(message.from_user.id)
        u = db.query(User).filter(User.telegram_id == tid).first()
        if not u:
            role = "admin" if is_admin(message.from_user.id) else "user"
            u = User(telegram_id=tid, username=message.from_user.username or "", role=role)
            db.add(u); db.commit()
        return u
    finally:
        db.close()

def ensure_user_by_id(tg_id: int, username: str = "") -> User:
    db = get_session()
    try:
        tid = str(tg_id)
        u = db.query(User).filter(User.telegram_id == tid).first()
        if not u:
            role = "admin" if is_admin(tg_id) else "user"
            u = User(telegram_id=tid, username=username, role=role)
            db.add(u); db.commit()
        return u
    finally:
        db.close()

def get_balance(db, user_id: int, currency: str = "USDC") -> float:
    b = db.query(Balance).filter(Balance.user_id == user_id, Balance.currency == currency).first()
    return b.amount if b else 0.0

def upsert_balance(db, user_id: int, currency: str, delta: float) -> float:
    b = db.query(Balance).filter(Balance.user_id == user_id, Balance.currency == currency).first()
    if not b:
        b = Balance(user_id=user_id, currency=currency, amount=0.0)
        db.add(b); db.flush()
    b.amount = max(0.0, b.amount + delta)
    db.commit()
    return b.amount

# ========= Preise =========
def coingecko_price(symbol: str) -> Optional[float]:
    aliases = {"SOL": "solana", "USDC": "usd-coin", "PEPE": "pepe", "BONK": "bonk", "DOGE": "dogecoin", "SHIB": "shiba-inu", "WIF": "dogwifcoin"}
    cid = aliases.get(symbol.upper(), symbol.lower())
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={cid}&vs_currencies=usd"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return float(r.json()[cid]["usd"])
    except Exception as e:
        dbg(f"coingecko error: {e}")
    return None

def get_price(symbol: str) -> Optional[float]:
    return coingecko_price(symbol)

# ========= Solana RPC =========
def sol_rpc(method: str, params: list):
    payload = {"jsonrpc":"2.0","id":1,"method":method,"params":params}
    r = requests.post(SOLANA_RPC_URL, json=payload, timeout=20)
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"])
    return j["result"]

def get_sigs_for_address(address: str, limit: int = 30):
    return sol_rpc("getSignaturesForAddress", [address, {"limit": limit}])

def get_tx(sig: str) -> dict:
    return sol_rpc("getTransaction", [sig, {"encoding":"json","maxSupportedTransactionVersion":0}])

def parse_token_transfers(tx_json: dict) -> List[dict]:
    out = []
    try:
        meta = tx_json.get("meta",{})
        pre_bal = meta.get("preBalances",[])
        post_bal = meta.get("postBalances",[])
        if pre_bal and post_bal:
            for idx,(pre,post) in enumerate(zip(pre_bal, post_bal)):
                diff = (post - pre) / 1e9
                if abs(diff) > 0:
                    out.append({"account_index":idx,"mint":"SOL","amount":diff})
        pre_token = {tb["accountIndex"]:float(tb["uiTokenAmount"]["uiAmount"] or 0.0) for tb in meta.get("preTokenBalances",[])}
        for tb in meta.get("postTokenBalances",[]):
            idx = tb["accountIndex"]
            pre = pre_token.get(idx, 0.0)
            post = float(tb["uiTokenAmount"]["uiAmount"] or 0.0)
            mint = tb["mint"]
            diff = post - pre
            if abs(diff) > 0:
                out.append({"account_index":idx,"mint":mint,"amount":diff})
    except Exception as e:
        dbg(f"parse_token_transfers error: {e}")
    return out

def get_account_keys(tx_json:dict)->List[str]:
    accs = tx_json.get("transaction",{}).get("message",{}).get("accountKeys",[])
    keys = []
    for a in accs:
        if isinstance(a, dict):
            keys.append(a.get("pubkey"))
        else:
            keys.append(a)
    return [k for k in keys if k]

# ========= Scheduler-Jobs =========
def settle_markets_job():
    db = get_session()
    try:
        now = now_utc()
        # lock
        to_lock = db.query(Market).filter(Market.status=="open", Market.lock_at <= now).all()
        for m in to_lock:
            m.status = "locked"; db.commit()
        # settle
        to_settle = db.query(Market).filter(Market.status=="locked", Market.settle_at <= now).all()
        for m in to_settle:
            p = get_price(m.symbol) or 0.0
            m.settle_price = p
            win_yes = False
            cond = m.condition.strip()
            if cond.endswith("%"):
                try:
                    pct = float(cond.strip("%").strip())
                    if m.reference_price>0:
                        change = ((p - m.reference_price)/m.reference_price)*100.0
                        win_yes = change >= pct
                except: pass
            elif cond.startswith("above:"):
                try:
                    thr = float(cond.split(":",1)[1])
                    win_yes = p >= thr
                except: pass
            yes_bets = db.query(Bet).filter(Bet.market_id==m.id, Bet.side=="YES").all()
            no_bets  = db.query(Bet).filter(Bet.market_id==m.id, Bet.side=="NO").all()
            pool_yes = sum(b.stake for b in yes_bets)
            pool_no  = sum(b.stake for b in no_bets)
            winners = yes_bets if win_yes else no_bets
            losers_pool = pool_no if win_yes else pool_yes
            winners_pool = pool_yes if win_yes else pool_no
            gross_pool = winners_pool + losers_pool
            house_fee = losers_pool * DEFAULT_FEE_PCT
            distributable = gross_pool - house_fee
            if winners:
                ssum = sum(b.stake for b in winners)
                for b in winners:
                    share = (b.stake/ssum) if ssum>0 else 0
                    payout = round(distributable*share, 6)
                    b.payout = payout; b.settled = True
                    upsert_balance(db, b.user_id, "USDC", payout)
            for b in (yes_bets + no_bets):
                if not b.settled:
                    b.payout = 0.0; b.settled = True
            m.status = "settled"; db.commit()
    finally:
        db.close()

scheduler = BackgroundScheduler(timezone="UTC")
scheduler.add_job(settle_markets_job, IntervalTrigger(seconds=20), id="settle_markets", max_instances=1, coalesce=True)
scheduler.start()

# ========= Keyboards =========
def main_menu_kbd(u: User, db) -> InlineKeyboardMarkup:
    usdc = get_balance(db, u.id, "USDC")
    k = InlineKeyboardMarkup(row_width=2)
    k.add(
        InlineKeyboardButton(f"📊 Märkte", callback_data="menu:markets"),
        InlineKeyboardButton(f"🪙 Coinflip", callback_data="menu:coinflip"),
    )
    k.add(
        InlineKeyboardButton(f"💬 Signale", callback_data="menu:signals"),
        InlineKeyboardButton(f"💰 Einzahlen/Auszahlen", callback_data="menu:wallet"),
    )
    if is_admin(int(u.telegram_id)):
        k.add(InlineKeyboardButton("🛠️ Admin", callback_data="menu:admin"))
    k.add(InlineKeyboardButton(f"ℹ️ Hilfe", callback_data="menu:help"))
    return k

def markets_kbd(m: Market, is_open: bool) -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    if is_open:
        k.add(InlineKeyboardButton("✅ YES setzen", callback_data=f"bet:{m.id}:YES"),
              InlineKeyboardButton("❌ NO setzen",  callback_data=f"bet:{m.id}:NO"))
    k.add(InlineKeyboardButton("🔙 Zurück", callback_data="menu:markets"))
    return k

def coinflip_menu_kbd() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup(row_width=2)
    k.add(
        InlineKeyboardButton("Kopf", callback_data="cf:HEADS"),
        InlineKeyboardButton("Zahl", callback_data="cf:TAILS"),
    )
    for amt in [1,5,10,25,50]:
        k.add(InlineKeyboardButton(f"{amt} USDC", callback_data=f"cfamt:{amt}"))
    k.add(InlineKeyboardButton("🔙 Zurück", callback_data="menu:main"))
    return k

def signals_menu_kbd(u: User, is_trader: bool, approved: bool) -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup(row_width=2)
    k.add(InlineKeyboardButton("📜 Öffentliche Trader", callback_data="sig:list"))
    if not is_trader:
        k.add(InlineKeyboardButton("📥 Trader werden", callback_data="sig:apply"))
    else:
        if approved:
            k.add(
                InlineKeyboardButton("➕ Signal – MemeCoin", callback_data="sig:new:memecoin"),
                InlineKeyboardButton("➕ Signal – Futures",  callback_data="sig:new:futures"),
            )
            k.add(InlineKeyboardButton("➕ Signal – Lux", callback_data="sig:new:lux"))
            k.add(InlineKeyboardButton("💵 Wochenpreis setzen", callback_data="sig:setprice"))
        else:
            k.add(InlineKeyboardButton("⏳ Wartet auf Freigabe", callback_data="sig:pending"))
    k.add(InlineKeyboardButton("🔙 Zurück", callback_data="menu:main"))
    return k

def wallet_menu_kbd() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup(row_width=2)
    k.add(InlineKeyboardButton("➕ Einzahlen", callback_data="pay:deposit"),
          InlineKeyboardButton("➖ Auszahlen", callback_data="pay:withdraw"))
    k.add(InlineKeyboardButton("🔙 Zurück", callback_data="menu:main"))
    return k

def admin_menu_kbd() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup(row_width=2)
    k.add(InlineKeyboardButton("➕ Market erstellen", callback_data="adm:newmkt"))
    k.add(InlineKeyboardButton("👤 Trader freischalten", callback_data="adm:approve"))
    k.add(InlineKeyboardButton("💳 Guthaben +/-", callback_data="adm:balance"))
    k.add(InlineKeyboardButton("📃 Märkte anzeigen", callback_data="adm:listmkt"))
    k.add(InlineKeyboardButton("🔒 Market locken", callback_data="adm:lock"))
    k.add(InlineKeyboardButton("✅ Market settlen", callback_data="adm:settle"))
    k.add(InlineKeyboardButton("🔙 Zurück", callback_data="menu:main"))
    return k

# ========= Haupt-Menüs =========
@bot.message_handler(commands=["start"])
def on_start(message: Message):
    u = ensure_user(message)
    db = get_session()
    try:
        usdc = get_balance(db, u.id, "USDC")
        text = (f"Willkommen bei <b>PulsePlay</b> 👋\n\n"
                f"👤 User: <b>@{esc(u.username) or u.telegram_id}</b>\n"
                f"💰 USDC: <b>{usdc:.2f}</b>\n\n"
                f"Wähle unten eine Funktion.")
        bot.send_message(message.chat.id, text, reply_markup=main_menu_kbd(u, db))
    finally:
        db.close()

def show_main(chat_id: int, tg_id: int):
    u = ensure_user_by_id(tg_id)
    db = get_session()
    try:
        usdc = get_balance(db, u.id, "USDC")
        text = (f"<b>Hauptmenü</b>\n\n"
                f"💰 USDC: <b>{usdc:.2f}</b>")
        bot.edit_message_text(text, chat_id, bot.get_last_message_id(chat_id) if False else None)
    finally:
        db.close()

# ========= Callbacks =========
@bot.callback_query_handler(func=lambda c: True)
def on_cb(call: CallbackQuery):
    try:
        u = ensure_user_by_id(call.from_user.id, call.from_user.username or "")
        data = call.data or ""
        db = get_session()
        try:
            # Menüs
            if data == "menu:main":
                usdc = get_balance(db, u.id, "USDC")
                bot.edit_message_text(f"<b>Hauptmenü</b>\n\n💰 USDC: <b>{usdc:.2f}</b>",
                                      call.message.chat.id, call.message.message_id,
                                      reply_markup=main_menu_kbd(u, db))
            elif data == "menu:markets":
                _show_markets(db, call, u)
            elif data == "menu:coinflip":
                bot.edit_message_text("🪙 <b>Coinflip</b>\nWähle Kopf/Zahl und Betrag.",
                                      call.message.chat.id, call.message.message_id,
                                      reply_markup=coinflip_menu_kbd())
            elif data == "menu:signals":
                _show_signals_menu(db, call, u)
            elif data == "menu:wallet":
                _show_wallet_menu(db, call, u)
            elif data == "menu:help":
                bot.edit_message_text(esc("Hilfe:\n- Märkte: YES/NO Wetten (+% oder above:Preis)\n- Coinflip: 50/50-Style (intern 30% Gewinnchance), Auszahlung 2x\n- Signale: Trader posten Signale (MemeCoin, Futures, Lux). Follower können Wochenabo bezahlen.\n- Ein-/Auszahlungen: USDC/SOL über zentrale Solana-Adresse. /verify nutzt RPC."),
                                      call.message.chat.id, call.message.message_id,
                                      reply_markup=main_menu_kbd(u, db))

            # Märkte
            elif data.startswith("bet:"):
                _, mid, side = data.split(":")
                _place_bet(db, u, call, int(mid), side)
            elif data.startswith("mkt:open:"):
                _, _, mid = data.split(":")
                _show_single_market(db, call, int(mid))

            # Coinflip
            elif data.startswith("cf:"):
                # Kopf/Zahl Vorauswahl
                _, side = data.split(":")
                state = _get_cf_state(u.telegram_id)
                state["side"] = side
                _set_cf_state(u.telegram_id, state)
                bot.answer_callback_query(call.id, f"Gewählt: {side}")
            elif data.startswith("cfamt:"):
                _, amt = data.split(":")
                state = _get_cf_state(u.telegram_id)
                state["amount"] = float(amt)
                _set_cf_state(u.telegram_id, state)
                _coinflip_play(db, u.id, call, state.get("side"), state.get("amount"))

            # Signale
            elif data == "sig:list":
                _signals_list_public(db, call, u)
            elif data == "sig:apply":
                _trader_apply(db, call, u)
            elif data == "sig:pending":
                bot.answer_callback_query(call.id, "Dein Trader-Profil wartet auf Freischaltung.")
            elif data.startswith("sig:new:"):
                _, _, cat = data.split(":")
                _trader_new_signal_prompt(db, call, u, cat)
            elif data == "sig:setprice":
                _trader_set_price_prompt(db, call, u)
            elif data.startswith("sig:follow:"):
                _, _, tid = data.split(":")
                _follow_trader(db, call, u, int(tid))
            elif data.startswith("sig:buyweek:"):
                _, _, tid, price = data.split(":")
                _buy_week(db, call, u, int(tid), float(price))

            # Wallet
            elif data == "pay:deposit":
                _deposit_prompt(db, call, u)
            elif data == "pay:withdraw":
                _withdraw_prompt(db, call, u)

            # Admin
            elif data == "menu:admin":
                if not is_admin(int(u.telegram_id)):
                    bot.answer_callback_query(call.id, "Kein Admin.")
                else:
                    bot.edit_message_text("<b>Admin</b>", call.message.chat.id, call.message.message_id,
                                          reply_markup=admin_menu_kbd())
            elif data == "adm:newmkt":
                _admin_new_market_prompt(db, call, u)
            elif data == "adm:approve":
                _admin_approve_trader_prompt(db, call, u)
            elif data == "adm:balance":
                _admin_balance_prompt(db, call, u)
            elif data == "adm:listmkt":
                _admin_list_markets(db, call, u)
            elif data == "adm:lock":
                _admin_lock_prompt(db, call, u)
            elif data == "adm:settle":
                _admin_settle_prompt(db, call, u)

            else:
                bot.answer_callback_query(call.id, "Unbekannte Aktion.")
        finally:
            db.close()
    except Exception as e:
        dbg(f"callback error: {e}")
        try:
            bot.answer_callback_query(call.id, f"Error: {e}")
        except: pass

# ========= Coinflip – einfacher State (im Speicher) =========
_CF_STATE = {}
def _get_cf_state(tg_id: int) -> dict:
    return _CF_STATE.get(tg_id, {"side": None, "amount": None})
def _set_cf_state(tg_id: int, state: dict):
    _CF_STATE[tg_id] = state

def _coinflip_play(db, user_id: int, call: CallbackQuery, side: Optional[str], amount: Optional[float]):
    if side not in ("HEADS","TAILS"):
        bot.answer_callback_query(call.id, "Bitte erst Kopf/Zahl wählen."); return
    if not amount or amount <= 0:
        bot.answer_callback_query(call.id, "Bitte Betrag wählen."); return
    bal = get_balance(db, user_id, "USDC")
    if bal < amount:
        bot.answer_callback_query(call.id, f"Zu wenig Guthaben ({bal:.2f} USDC)."); return
    # abziehen
    upsert_balance(db, user_id, "USDC", -amount)
    win = (random.random() < COINFLIP_WIN_PROB)
    if win:
        payout = amount * COINFLIP_PAYOUT_MULT
        upsert_balance(db, user_id, "USDC", payout)
        bot.edit_message_text(f"🪙 <b>Coinflip</b>\nDu hast <b>{side}</b> gewählt.\n"
                              f"🎉 <b>Gewonnen!</b> Auszahlung: <b>{payout:.2f} USDC</b>",
                              call.message.chat.id, call.message.message_id,
                              reply_markup=coinflip_menu_kbd())
    else:
        bot.edit_message_text(f"🪙 <b>Coinflip</b>\nDu hast <b>{side}</b> gewählt.\n"
                              f"💥 <b>Verloren.</b> Einsatz: {amount:.2f} USDC",
                              call.message.chat.id, call.message.message_id,
                              reply_markup=coinflip_menu_kbd())

# ========= Märkte =========
def _show_markets(db, call, u: User):
    ms = db.query(Market).filter(Market.status.in_(["open","locked"])).order_by(Market.lock_at.asc()).limit(10).all()
    if not ms:
        bot.edit_message_text("📊 Keine offenen Märkte.", call.message.chat.id, call.message.message_id,
                              reply_markup=main_menu_kbd(u, db))
        return
    lines = ["<b>Offene/Lockte Märkte</b>"]
    k = InlineKeyboardMarkup()
    for m in ms:
        left_lock = max(0, int((m.lock_at - now_utc()).total_seconds())) if m.lock_at else 0
        left_settle = max(0, int((m.settle_at - now_utc()).total_seconds())) if m.settle_at else 0
        lines.append(f"#{m.id} {m.symbol} {m.condition} | {m.status} | "
                     f"Lock in: {left_lock//60}m | Settle in: {left_settle//60}m")
        k.add(InlineKeyboardButton(f"Market #{m.id} öffnen", callback_data=f"mkt:open:{m.id}"))
    k.add(InlineKeyboardButton("🔙 Zurück", callback_data="menu:main"))
    bot.edit_message_text("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=k)

def _show_single_market(db, call, mid: int):
    m = db.query(Market).filter(Market.id==mid).first()
    if not m:
        bot.answer_callback_query(call.id, "Market nicht gefunden."); return
    is_open = (m.status=="open" and m.lock_at>now_utc())
    p = get_price(m.symbol) or 0.0
    left_lock = max(0, int((m.lock_at - now_utc()).total_seconds()))
    text = (f"<b>Market #{m.id}</b>\n"
            f"Symbol: <b>{esc(m.symbol)}</b>\n"
            f"Bedingung: <b>{esc(m.condition)}</b>\n"
            f"Status: <b>{m.status}</b>\n"
            f"Referenz: <b>{m.reference_price:.6f} USD</b>\n"
            f"Aktuell: <b>{p:.6f} USD</b>\n"
            f"Lock in: <b>{left_lock//60}m</b>\n"
            f"Settle: <b>{m.settle_at.strftime('%Y-%m-%d %H:%M UTC')}</b>")
    bot.edit_message_text(text, call.message.chat.id, call.message.message_id,
                          reply_markup=markets_kbd(m, is_open))

def _place_bet(db, u: User, call, mid: int, side: str):
    m = db.query(Market).filter(Market.id==mid).first()
    if not m:
        bot.answer_callback_query(call.id, "Market nicht gefunden."); return
    if m.status!="open" or m.lock_at<=now_utc():
        bot.answer_callback_query(call.id, "Market ist gelockt."); return
    # Einsatz per Schnellbuttons anbieten
    k = InlineKeyboardMarkup()
    for amt in [1,5,10,25,50]:
        k.add(InlineKeyboardButton(f"{side} – {amt} USDC setzen", callback_data=f"betplace:{mid}:{side}:{amt}"))
    k.add(InlineKeyboardButton("Abbrechen", callback_data=f"mkt:open:{mid}"))
    bot.edit_message_text(f"Einsatz wählen für #{mid} {m.symbol} ({side})", call.message.chat.id, call.message.message_id, reply_markup=k)

@bot.callback_query_handler(func=lambda c: c.data.startswith("betplace:"))
def on_bet_place(call: CallbackQuery):
    try:
        u = ensure_user_by_id(call.from_user.id, call.from_user.username or "")
        _, mid, side, amt = call.data.split(":")
        mid = int(mid); amt = float(amt)
        db = get_session()
        try:
            m = db.query(Market).filter(Market.id==mid).first()
            if not m or m.status!="open" or m.lock_at<=now_utc():
                bot.answer_callback_query(call.id, "Market nicht offen."); return
            bal = get_balance(db, u.id, "USDC")
            if bal < amt:
                bot.answer_callback_query(call.id, f"Zu wenig Guthaben ({bal:.2f})."); return
            upsert_balance(db, u.id, "USDC", -amt)
            b = Bet(market_id=mid, user_id=u.id, side=side, stake=amt, fee=0.0, payout=0.0)
            db.add(b); db.commit()
            bot.edit_message_text(f"✅ Einsatz platziert: {side} {amt:.2f} USDC auf Market #{mid}",
                                  call.message.chat.id, call.message.message_id,
                                  reply_markup=markets_kbd(m, True))
        finally:
            db.close()
    except Exception as e:
        dbg(f"betplace error: {e}")
        try: bot.answer_callback_query(call.id, f"Error: {e}")
        except: pass

# ========= Signale =========
def _get_trader(db, user_id: int) -> Optional[Trader]:
    return db.query(Trader).filter(Trader.user_id==user_id).first()

def _show_signals_menu(db, call, u: User):
    t = _get_trader(db, u.id)
    is_trader = t is not None
    approved = t.is_approved if t else False
    bot.edit_message_text("<b>Signale</b>", call.message.chat.id, call.message.message_id,
                          reply_markup=signals_menu_kbd(u, is_trader, approved))

def _signals_list_public(db, call, u: User):
    ts = db.query(Trader).filter(Trader.is_approved==True).all()
    if not ts:
        bot.edit_message_text("Es gibt noch keine freigeschalteten Trader.",
                              call.message.chat.id, call.message.message_id,
                              reply_markup=signals_menu_kbd(u, _get_trader(db,u.id) is not None, (_get_trader(db,u.id) or Trader(is_approved=False)).is_approved if _get_trader(db,u.id) else False))
        return
    lines = ["<b>Trader</b>"]
    k = InlineKeyboardMarkup()
    for t in ts:
        owner = db.query(User).filter(User.id==t.user_id).first()
        uname = owner.username or owner.telegram_id
        price = t.weekly_price_usdc
        lines.append(f"• @{esc(uname)} – Woche: {price:.2f} USDC")
        k.add(InlineKeyboardButton(f"Folgen/Abonnieren @{uname}", callback_data=f"sig:follow:{t.id}"))
        k.add(InlineKeyboardButton(f"Woche kaufen ({price:.2f} USDC)", callback_data=f"sig:buyweek:{t.id}:{price}"))
    k.add(InlineKeyboardButton("🔙 Zurück", callback_data="menu:signals"))
    bot.edit_message_text("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=k)

def _follow_trader(db, call, u: User, tid: int):
    t = db.query(Trader).filter(Trader.id==tid, Trader.is_approved==True).first()
    if not t:
        bot.answer_callback_query(call.id, "Trader nicht gefunden."); return
    # follow ohne Zahlung (reine Follow-Liste)
    try:
        f = Follow(follower_id=u.id, trader_id=tid)
        db.add(f); db.commit()
    except Exception:  # unique
        pass
    bot.answer_callback_query(call.id, "Du folgst diesem Trader jetzt.")

def _buy_week(db, call, u: User, tid: int, price: float):
    bal = get_balance(db, u.id, "USDC")
    if bal < price:
        bot.answer_callback_query(call.id, f"Zu wenig Guthaben ({bal:.2f})."); return
    upsert_balance(db, u.id, "USDC", -price)
    bot.answer_callback_query(call.id, f"Abo-Woche gekauft ({price:.2f} USDC).")
    # (Optional: Zugriff/Whitelist markieren)

def _trader_apply(db, call, u: User):
    t = _get_trader(db, u.id)
    if t:
        bot.answer_callback_query(call.id, "Du bist bereits registriert (wartend oder frei)."); return
    t = Trader(user_id=u.id, is_approved=False, weekly_price_usdc=19.0)
    db.add(t); db.commit()
    bot.answer_callback_query(call.id, "Antrag gesendet. Admin wird dich freischalten.")
    # Admin informieren
    for aid in ADMIN_IDS:
        try:
            bot.send_message(aid, f"🔔 Trader-Antrag von @{u.username or u.telegram_id}")
        except: pass

def _trader_new_signal_prompt(db, call, u: User, category: str):
    t = _get_trader(db, u.id)
    if not t or not t.is_approved:
        bot.answer_callback_query(call.id, "Nur freigeschaltete Trader."); return
    bot.edit_message_text(f"Schicke den Signal-Text hier im Chat.\nKategorie: <b>{category}</b>\n(Die nächste Nachricht wird als Signal gespeichert und an Follower gesendet.)",
                          call.message.chat.id, call.message.message_id)
    _PENDING_SIGNAL[u.telegram_id] = category

_PENDING_SIGNAL = {}  # tg_id -> category

@bot.message_handler(func=lambda m: m.chat.type in ("private","group","supergroup"))
def on_text(message: Message):
    # Signal-Eingabe?
    tg_id = message.from_user.id
    if tg_id in _PENDING_SIGNAL:
        category = _PENDING_SIGNAL.pop(tg_id)
        u = ensure_user(message)
        db = get_session()
        try:
            t = _get_trader(db, u.id)
            if not t or not t.is_approved:
                bot.reply_to(message, "Nur freigeschaltete Trader."); return
            s = ManualSignal(trader_id=t.id, category=category, text=message.text)
            db.add(s); db.commit()
            # an Follower senden
            fols = db.query(Follow).filter(Follow.trader_id==t.id).all()
            owner = db.query(User).filter(User.id==t.user_id).first()
            uname = owner.username or owner.telegram_id
            out = (f"📢 <b>Signal</b> – <i>{category}</i>\n"
                   f"von @{esc(uname)}\n\n{esc(message.text)}")
            for f in fols:
                try:
                    fu = db.query(User).filter(User.id==f.follower_id).first()
                    bot.send_message(int(fu.telegram_id), out)
                except Exception as e:
                    dbg(f"send follower signal error: {e}")
            bot.reply_to(message, "Signal gesendet ✅")
        finally:
            db.close()
        return

# ========= Wallet – Ein-/Auszahlungen =========
def _show_wallet_menu(db, call, u: User):
    usdc = get_balance(db, u.id, "USDC")
    txt = (f"<b>Wallet</b>\n"
           f"USDC: <b>{usdc:.2f}</b>\n\n"
           f"Einzahlung geht an:\n<code>{CENTRAL_DEPOSIT_ADDRESS}</code>\n"
           f"Nach Senden -> Verifizieren.")
    bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=wallet_menu_kbd())

def _deposit_prompt(db, call, u: User):
    txt = ("🔹 <b>Einzahlen</b>\n"
           "Sende SOL oder USDC an die zentrale Adresse und antworte hier:\n"
           "<code>/deposit &lt;amount&gt; &lt;dein_sender_wallet&gt;</code>\n"
           "Beispiel:\n<code>/deposit 25 CKZEp...M1s</code>\n"
           "Danach: <code>/verify &lt;payment_id&gt;</code>")
    bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=wallet_menu_kbd())

def _withdraw_prompt(db, call, u: User):
    txt = ("🔹 <b>Auszahlen</b>\n"
           "Antworte hier:\n<code>/withdraw &lt;amount&gt; &lt;ziel_wallet&gt;</code>")
    bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=wallet_menu_kbd())

@bot.message_handler(commands=["deposit"])
def cmd_deposit(message: Message):
    u = ensure_user(message)
    parts = message.text.split()
    if len(parts)!=3:
        bot.reply_to(message, esc("Usage: /deposit <amount> <your_sender_wallet>")); return
    try:
        amt = float(parts[1]); assert amt>0
    except:
        bot.reply_to(message, "Amount muss >0 sein."); return
    sender = parts[2]
    db = get_session()
    try:
        p = Payment(user_id=u.id, type="deposit", method="solana", amount=amt, status="pending", meta=json.dumps({"sender":sender}))
        db.add(p); db.commit()
        bot.reply_to(message, f"Einzahlungs-Request #{p.id} erstellt.\nSende an: <code>{CENTRAL_DEPOSIT_ADDRESS}</code>\nNach dem Senden: /verify {p.id}")
    finally:
        db.close()

@bot.message_handler(commands=["verify"])
def cmd_verify(message: Message):
    u = ensure_user(message)
    parts = message.text.split()
    if len(parts)!=2 or not parts[1].isdigit():
        bot.reply_to(message, esc("Usage: /verify <payment_id>")); return
    pid = int(parts[1])
    db = get_session()
    try:
        p = db.query(Payment).filter(Payment.id==pid, Payment.user_id==u.id, Payment.type=="deposit", Payment.status!="completed").first()
        if not p:
            bot.reply_to(message, "Einzahlung nicht gefunden/abgeschlossen."); return
        meta = json.loads(p.meta or "{}")
        sender = meta.get("sender")
        if not sender:
            bot.reply_to(message, "Kein Sender-Wallet hinterlegt."); return
        sigs = get_sigs_for_address(CENTRAL_DEPOSIT_ADDRESS, limit=40)
        matched = False
        for s in sigs:
            sig = s["signature"]
            tx = get_tx(sig)
            keys = get_account_keys(tx)
            if sender not in keys:
                continue
            transfers = parse_token_transfers(tx)
            inflow_sol = 0.0; inflow_usdc = 0.0
            for t in transfers:
                if t["mint"]=="SOL":
                    inflow_sol += max(0.0, t["amount"])
                elif t["mint"]==USDC_SOLANA_MINT:
                    inflow_usdc += max(0.0, t["amount"])
            if inflow_sol>=p.amount or inflow_usdc>=p.amount:
                currency = "USDC" if inflow_usdc>0 else "SOL"
                credit = inflow_usdc if inflow_usdc>0 else (inflow_sol * (get_price("SOL") or 0.0))
                upsert_balance(db, u.id, "USDC", credit)
                p.status="completed"; p.tx=sig; db.commit()
                bot.reply_to(message, f"✅ Deposit verifiziert. Gutschrift: {credit:.4f} USDC (via {currency}).")
                matched = True; break
        if not matched:
            bot.reply_to(message, "Keine passende Transaktion gefunden. Später erneut versuchen.")
    except Exception as e:
        bot.reply_to(message, f"Error: {e}")
    finally:
        db.close()

@bot.message_handler(commands=["withdraw"])
def cmd_withdraw(message: Message):
    u = ensure_user(message)
    parts = message.text.split()
    if len(parts)!=3:
        bot.reply_to(message, esc("Usage: /withdraw <amount> <dest_wallet>")); return
    try:
        amt = float(parts[1]); assert amt>0
    except:
        bot.reply_to(message, "Amount muss >0 sein."); return
    dest = parts[2]
    db = get_session()
    try:
        bal = get_balance(db, u.id, "USDC")
        if bal < amt:
            bot.reply_to(message, f"Zu wenig Guthaben ({bal:.2f})."); return
        upsert_balance(db, u.id, "USDC", -amt)
        p = Payment(user_id=u.id, type="withdraw", method="solana", amount=amt, status="pending", meta=json.dumps({"dest":dest}))
        db.add(p); db.commit()
        bot.reply_to(message, f"✅ Auszahlungs-Request #{p.id} erstellt ({amt:.2f} USDC → {dest}). Admin verarbeitet.")
        for aid in ADMIN_IDS:
            try: bot.send_message(aid, f"⚠️ Withdraw #{p.id} von @{u.username or u.telegram_id}: {amt:.2f} → {dest}")
            except: pass
    finally:
        db.close()

# ========= Admin =========
def _admin_new_market_prompt(db, call, u: User):
    txt = ("➕ <b>Market erstellen</b>\n"
           "Antworte mit:\n<code>/newmarket SYMBOL CONDITION TIMEFRAME</code>\n"
           "Beispiele:\n<code>/newmarket SOL +2% 1h</code>\n<code>/newmarket SOL above:150 4h</code>")
    bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kbd())

@bot.message_handler(commands=["newmarket"])
def cmd_newmarket(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts)!=4:
        bot.reply_to(message, esc("Usage: /newmarket <SYMBOL> <+PCT|above:PRICE> <1h|4h|24h>")); return
    symbol, cond, tf = parts[1].upper(), parts[2], parts[3]
    if tf not in ("1h","4h","24h"):
        bot.reply_to(message, "Timeframe: 1h|4h|24h"); return
    ref = get_price(symbol)
    if ref is None:
        bot.reply_to(message, "Preis konnte nicht geladen werden."); return
    start = now_utc()
    hours = {"1h":1,"4h":4,"24h":24}[tf]
    lock = start + timedelta(minutes=5)
    settle = start + timedelta(hours=hours)
    db = get_session()
    try:
        m = Market(symbol=symbol, timeframe=tf, condition=cond, start_at=start, lock_at=lock, settle_at=settle, status="open", reference_price=ref)
        db.add(m); db.commit()
        bot.reply_to(message, f"✅ Market #{m.id} erstellt: {symbol} {cond}\nRef: {ref:.6f} USD\nLock: {lock.strftime('%Y-%m-%d %H:%M UTC')}\nSettle: {settle.strftime('%Y-%m-%d %H:%M UTC')}")
    finally:
        db.close()

def _admin_approve_trader_prompt(db, call, u: User):
    ts = db.query(Trader).filter(Trader.is_approved==False).all()
    if not ts:
        bot.edit_message_text("Keine ausstehenden Trader-Anträge.", call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kbd()); return
    k = InlineKeyboardMarkup()
    for t in ts:
        owner = db.query(User).filter(User.id==t.user_id).first()
        k.add(InlineKeyboardButton(f"Freischalten @{owner.username or owner.telegram_id}", callback_data=f"adm:approve:{t.id}"))
    k.add(InlineKeyboardButton("🔙 Zurück", callback_data="menu:admin"))
    bot.edit_message_text("Trader-Anträge:", call.message.chat.id, call.message.message_id, reply_markup=k)

@bot.callback_query_handler(func=lambda c: c.data.startswith("adm:approve:"))
def on_adm_approve(call: CallbackQuery):
    if not is_admin(call.from_user.id): 
        bot.answer_callback_query(call.id, "Kein Admin."); return
    _, _, tid = call.data.split(":")
    tid = int(tid)
    db = get_session()
    try:
        t = db.query(Trader).filter(Trader.id==tid).first()
        if not t: bot.answer_callback_query(call.id, "Nicht gefunden."); return
        t.is_approved = True; db.commit()
        bot.answer_callback_query(call.id, "Freigeschaltet.")
        owner = db.query(User).filter(User.id==t.user_id).first()
        try: bot.send_message(int(owner.telegram_id), "✅ Du wurdest als Trader freigeschaltet.")
        except: pass
    finally:
        db.close()

def _trader_set_price_prompt(db, call, u: User):
    t = _get_trader(db, u.id)
    if not t or not t.is_approved:
        bot.answer_callback_query(call.id, "Nur freigeschaltete Trader."); return
    bot.edit_message_text("Sende neuen Wochenpreis als Zahl, z.B. <code>/setprice 25</code>",
                          call.message.chat.id, call.message.message_id)

@bot.message_handler(commands=["setprice"])
def cmd_setprice(message: Message):
    u = ensure_user(message)
    db = get_session()
    try:
        t = _get_trader(db, u.id)
        if not t or not t.is_approved:
            bot.reply_to(message, "Nur freigeschaltete Trader."); return
        parts = message.text.split()
        if len(parts)!=2:
            bot.reply_to(message, "Usage: /setprice <betrag_usdc>"); return
        try:
            val = float(parts[1]); assert val>0
        except:
            bot.reply_to(message, "Zahl >0 angeben."); return
        t.weekly_price_usdc = val; db.commit()
        bot.reply_to(message, f"Preis gesetzt: {val:.2f} USDC/Woche ✅")
    finally:
        db.close()

def _admin_balance_prompt(db, call, u: User):
    bot.edit_message_text("Antworte:\n<code>/credit TELEGRAM_ID AMOUNT</code>\n<code>/debit TELEGRAM_ID AMOUNT</code>",
                          call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kbd())

@bot.message_handler(commands=["credit"])
def cmd_credit(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts)!=3: bot.reply_to(message, "Usage: /credit TELEGRAM_ID AMOUNT"); return
    _, tid, amt = parts
    try: amt = float(amt); assert amt>=0
    except: bot.reply_to(message, "Betrag ungültig."); return
    db = get_session()
    try:
        u = db.query(User).filter(User.telegram_id==tid).first()
        if not u: bot.reply_to(message, "User nicht gefunden"); return
        upsert_balance(db, u.id, "USDC", amt)
        bot.reply_to(message, f"Gutschrift: {amt:.2f} USDC an {tid}")
    finally:
        db.close()

@bot.message_handler(commands=["debit"])
def cmd_debit(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts)!=3: bot.reply_to(message, "Usage: /debit TELEGRAM_ID AMOUNT"); return
    _, tid, amt = parts
    try: amt = float(amt); assert amt>=0
    except: bot.reply_to(message, "Betrag ungültig."); return
    db = get_session()
    try:
        u = db.query(User).filter(User.telegram_id==tid).first()
        if not u: bot.reply_to(message, "User nicht gefunden"); return
        upsert_balance(db, u.id, "USDC", -amt)
        bot.reply_to(message, f"Belastung: {amt:.2f} USDC von {tid}")
    finally:
        db.close()

def _admin_list_markets(db, call, u: User):
    ms = db.query(Market).order_by(Market.id.desc()).limit(20).all()
    if not ms:
        bot.edit_message_text("Keine Märkte.", call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kbd()); return
    lines = ["<b>Märkte</b>"]
    for m in ms:
        lines.append(f"#{m.id} {m.symbol} {m.condition} | {m.status} | Ref:{m.reference_price:.6f} Settle:{m.settle_price:.6f}")
    bot.edit_message_text("\n".join(lines), call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kbd())

def _admin_lock_prompt(db, call, u: User):
    bot.edit_message_text("Lock per Command:\n<code>/lock MARKET_ID</code>", call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kbd())

@bot.message_handler(commands=["lock"])
def cmd_lock(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts)!=2 or not parts[1].isdigit(): bot.reply_to(message, "Usage: /lock MARKET_ID"); return
    mid = int(parts[1])
    db = get_session()
    try:
        m = db.query(Market).filter(Market.id==mid).first()
        if not m: bot.reply_to(message, "Market nicht gefunden"); return
        m.status="locked"; m.lock_at=now_utc(); db.commit()
        bot.reply_to(message, "Locked.")
    finally:
        db.close()

def _admin_settle_prompt(db, call, u: User):
    bot.edit_message_text("Settle per Command:\n<code>/settle MARKET_ID</code>", call.message.chat.id, call.message.message_id, reply_markup=admin_menu_kbd())

@bot.message_handler(commands=["settle"])
def cmd_settle(message: Message):
    if not is_admin(message.from_user.id): return
    parts = message.text.split()
    if len(parts)!=2 or not parts[1].isdigit(): bot.reply_to(message, "Usage: /settle MARKET_ID"); return
    mid = int(parts[1])
    db = get_session()
    try:
        m = db.query(Market).filter(Market.id==mid).first()
        if not m: bot.reply_to(message, "Market nicht gefunden"); return
        m.settle_at = now_utc() - timedelta(seconds=1); db.commit()
        # sofort Job ausführen
        settle_markets_job()
        bot.reply_to(message, "Settled.")
    finally:
        db.close()

# ========= Run =========
if __name__ == "__main__":
    print("PulsePlay bot starting...")
    try:
        bot.infinity_polling(timeout=60, long_polling_timeout=60)
    finally:
        try: scheduler.shutdown(wait=False)
        except: pass
