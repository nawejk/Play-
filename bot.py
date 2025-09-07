#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PulsePlay – FULL single-file Telegram bot (buttons only)
-------------------------------------------------------
Features:
  • Coinflip (hidden 30% win prob, x2 payout) via buttons
  • Trader system: apply -> admin approve -> followers can subscribe (7-day access), 80/20 split
  • Manual signals by approved traders (MemeCoin / Futures / Lux) to active followers
  • Prediction markets (admin wizard with buttons, users bet YES/NO via buttons; auto-lock & settle)
  • Wallet follow notifications (manual signals only; blockchain copy-trade can be added later)
  • Balance with deposits (Solana USDC/SOL via RPC verification) & withdraw requests

Tech:
  Python 3.10+
  Libraries: pyTelegramBotAPI, SQLAlchemy (2.0.36+), requests, APScheduler

ENV (create .env or set env vars before start):
  ENV_BOT_TOKEN="8200746289:AAGbzwf7sUHVHlLDb3foXbZpj9SVGnqLeNU"
  ENV_ADMIN_IDS="7919108078"
  ENV_SOLANA_RPC_URL="https://api.mainnet-beta.solana.com"
  ENV_CENTRAL_DEPOSIT_ADDRESS="CKZEpwiVqAHLiSbdc8Ebf8xaQ2fofgPCNmzi4cV32M1s"
  ENV_DEFAULT_FEE_PCT="0.04"
  ENV_PRICE_SOURCE="coingecko"
  ENV_DEBUG="1"

Start:
  python3 bot.py
"""

import os, json, threading, random, time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List

import requests
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from sqlalchemy import (create_engine, Column, Integer, String, DateTime, Boolean,
                        ForeignKey, Float, Text, UniqueConstraint, Index)
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session, relationship

# ========= CONFIG =========
def getenv_int_list(varname: str, default_csv: str) -> List[int]:
    raw = os.getenv(varname, default_csv)
    out = []
    for x in raw.split(","):
        x = x.strip()
        if not x: continue
        try:
            out.append(int(x))
        except: pass
    return out

BOT_TOKEN = os.getenv("ENV_BOT_TOKEN", "8200746289:AAGbzwf7sUHVHlLDb3foXbZpj9SVGnqLeNU")
ADMIN_IDS = getenv_int_list("ENV_ADMIN_IDS", "7919108078")
CENTRAL_DEPOSIT_ADDRESS = os.getenv("ENV_CENTRAL_DEPOSIT_ADDRESS", "CKZEpwiVqAHLiSbdc8Ebf8xaQ2fofgPCNmzi4cV32M1s")
SOLANA_RPC_URL = os.getenv("ENV_SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
USDC_SOLANA_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

DEFAULT_FEE_PCT = float(os.getenv("ENV_DEFAULT_FEE_PCT", "0.04"))  # 4%
COINFLIP_WIN_PROB = float(os.getenv("ENV_COINFLIP_PROB", "0.30"))  # 30% win probability (hidden)
COINFLIP_PAYOUT_MULT = float(os.getenv("ENV_COINFLIP_MULT", "2.0"))  # x2 payout on win
DEBUG = os.getenv("ENV_DEBUG", "1") == "1"
PRICE_SOURCE = os.getenv("ENV_PRICE_SOURCE", "coingecko")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

Base = declarative_base()
engine = create_engine("sqlite:///pulseplay_full.db", connect_args={"check_same_thread": False})
SessionLocal = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))

def now_utc():
    return datetime.now(timezone.utc)

def esc(s: str) -> str:
    return s.replace("<", "&lt;").replace(">", "&gt;")

def dbg(msg: str):
    if DEBUG:
        print(f"[{datetime.utcnow().isoformat()}] {msg}")

# ========= MODELS =========
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True, index=True)
    username = Column(String, default="")
    role = Column(String, default="user")  # user/trader/admin
    created_at = Column(DateTime, default=lambda: datetime.utcnow())

class Balance(Base):
    __tablename__ = "balances"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    currency = Column(String, default="USDC")
    amount = Column(Float, default=0.0)

class Trader(Base):
    __tablename__ = "traders"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True)
    approved = Column(Boolean, default=False)
    profile = Column(Text, default="")  # short bio
    weekly_price = Column(Float, default=20.0)  # USDC/week

class Follow(Base):
    __tablename__ = "follows"
    id = Column(Integer, primary_key=True)
    follower_user_id = Column(Integer, ForeignKey("users.id"), index=True)
    trader_id = Column(Integer, ForeignKey("traders.id"), index=True)
    started_at = Column(DateTime, default=lambda: datetime.utcnow())
    expires_at = Column(DateTime)  # 7 days from start
    __table_args__ = (UniqueConstraint("follower_user_id", "trader_id", name="uq_follow_once"),)

class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    type = Column(String)  # deposit/withdraw/subscription
    method = Column(String, default="solana")
    amount = Column(Float, default=0.0)
    tx = Column(String, default="")
    status = Column(String, default="pending")
    created_at = Column(DateTime, default=lambda: datetime.utcnow())
    meta = Column(Text, default="{}")

class Market(Base):
    __tablename__ = "markets"
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    condition = Column(String)  # "+2%" or "above:150.0"
    timeframe = Column(String)  # "1h","4h","24h"
    status = Column(String, default="open")  # open/locked/settled/canceled
    start_at = Column(DateTime)
    lock_at = Column(DateTime)
    settle_at = Column(DateTime)
    reference_price = Column(Float, default=0.0)
    settle_price = Column(Float, default=0.0)

class Bet(Base):
    __tablename__ = "bets"
    id = Column(Integer, primary_key=True)
    market_id = Column(Integer, ForeignKey("markets.id"))
    user_id = Column(Integer, ForeignKey("users.id"))
    side = Column(String)  # YES/NO
    stake = Column(Float, default=0.0)
    fee = Column(Float, default=0.0)
    payout = Column(Float, default=0.0)
    settled = Column(Boolean, default=False)
    placed_at = Column(DateTime, default=lambda: datetime.utcnow())

Base.metadata.create_all(bind=engine)

# ========= HELPERS =========
def db_sess():
    return SessionLocal()

def get_or_create_user(message: Message) -> User:
    db = db_sess()
    try:
        tid = str(message.from_user.id)
        u = db.query(User).filter(User.telegram_id == tid).first()
        if not u:
            role = "admin" if message.from_user.id in ADMIN_IDS else "user"
            u = User(telegram_id=tid, username=message.from_user.username or "", role=role)
            db.add(u); db.commit()
            if role == "admin":
                # also ensure Trader row (not approved by default)
                tr = db.query(Trader).filter(Trader.user_id==u.id).first()
                if not tr:
                    tr = Trader(user_id=u.id, approved=True, profile="Admin Trader", weekly_price=20.0)
                    db.add(tr); db.commit()
        return u
    finally:
        db.close()

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def get_balance(db, user_id: int, currency="USDC") -> float:
    b = db.query(Balance).filter(Balance.user_id==user_id, Balance.currency==currency).first()
    return b.amount if b else 0.0

def upsert_balance(db, user_id: int, currency: str, delta: float) -> float:
    b = db.query(Balance).filter(Balance.user_id==user_id, Balance.currency==currency).first()
    if not b:
        b = Balance(user_id=user_id, currency=currency, amount=0.0)
        db.add(b); db.flush()
    b.amount = max(0.0, b.amount + delta)
    db.commit()
    return b.amount

# ========= PRICES =========
def coingecko_price(symbol: str) -> Optional[float]:
    aliases = {"SOL":"solana","USDC":"usd-coin","PEPE":"pepe","BONK":"bonk","BTC":"bitcoin","ETH":"ethereum","LUX":"lux-btc"}
    cid = aliases.get(symbol.upper(), symbol.lower())
    try:
        r = requests.get(f"https://api.coingecko.com/api/v3/simple/price?ids={cid}&vs_currencies=usd", timeout=12)
        if r.status_code == 200:
            return float(r.json()[cid]["usd"])
    except Exception as e:
        dbg(f"coingecko error: {e}")
    return None

def get_price(symbol: str) -> Optional[float]:
    return coingecko_price(symbol)

# ========= SOLANA RPC =========
def sol_rpc(method: str, params: list):
    payload = {"jsonrpc":"2.0","id":1,"method":method,"params":params}
    r = requests.post(SOLANA_RPC_URL, json=payload, timeout=20)
    r.raise_for_status()
    j = r.json()
    if "error" in j:
        raise RuntimeError(j["error"])
    return j["result"]

def get_sigs_for_address(address: str, limit=40):
    return sol_rpc("getSignaturesForAddress", [address, {"limit": limit}])

def get_tx(sig: str):
    return sol_rpc("getTransaction", [sig, {"encoding":"json","maxSupportedTransactionVersion":0}])

def parse_token_transfers(tx_json: dict) -> List[dict]:
    out = []
    try:
        meta = tx_json.get("meta",{})
        pre_bal = meta.get("preBalances",[])
        post_bal = meta.get("postBalances",[])
        for pre, post in zip(pre_bal, post_bal):
            diff = (post-pre)/1e9
            if abs(diff)>0: out.append({"mint":"SOL","amount":diff})
        pre_token = {tb["accountIndex"]:float(tb["uiTokenAmount"]["uiAmount"] or 0.0) for tb in meta.get("preTokenBalances",[])}
        for tb in meta.get("postTokenBalances",[]):
            idx = tb["accountIndex"]
            pre = pre_token.get(idx,0.0); post = float(tb["uiTokenAmount"]["uiAmount"] or 0.0)
            out.append({"mint":tb["mint"],"amount":(post-pre)})
    except Exception as e:
        dbg(f"parse_token_transfers err {e}")
    return out

# ========= SCHEDULER =========
scheduler = BackgroundScheduler(timezone="UTC")
SCHED_LOCK = threading.Lock()

def check_market_lifecycle():
    with SCHED_LOCK:
        db = db_sess()
        try:
            now = now_utc()
            # lock
            to_lock = db.query(Market).filter(Market.status=="open", Market.lock_at<=now).all()
            for m in to_lock:
                m.status = "locked"
                db.commit()
            # settle
            to_settle = db.query(Market).filter(Market.status=="locked", Market.settle_at<=now).all()
            for m in to_settle:
                sp = get_price(m.symbol) or 0.0
                m.settle_price = sp
                # evaluate
                win_yes = False
                cond = m.condition.strip()
                if cond.endswith("%"):
                    try:
                        pct = float(cond.replace("%","").replace("+","").strip())
                        if m.reference_price>0:
                            chg = (sp - m.reference_price)/m.reference_price*100.0
                            win_yes = chg >= pct
                    except: pass
                elif cond.startswith("above:"):
                    try:
                        thr = float(cond.split(":",1)[1])
                        win_yes = sp >= thr
                    except: pass
                yes_bets = db.query(Bet).filter(Bet.market_id==m.id, Bet.side=="YES").all()
                no_bets = db.query(Bet).filter(Bet.market_id==m.id, Bet.side=="NO").all()
                pool_yes = sum(b.stake for b in yes_bets)
                pool_no = sum(b.stake for b in no_bets)
                winners = yes_bets if win_yes else no_bets
                losers_pool = pool_no if win_yes else pool_yes
                winners_pool = pool_yes if win_yes else pool_no
                fee = losers_pool*DEFAULT_FEE_PCT
                distributable = winners_pool + losers_pool - fee
                if winners:
                    ssum = sum(b.stake for b in winners)
                    for b in winners:
                        share = (b.stake/ssum) if ssum>0 else 0
                        pay = round(distributable*share,6)
                        b.payout = pay; b.settled=True
                        upsert_balance(db, b.user_id, "USDC", pay)
                for b in yes_bets+no_bets:
                    if not b.settled:
                        b.settled=True; b.payout=0.0
                m.status = "settled"
                db.commit()
        finally:
            db.close()

scheduler.add_job(check_market_lifecycle, IntervalTrigger(seconds=20), max_instances=1, coalesce=True)
scheduler.start()

# ========= UI BUILDERS =========
def kb_main(u: User, db) -> InlineKeyboardMarkup:
    usdc = get_balance(db, u.id, "USDC")
    sol  = get_balance(db, u.id, "SOL")
    k = InlineKeyboardMarkup()
    k.row(InlineKeyboardButton(f"💰 Balance: USDC {usdc:.2f}", callback_data="nav:balance"),
          InlineKeyboardButton("🎮 Coinflip", callback_data="nav:coinflip"))
    k.row(InlineKeyboardButton("📣 Trader", callback_data="nav:traders"),
          InlineKeyboardButton("📊 Markets", callback_data="nav:markets"))
    if u.role=="admin":
        k.row(InlineKeyboardButton("🛠 Admin-Panel", callback_data="nav:admin"))
    k.row(InlineKeyboardButton("ℹ️ Hilfe", callback_data="nav:help"))
    return k

def kb_coinflip() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    for a in [1,5,10,25,50]:
        k.row(InlineKeyboardButton(f"🪙 Kopf {a}", callback_data=f"cf:K:{a}"),
              InlineKeyboardButton(f"🪙 Zahl {a}", callback_data=f"cf:Z:{a}"))
    k.row(InlineKeyboardButton("ALL-IN Kopf", callback_data="cf:K:ALL"),
          InlineKeyboardButton("ALL-IN Zahl", callback_data="cf:Z:ALL"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:home"))
    return k

def kb_balance() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    k.row(InlineKeyboardButton("➕ Deposit", callback_data="bal:dep"),
          InlineKeyboardButton("➖ Withdraw", callback_data="bal:wdr"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:home"))
    return k

def kb_trader_hub(is_trader: bool, approved: bool) -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    if not is_trader:
        k.row(InlineKeyboardButton("📝 Trader werden", callback_data="tr:apply"))
    elif is_trader and not approved:
        k.row(InlineKeyboardButton("⏳ Wartet auf Freigabe", callback_data="noop"))
    else:
        k.row(InlineKeyboardButton("📨 Signal senden", callback_data="tr:signal"))
        k.row(InlineKeyboardButton("💵 Preis setzen", callback_data="tr:price"))
    k.row(InlineKeyboardButton("👥 Trader-Liste", callback_data="tr:list"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:home"))
    return k

def kb_trader_list(traders: List[Trader], page: int=0, page_size: int=5) -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    start = page*page_size
    for tr in traders[start:start+page_size]:
        tag = f"@{tr.user_id}"
        k.row(InlineKeyboardButton(f"👤 Trader {tr.user_id} • {tr.weekly_price:.2f}/w", callback_data=f"tr:view:{tr.id}"))
    nav = []
    if page>0: nav.append(InlineKeyboardButton("◀️", callback_data=f"tr:page:{page-1}"))
    if start+page_size < len(traders): nav.append(InlineKeyboardButton("▶️", callback_data=f"tr:page:{page+1}"))
    if nav: k.row(*nav)
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:traders"))
    return k

def kb_trader_view(trader: Trader, you_follow: bool, price: float) -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    if you_follow:
        k.row(InlineKeyboardButton("❌ Unfollow (Abo beenden)", callback_data=f"tr:unfollow:{trader.id}"))
    else:
        k.row(InlineKeyboardButton(f"✅ Follow • {price:.2f} USDC / 7 Tage", callback_data=f"tr:follow:{trader.id}"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="tr:list"))
    return k

def kb_signal_compose() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    k.row(InlineKeyboardButton("🟢 MemeCoin", callback_data="sg:type:memecoin"),
          InlineKeyboardButton("🔵 Futures", callback_data="sg:type:futures"))
    k.row(InlineKeyboardButton("💜 Lux", callback_data="sg:type:lux"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:traders"))
    return k

def kb_markets_admin() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    k.row(InlineKeyboardButton("➕ Markt anlegen", callback_data="mk:new"))
    k.row(InlineKeyboardButton("📋 Offene Märkte", callback_data="mk:list"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:home"))
    return k

def kb_markets_user() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    k.row(InlineKeyboardButton("📋 Märkte anzeigen", callback_data="mk:list"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:home"))
    return k

def kb_market_view(m: Market, you_can_bet: bool=True) -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    if you_can_bet and m.status=="open":
        for a in [1,5,10,25,50]:
            k.row(InlineKeyboardButton(f"YES {a}", callback_data=f"mkb:yes:{m.id}:{a}"),
                  InlineKeyboardButton(f"NO {a}",  callback_data=f"mkb:no:{m.id}:{a}"))
        k.row(InlineKeyboardButton("ALL-IN YES", callback_data=f"mkb:yes:{m.id}:ALL"),
              InlineKeyboardButton("ALL-IN NO",  callback_data=f"mkb:no:{m.id}:ALL"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="mk:list"))
    return k

def kb_admin_panel() -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    k.row(InlineKeyboardButton("👤 Trader-Anträge", callback_data="ad:tr_reqs"))
    k.row(InlineKeyboardButton("📊 Prediction Markets", callback_data="nav:mk_admin"))
    k.row(InlineKeyboardButton("💵 Auszahlungen", callback_data="ad:wdrs"))
    k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:home"))
    return k

# ========= COMMANDS =========
@bot.message_handler(commands=["start"])
def cmd_start(message: Message):
    u = get_or_create_user(message)
    db = db_sess()
    try:
        if is_admin(int(u.telegram_id)):
            u.role = "admin"; db.commit()
        text = (
            "<b>PulsePlay</b>\n"
            "Willkommen! Nutze die Buttons, kein Tippen nötig.\n"
            "• Coinflip (30% hidden odds)\n"
            "• Trader folgen (Abo 7 Tage, 80/20 Split)\n"
            "• Manuelle Signale (MemeCoin/Futures/Lux)\n"
            "• Prediction Markets (YES/NO)\n"
            "• Solana-Deposit & Withdraw\n"
        )
        bot.send_message(message.chat.id, text, reply_markup=kb_main(u, db))
    finally:
        db.close()

# ========= NAVIGATION =========
@bot.callback_query_handler(func=lambda c: c.data.startswith("nav:"))
def on_nav(call: CallbackQuery):
    db = db_sess()
    try:
        u = db.query(User).filter(User.telegram_id==str(call.from_user.id)).first()
        if not u:
            u = get_or_create_user(call.message)
        if call.data=="nav:home":
            bot.edit_message_text("🏠 Hauptmenü", call.message.chat.id, call.message.message_id, reply_markup=kb_main(u, db))
        elif call.data=="nav:coinflip":
            bot.edit_message_text("🎮 <b>Coinflip</b>\nWähle Kopf/Zahl und Einsatz.", call.message.chat.id, call.message.message_id, reply_markup=kb_coinflip())
        elif call.data=="nav:balance":
            usdc = get_balance(db, u.id, "USDC"); sol = get_balance(db, u.id, "SOL")
            bot.edit_message_text(f"💰 <b>Dein Kontostand</b>\nUSDC: {usdc:.2f}\nSOL: {sol:.4f}", call.message.chat.id, call.message.message_id, reply_markup=kb_balance())
        elif call.data=="nav:traders":
            tr = db.query(Trader).filter(Trader.user_id==u.id).first()
            bot.edit_message_text("📣 <b>Trader-Hub</b>", call.message.chat.id, call.message.message_id, reply_markup=kb_trader_hub(bool(tr), bool(tr and tr.approved)))
        elif call.data=="nav:markets":
            text = "📊 Prediction Markets"
            if is_admin(call.from_user.id):
                bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb_markets_admin())
            else:
                bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb_markets_user())
        elif call.data=="nav:mk_admin":
            bot.edit_message_text("👔 Admin • Markets", call.message.chat.id, call.message.message_id, reply_markup=kb_markets_admin())
        elif call.data=="nav:admin":
            if not is_admin(call.from_user.id):
                bot.answer_callback_query(call.id, "Nur für Admins.")
                return
            bot.edit_message_text("🛠 Admin-Panel", call.message.chat.id, call.message.message_id, reply_markup=kb_admin_panel())
        elif call.data=="nav:help":
            bot.edit_message_text(esc("Hilfe:\n• /start – Menü neu öffnen\n• Buttons benutzen 🙂"), call.message.chat.id, call.message.message_id, reply_markup=kb_main(u, db))
    finally:
        db.close()

# ========= COINFLIP =========
def _coinflip_play(db, user_id: int, call: CallbackQuery, side: str, amount: float):
    bal = get_balance(db, user_id, "USDC")
    stake = bal if amount == -1 else amount
    if stake <= 0.0:
        bot.answer_callback_query(call.id, "Kein Einsatz.")
        return
    if bal < stake:
        bot.answer_callback_query(call.id, "Zu wenig Guthaben.")
        return
    # deduct
    upsert_balance(db, user_id, "USDC", -stake)
    win = random.random() < COINFLIP_WIN_PROB
    if win:
        payout = round(stake * COINFLIP_PAYOUT_MULT, 6)
        upsert_balance(db, user_id, "USDC", payout)
        bot.answer_callback_query(call.id, f"Gewonnen! +{payout:.2f} USDC")
    else:
        bot.answer_callback_query(call.id, f"Verloren. -{stake:.2f} USDC")

@bot.callback_query_handler(func=lambda c: c.data.startswith("cf:"))
def on_cf(call: CallbackQuery):
    try:
        _, side, amt = call.data.split(":")
        if amt == "ALL":
            amount = -1.0
        else:
            amount = float(amt)
    except:
        bot.answer_callback_query(call.id, "Fehlerhafte Eingabe.")
        return
    db = db_sess()
    try:
        u = db.query(User).filter(User.telegram_id==str(call.from_user.id)).first()
        if not u:
            bot.answer_callback_query(call.id, "Bitte /start nutzen.")
            return
        _coinflip_play(db, u.id, call, side, amount)
        # refresh balance card
        usdc = get_balance(db, u.id, "USDC"); sol = get_balance(db, u.id, "SOL")
        bot.edit_message_text(f"🎮 Coinflip\nUSDC: {usdc:.2f} | SOL: {sol:.4f}", call.message.chat.id, call.message.message_id, reply_markup=kb_coinflip())
    finally:
        db.close()

# ========= BALANCE =========
@bot.callback_query_handler(func=lambda c: c.data.startswith("bal:"))
def on_bal(call: CallbackQuery):
    db = db_sess()
    try:
        u = db.query(User).filter(User.telegram_id==str(call.from_user.id)).first()
        if not u:
            bot.answer_callback_query(call.id, "Bitte /start nutzen.")
            return
        if call.data == "bal:dep":
            p = Payment(user_id=u.id, type="deposit", amount=0.0, status="pending", meta=json.dumps({"info":"awaiting"}))
            db.add(p); db.commit()
            txt = (f"🔹 <b>Deposit erstellen</b>\n"
                   f"• Ziel (zentral): <code>{CENTRAL_DEPOSIT_ADDRESS}</code>\n"
                   f"• Sende USDC (Token) oder SOL\n"
                   f"• Danach: /verify {p.id}\n")
            bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=kb_balance())
        elif call.data == "bal:wdr":
            bot.answer_callback_query(call.id, "Schreibe dem Admin deinen Auszahlungswunsch mit Adresse.")
    finally:
        db.close()

# /verify <payment_id>
@bot.message_handler(commands=["verify"])
def cmd_verify(message: Message):
    parts = message.text.split()
    if len(parts)!=2 or not parts[1].isdigit():
        bot.reply_to(message, esc("Nutzung: /verify <payment_id>"))
        return
    pid = int(parts[1])
    db = db_sess()
    try:
        u = get_or_create_user(message)
        p: Payment = db.query(Payment).filter(Payment.id==pid, Payment.user_id==u.id, Payment.type=="deposit").first()
        if not p or p.status=="completed":
            bot.reply_to(message, "Deposit nicht gefunden oder bereits verbucht.")
            return
        sigs = get_sigs_for_address(CENTRAL_DEPOSIT_ADDRESS, limit=40)
        matched=False
        for s in sigs:
            sig = s["signature"]
            txj = get_tx(sig)
            transfers = parse_token_transfers(txj)
            inflow_sol = sum(max(0.0,t["amount"]) for t in transfers if t["mint"]=="SOL")
            inflow_usdc = sum(max(0.0,t["amount"]) for t in transfers if t["mint"]==USDC_SOLANA_MINT)
            if inflow_sol>0 or inflow_usdc>0:
                credit = inflow_usdc if inflow_usdc>0 else (inflow_sol*(get_price("SOL") or 0))
                upsert_balance(db, u.id, "USDC", credit)
                p.status="completed"; p.tx=sig; p.amount=credit; db.commit()
                bot.reply_to(message, f"✅ Deposit verbucht: +{credit:.4f} USDC")
                matched=True
                break
        if not matched:
            bot.reply_to(message, "Noch keine passende Transaktion gefunden. Später erneut /verify ausführen.")
    except Exception as e:
        bot.reply_to(message, f"Fehler: {e}")
    finally:
        db.close()

# ========= TRADER =========
@bot.callback_query_handler(func=lambda c: c.data.startswith("tr:"))
def on_trader(call: CallbackQuery):
    db = db_sess()
    try:
        u = db.query(User).filter(User.telegram_id==str(call.from_user.id)).first()
        if not u: 
            bot.answer_callback_query(call.id, "Bitte /start nutzen."); return
        data = call.data.split(":")
        if data[1]=="apply":
            tr = db.query(Trader).filter(Trader.user_id==u.id).first()
            if tr:
                bot.answer_callback_query(call.id, "Du hast bereits einen Antrag gestellt.")
                return
            tr = Trader(user_id=u.id, approved=False, profile=f"Trader @{u.username}", weekly_price=20.0)
            db.add(tr); db.commit()
            bot.answer_callback_query(call.id, "Antrag gesendet. Warte auf Freigabe.")
            for aid in ADMIN_IDS:
                try:
                    bot.send_message(aid, f"📥 Trader-Antrag von @{u.username or u.telegram_id} (user_id={u.id}) – im Admin-Panel genehmigen.")
                except: pass
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb_trader_hub(True, False))
        elif data[1]=="list":
            trs = db.query(Trader).filter(Trader.approved==True).all()
            if not trs:
                bot.answer_callback_query(call.id, "Noch keine Trader.")
                return
            bot.edit_message_text("👥 <b>Trader-Liste</b>", call.message.chat.id, call.message.message_id, reply_markup=kb_trader_list(trs, 0))
        elif data[1]=="page":
            page=int(data[2]); trs = db.query(Trader).filter(Trader.approved==True).all()
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb_trader_list(trs,page))
        elif data[1]=="view":
            trid=int(data[2]); tr = db.query(Trader).get(trid)
            if not tr or not tr.approved:
                bot.answer_callback_query(call.id, "Trader nicht gefunden."); return
            you_follow = db.query(Follow).filter(Follow.follower_user_id==u.id, Follow.trader_id==tr.id, Follow.expires_at>datetime.utcnow()).first() is not None
            bot.edit_message_text(f"👤 Trader {tr.user_id}\nPreis: {tr.weekly_price:.2f} USDC / Woche\n{esc(tr.profile or '')}",
                                  call.message.chat.id, call.message.message_id,
                                  reply_markup=kb_trader_view(tr, you_follow, tr.weekly_price))
        elif data[1]=="follow":
            trid=int(data[2]); tr = db.query(Trader).get(trid)
            if not tr or not tr.approved:
                bot.answer_callback_query(call.id, "Trader nicht verfügbar."); return
            price = tr.weekly_price
            bal = get_balance(db, u.id, "USDC")
            if bal < price:
                bot.answer_callback_query(call.id, f"Zu wenig Guthaben ({bal:.2f} < {price:.2f}).")
                return
            upsert_balance(db, u.id, "USDC", -price)
            # split
            trader_cut = round(price*0.80,6); house_cut = round(price*0.20,6)
            upsert_balance(db, tr.user_id, "USDC", trader_cut)
            upsert_balance(db, ADMIN_IDS[0], "USDC", house_cut)
            exp = datetime.utcnow() + timedelta(days=7)
            # upsert follow
            f = db.query(Follow).filter(Follow.follower_user_id==u.id, Follow.trader_id==tr.id).first()
            if not f:
                f = Follow(follower_user_id=u.id, trader_id=tr.id, started_at=datetime.utcnow(), expires_at=exp)
                db.add(f)
            else:
                f.started_at = datetime.utcnow(); f.expires_at = exp
            db.commit()
            bot.answer_callback_query(call.id, "Abo für 7 Tage aktiv.")
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb_trader_view(tr, True, tr.weekly_price))
        elif data[1]=="unfollow":
            trid=int(data[2]); f = db.query(Follow).filter(Follow.follower_user_id==u.id, Follow.trader_id==trid).first()
            if not f:
                bot.answer_callback_query(call.id, "Kein aktives Abo."); return
            db.delete(f); db.commit()
            bot.answer_callback_query(call.id, "Abo beendet.")
            tr = db.query(Trader).get(trid)
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb_trader_view(tr, False, tr.weekly_price if tr else 0.0))
        elif data[1]=="signal":
            tr = db.query(Trader).filter(Trader.user_id==u.id, Trader.approved==True).first()
            if not tr:
                bot.answer_callback_query(call.id, "Nur freigegebene Trader dürfen Signale senden."); return
            bot.edit_message_text("Signal-Typ wählen:", call.message.chat.id, call.message.message_id, reply_markup=kb_signal_compose())
        elif data[1]=="price":
            tr = db.query(Trader).filter(Trader.user_id==u.id).first()
            if not tr or not tr.approved:
                bot.answer_callback_query(call.id, "Nur freigegebene Trader."); return
            tr.weekly_price = max(1.0, tr.weekly_price + 5.0)  # simple demo: +5 USDC pro Klick
            db.commit()
            bot.answer_callback_query(call.id, f"Neuer Preis: {tr.weekly_price:.2f} USDC/Woche")
        else:
            bot.answer_callback_query(call.id, "Unbekannte Aktion.")
    finally:
        db.close()

# Signal typing state cache (very simple, in-memory)
SIGNAL_STATE: Dict[int, Dict[str,str]] = {}

@bot.callback_query_handler(func=lambda c: c.data.startswith("sg:type:"))
def on_signal_type(call: CallbackQuery):
    db = db_sess()
    try:
        u = db.query(User).filter(User.telegram_id==str(call.from_user.id)).first()
        tr = db.query(Trader).filter(Trader.user_id==u.id, Trader.approved==True).first()
        if not tr:
            bot.answer_callback_query(call.id, "Nur freigegebene Trader."); return
        _,_,stype = call.data.split(":")
        SIGNAL_STATE[u.id] = {"type": stype}
        bot.answer_callback_query(call.id, f"{stype} ausgewählt. Antworte mit dem Signaltext (einfach hier in den Chat schreiben).")
        bot.send_message(call.message.chat.id, "Bitte sende jetzt deinen <b>Signaltext</b> (wird an aktive Follower geschickt).")
    finally:
        db.close()

@bot.message_handler(func=lambda m: True, content_types=['text'])
def on_any_text(message: Message):
    # if trader is composing a signal
    db = db_sess()
    try:
        u = get_or_create_user(message)
        if u.id in SIGNAL_STATE and "type" in SIGNAL_STATE[u.id]:
            stype = SIGNAL_STATE[u.id]["type"]
            text = message.text.strip()
            # deliver to followers
            tr = db.query(Trader).filter(Trader.user_id==u.id, Trader.approved==True).first()
            if tr:
                active = db.query(Follow).filter(Follow.trader_id==tr.id, Follow.expires_at>datetime.utcnow()).all()
                sent = 0
                for f in active:
                    try:
                        follower = db.query(User).get(f.follower_user_id)
                        if follower:
                            bot.send_message(int(follower.telegram_id),
                                             f"📣 <b>Signal von</b> @{u.username or u.telegram_id} (ID {u.telegram_id})\n"
                                             f"Typ: <b>{stype.upper()}</b>\n"
                                             f"{esc(text)}")
                            sent += 1
                    except Exception as e:
                        dbg(f"send signal error: {e}")
                bot.reply_to(message, f"✅ Signal gesendet an {sent} Abonnenten.")
            SIGNAL_STATE.pop(u.id, None)
            return
        # otherwise default: refresh home UI
        if message.text.strip().lower() in ("menu","menü","start"):
            bot.reply_to(message, "Menü:", reply_markup=kb_main(u, db))
    finally:
        db.close()

# ========= ADMIN =========
@bot.callback_query_handler(func=lambda c: c.data.startswith("ad:"))
def on_admin(call: CallbackQuery):
    if not is_admin(call.from_user.id):
        bot.answer_callback_query(call.id, "Nur Admin.")
        return
    db = db_sess()
    try:
        if call.data=="ad:tr_reqs":
            reqs = db.query(Trader).filter(Trader.approved==False).all()
            if not reqs:
                bot.answer_callback_query(call.id, "Keine offenen Anträge.")
                return
            k = InlineKeyboardMarkup()
            for r in reqs:
                k.row(InlineKeyboardButton(f"👍 Approve {r.user_id}", callback_data=f"ad:approve:{r.id}"),
                      InlineKeyboardButton(f"👎 Reject {r.user_id}",  callback_data=f"ad:reject:{r.id}"))
            k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:admin"))
            bot.edit_message_text("Offene Trader-Anträge:", call.message.chat.id, call.message.message_id, reply_markup=k)
        elif call.data.startswith("ad:approve:"):
            trid=int(call.data.split(":")[2]); tr=db.query(Trader).get(trid)
            if tr: tr.approved=True; db.commit()
            bot.answer_callback_query(call.id, "Freigeschaltet."); bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb_admin_panel())
        elif call.data.startswith("ad:reject:"):
            trid=int(call.data.split(":")[2]); tr=db.query(Trader).get(trid)
            if tr: db.delete(tr); db.commit()
            bot.answer_callback_query(call.id, "Abgelehnt."); bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb_admin_panel())
        elif call.data=="ad:wdrs":
            bot.answer_callback_query(call.id, "Auszahlungsübersicht folgt. (MVP)")
    finally:
        db.close()

# ========= MARKETS =========
NEW_MARKET_STATE: Dict[int, Dict[str,str]] = {}

def kb_market_wizard(state: Dict[str,str]) -> InlineKeyboardMarkup:
    k = InlineKeyboardMarkup()
    step = state.get("step","symbol")
    if step=="symbol":
        for s in ["SOL","BTC","ETH","PEPE","BONK","LUX"]:
            k.row(InlineKeyboardButton(s, callback_data=f"mk:wz:sym:{s}"))
    elif step=="cond":
        k.row(InlineKeyboardButton("+2%", callback_data="mk:wz:cond:+2%"),
              InlineKeyboardButton("+5%", callback_data="mk:wz:cond:+5%"))
        k.row(InlineKeyboardButton("above:100", callback_data="mk:wz:cond:above:100"))
    elif step=="tf":
        for tf in ["1h","4h","24h"]:
            k.row(InlineKeyboardButton(tf, callback_data=f"mk:wz:tf:{tf}"))
    elif step=="confirm":
        k.row(InlineKeyboardButton("✅ Anlegen", callback_data="mk:wz:ok"),
              InlineKeyboardButton("❌ Abbrechen", callback_data="mk:wz:cancel"))
    k.row(InlineKeyboardButton("⬅️ Admin", callback_data="nav:mk_admin"))
    return k

@bot.callback_query_handler(func=lambda c: c.data.startswith("mk:"))
def on_mk(call: CallbackQuery):
    db = db_sess()
    try:
        u = db.query(User).filter(User.telegram_id==str(call.from_user.id)).first()
        if call.data=="mk:new":
            if not is_admin(call.from_user.id):
                bot.answer_callback_query(call.id, "Nur Admin."); return
            NEW_MARKET_STATE[call.from_user.id] = {"step":"symbol"}
            bot.edit_message_text("Neuer Markt • Symbol wählen:", call.message.chat.id, call.message.message_id, reply_markup=kb_market_wizard(NEW_MARKET_STATE[call.from_user.id]))
        elif call.data=="mk:list":
            ms = db.query(Market).filter(Market.status.in_(["open","locked"])).order_by(Market.lock_at.asc()).all()
            if not ms:
                bot.answer_callback_query(call.id, "Keine offenen Märkte.")
                return
            k = InlineKeyboardMarkup()
            for m in ms[:10]:
                k.row(InlineKeyboardButton(f"#{m.id} {m.symbol} {m.condition} • {m.status}", callback_data=f"mk:view:{m.id}"))
            k.row(InlineKeyboardButton("⬅️ Zurück", callback_data="nav:markets"))
            bot.edit_message_text("Offene/gesperrte Märkte:", call.message.chat.id, call.message.message_id, reply_markup=k)
        elif call.data.startswith("mk:view:"):
            mid = int(call.data.split(":")[2]); m = db.query(Market).get(mid)
            if not m:
                bot.answer_callback_query(call.id, "Markt nicht gefunden."); return
            now = now_utc()
            can_bet = (m.status=="open") and (m.lock_at > now)
            ref = m.reference_price; sp = m.settle_price
            txt = (f"#{m.id} <b>{m.symbol}</b> {m.condition}\n"
                   f"Ref: {ref:.6f}\n"
                   f"Lock: {m.lock_at.strftime('%Y-%m-%d %H:%M UTC')}\n"
                   f"Settle: {m.settle_at.strftime('%Y-%m-%d %H:%M UTC')}\n"
                   f"Status: {m.status}\n"
                   f"Final: {sp:.6f}")
            bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=kb_market_view(m, can_bet))
        elif call.data.startswith("mk:wz:"):
            if not is_admin(call.from_user.id):
                bot.answer_callback_query(call.id, "Nur Admin."); return
            st = NEW_MARKET_STATE.get(call.from_user.id, {"step":"symbol"})
            parts = call.data.split(":")
            if parts[2]=="sym":
                st["symbol"]=parts[3]; st["step"]="cond"
            elif parts[2]=="cond":
                st["cond"]=":".join(parts[3:]); st["step"]="tf"
            elif parts[2]=="tf":
                st["tf"]=parts[3]; st["step"]="confirm"
            elif parts[2]=="ok":
                sym=st.get("symbol"); cond=st.get("cond"); tf=st.get("tf")
                if not sym or not cond or not tf:
                    bot.answer_callback_query(call.id, "Unvollständig."); return
                ref = get_price(sym) or 0.0
                start = now_utc()
                lock = start + timedelta(minutes=5)
                hours = {"1h":1,"4h":4,"24h":24}[tf]
                settle = start + timedelta(hours=hours)
                m = Market(symbol=sym, condition=cond, timeframe=tf, status="open",
                           start_at=start, lock_at=lock, settle_at=settle, reference_price=ref)
                db.add(m); db.commit()
                NEW_MARKET_STATE.pop(call.from_user.id, None)
                bot.answer_callback_query(call.id, f"Markt #{m.id} erstellt.")
                bot.edit_message_text("Admin • Markets", call.message.chat.id, call.message.message_id, reply_markup=kb_markets_admin())
            elif parts[2]=="cancel":
                NEW_MARKET_STATE.pop(call.from_user.id, None)
                bot.edit_message_text("Admin • Markets", call.message.chat.id, call.message.message_id, reply_markup=kb_markets_admin())
            NEW_MARKET_STATE[call.from_user.id]=st
            if st.get("step")!="confirm":
                stepshow={"symbol":"Symbol","cond":"Bedingung","tf":"Timeframe"}
                bot.edit_message_text(f"Neuer Markt • {stepshow.get(st['step'],'...')} wählen:", call.message.chat.id, call.message.message_id, reply_markup=kb_market_wizard(st))
            else:
                bot.edit_message_text(f"Bestätigen:\nSymbol {st['symbol']}\nCond {st['cond']}\nTF {st['tf']}", call.message.chat.id, call.message.message_id, reply_markup=kb_market_wizard(st))
        else:
            bot.answer_callback_query(call.id, "Unbekannte Aktion.")
    finally:
        db.close()

# place bet buttons
@bot.callback_query_handler(func=lambda c: c.data.startswith("mkb:"))
def on_mkb(call: CallbackQuery):
    db = db_sess()
    try:
        _, side, mid, amt = call.data.split(":")
        mid = int(mid)
        m = db.query(Market).get(mid)
        if not m:
            bot.answer_callback_query(call.id, "Markt nicht gefunden."); return
        now = now_utc()
        if m.status!="open" or m.lock_at<=now:
            bot.answer_callback_query(call.id, "Markt ist gesperrt."); return
        u = db.query(User).filter(User.telegram_id==str(call.from_user.id)).first()
        if not u:
            bot.answer_callback_query(call.id, "Bitte /start nutzen."); return
        amt_f = -1.0 if amt=="ALL" else float(amt)
        bal = get_balance(db, u.id, "USDC")
        stake = bal if amt_f<0 else amt_f
        if stake<=0 or bal<stake:
            bot.answer_callback_query(call.id, "Zu wenig Guthaben."); return
        upsert_balance(db, u.id, "USDC", -stake)
        b = Bet(market_id=mid, user_id=u.id, side=side.upper(), stake=stake, fee=0.0, payout=0.0, settled=False)
        db.add(b); db.commit()
        bot.answer_callback_query(call.id, f"{side.upper()} gesetzt: {stake:.2f} USDC")
        # refresh view
        txt = (f"#{m.id} <b>{m.symbol}</b> {m.condition}\n"
               f"Ref: {m.reference_price:.6f}\nLock: {m.lock_at.strftime('%Y-%m-%d %H:%M UTC')}\n"
               f"Settle: {m.settle_at.strftime('%Y-%m-%d %H:%M UTC')}\nStatus: {m.status}\nFinal: {m.settle_price:.6f}")
        can_bet = (m.status=="open") and (m.lock_at>now)
        bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=kb_market_view(m, can_bet))
    finally:
        db.close()

# ========= RUN =========
if __name__ == "__main__":
    print("PulsePlay bot starting...")
    try:
        bot.infinity_polling(timeout=60, long_polling_timeout=60)
    finally:
        try:
            scheduler.shutdown(wait=False)
        except:
            pass
