#usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PulsePlay – Full Inline Bot (ALL FEATURES)
- Inline-only UI (no slash commands needed for users)
- Signals marketplace
  • Wallet categories: memecoin / futures / other
  • Weekly subscription price per wallet
  • Subscribers receive trade signals (with source wallet + category shown)
- Markets (prediction bets)
  • Create markets (+% or above:PRICE), reference price, lock & settle time
  • Detail view shows timers, price ref, and inline betting (YES/NO, +1/+5/+10 or custom)
  • Settlement redistributes pools with house fee
- Coinflip
  • 30% win prob (hidden), x2 payout on win
  • Flow works side-first or amount-first (preset or custom)
- Balance
  • USDC / SOL display, deposit (verify via Solana RPC to central address), withdraw request
- Admin
  • Wallet add/edit (title, category, weekly price), delete
  • Market presets + custom
  • Credit/Debit balances
Dependencies:
  pyTelegramBotAPI>=4.14.0
  SQLAlchemy>=2.0.36  (Python 3.13 compatible)
  APScheduler==3.10.4
  requests==2.31.0
"""

import os, json, threading, random
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
BOT_TOKEN = os.getenv("ENV_BOT_TOKEN", "8200746289:AAGbzwf7sUHVHlLDb3foXbZpj9SVGnqLeNU")
ADMIN_IDS = [int(x) for x in os.getenv("ENV_ADMIN_IDS", "7919108078").split(",") if x.strip()]
CENTRAL_DEPOSIT_ADDRESS = os.getenv("ENV_CENTRAL_DEPOSIT_ADDRESS", "CKZEpwiVqAHLiSbdc8Ebf8xaQ2fofgPCNmzi4cV32M1s")
SOLANA_RPC_URL = os.getenv("ENV_SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
USDC_SOLANA_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

DEFAULT_FEE_PCT = float(os.getenv("ENV_DEFAULT_FEE_PCT", "0.04"))  # 4%
COINFLIP_WIN_PROB = float(os.getenv("ENV_COINFLIP_PROB", "0.30"))  # 30% win probability (hidden)
COINFLIP_PAYOUT_MULT = float(os.getenv("ENV_COINFLIP_MULT", "2.0"))  # X2 payout on win
DEBUG = os.getenv("ENV_DEBUG", "1") == "1"

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ========= DB =========
Base = declarative_base()
engine = create_engine("sqlite:///pulseplay_all.db", connect_args={"check_same_thread": False})
SessionLocal = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))

def now_utc(): return datetime.now(timezone.utc)
def esc(x:str)->str: return x.replace("<","&lt;").replace(">","&gt;")
def dbg(x):
    if DEBUG:
        print(f"[{datetime.utcnow().isoformat()}] {x}")

# ========= MODELS =========
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    telegram_id = Column(String, unique=True, index=True)
    username = Column(String, default="")
    role = Column(String, default="user")  # user/admin
    created_at = Column(DateTime, default=datetime.utcnow)

class Wallet(Base):
    __tablename__ = "wallets"
    id = Column(Integer, primary_key=True)
    chain = Column(String, default="solana")
    address = Column(String, index=True)
    title = Column(String, default="")
    category = Column(String, default="other")  # memecoin / futures / other
    weekly_price = Column(Float, default=0.0)   # USDC per 7 days
    owner_user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    is_public = Column(Boolean, default=True)
    active = Column(Boolean, default=True)
    last_sig = Column(String, nullable=True)
    __table_args__ = (UniqueConstraint("chain", "address", name="uq_chain_address"),)

class Subscription(Base):
    __tablename__ = "subscriptions"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    wallet_id = Column(Integer, ForeignKey("wallets.id"), index=True)
    expires_at = Column(DateTime, index=True)
    active = Column(Boolean, default=True)
    __table_args__ = (UniqueConstraint("user_id", "wallet_id", name="uq_user_wallet_sub"),)

class Signal(Base):
    __tablename__ = "signals"
    id = Column(Integer, primary_key=True)
    wallet_id = Column(Integer, ForeignKey("wallets.id"), index=True)
    tx_sig = Column(String, index=True)
    side = Column(String)  # buy/sell/unknown
    token = Column(String)
    amount = Column(Float, default=0.0)
    price_usd = Column(Float, default=0.0)
    ts = Column(DateTime, default=datetime.utcnow)
    Index("ix_wallet_tx", wallet_id, tx_sig)

class Market(Base):
    __tablename__ = "markets"
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    condition = Column(String)  # "+2%" | "above:0.25"
    timeframe = Column(String, default="4h")  # 1h/4h/24h
    start_at = Column(DateTime)
    lock_at = Column(DateTime)
    settle_at = Column(DateTime)
    status = Column(String, default="open")  # open/locked/settled
    reference_price = Column(Float, default=0.0)
    settle_price = Column(Float, default=0.0)

class Bet(Base):
    __tablename__ = "bets"
    id = Column(Integer, primary_key=True)
    market_id = Column(Integer, ForeignKey("markets.id"), index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    side = Column(String)  # YES/NO
    stake = Column(Float, default=0.0)
    payout = Column(Float, default=0.0)
    settled = Column(Boolean, default=False)
    placed_at = Column(DateTime, default=datetime.utcnow)

class Balance(Base):
    __tablename__ = "balances"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    currency = Column(String, default="USDC")
    amount = Column(Float, default=0.0)

class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    type = Column(String)  # deposit/withdraw
    method = Column(String, default="solana")
    amount = Column(Float, default=0.0)
    tx = Column(String, default="")
    status = Column(String, default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    meta = Column(Text, default="{}")

Base.metadata.create_all(bind=engine)

# ========= STATE =========
STATE: Dict[int, Dict] = {}
def set_state(uid, name=None, ctx=None):
    if name is None: STATE.pop(uid, None)
    else: STATE[uid] = {"name": name, "ctx": ctx or {}}
def get_state(uid): return STATE.get(uid)

# ========= HELPERS =========
def get_session(): return SessionLocal()
def is_admin(tid:int)->bool: return tid in ADMIN_IDS

def ensure_user(obj)->User:
    db=get_session()
    try:
        if isinstance(obj, Message):
            tid=str(obj.from_user.id); username=obj.from_user.username or ""
        else:
            tid=str(obj.from_user.id); username=obj.from_user.username or ""
        u=db.query(User).filter(User.telegram_id==tid).first()
        if not u:
            u=User(telegram_id=tid, username=username, role=("admin" if is_admin(int(tid)) else "user"))
            db.add(u); db.commit()
        return u
    finally:
        db.close()

def upsert_balance(db, user_id:int, currency:str, delta:float):
    bal = db.query(Balance).filter(Balance.user_id==user_id, Balance.currency==currency).first()
    if not bal:
        bal = Balance(user_id=user_id, currency=currency, amount=0.0)
        db.add(bal); db.flush()
    bal.amount = max(0.0, bal.amount + delta)
    db.commit()
    return bal.amount

def get_balance(db, user_id:int, currency:str="USDC")->float:
    bal = db.query(Balance).filter(Balance.user_id==user_id, Balance.currency==currency).first()
    return bal.amount if bal else 0.0

def sub_active(db, user_id:int, wallet_id:int)->bool:
    sub = db.query(Subscription).filter(Subscription.user_id==user_id, Subscription.wallet_id==wallet_id, Subscription.active==True).first()
    return bool(sub and sub.expires_at and sub.expires_at > now_utc())

# ========= PRICES =========
def cg_price(symbol:str)->Optional[float]:
    aliases={"SOL":"solana","USDC":"usd-coin","PEPE":"pepe","BONK":"bonk","DOGE":"dogecoin","SHIB":"shiba-inu","WIF":"dogwifcoin"}
    cid=aliases.get(symbol.upper(), symbol.lower())
    url=f"https://api.coingecko.com/api/v3/simple/price?ids={cid}&vs_currencies=usd"
    try:
        r=requests.get(url, timeout=10); j=r.json()
        return float(j[cid]["usd"])
    except Exception as e:
        dbg(f"price err: {e}"); return None
def get_price(symbol:str)->float:
    p=cg_price(symbol); return p if p is not None else 0.0

# ========= Solana RPC =========
def sol_rpc(method:str, params:list):
    payload={"jsonrpc":"2.0","id":1,"method":method,"params":params}
    r=requests.post(SOLANA_RPC_URL, json=payload, timeout=25)
    r.raise_for_status()
    j=r.json()
    if "error" in j: raise RuntimeError(j["error"])
    return j["result"]

def get_sigs_for_address(address:str, limit:int=20, before:Optional[str]=None)->List[dict]:
    p=[address, {"limit":limit}]
    if before: p[1]["before"]=before
    return sol_rpc("getSignaturesForAddress", p)

def get_tx(sig:str)->dict:
    return sol_rpc("getTransaction", [sig, {"encoding":"json","maxSupportedTransactionVersion":0}])

def parse_token_transfers(tx_json:dict)->List[dict]:
    out=[]
    try:
        meta=tx_json.get("meta",{})
        pre_bal=meta.get("preBalances",[]); post_bal=meta.get("postBalances",[])
        for pre,post in zip(pre_bal,post_bal):
            diff=(post-pre)/1e9
            if abs(diff)>0: out.append({"mint":"SOL","amount":diff})
        pre_token={tb["accountIndex"]:float(tb["uiTokenAmount"]["uiAmount"] or 0.0) for tb in meta.get("preTokenBalances",[])}
        for tb in meta.get("postTokenBalances",[]):
            idx=tb["accountIndex"]; pre=pre_token.get(idx,0.0); post=float(tb["uiTokenAmount"]["uiAmount"] or 0.0)
            diff=post-pre
            if abs(diff)>0: out.append({"mint":tb["mint"],"amount":diff})
    except Exception as e: dbg(f"parse token err: {e}")
    return out

def get_account_keys(tx_json:dict)->List[str]:
    accs=tx_json.get("transaction",{}).get("message",{}).get("accountKeys",[])
    keys=[]
    for a in accs:
        if isinstance(a,dict): keys.append(a.get("pubkey"))
        else: keys.append(a)
    return [k for k in keys if k]

# ========= SIGNALS POLLER =========
POLL_LOCK=threading.Lock()

def notify_subscribers(db, w:Wallet, text:str):
    subs=db.query(Subscription).filter(Subscription.wallet_id==w.id, Subscription.active==True).all()
    for s in subs:
        if s.expires_at and s.expires_at>now_utc():
            try:
                chat_id=int(db.query(User).get(s.user_id).telegram_id)
                bot.send_message(chat_id, text)
            except Exception as e:
                dbg(f"notify error: {e}")

def poll_wallet_once(db, w:Wallet):
    try:
        sigs=get_sigs_for_address(w.address, limit=20)
        last=w.last_sig
        new=[]
        for s in sigs:
            if s["signature"]==last: break
            new.append(s["signature"])
        if not new: return
        for sig in reversed(new):
            tx=get_tx(sig)
            transfers=parse_token_transfers(tx)
            side="unknown"; token="SOL"; amt=0.0; price=get_price("SOL") or 0.0
            if transfers:
                t=sorted(transfers,key=lambda x:abs(x["amount"]),reverse=True)[0]
                token="USDC" if t["mint"]==USDC_SOLANA_MINT else ("SOL" if t["mint"]=="SOL" else t["mint"][:4]+"…")
                amt=float(t["amount"]); side="buy" if amt>0 else "sell"
                p=get_price("SOL" if token=="SOL" else "USDC"); price=p if p is not None else price
            db.add(Signal(wallet_id=w.id, tx_sig=sig, side=side, token=token, amount=amt, price_usd=price, ts=now_utc())); db.commit()
            text=(f"📣 <b>Signal</b> — <i>{w.category}</i>\n"
                  f"From: <b>{esc(w.title or 'Trader')}</b> (#{w.id})\n"
                  f"Wallet: <code>{w.address[:6]}…{w.address[-4:]}</code>\n"
                  f"Action: <b>{side.upper()}</b>  Token: <b>{token}</b>\n"
                  f"Amount: <b>{amt:.6f}</b>  Price: <b>${price:.6f}</b>\n"
                  f"TX: <code>{sig[:8]}…{sig[-6:]}</code>")
            notify_subscribers(db, w, text)
        w.last_sig=sigs[0]["signature"]; db.commit()
    except Exception as e:
        dbg(f"poll wallet err: {e}")

def poll_all_wallets():
    with POLL_LOCK:
        db=get_session()
        try:
            for w in db.query(Wallet).filter(Wallet.active==True).all():
                poll_wallet_once(db,w)
        finally:
            db.close()

# ========= MARKETS / SETTLEMENT =========
def parse_condition(cond:str, ref:float, final:float)->bool:
    cond=cond.strip()
    if cond.endswith("%"):
        try:
            pct=float(cond.strip("%").strip())
            if ref>0:
                change=((final-ref)/ref)*100.0
                return change>=pct
        except: return False
    if cond.startswith("above:"):
        try:
            thr=float(cond.split(":",1)[1]); return final>=thr
        except: return False
    return False

def lock_and_settle_markets():
    db=get_session()
    try:
        now=now_utc()
        # Lock
        for m in db.query(Market).filter(Market.status=="open", Market.lock_at<=now).all():
            m.status="locked"; db.commit()
        # Settle
        for m in db.query(Market).filter(Market.status=="locked", Market.settle_at<=now).all():
            final=get_price(m.symbol) or 0.0
            m.settle_price=final
            win_yes=parse_condition(m.condition, m.reference_price, final)
            yes=db.query(Bet).filter(Bet.market_id==m.id, Bet.side=="YES").all()
            no =db.query(Bet).filter(Bet.market_id==m.id, Bet.side=="NO").all()
            pool_yes=sum(b.stake for b in yes); pool_no=sum(b.stake for b in no)
            winners=yes if win_yes else no
            losers_pool=pool_no if win_yes else pool_yes
            winners_pool=pool_yes if win_yes else pool_no
            gross=winners_pool+losers_pool
            fee=losers_pool*DEFAULT_FEE_PCT
            distributable=gross-fee
            if winners:
                tot=sum(b.stake for b in winners)
                for b in winners:
                    share=(b.stake/tot) if tot>0 else 0
                    payout = round(distributable*share, 6)
                    b.payout=payout; b.settled=True
                    upsert_balance(db, b.user_id, "USDC", payout)
            for b in (yes+no):
                if not b.settled: b.payout=0.0; b.settled=True
            m.status="settled"; db.commit()
    finally:
        db.close()

# ========= SCHEDULER =========
scheduler=BackgroundScheduler(timezone="UTC")
scheduler.add_job(poll_all_wallets, IntervalTrigger(seconds=30), id="poll", max_instances=1, coalesce=True)
scheduler.add_job(lock_and_settle_markets, IntervalTrigger(seconds=30), id="settle", max_instances=1, coalesce=True)
scheduler.start()

# ========= UI =========
def menu(uid:int):
    kb=InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📈 Signals", callback_data="sig"),
           InlineKeyboardButton("🎲 Markets", callback_data="mkt"))
    kb.add(InlineKeyboardButton("🥇 Coinflip", callback_data="cf"))
    kb.add(InlineKeyboardButton("💰 Balance", callback_data="bal"),
           InlineKeyboardButton("ℹ️ Help", callback_data="help"))
    if is_admin(uid): kb.add(InlineKeyboardButton("🛠 Admin", callback_data="adm"))
    return kb

def header(uid:int)->str:
    db=get_session()
    try:
        u=db.query(User).filter(User.telegram_id==str(uid)).first()
        if not u: return "👋 <b>PulsePlay</b>"
        usdc=get_balance(db,u.id,"USDC"); sol=get_balance(db,u.id,"SOL")
        sub_count=db.query(Subscription).filter(Subscription.user_id==u.id, Subscription.active==True, Subscription.expires_at>now_utc()).count()
        open_mk=db.query(Market).filter(Market.status=="open").count()
        return (f"👋 <b>PulsePlay</b>\n"
                f"User: @{esc(u.username) if u.username else uid}\n"
                f"Balance: <b>{usdc:.2f} USDC</b> | {sol:.4f} SOL\n"
                f"Subscriptions: {sub_count} | Open markets: {open_mk}")
    finally:
        db.close()

@bot.message_handler(commands=["start"])
def on_start(msg: Message):
    ensure_user(msg)
    bot.send_message(msg.chat.id, header(msg.from_user.id), reply_markup=menu(msg.from_user.id))

@bot.callback_query_handler(func=lambda c: True)
def on_cb(call: CallbackQuery):
    uid=call.from_user.id
    db=get_session()
    try:
        u=db.query(User).filter(User.telegram_id==str(uid)).first()
        if not u:
            u=User(telegram_id=str(uid), username=call.from_user.username or "", role=("admin" if is_admin(uid) else "user"))
            db.add(u); db.commit()
        d=call.data

        # ===== Main nav =====
        if d=="menu":
            bot.edit_message_text(header(uid), call.message.chat.id, call.message.message_id, reply_markup=menu(uid))

        elif d=="help":
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text(esc("• Signals: subscribe per wallet per week\n• Markets: bet YES/NO on +% or above:price\n• Coinflip: simple x2 style game\n• Balance: deposit/withdraw via Solana"),
                                  call.message.chat.id, call.message.message_id, reply_markup=kb)

        # ===== Signals Marketplace =====
        elif d=="sig":
            kb=InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("📊 All", callback_data="sig_cat:all"),
                   InlineKeyboardButton("🐶 Memecoin", callback_data="sig_cat:memecoin"),
                   InlineKeyboardButton("📈 Futures", callback_data="sig_cat:futures"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text("<b>Signals Marketplace</b>\nChoose a category:", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("sig_cat:"):
            cat=d.split(":")[1]
            q=db.query(Wallet).filter(Wallet.active==True, Wallet.is_public==True)
            if cat!="all": q=q.filter(Wallet.category==cat)
            ws=q.order_by(Wallet.id.asc()).all()
            kb=InlineKeyboardMarkup()
            txt=f"📂 <b>{cat.title()}</b> wallets\n"
            if not ws: txt+="No wallets."
            else:
                for w in ws[:10]:
                    price=f"{w.weekly_price:.2f} USDC/wk" if w.weekly_price>0 else "Free"
                    kb.add(InlineKeyboardButton(f"#{w.id} {w.title or 'Trader'} • {price}", callback_data=f"w:{w.id}"))
            kb.add(InlineKeyboardButton("🔙 Categories", callback_data="sig"))
            bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("w:"):
            wid=int(d.split(":")[1]); w=db.query(Wallet).get(wid)
            if not w: bot.answer_callback_query(call.id,"Not found"); return
            active=sub_active(db,u.id,w.id)
            price=f"{w.weekly_price:.2f} USDC/wk" if w.weekly_price>0 else "Free"
            exp_text="Active" if active else "Not subscribed"
            kb=InlineKeyboardMarkup()
            if active:
                sub=db.query(Subscription).filter(Subscription.user_id==u.id, Subscription.wallet_id==w.id).first()
                left = max(0, int((sub.expires_at - now_utc()).total_seconds()//86400)) if sub and sub.expires_at else 0
                kb.add(InlineKeyboardButton(f"✅ Active ({left}d left) – Renew 7d ({price})", callback_data=f"sub_buy:{w.id}"))
                kb.add(InlineKeyboardButton("🔕 Unsubscribe", callback_data=f"sub_off:{w.id}"))
            else:
                kb.add(InlineKeyboardButton(f"🔔 Subscribe 7d ({price})", callback_data=f"sub_buy:{w.id}"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data=f"sig_cat:{w.category if w.category in ('memecoin','futures') else 'all'}"))
            txt=(f"👛 <b>Wallet #{w.id}</b>\n"
                 f"Title: <b>{esc(w.title or 'Trader')}</b>\n"
                 f"Category: <i>{w.category}</i>\n"
                 f"Address: <code>{w.address}</code>\n"
                 f"Subscription: {exp_text}\n"
                 f"Price: {price}")
            if is_admin(uid):
                kb.add(InlineKeyboardButton("✏️ Title", callback_data=f"w_edit_title:{w.id}"),
                       InlineKeyboardButton("🏷 Price", callback_data=f"w_edit_price:{w.id}"))
                kb.add(InlineKeyboardButton("🗂 Category", callback_data=f"w_edit_cat:{w.id}"),
                       InlineKeyboardButton("🗑 Remove", callback_data=f"w_del:{w.id}"))
            bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("sub_buy:"):
            wid=int(d.split(":")[1]); w=db.query(Wallet).get(wid)
            if not w: bot.answer_callback_query(call.id,"Not found"); return
            cost=max(0.0, w.weekly_price)
            bal=get_balance(db,u.id,"USDC")
            if bal < cost:
                bot.answer_callback_query(call.id, f"Need {cost:.2f} USDC")
                return
            upsert_balance(db,u.id,"USDC",-cost)
            sub=db.query(Subscription).filter(Subscription.user_id==u.id, Subscription.wallet_id==w.id).first()
            base=now_utc()
            if sub and sub.active and sub.expires_at and sub.expires_at>base:
                sub.expires_at = sub.expires_at + timedelta(days=7)
            else:
                if not sub:
                    sub=Subscription(user_id=u.id, wallet_id=w.id, active=True, expires_at=base+timedelta(days=7))
                    db.add(sub)
                else:
                    sub.active=True; sub.expires_at=base+timedelta(days=7)
            db.commit()
            bot.answer_callback_query(call.id,"Subscribed ✅")

        elif d.startswith("sub_off:"):
            wid=int(d.split(":")[1])
            sub=db.query(Subscription).filter(Subscription.user_id==u.id, Subscription.wallet_id==wid).first()
            if sub: sub.active=False; db.commit(); bot.answer_callback_query(call.id,"Unsubscribed ✅")
            else: bot.answer_callback_query(call.id,"No subscription")

        elif d.startswith("w_edit_title:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            wid=int(d.split(":")[1]); set_state(uid,"w_title",{"wid":wid})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="sig"))
            bot.edit_message_text("✏️ Send new title.", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("w_edit_price:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            wid=int(d.split(":")[1]); set_state(uid,"w_price",{"wid":wid})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="sig"))
            bot.edit_message_text("🏷 Send weekly price in USDC (e.g., 9.99).", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("w_edit_cat:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            wid=int(d.split(":")[1])
            kb=InlineKeyboardMarkup()
            for c in ("memecoin","futures","other"):
                kb.add(InlineKeyboardButton(c.title(), callback_data=f"w_cat_set:{wid}:{c}"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="sig"))
            bot.edit_message_text("🗂 Choose category:", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("w_cat_set:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            _, wid, cat = d.split(":")
            w=db.query(Wallet).get(int(wid));
            if not w: bot.answer_callback_query(call.id,"Not found"); return
            w.category=cat; db.commit(); bot.answer_callback_query(call.id,"Updated ✅")

        elif d.startswith("w_del:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            wid=int(d.split(":")[1]); w=db.query(Wallet).get(wid)
            if not w: bot.answer_callback_query(call.id,"Not found"); return
            db.delete(w); db.commit(); bot.answer_callback_query(call.id,"Removed ✅")

        # ===== Markets =====
        elif d=="mkt":
            kb=InlineKeyboardMarkup()
            mk=db.query(Market).order_by(Market.lock_at.asc()).all()
            if not mk:
                txt="🎲 No markets. "
            else:
                txt="🎲 <b>Markets</b>\n"
                for m in mk[:10]:
                    txt+=f"#{m.id} {m.symbol} {m.condition} [{m.status}]\n"
                    kb.add(InlineKeyboardButton(f"View #{m.id}", callback_data=f"m:{m.id}"))
            if is_admin(uid): kb.add(InlineKeyboardButton("➕ New", callback_data="m_new"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("m:"):
            mid=int(d.split(":")[1]); m=db.query(Market).get(mid)
            if not m: bot.answer_callback_query(call.id,"Not found"); return
            ref=m.reference_price; now=now_utc()
            left_lock=max(0, int((m.lock_at-now).total_seconds()))
            left_settle=max(0, int((m.settle_at-now).total_seconds()))
            txt=(f"📊 <b>Market #{m.id}</b>\n"
                 f"Symbol: <b>{m.symbol}</b>\n"
                 f"Condition: <code>{m.condition}</code>\n"
                 f"Timeframe: {m.timeframe}\n"
                 f"Ref price: {ref:.6f}\n"
                 f"Lock in: {left_lock//60}m {left_lock%60}s | Settle in: {left_settle//60}m {left_settle%60}s\n"
                 f"Status: {m.status}")
            kb=InlineKeyboardMarkup()
            if m.status=="open" and m.lock_at>now:
                for a in (1,5,10):
                    kb.add(InlineKeyboardButton(f"YES +{a}", callback_data=f"m_bet:{mid}:YES:{a}"),
                           InlineKeyboardButton(f"NO +{a}", callback_data=f"m_bet:{mid}:NO:{a}"))
                kb.add(InlineKeyboardButton("💵 Custom", callback_data=f"m_custom:{mid}"))
            if is_admin(uid):
                if m.status=="open": kb.add(InlineKeyboardButton("🔒 Lock", callback_data=f"m_lock:{mid}"))
                if m.status in ("open","locked"): kb.add(InlineKeyboardButton("✅ Settle", callback_data=f"m_settle:{mid}"))
                kb.add(InlineKeyboardButton("🗑 Delete", callback_data=f"m_del:{mid}"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="mkt"))
            bot.edit_message_text(txt, call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("m_bet:"):
            _, mid, side, amt = d.split(":")
            mid=int(mid); amt=float(amt)
            mkt=db.query(Market).get(mid)
            if not mkt or mkt.status!="open" or mkt.lock_at<=now_utc(): bot.answer_callback_query(call.id,"Locked"); return
            bal=get_balance(db,u.id,"USDC")
            if bal<amt: bot.answer_callback_query(call.id,f"Need {amt:.2f} USDC"); return
            upsert_balance(db,u.id,"USDC",-amt)
            db.add(Bet(market_id=mid, user_id=u.id, side=side, stake=amt)); db.commit()
            bot.answer_callback_query(call.id, f"Bet {side} {amt:.2f} ✅")

        elif d.startswith("m_custom:"):
            mid=int(d.split(":")[1]); set_state(uid,"bet_amt",{"mid":mid})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data=f"m:{mid}"))
            bot.edit_message_text("💵 Send stake amount (USDC).", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("m_lock:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            mid=int(d.split(":")[1]); m=db.query(Market).get(mid)
            if not m: bot.answer_callback_query(call.id,"Not found"); return
            m.status="locked"; m.lock_at=now_utc(); db.commit(); bot.answer_callback_query(call.id,"Locked ✅")

        elif d.startswith("m_settle:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            mid=int(d.split(":")[1]); m=db.query(Market).get(mid)
            if not m: bot.answer_callback_query(call.id,"Not found"); return
            m.settle_at=now_utc()-timedelta(seconds=1); db.commit(); lock_and_settle_markets(); bot.answer_callback_query(call.id,"Settled ✅")

        elif d.startswith("m_del:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            mid=int(d.split(":")[1]); m=db.query(Market).get(mid)
            if not m: bot.answer_callback_query(call.id,"Not found"); return
            db.delete(m); db.commit(); bot.answer_callback_query(call.id,"Deleted ✅")

        elif d=="m_new":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            kb=InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("+2% / 4h (SOL)", callback_data="m_new_preset:SOL:+2%:4h"))
            kb.add(InlineKeyboardButton("+2% / 24h (SOL)", callback_data="m_new_preset:SOL:+2%:24h"))
            kb.add(InlineKeyboardButton("above:100 / 24h (BTC)", callback_data="m_new_preset:BTC:above:100:24h"))
            kb.add(InlineKeyboardButton("✏️ Custom", callback_data="m_new_custom"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="mkt"))
            bot.edit_message_text("➕ New Market — choose a preset or Custom:", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("m_new_preset:"):
            parts=d.split(":")
            symbol=parts[1]
            if parts[2].startswith("+"):
                cond=parts[2]; tf=parts[3]
            else:
                cond=f"{parts[2]}:{parts[3]}"; tf=parts[4]
            _create_market(db, symbol, cond, tf)
            bot.answer_callback_query(call.id,"Market created ✅")

        elif d=="m_new_custom":
            set_state(uid,"new_m_symbol",{})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="mkt"))
            bot.edit_message_text("Symbol? (e.g., SOL)", call.message.chat.id, call.message.message_id, reply_markup=kb)

        # ===== Coinflip =====
        elif d=="cf":
            kb=InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("🌕 Kopf", callback_data="cf_side:HEADS"),
                   InlineKeyboardButton("🌑 Zahl", callback_data="cf_side:TAILS"))
            for a in (1,5,10):
                kb.add(InlineKeyboardButton(f"{a} USDC", callback_data=f"cf_amt:{a}"))
            kb.add(InlineKeyboardButton("💵 Custom", callback_data="cf_custom"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text("🥇 <b>Coinflip</b>\nWähle Seite und Einsatz.", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d.startswith("cf_side:"):
            side=d.split(":")[1]
            st=get_state(uid) or {"ctx":{}}
            ctx=st.get("ctx",{}); ctx["cf_side"]=side
            set_state(uid, "cf_wait_amt", ctx)
            bot.answer_callback_query(call.id, f"Seite: {'Kopf' if side=='HEADS' else 'Zahl'}")

        elif d.startswith("cf_amt:"):
            amt=float(d.split(":")[1])
            st=get_state(uid) or {"ctx":{}}
            side=(st.get("ctx") or {}).get("cf_side")
            if not side:
                set_state(uid,"cf_wait_side",{"cf_amt":amt})
                bot.answer_callback_query(call.id, f"Einsatz {amt:.2f} USDC – jetzt Kopf/Zahl wählen")
                return
            _coinflip_play(db, u.id, call, side, amt)

        elif d=="cf_custom":
            set_state(uid,"cf_custom_amt",{})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="cf"))
            bot.edit_message_text("Einsatz senden (USDC).", call.message.chat.id, call.message.message_id, reply_markup=kb)

        # ===== Balance =====
        elif d=="bal":
            usdc=get_balance(db,u.id,"USDC"); sol=get_balance(db,u.id,"SOL")
            kb=InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("💸 Deposit", callback_data="dep"),
                   InlineKeyboardButton("🔎 Verify", callback_data="dep_verify"))
            kb.add(InlineKeyboardButton("🏧 Withdraw", callback_data="wd"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text(f"💰 <b>Balance</b>\nUSDC: {usdc:.2f}\nSOL: {sol:.4f}\n"
                                  f"Central address:\n<code>{CENTRAL_DEPOSIT_ADDRESS}</code>",
                                  call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d=="dep":
            set_state(uid,"dep_amt",{})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="bal"))
            bot.edit_message_text("💸 Amount senden (USDC oder SOL) und danach Sender-Wallet.", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d=="dep_verify":
            set_state(uid,"dep_verify",{})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="bal"))
            bot.edit_message_text("🔎 Sende deine Sender-Wallet für die Verifizierung.", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d=="wd":
            set_state(uid,"wd_amt",{})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="bal"))
            bot.edit_message_text("🏧 Sende den USDC-Betrag für die Auszahlung.", call.message.chat.id, call.message.message_id, reply_markup=kb)

        # ===== Admin =====
        elif d=="adm":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            kb=InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("➕ Wallet", callback_data="adm_w_add"),
                   InlineKeyboardButton("🗂 Edit Wallets", callback_data="sig"))
            kb.add(InlineKeyboardButton("➕ Market", callback_data="m_new"))
            kb.add(InlineKeyboardButton("💳 Credit", callback_data="adm_credit"),
                   InlineKeyboardButton("💳 Debit", callback_data="adm_debit"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text("🛠 <b>Admin Panel</b>", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d=="adm_w_add":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            set_state(uid,"w_add_addr",{})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="adm"))
            bot.edit_message_text("➕ Wallet: sende Solana-Adresse.", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d=="adm_credit":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            set_state(uid,"credit_uid",{})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="adm"))
            bot.edit_message_text("💳 Credit: sende Telegram-ID.", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif d=="adm_debit":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            set_state(uid,"debit_uid",{})
            kb=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="adm"))
            bot.edit_message_text("💳 Debit: sende Telegram-ID.", call.message.chat.id, call.message.message_id, reply_markup=kb)

        else:
            bot.answer_callback_query(call.id,"…")
    finally:
        db.close()

def _create_market(db, symbol:str, cond:str, tf:str):
    ref=get_price(symbol) or 0.0
    start=now_utc()
    lock=start+timedelta(minutes=5)
    hours={"1h":1,"4h":4,"24h":24}.get(tf,4)
    settle=start+timedelta(hours=hours)
    m=Market(symbol=symbol.upper(), condition=cond, timeframe=tf, start_at=start, lock_at=lock, settle_at=settle, status="open", reference_price=ref)
    db.add(m); db.commit()

def _coinflip_play(db, user_id:int, call:CallbackQuery, side:str, amt:float):
    u=db.query(User).filter(User.telegram_id==str(user_id)).first()
    bal=get_balance(db,u.id,"USDC")
    if amt<=0: bot.answer_callback_query(call.id,"Einsatz > 0"); return
    if bal<amt: bot.answer_callback_query(call.id,f"Balance {bal:.2f}"); return
    upsert_balance(db,u.id,"USDC",-amt)
    win = (random.random() < COINFLIP_WIN_PROB)
    outcome = side if win else ("TAILS" if side=="HEADS" else "HEADS")
    payout = round(amt*COINFLIP_PAYOUT_MULT, 6) if win else 0.0
    if win: upsert_balance(db,u.id,"USDC",payout)
    kb=InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔁 Nochmal", callback_data="cf"), InlineKeyboardButton("💰 Balance", callback_data="bal"), InlineKeyboardButton("🏠 Menü", callback_data="menu"))
    bot.answer_callback_query(call.id, "Ergebnis da.")
    bot.edit_message_text(f"🥇 <b>Coinflip</b>\nSeite: {'Kopf' if side=='HEADS' else 'Zahl'} | Einsatz: {amt:.2f}\n"
                          f"Result: <b>{'Kopf' if outcome=='HEADS' else 'Zahl'}</b>\n"
                          f"{'Payout: +' + str(payout) + ' USDC' if win else 'Payout: 0.0 USDC'}",
                          call.message.chat.id, call.message.message_id, reply_markup=kb)

@bot.message_handler(func=lambda m: True)
def on_text(msg: Message):
    uid=msg.from_user.id
    st=get_state(uid)
    if not st:
        if msg.text.strip().lower() in ("/start","start"): return on_start(msg)
        return
    name=st["name"]; ctx=st["ctx"]
    db=get_session()
    try:
        u=db.query(User).filter(User.telegram_id==str(uid)).first()
        # Admin add wallet flow
        if name=="w_add_addr":
            ctx["addr"]=msg.text.strip(); set_state(uid,"w_add_title",ctx); bot.reply_to(msg,"Titel senden.")
        elif name=="w_add_title":
            ctx["title"]=msg.text.strip(); set_state(uid,"w_add_cat",ctx)
            kb=InlineKeyboardMarkup()
            for c in ("memecoin","futures","other"):
                kb.add(InlineKeyboardButton(c.title(), callback_data=f"w_add_cat:{c}"))
            bot.reply_to(msg,"Kategorie wählen:", reply_markup=kb)
        elif name=="w_add_price":
            try: price=float(msg.text.strip())
            except: bot.reply_to(msg,"Zahl senden."); return
            addr=ctx["addr"]; title=ctx["title"]; cat=ctx["cat"]
            w=Wallet(chain="solana", address=addr, title=title, category=cat, weekly_price=max(0.0,price), is_public=True, active=True, owner_user_id=u.id)
            db.add(w); db.commit(); set_state(uid,None); bot.reply_to(msg, f"✅ Wallet #{w.id} erstellt.")
        # helper for category (pressed button)
        elif name=="w_title":
            wid=ctx["wid"]; w=db.query(Wallet).get(wid)
            if not w: set_state(uid,None); bot.reply_to(msg,"Not found"); return
            w.title=msg.text.strip(); db.commit(); set_state(uid,None); bot.reply_to(msg,"Updated.")
        elif name=="w_price":
            wid=ctx["wid"]; w=db.query(Wallet).get(wid)
            if not w: set_state(uid,None); bot.reply_to(msg,"Not found"); return
            try: p=float(msg.text.strip())
            except: bot.reply_to(msg,"Zahl senden."); return
            w.weekly_price=max(0.0,p); db.commit(); set_state(uid,None); bot.reply_to(msg,"Updated.")
        # New market (custom)
        elif name=="new_m_symbol":
            ctx["sym"]=msg.text.strip().upper(); set_state(uid,"new_m_cond",ctx); bot.reply_to(msg,"Condition (e.g., +2% or above:0.25).")
        elif name=="new_m_cond":
            ctx["cond"]=msg.text.strip(); set_state(uid,"new_m_tf",ctx); bot.reply_to(msg,"Timeframe: 1h, 4h, 24h.")
        elif name=="new_m_tf":
            tf=msg.text.strip()
            if tf not in ("1h","4h","24h"): bot.reply_to(msg,"Ungültig."); return
            _create_market(db, ctx["sym"], ctx["cond"], tf); set_state(uid,None); bot.reply_to(msg,"✅ Market erstellt.")
        # Markets custom bet
        elif name=="bet_amt":
            try: amt=float(msg.text.strip())
            except: bot.reply_to(msg,"Zahl senden."); return
            if amt<=0: bot.reply_to(msg,"> 0"); return
            mid=ctx["mid"]; mkt=db.query(Market).get(mid)
            if not mkt or mkt.status!="open" or mkt.lock_at<=now_utc(): set_state(uid,None); bot.reply_to(msg,"Locked."); return
            bal=get_balance(db,u.id,"USDC")
            if bal<amt: bot.reply_to(msg,f"Need {amt:.2f}"); return
            upsert_balance(db,u.id,"USDC",-amt); db.add(Bet(market_id=mid,user_id=u.id,side="YES",stake=amt)); db.commit(); set_state(uid,None)
            bot.reply_to(msg,f"✅ Bet YES {amt:.2f}")
        # Balance flows
        elif name=="dep_amt":
            try: amt=float(msg.text.strip())
            except: bot.reply_to(msg,"Zahl senden."); return
            if amt<=0: bot.reply_to(msg,"> 0"); return
            ctx["amt"]=amt; set_state(uid,"dep_sender",ctx)
            bot.reply_to(msg,f"Sender-Wallet senden.\nZiel:\n<code>{CENTRAL_DEPOSIT_ADDRESS}</code>")
        elif name=="dep_sender":
            sender=msg.text.strip(); amt=ctx["amt"]
            p=Payment(user_id=u.id, type="deposit", method="solana", amount=amt, status="pending", meta=json.dumps({"sender":sender}))
            db.add(p); db.commit(); set_state(uid,None)
            bot.reply_to(msg,f"✅ Deposit #{p.id}. Nach Senden → Verify drücken.")
        elif name=="dep_verify":
            sender=msg.text.strip()
            sigs=get_sigs_for_address(CENTRAL_DEPOSIT_ADDRESS, limit=40); matched=False
            for s in sigs:
                sig=s["signature"]; tx=get_tx(sig)
                if sender not in get_account_keys(tx): continue
                transfers=parse_token_transfers(tx)
                inflow_sol=sum(max(0.0,t['amount']) for t in transfers if t["mint"]=="SOL")
                inflow_usdc=sum(max(0.0,t['amount']) for t in transfers if t["mint"]==USDC_SOLANA_MINT)
                if inflow_sol>0 or inflow_usdc>0:
                    credit = inflow_usdc if inflow_usdc>0 else inflow_sol*(get_price("SOL") or 0.0)
                    upsert_balance(db,u.id,"USDC",credit)
                    db.add(Payment(user_id=u.id, type="deposit", method="solana", amount=credit, status="completed", tx=sig, meta=json.dumps({"sender":sender})))
                    db.commit(); matched=True; bot.reply_to(msg,f"✅ Verified. +{credit:.4f} USDC"); break
            if not matched: bot.reply_to(msg,"Kein passender TX gefunden. Später erneut.")
            set_state(uid,None)
        elif name=="wd_amt":
            try: amt=float(msg.text.strip())
            except: bot.reply_to(msg,"Zahl senden."); return
            if amt<=0: bot.reply_to(msg,"> 0"); return
            if get_balance(db,u.id,"USDC")<amt: bot.reply_to(msg,"Balance reicht nicht."); return
            ctx["amt"]=amt; set_state(uid,"wd_dest",ctx); bot.reply_to(msg,"Ziel-Wallet (Solana) senden.")
        elif name=="wd_dest":
            amt=ctx["amt"]; dest=msg.text.strip()
            upsert_balance(db,u.id,"USDC",-amt)
            p=Payment(user_id=u.id, type="withdraw", method="solana", amount=amt, status="pending", meta=json.dumps({"dest":dest}))
            db.add(p); db.commit(); set_state(uid,None)
            bot.reply_to(msg,f"✅ Withdraw #{p.id}. Admin processed.")
        # Admin credit/debit
        elif name=="credit_uid":
            ctx["tid"]=msg.text.strip(); set_state(uid,"credit_amt",ctx); bot.reply_to(msg,"Amount (USDC).")
        elif name=="credit_amt":
            amt=float(msg.text.strip()); tu=db.query(User).filter(User.telegram_id==ctx["tid"]).first()
            if not tu: set_state(uid,None); bot.reply_to(msg,"User not found"); return
            upsert_balance(db,tu.id,"USDC",amt); set_state(uid,None); bot.reply_to(msg,"Credited.")
        elif name=="debit_uid":
            ctx["tid"]=msg.text.strip(); set_state(uid,"debit_amt",ctx); bot.reply_to(msg,"Amount (USDC).")
        elif name=="debit_amt":
            amt=float(msg.text.strip()); tu=db.query(User).filter(User.telegram_id==ctx["tid"]).first()
            if not tu: set_state(uid,None); bot.reply_to(msg,"User not found"); return
            upsert_balance(db,tu.id,"USDC",-amt); set_state(uid,None); bot.reply_to(msg,"Debited.")
        # Coinflip custom
        elif name=="cf_custom_amt":
            try: amt=float(msg.text.strip())
            except: bot.reply_to(msg,"Zahl senden."); return
            if amt<=0: bot.reply_to(msg,"> 0"); return
            set_state(uid,"cf_wait_side",{"cf_amt":amt}); bot.reply_to(msg,"Jetzt Kopf/Zahl drücken im Coinflip.")
        elif name=="cf_wait_amt":
            try: amt=float(msg.text.strip())
            except: bot.reply_to(msg,"Zahl senden."); return
            side=ctx.get("cf_side","HEADS")
            dummy=type("C",(),{})(); dummy.id="0"; dummy.message=msg
            _coinflip_play(db, uid, dummy, side, amt); set_state(uid,None)
        elif name=="cf_wait_side":
            raw=msg.text.strip().lower()
            side = "HEADS" if ("kopf" in raw or "head" in raw) else ("TAILS" if ("zahl" in raw or "tail" in raw) else None)
            if not side: bot.reply_to(msg,"Schreibe Kopf/Heads oder Zahl/Tails."); return
            amt=float(ctx.get("cf_amt",1.0))
            dummy=type("C",(),{})(); dummy.id="0"; dummy.message=msg
            _coinflip_play(db, uid, dummy, side, amt); set_state(uid,None)
        else:
            set_state(uid,None)
    except Exception as e:
        dbg(f"state err: {e}"); set_state(uid,None); bot.reply_to(msg, f"Error: {e}")
    finally:
        db.close()

# category selection button during add wallet
@bot.callback_query_handler(func=lambda c: c.data.startswith("w_add_cat:"))
def on_add_cat(call: CallbackQuery):
    uid=call.from_user.id
    st=get_state(uid)
    if not st or st["name"]!="w_add_cat": bot.answer_callback_query(call.id,"…"); return
    cat=call.data.split(":")[1]
    ctx=st["ctx"]; ctx["cat"]=cat
    set_state(uid,"w_add_price",ctx)
    bot.answer_callback_query(call.id,f"Category: {cat}")
    bot.edit_message_text("Preis pro Woche (USDC) senden.", call.message.chat.id, call.message.message_id)

# ======== Entry ========
if __name__=="__main__":
    print("PulsePlay ALL inline bot starting…")
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
