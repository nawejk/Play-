#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PulsePlay – Full Inline Telegram Bot (Single file)
Features:
- Inline-only UI (no slash commands for users)
- Copy Signals: follow Solana wallets, live trade pings (RPC)
- Markets (YES/NO): create/lock/settle with automatic payouts
- Balance: USDC, deposit via Solana RPC verify to central address, withdraw request
- Coinflip game: 30% win chance, payout multiplier configurable
- Admin Panel via buttons
Dependencies:
  pip install pyTelegramBotAPI SQLAlchemy requests APScheduler
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
                        ForeignKey, Float, Text, UniqueConstraint)
from sqlalchemy.orm import sessionmaker, declarative_base, scoped_session

# ========= CONFIG =========
BOT_TOKEN = "8200746289:AAGbzwf7sUHVHlLDb3foXbZpj9SVGnqLeNU"   # your token
ADMIN_IDS = [7919108078]                                       # your Telegram user id(s)
CENTRAL_DEPOSIT_ADDRESS = "CKZEpwiVqAHLiSbdc8Ebf8xaQ2fofgPCNmzi4cV32M1s"  # central Solana address
SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
USDC_SOLANA_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

DEFAULT_FEE_PCT = 0.04  # 4% fee on losing pool (markets)
COINFLIP_WIN_PROB = 0.30
COINFLIP_PAYOUT_MULT = 3.0
DEBUG = True

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")

# ========= DB =========
Base = declarative_base()
engine = create_engine("sqlite:///pulseplay_full.db", connect_args={"check_same_thread": False})
SessionLocal = scoped_session(sessionmaker(bind=engine, autocommit=False, autoflush=False))

def now_utc(): return datetime.now(timezone.utc)
def dbg(x):
    if DEBUG: print(f"[{datetime.utcnow().isoformat()}] {x}")

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
    is_public = Column(Boolean, default=True)
    active = Column(Boolean, default=True)
    last_sig = Column(String, nullable=True)
    __table_args__ = (UniqueConstraint("chain", "address", name="uq_chain_address"),)

class Follow(Base):
    __tablename__ = "follows"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True)
    wallet_id = Column(Integer, ForeignKey("wallets.id"), index=True)
    __table_args__ = (UniqueConstraint("user_id", "wallet_id", name="uq_user_wallet"),)

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

class Market(Base):
    __tablename__ = "markets"
    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    condition = Column(String)  # "+10%" or "above:0.00000123"
    timeframe = Column(String, default="1h")
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
    fee = Column(Float, default=0.0)
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

def ensure_user(msg_or_call)->User:
    db=get_session()
    try:
        if isinstance(msg_or_call, Message):
            tid=str(msg_or_call.from_user.id); username=msg_or_call.from_user.username or ""
        else:
            tid=str(msg_or_call.from_user.id); username=msg_or_call.from_user.username or ""
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
            followers=db.query(Follow).filter(Follow.wallet_id==w.id).all()
            text=(f"📣 <b>Signal</b>\nWallet: <code>{w.address[:6]}…{w.address[-4:]}</code>\n"
                  f"Action: <b>{side.upper()}</b>\nToken: <b>{token}</b>\nAmount: <b>{amt:.6f}</b>\n"
                  f"Price: <b>${price:.6f}</b>\nTX: <code>{sig[:8]}…{sig[-6:]}</code>")
            for f in followers:
                try:
                    chat=int(db.query(User).get(f.user_id).telegram_id)
                    bot.send_message(chat, text)
                except Exception as e: dbg(f"send follower msg err: {e}")
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
            win_yes=False
            cond=m.condition.strip()
            if cond.endswith("%"):
                try:
                    pct=float(cond.strip("%").strip())
                    if m.reference_price>0:
                        change=((final-m.reference_price)/m.reference_price)*100.0
                        win_yes = change>=pct
                except: pass
            elif cond.startswith("above:"):
                try:
                    thr=float(cond.split(":",1)[1]); win_yes = final>=thr
                except: pass
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
def menu_kb(uid:int):
    kb=InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📈 Copy Signals", callback_data="copy"),
           InlineKeyboardButton("🎲 Markets", callback_data="bets"))
    kb.add(InlineKeyboardButton("🥇 Coinflip 30%", callback_data="cf"))
    kb.add(InlineKeyboardButton("💰 Balance", callback_data="balance"),
           InlineKeyboardButton("ℹ️ Help", callback_data="help"))
    if is_admin(uid): kb.add(InlineKeyboardButton("🛠 Admin", callback_data="admin"))
    return kb

def header(uid:int)->str:
    db=get_session()
    try:
        u=db.query(User).filter(User.telegram_id==str(uid)).first()
        if not u: return "👋 <b>PulsePlay</b>"
        usdc=get_balance(db,u.id,"USDC"); sol=get_balance(db,u.id,"SOL")
        fcount=db.query(Follow).filter(Follow.user_id==u.id).count()
        open_mk=db.query(Market).filter(Market.status=="open").count()
        return (f"👋 <b>PulsePlay</b>\n"
                f"User: @{u.username or uid}\n"
                f"Balance: <b>{usdc:.2f} USDC</b> | {sol:.4f} SOL\n"
                f"Following: {fcount} | Open markets: {open_mk}")
    finally:
        db.close()

@bot.message_handler(commands=["start"])
def on_start(message: Message):
    ensure_user(message)
    bot.send_message(message.chat.id, header(message.from_user.id), reply_markup=menu_kb(message.from_user.id))

@bot.callback_query_handler(func=lambda c: True)
def on_cb(call: CallbackQuery):
    uid=call.from_user.id
    db=get_session()
    try:
        u=db.query(User).filter(User.telegram_id==str(uid)).first()
        if not u:
            u=User(telegram_id=str(uid), username=call.from_user.username or "", role=("admin" if is_admin(uid) else "user"))
            db.add(u); db.commit()

        data=call.data

        # ===== Main =====
        if data=="menu":
            bot.edit_message_text(header(uid), call.message.chat.id, call.message.message_id, reply_markup=menu_kb(uid))

        elif data=="help":
            bot.edit_message_text("ℹ️ Help\n• Buttons only.\n• Deposit to central address, then Verify.\n• Coinflip: 30% win, x3 payout.\n• Admin panel for management.",
                                  call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="menu")))

        # ===== Copy Signals =====
        elif data=="copy":
            ws=db.query(Wallet).filter(Wallet.is_public==True, Wallet.active==True).order_by(Wallet.id.asc()).all()
            text="📈 <b>Trader Wallets</b>\n"
            kb=InlineKeyboardMarkup()
            if not ws: text+="No wallets yet."
            else:
                for w in ws[:8]:
                    kb.add(InlineKeyboardButton(f"#{w.id} {w.title or 'Trader'}", callback_data=f"w_view:{w.id}"))
            if is_admin(uid): kb.add(InlineKeyboardButton("➕ Add Wallet", callback_data="w_add"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif data.startswith("w_view:"):
            wid=int(data.split(":")[1]); w=db.query(Wallet).get(wid)
            if not w: bot.answer_callback_query(call.id,"Not found"); return
            is_follow=db.query(Follow).filter(Follow.user_id==u.id, Follow.wallet_id==w.id).first() is not None
            text=f"👛 <b>Wallet #{w.id}</b>\nAddress: <code>{w.address}</code>\nTitle: {w.title or '-'}"
            kb=InlineKeyboardMarkup()
            if is_follow: kb.add(InlineKeyboardButton("🔕 Unfollow", callback_data=f"w_unf:{w.id}"))
            else: kb.add(InlineKeyboardButton("🔔 Follow", callback_data=f"w_f:{w.id}"))
            if is_admin(uid):
                kb.add(InlineKeyboardButton("✏️ Title", callback_data=f"w_edit:{w.id}"),
                       InlineKeyboardButton("🗑 Remove", callback_data=f"w_del:{w.id}"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="copy"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif data.startswith("w_f:"):
            wid=int(data.split(":")[1])
            try: db.add(Follow(user_id=u.id, wallet_id=wid)); db.commit(); bot.answer_callback_query(call.id,"Following ✅")
            except: bot.answer_callback_query(call.id,"Already following")

        elif data.startswith("w_unf:"):
            wid=int(data.split(":")[1])
            f=db.query(Follow).filter(Follow.user_id==u.id, Follow.wallet_id==wid).first()
            if f: db.delete(f); db.commit(); bot.answer_callback_query(call.id,"Unfollowed ✅")
            else: bot.answer_callback_query(call.id,"You don't follow")

        elif data=="w_add":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            set_state(uid,"add_wallet_addr",{})
            bot.edit_message_text("➕ Add Wallet\nSend Solana address.", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="copy")))

        elif data.startswith("w_edit:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            wid=int(data.split(":")[1]); set_state(uid,"edit_wallet_title",{"wid":wid})
            bot.edit_message_text(f"✏️ Send new title for wallet #{wid}.", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="copy")))

        elif data.startswith("w_del:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            wid=int(data.split(":")[1]); w=db.query(Wallet).get(wid)
            if not w: bot.answer_callback_query(call.id,"Not found"); return
            db.delete(w); db.commit(); bot.answer_callback_query(call.id,"Removed ✅")

        # ===== Markets =====
        elif data=="bets":
            mk=db.query(Market).order_by(Market.lock_at.asc()).all()
            text="🎲 <b>Markets</b>\n"
            kb=InlineKeyboardMarkup()
            if not mk: text+="No markets."
            else:
                for mkt in mk[:8]:
                    kb.add(InlineKeyboardButton(f"#{mkt.id} {mkt.symbol} {mkt.condition} [{mkt.status}]", callback_data=f"m_view:{mkt.id}"))
            if is_admin(uid): kb.add(InlineKeyboardButton("➕ New Market", callback_data="m_new"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif data.startswith("m_view:"):
            mid=int(data.split(":")[1]); mkt=db.query(Market).get(mid)
            if not mkt: bot.answer_callback_query(call.id,"Not found"); return
            text=(f"📊 <b>Market #{mkt.id}</b>\nSymbol: {mkt.symbol}\nCondition: {mkt.condition}\n"
                  f"Status: {mkt.status}\nLock: {mkt.lock_at.strftime('%Y-%m-%d %H:%M UTC')}\n"
                  f"Settle: {mkt.settle_at.strftime('%Y-%m-%d %H:%M UTC')}\nRef: {mkt.reference_price:.6f}\nFinal: {mkt.settle_price:.6f}")
            kb=InlineKeyboardMarkup()
            if mkt.status=="open" and mkt.lock_at>now_utc():
                kb.add(InlineKeyboardButton("✅ YES +1", callback_data=f"m_bet:{mid}:YES"),
                       InlineKeyboardButton("❌ NO +1", callback_data=f"m_bet:{mid}:NO"))
                kb.add(InlineKeyboardButton("💵 Custom Stake", callback_data=f"m_custom:{mid}"))
            if is_admin(uid):
                if mkt.status=="open": kb.add(InlineKeyboardButton("🔒 Lock now", callback_data=f"m_lock:{mid}"))
                if mkt.status in ("open","locked"): kb.add(InlineKeyboardButton("✅ Settle now", callback_data=f"m_settle:{mid}"))
                kb.add(InlineKeyboardButton("🗑 Delete", callback_data=f"m_del:{mid}"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="bets"))
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif data.startswith("m_bet:"):
            mid, side = int(data.split(":")[1]), data.split(":")[2]
            mkt=db.query(Market).get(mid)
            if not mkt or mkt.status!="open" or mkt.lock_at<=now_utc(): bot.answer_callback_query(call.id,"Locked"); return
            bal=get_balance(db,u.id,"USDC"); stake=1.0
            if bal<stake: bot.answer_callback_query(call.id,f"Not enough balance ({bal:.2f})"); return
            upsert_balance(db,u.id,"USDC",-stake)
            db.add(Bet(market_id=mid, user_id=u.id, side=side, stake=stake)); db.commit()
            bot.answer_callback_query(call.id, f"Bet {side} 1.00 USDC ✅")

        elif data.startswith("m_custom:"):
            mid=int(data.split(":")[1]); set_state(uid,"bet_amount",{"mid":mid})
            bot.edit_message_text("💵 Send stake amount (USDC).", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="bets")))

        elif data.startswith("m_lock:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            mid=int(data.split(":")[1]); mkt=db.query(Market).get(mid)
            if not mkt: bot.answer_callback_query(call.id,"Not found"); return
            mkt.status="locked"; mkt.lock_at=now_utc(); db.commit(); bot.answer_callback_query(call.id,"Locked ✅")

        elif data.startswith("m_settle:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            mid=int(data.split(":")[1]); mkt=db.query(Market).get(mid)
            if not mkt: bot.answer_callback_query(call.id,"Not found"); return
            mkt.settle_at=now_utc()-timedelta(seconds=1); db.commit(); lock_and_settle_markets(); bot.answer_callback_query(call.id,"Settled ✅")

        elif data.startswith("m_del:"):
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            mid=int(data.split(":")[1]); mkt=db.query(Market).get(mid)
            if not mkt: bot.answer_callback_query(call.id,"Not found"); return
            db.delete(mkt); db.commit(); bot.answer_callback_query(call.id,"Deleted ✅")

        elif data=="m_new":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            set_state(uid,"new_market_symbol",{})
            bot.edit_message_text("➕ New Market\nSend symbol (e.g., SOL).", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="bets")))

        # ===== Coinflip =====
        elif data=="cf":
            kb=InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("🌕 HEADS", callback_data="cf_side:HEADS"),
                   InlineKeyboardButton("🌑 TAILS", callback_data="cf_side:TAILS"))
            kb.add(InlineKeyboardButton("1 USDC", callback_data="cf_amt:1"),
                   InlineKeyboardButton("5 USDC", callback_data="cf_amt:5"),
                   InlineKeyboardButton("10 USDC", callback_data="cf_amt:10"))
            kb.add(InlineKeyboardButton("💵 Custom", callback_data="cf_custom"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text(f"🥇 <b>Coinflip</b>\nWin chance: {int(COINFLIP_WIN_PROB*100)}%\nPayout: x{COINFLIP_PAYOUT_MULT:.2f}\nChoose side & stake.",
                                  call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif data.startswith("cf_side:"):
            side=data.split(":")[1]
            st=get_state(uid) or {"ctx":{}}
            ctx=st.get("ctx",{}); ctx["cf_side"]=side
            set_state(uid, "cf_wait_amt", ctx)
            bot.answer_callback_query(call.id, f"Side: {side}")

        elif data.startswith("cf_amt:"):
            amt=float(data.split(":")[1])
            st=get_state(uid) or {"ctx":{}}
            side= (st.get("ctx") or {}).get("cf_side")
            if not side:
                set_state(uid, "cf_wait_side", {"cf_amt": amt})
                bot.answer_callback_query(call.id, f"Stake: {amt} USDC — now choose HEADS/TAILS")
                return
            _coinflip_play(db, u.id, call, side, amt)

        elif data=="cf_custom":
            set_state(uid,"cf_custom_amt",{})
            bot.edit_message_text("💵 Send custom stake (USDC).", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="cf")))

        # ===== Balance =====
        elif data=="balance":
            usdc=get_balance(db,u.id,"USDC"); sol=get_balance(db,u.id,"SOL")
            kb=InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("💸 Deposit", callback_data="dep"), InlineKeyboardButton("✅ Verify", callback_data="dep_verify"))
            kb.add(InlineKeyboardButton("🏧 Withdraw", callback_data="wd"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text(f"💰 <b>Balance</b>\nUSDC: {usdc:.2f}\nSOL: {sol:.4f}\n\nCentral address:\n<code>{CENTRAL_DEPOSIT_ADDRESS}</code>",
                                  call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif data=="dep":
            set_state(uid,"deposit_amount",{})
            bot.edit_message_text(f"💸 Deposit\n1) Send amount (USDC or SOL)\n2) Then send your Sender-Wallet\nTarget:\n<code>{CENTRAL_DEPOSIT_ADDRESS}</code>",
                                  call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="balance")))

        elif data=="dep_verify":
            set_state(uid,"verify_payment",{})
            bot.edit_message_text("🔎 Verify Deposit\nSend your Sender-Wallet.", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="balance")))

        elif data=="wd":
            set_state(uid,"withdraw_amount",{})
            bot.edit_message_text("🏧 Withdraw\nSend the amount (USDC).", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="balance")))

        # ===== Admin =====
        elif data=="admin":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            kb=InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("➕ Wallet", callback_data="w_add"),
                   InlineKeyboardButton("➕ Market", callback_data="m_new"))
            kb.add(InlineKeyboardButton("➕ Credit User", callback_data="adm_credit"),
                   InlineKeyboardButton("➖ Debit User", callback_data="adm_debit"))
            kb.add(InlineKeyboardButton("🔙 Back", callback_data="menu"))
            bot.edit_message_text("🛠 <b>Admin Panel</b>", call.message.chat.id, call.message.message_id, reply_markup=kb)

        elif data=="adm_credit":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            set_state(uid,"credit_user_id",{})
            bot.edit_message_text("💳 Credit: send user Telegram-ID.", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="admin")))

        elif data=="adm_debit":
            if not is_admin(uid): bot.answer_callback_query(call.id,"Admins only"); return
            set_state(uid,"debit_user_id",{})
            bot.edit_message_text("💳 Debit: send user Telegram-ID.", call.message.chat.id, call.message.message_id, reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("🔙 Back", callback_data="admin")))

        else:
            bot.answer_callback_query(call.id,"Unknown")
    finally:
        db.close()

def _coinflip_play(db, user_id:int, call:CallbackQuery, side:str, amt:float):
    u=db.query(User).filter(User.telegram_id==str(user_id)).first()
    bal=get_balance(db,u.id,"USDC")
    if amt<=0: bot.answer_callback_query(call.id,"Stake > 0"); return
    if bal<amt: bot.answer_callback_query(call.id,f"Not enough balance ({bal:.2f})"); return
    upsert_balance(db,u.id,"USDC",-amt)
    win = (random.random() < COINFLIP_WIN_PROB)
    outcome = side if win else ("TAILS" if side=="HEADS" else "HEADS")
    payout = round(amt*COINFLIP_PAYOUT_MULT, 6) if win else 0.0
    if win: upsert_balance(db,u.id,"USDC",payout)
    bot.answer_callback_query(call.id, f"{'WIN 🎉' if win else 'LOSE 😵'} | Result: {outcome}")
    kb=InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Again", callback_data="cf"), InlineKeyboardButton("💰 Balance", callback_data="balance"), InlineKeyboardButton("🔙 Back", callback_data="menu"))
    bot.edit_message_text(f"🥇 <b>Coinflip</b>\nYour side: {side} | Stake: {amt:.2f}\nResult: <b>{outcome}</b>\n{'Payout: +' + str(payout) + ' USDC' if win else 'Payout: 0.0 USDC'}",
                          call.message.chat.id, call.message.message_id, reply_markup=kb)

@bot.message_handler(func=lambda m: True)
def on_text(message: Message):
    uid=message.from_user.id
    st=get_state(uid)
    if not st:
        if message.text.strip().lower() in ("/start","start"): return on_start(message)
        return
    name=st["name"]; ctx=st["ctx"]
    db=get_session()
    try:
        u=db.query(User).filter(User.telegram_id==str(uid)).first()
        # Wallet add/edit
        if name=="add_wallet_addr":
            ctx["addr"]=message.text.strip(); set_state(uid,"add_wallet_title",ctx); bot.reply_to(message,"Send wallet title (or '-' for empty).")
        elif name=="add_wallet_title":
            addr=ctx["addr"]; title=message.text.strip(); title="" if title=="-" else title
            w=Wallet(chain="solana", address=addr, title=title, is_public=True, active=True)
            db.add(w); db.commit(); set_state(uid,None); bot.reply_to(message,f"✅ Wallet #{w.id} added.")
        elif name=="edit_wallet_title":
            wid=ctx["wid"]; w=db.query(Wallet).get(wid)
            if not w: bot.reply_to(message,"Not found"); set_state(uid,None); return
            w.title=message.text.strip(); db.commit(); set_state(uid,None); bot.reply_to(message,"✅ Title updated.")

        # New market
        elif name=="new_market_symbol":
            sym=message.text.strip().upper(); ctx["sym"]=sym; set_state(uid,"new_market_cond",ctx); bot.reply_to(message,"Condition (e.g., +10% or above:0.5).")
        elif name=="new_market_cond":
            cond=message.text.strip(); ctx["cond"]=cond; set_state(uid,"new_market_tf",ctx); bot.reply_to(message,"Timeframe: 1h, 4h or 24h.")
        elif name=="new_market_tf":
            tf=message.text.strip()
            if tf not in ("1h","4h","24h"): bot.reply_to(message,"Invalid."); return
            sym=ctx["sym"]; cond=ctx["cond"]; ref=get_price(sym); start=now_utc()
            lock=start+timedelta(minutes=5); settle=start+timedelta(hours={"1h":1,"4h":4,"24h":24}[tf])
            m=Market(symbol=sym, condition=cond, timeframe=tf, start_at=start, lock_at=lock, settle_at=settle, status="open", reference_price=ref)
            db.add(m); db.commit(); set_state(uid,None); bot.reply_to(message,f"✅ Market #{m.id} {sym} {cond} created.")

        # Market bet custom
        elif name=="bet_amount":
            try: amt=float(message.text.strip())
            except: bot.reply_to(message,"Send a number."); return
            if amt<=0: bot.reply_to(message,"> 0"); return
            mid=ctx["mid"]; mkt=db.query(Market).get(mid)
            if not mkt or mkt.status!="open" or mkt.lock_at<=now_utc(): set_state(uid,None); bot.reply_to(message,"Locked."); return
            bal=get_balance(db,u.id,"USDC")
            if bal<amt: bot.reply_to(message,f"Not enough balance ({bal:.2f})"); return
            upsert_balance(db,u.id,"USDC",-amt); db.add(Bet(market_id=mid, user_id=u.id, side="YES", stake=amt)); db.commit(); set_state(uid,None)
            bot.reply_to(message,f"✅ Bet YES {amt:.2f} USDC")

        # Deposit flow
        elif name=="deposit_amount":
            try: amt=float(message.text.strip())
            except: bot.reply_to(message,"Send a number."); return
            if amt<=0: bot.reply_to(message,"> 0"); return
            ctx["amount"]=amt; set_state(uid,"deposit_sender",ctx)
            bot.reply_to(message,f"Send your sender wallet now.\nTarget:\n<code>{CENTRAL_DEPOSIT_ADDRESS}</code>")
        elif name=="deposit_sender":
            sender=message.text.strip(); amt=ctx["amount"]
            p=Payment(user_id=u.id, type="deposit", method="solana", amount=amt, status="pending", meta=json.dumps({"sender":sender}))
            db.add(p); db.commit(); set_state(uid,None)
            bot.reply_to(message,f"✅ Deposit request #{p.id}. After sending funds: click Verify in Balance.")

        elif name=="verify_payment":
            sender=message.text.strip()
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
                    db.commit(); matched=True; bot.reply_to(message,f"✅ Verified. +{credit:.4f} USDC"); break
            if not matched: bot.reply_to(message,"No matching tx found. Try again in 1–2 min.")
            set_state(uid,None)

        # Withdraw flow
        elif name=="withdraw_amount":
            try: amt=float(message.text.strip())
            except: bot.reply_to(message,"Send a number."); return
            if amt<=0: bot.reply_to(message,"> 0"); return
            if get_balance(db,u.id,"USDC")<amt: bot.reply_to(message,"Not enough balance."); return
            ctx["amount"]=amt; set_state(uid,"withdraw_dest",ctx); bot.reply_to(message,"Send destination wallet (Solana).")
        elif name=="withdraw_dest":
            amt=ctx["amount"]; dest=message.text.strip()
            upsert_balance(db,u.id,"USDC",-amt)
            p=Payment(user_id=u.id, type="withdraw", method="solana", amount=amt, status="pending", meta=json.dumps({"dest":dest}))
            db.add(p); db.commit(); set_state(uid,None)
            bot.reply_to(message,f"✅ Withdraw request #{p.id}. Admin will process.")

        # Admin credit/debit
        elif name=="credit_user_id":
            ctx["tid"]=message.text.strip(); set_state(uid,"credit_amount",ctx); bot.reply_to(message,"Amount (USDC).")
        elif name=="credit_amount":
            amt=float(message.text.strip()); tid=ctx["tid"]
            tu=db.query(User).filter(User.telegram_id==tid).first()
            if not tu: bot.reply_to(message,"User not found"); set_state(uid,None); return
            upsert_balance(db, tu.id, "USDC", amt); set_state(uid,None); bot.reply_to(message,f"✅ Credited {amt:.2f} USDC to {tid}")
        elif name=="debit_user_id":
            ctx["tid"]=message.text.strip(); set_state(uid,"debit_amount",ctx); bot.reply_to(message,"Amount (USDC).")
        elif name=="debit_amount":
            amt=float(message.text.strip()); tid=ctx["tid"]
            tu=db.query(User).filter(User.telegram_id==tid).first()
            if not tu: bot.reply_to(message,"User not found"); set_state(uid,None); return
            upsert_balance(db, tu.id, "USDC", -amt); set_state(uid,None); bot.reply_to(message,f"✅ Debited {amt:.2f} USDC from {tid}")

        # Coinflip custom amount / waiting flows
        elif name=="cf_custom_amt":
            try: amt=float(message.text.strip())
            except: bot.reply_to(message,"Send a number."); return
            if amt<=0: bot.reply_to(message,"> 0"); return
            set_state(uid,"cf_wait_side",{"cf_amt":amt}); bot.reply_to(message,"Now choose HEADS/TAILS in Coinflip.")
        elif name=="cf_wait_amt":
            try: amt=float(message.text.strip())
            except: bot.reply_to(message,"Send a number."); return
            side=ctx.get("cf_side","HEADS")
            dummy=type("C",(),{})(); dummy.id="0"; dummy.message=message
            _coinflip_play(db, uid, dummy, side, amt); set_state(uid,None)
        elif name=="cf_wait_side":
            raw=message.text.strip().lower()
            side = "HEADS" if "head" in raw or "kopf" in raw else ("TAILS" if "tail" in raw or "zahl" in raw else None)
            if not side: bot.reply_to(message,"Write 'Kopf/Heads' or 'Zahl/Tails'."); return
            amt=float(ctx.get("cf_amt",1.0))
            dummy=type("C",(),{})(); dummy.id="0"; dummy.message=message
            _coinflip_play(db, uid, dummy, side, amt); set_state(uid,None)

        else:
            set_state(uid,None)
    except Exception as e:
        dbg(f"state err: {e}"); set_state(uid,None); bot.reply_to(message, f"Error: {e}")
    finally:
        db.close()

# ======== Entry ========
print("PulsePlay FULL inline bot starting…")
bot.infinity_polling(timeout=60, long_polling_timeout=60)
