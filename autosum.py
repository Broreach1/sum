#!/usr/bin/env python3
import os
import re
import sqlite3
import asyncio
import io
import pandas as pd
from datetime import datetime, date, time, timedelta

from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InputFile
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

# ==============================
# ---------- Configuration -----
# ==============================
DB_PATH = "totals.db"
OUTPUT_FILE = "totals_export.xlsx"

# ⚠️ For safety, prefer BOT_TOKEN from environment if present.
BOT_TOKEN = os.getenv("BOT_TOKEN", "8103291457:AAFhfsVKjY05_0-cLFYxTAB71C3i_nsATZg")

# Admins who can use /dump and /recalc
ADMINS = {2122623994}  # set of ints

# Phnom Penh timezone is +07:00; if your server is already in local time, no tz conversion needed.
# If you deploy on a UTC server, consider adding pytz/zoneinfo and converting to Asia/Phnom_Penh.


# ==============================
# ---------- Database ----------
# ==============================
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS totals (
            chat_id INTEGER,
            date TEXT,
            shift TEXT,
            currency TEXT,
            total REAL,
            invoices INTEGER,
            PRIMARY KEY (chat_id, date, shift, currency)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS old_totals (
            chat_id INTEGER,
            date TEXT,
            shift TEXT,
            currency TEXT,
            total REAL,
            invoices INTEGER
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            datetime TEXT,
            business_date TEXT,    -- 👈 added to keep "Shift 3" after midnight on the prior day
            shift TEXT,
            currency TEXT,
            amount REAL
        )
    """
    )

    # Add business_date if older DB didn’t have it
    try:
        cursor.execute("SELECT business_date FROM history LIMIT 1")
    except sqlite3.OperationalError:
        cursor.execute("ALTER TABLE history ADD COLUMN business_date TEXT")

    conn.commit()
    conn.close()


# ==============================
# ---------- Shifts ------------
# ==============================
# Shift windows requested:
# shift 1: 06:00–14:00
# shift 2: 14:01–20:00
# shift 3: 20:01–06:00 (crosses midnight)

SHIFT1_START = time(6, 0, 0)
SHIFT1_END = time(18, 0, 0)
SHIFT2_START = time(18, 1, 0)
SHIFT2_END = time(22, 0, 0)
SHIFT3_START = time(20, 1, 0)
SHIFT3_END = time(6, 0, 0)  # next day

def get_shift_and_business_date(now_dt: datetime | None = None):
    """
    Returns (shift_name, business_date_str)
    business_date handles the midnight crossover for shift3.
    """
    now_dt = now_dt or datetime.now()
    t = now_dt.time()
    today = now_dt.date()

    if SHIFT1_START <= t <= SHIFT1_END:
        return "shift1", today.strftime("%Y-%m-%d")
    if SHIFT2_START <= t <= SHIFT2_END:
        return "shift2", today.strftime("%Y-%m-%d")

    # Shift 3:
    # - from 20:01 -> 23:59 -> business_date = today
    # - from 00:00 -> 06:00 -> business_date = yesterday
    if t >= SHIFT3_START:  # 20:01–23:59
        return "shift3", today.strftime("%Y-%m-%d")
    # 00:00–06:00
    if t < SHIFT3_END:
        biz = (today - timedelta(days=1)).strftime("%Y-%m-%d")
        return "shift3", biz

    # Fallback (shouldn’t reach)
    return "shift3", today.strftime("%Y-%m-%d")


def get_today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


# ==============================
# ---- Recalculate Totals ------
# ==============================
def recalc_totals_from_history():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM totals")

    cursor.execute(
        """
        SELECT chat_id, business_date, shift, currency, SUM(amount) AS s, COUNT(*) AS n
        FROM history
        GROUP BY chat_id, business_date, shift, currency
        """
    )
    rows = cursor.fetchall()

    for chat_id, biz_date, shift, currency, total, invoices in rows:
        cursor.execute(
            """INSERT OR REPLACE INTO totals
               (chat_id, date, shift, currency, total, invoices)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (chat_id, biz_date, shift, currency, float(total or 0), int(invoices or 0)),
        )

    conn.commit()
    conn.close()
    print("🔄 Totals recalculated from history.")


# ==============================
# ------- Data Operations ------
# ==============================
def update_total(chat_id: int, currency: str, amount: float) -> bool:
    now_dt = datetime.now()
    shift, business_date = get_shift_and_business_date(now_dt)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Permanent log (with business_date)
    cursor.execute(
        """INSERT INTO history (chat_id, datetime, business_date, shift, currency, amount)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            chat_id,
            now_dt.strftime("%Y-%m-%d %H:%M:%S"),
            business_date,
            shift,
            currency,
            float(amount),
        ),
    )

    # Update running totals
    cursor.execute(
        """SELECT total, invoices FROM totals
           WHERE chat_id = ? AND date = ? AND shift = ? AND currency = ?""",
        (chat_id, business_date, shift, currency),
    )
    row = cursor.fetchone()

    if row:
        total, invoices = row
        cursor.execute(
            """UPDATE totals
               SET total = ?, invoices = ?
               WHERE chat_id = ? AND date = ? AND shift = ? AND currency = ?""",
            (
                float(total) + float(amount),
                int(invoices) + 1,
                chat_id,
                business_date,
                shift,
                currency,
            ),
        )
    else:
        cursor.execute(
            """INSERT INTO totals (chat_id, date, shift, currency, total, invoices)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (chat_id, business_date, shift, currency, float(amount), 1),
        )

    conn.commit()
    conn.close()
    return True


def get_totals(chat_id: int, date_str: str | None = None, shift: str | None = None):
    date_str = date_str or get_today_str()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    if shift:
        cursor.execute(
            """SELECT currency, total, invoices FROM totals
               WHERE chat_id = ? AND date = ? AND shift = ?""",
            (chat_id, date_str, shift),
        )
    else:
        cursor.execute(
            """SELECT currency, SUM(total), SUM(invoices) FROM totals
               WHERE chat_id = ? AND date = ?
               GROUP BY currency""",
            (chat_id, date_str),
        )

    rows = cursor.fetchall()
    conn.close()

    data = {"USD": {"total": 0.0, "invoices": 0}, "KHR": {"total": 0.0, "invoices": 0}}
    for currency, total, invoices in rows:
        if currency:
            data[currency] = {
                "total": float(total or 0),
                "invoices": int(invoices or 0),
            }
    return data


def move_to_old(chat_id: int, shift: str, date_str: str):
    """
    Move current shift totals for (chat_id, date_str, shift) to old_totals, then zero the active totals.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """SELECT currency, total, invoices FROM totals
           WHERE chat_id = ? AND date = ? AND shift = ?""",
        (chat_id, date_str, shift),
    )
    rows = cursor.fetchall()

    if rows:
        for currency, total, invoices in rows:
            cursor.execute(
                """INSERT INTO old_totals (chat_id, date, shift, currency, total, invoices)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (chat_id, date_str, shift, currency, float(total or 0), int(invoices or 0)),
            )
        cursor.execute(
            """UPDATE totals SET total = 0, invoices = 0
               WHERE chat_id = ? AND date = ? AND shift = ?""",
            (chat_id, date_str, shift),
        )

    conn.commit()
    conn.close()


def get_old_totals(chat_id: int, date_str: str | None = None):
    date_str = date_str or get_today_str()
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        """SELECT currency, SUM(total), SUM(invoices)
           FROM old_totals
           WHERE chat_id = ? AND date = ?
           GROUP BY currency""",
        (chat_id, date_str),
    )
    rows = cursor.fetchall()
    conn.close()

    data = {"USD": {"total": 0.0, "invoices": 0}, "KHR": {"total": 0.0, "invoices": 0}}
    for currency, total, invoices in rows:
        if currency:
            data[currency] = {
                "total": float(total or 0),
                "invoices": int(invoices or 0),
            }
    return data


# ==============================
# --------- Exporting ----------
# ==============================
def export_db_to_excel() -> str:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            "SELECT id, chat_id, datetime, business_date, shift, currency, amount FROM history ORDER BY datetime",
            conn,
        )
    finally:
        conn.close()

    # Ensure a file exists even if empty
    if df is None or df.empty:
        df = pd.DataFrame(columns=["id", "chat_id", "datetime", "business_date", "shift", "currency", "amount"])

    df.to_excel(OUTPUT_FILE, index=False)
    print(f"Exported data to {OUTPUT_FILE}")
    return OUTPUT_FILE


async def export_excel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    path = export_db_to_excel()
    with open(path, "rb") as f:
        await update.message.reply_document(
            document=InputFile(f, filename=os.path.basename(path)),
            caption="📊 Exported full history as Excel.",
        )


def export_pdf_data(chat_id: int, label: str = "daily", shift: str | None = None, date_str: str | None = None) -> InputFile:
    date_str = date_str or get_today_str()
    data = get_totals(chat_id, date_str=date_str, shift=shift)

    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    c.setFont("Helvetica-Bold", 16)
    c.drawString(72, height - 60, f"Invoice Summary ({label.title()})")
    c.setFont("Helvetica", 11)
    c.drawString(72, height - 80, f"Business Date: {date_str}")
    c.drawString(72, height - 96, f"Generated: {now}")

    y = height - 140
    c.setFont("Helvetica", 12)
    c.drawString(72, y, f"USD: {data['USD']['total']:.2f}$ ({data['USD']['invoices']} invoices)")
    c.drawString(72, y - 20, f"KHR: {data['KHR']['total']:,.0f}៛ ({data['KHR']['invoices']} invoices)")

    c.showPage()
    c.save()
    buffer.seek(0)
    return InputFile(buffer, filename=f"totals_{label}_{date_str}.pdf")


# ==============================
# ------------ UI --------------
# ==============================
def reply_menu(is_admin: bool = False):
    rows = [
        ["🆕 New Data", "📦 Old Data"],
        ["📊 Total", "📊 Total All"],
        ["🕐 Shift 1", "🕑 Shift 2", "🌙 Shift 3"],
        ["📤 Export", "🔄 Reset"],
    ]
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=False)


async def auto_close_keyboard(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int):
    await asyncio.sleep(5)
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=ReplyKeyboardRemove(),
        )
    except Exception as e:
        # Not critical if this fails
        print(f"Keyboard remove failed: {e}")


# ==============================
# ----- Currency Parser --------
# ==============================
def extract_currency_amounts(text: str):
    text = text.replace(",", "")
    pattern = r"""
        (?:
            (?P<symbol_before>[$៛])\s*(?P<amount1>[-+]?\d*\.?\d+)
            |
            (?P<amount2>[-+]?\d*\.?\d+)\s*(?P<code_after>USD|KHR)
            |
            (?P<amount3>[-+]?\d*\.?\d+)\s*(?P<symbol_after>[$៛])
        )
    """
    matches = re.finditer(pattern, text, re.IGNORECASE | re.VERBOSE)
    results: list[tuple[float, str]] = []
    for m in matches:
        amount = None
        currency = None
        if m.group("symbol_before") and m.group("amount1"):
            amount = float(m.group("amount1"))
            currency = "USD" if m.group("symbol_before") == "$" else "KHR"
        elif m.group("amount2") and m.group("code_after"):
            amount = float(m.group("amount2"))
            currency = m.group("code_after").upper()
        elif m.group("amount3") and m.group("symbol_after"):
            amount = float(m.group("amount3"))
            currency = "USD" if m.group("symbol_after") == "$" else "KHR"
        if amount is not None and currency is not None:
            results.append((amount, currency))
    return results


# ==============================
# ------- Message Flow ---------
# ==============================
async def send_totals(update: Update, totals: dict, label: str, is_admin: bool, context: ContextTypes.DEFAULT_TYPE):
    lines = [label]
    if totals["USD"]["invoices"]:
        lines.append(f"🇺🇸 USD: {totals['USD']['total']:.2f}$ ({totals['USD']['invoices']} invoices)")
    if totals["KHR"]["invoices"]:
        lines.append(f"🇰🇭 KHR: {totals['KHR']['total']:,.0f}៛ ({totals['KHR']['invoices']} invoices)")
    if len(lines) == 1:
        lines.append("💤 No data yet.")
    sent_msg = await update.message.reply_text("\n".join(lines), reply_markup=reply_menu(is_admin))
    context.application.create_task(auto_close_keyboard(context, update.effective_chat.id, sent_msg.message_id))


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    is_admin = user_id in ADMINS
    text = (update.message.text or "").strip()

    # Determine current shift and business date
    shift_now, biz_date_now = get_shift_and_business_date()

    if text == "🆕 New Data":
        # Move only the current shift for the current business date
        move_to_old(chat_id, shift_now, biz_date_now)
        totals = get_old_totals(chat_id, date_str=biz_date_now)
        await send_totals(update, totals, f"✅ {shift_now.title()} moved to Old Data ({biz_date_now})", is_admin, context)

    elif text == "📦 Old Data":
        totals = get_old_totals(chat_id, date_str=biz_date_now)
        await send_totals(update, totals, f"📦 Old Totals ({biz_date_now})", is_admin, context)

    elif text == "📊 Total":
        totals = get_totals(chat_id, date_str=biz_date_now, shift=shift_now)
        await send_totals(update, totals, f"📊 Total for {shift_now.title()} ({biz_date_now})", is_admin, context)

    elif text == "📊 Total All":
        totals = get_totals(chat_id, date_str=biz_date_now)
        await send_totals(update, totals, f"📊 Total for All Shifts ({biz_date_now})", is_admin, context)

    elif text == "🔄 Reset":
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("UPDATE totals SET total = 0, invoices = 0 WHERE chat_id = ? AND date = ?", (chat_id, biz_date_now))
        conn.commit()
        conn.close()
        sent_msg = await update.message.reply_text("🔄 Reset done for today’s business date.", reply_markup=reply_menu(is_admin))
        context.application.create_task(auto_close_keyboard(context, chat_id, sent_msg.message_id))

    elif text in ["🕐 Shift 1", "🕑 Shift 2", "🌙 Shift 3"]:
        shift_map = {"🕐 Shift 1": "shift1", "🕑 Shift 2": "shift2", "🌙 Shift 3": "shift3"}
        sh = shift_map[text]
        file = export_pdf_data(chat_id, label=sh, shift=sh, date_str=biz_date_now)
        sent_msg = await update.message.reply_document(file, caption=f"{text} export complete ({biz_date_now}).", reply_markup=reply_menu(is_admin))
        context.application.create_task(auto_close_keyboard(context, chat_id, sent_msg.message_id))

    elif text == "📤 Export":
        path = export_db_to_excel()
        with open(path, "rb") as f:
            sent_msg = await update.message.reply_document(f, caption="📤 Full history exported.", reply_markup=reply_menu(is_admin))
        context.application.create_task(auto_close_keyboard(context, chat_id, sent_msg.message_id))

    else:
        amounts = extract_currency_amounts(text)
        if not amounts:
            # Ignore unrelated text
            return
        response_lines = []
        for amount, currency in amounts:
            update_total(chat_id, currency, amount)
        # After all inserts, show updated shift totals
        totals = get_totals(chat_id, date_str=biz_date_now, shift=shift_now)
        # Khmer confirmations
        for amount, currency in amounts:
            if currency == "USD":
                response_lines.append(f"✅ បានទទួលដុល្លា: {amount:.2f}$")
            else:
                response_lines.append(f"✅ បានទទួលប្រាក់ខ្មែរ: {amount:,.0f}៛")
        response_lines.append(
            f"សរុប"
            f"🇺🇸 USD: {totals['USD']['total']:.2f}$ ({totals['USD']['invoices']} invoices) | "
            f"🇰🇭 KHR: {totals['KHR']['total']:,.0f}៛ ({totals['KHR']['invoices']} invoices)"
        )
        sent_msg = await update.message.reply_text("\n".join(response_lines), reply_markup=reply_menu(is_admin))
        context.application.create_task(auto_close_keyboard(context, chat_id, sent_msg.message_id))


# ==============================
# -------- Admin Views ---------
# ==============================
async def view_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        sent_msg = await update.message.reply_text("🚫 Not allowed.")
        context.application.create_task(auto_close_keyboard(context, update.effective_chat.id, sent_msg.message_id))
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id, chat_id, datetime, business_date, shift, currency, amount FROM history ORDER BY datetime DESC LIMIT 50"
    )
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        sent_msg = await update.message.reply_text("📦 History empty.")
        context.application.create_task(auto_close_keyboard(context, update.effective_chat.id, sent_msg.message_id))
        return

    lines = []
    for r in rows:
        _id, c_id, dt, bdate, sh, cur, amt = r
        lines.append(f"{dt} | biz:{bdate} | {sh} | {cur} {amt}")
    text = "📂 Last 50 History:\n" + "\n".join(lines)
    # Telegram message limit ~4096 chars
    if len(text) > 4000:
        text = text[:3990] + "\n…"
    sent_msg = await update.message.reply_text(text)
    context.application.create_task(auto_close_keyboard(context, update.effective_chat.id, sent_msg.message_id))


async def recalc_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in ADMINS:
        await update.message.reply_text("🚫 Not allowed.")
        return
    recalc_totals_from_history()
    await update.message.reply_text("✅ Recalculated totals from history.")


# ==============================
# ------------ Main ------------
# ==============================
async def main():
    print("⚠️ Reminder:")
    print(" - totals.db will be created if missing")
    print(" - history table stores ALL data permanently (with business_date)")
    print(" - Use /exportexcel or 📤 Export to download full Excel")
    print(" - Use /recalc to rebuild totals from history")
    print("------------------------------------------------------")

    init_db()
    recalc_totals_from_history()  # ✅ Auto recalc before running bot

    if not BOT_TOKEN or len(BOT_TOKEN) < 20:
        raise RuntimeError("BOT_TOKEN missing. Set BOT_TOKEN env var or edit the code.")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
        await update.message.reply_text("សួស្តី! Bot is ready ✅", reply_markup=reply_menu())

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("dump", view_db))
    app.add_handler(CommandHandler("recalc", recalc_cmd))
    app.add_handler(CommandHandler("exportexcel", export_excel_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("✅ Bot is running...")
    await app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    # Allows nested event loops (VS Code/Notebook)
    import nest_asyncio
    nest_asyncio.apply()
    asyncio.run(main())
