"""
bot.py — Bot principal do Telegram
Menu completo: conectar conta, configurações, aprovar trades, histórico
"""

import asyncio
import logging
import os
from dotenv import load_dotenv
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ConversationHandler, ContextTypes, filters,
)
from telegram.constants import ParseMode

import db
import scanner
import executor as exe

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "SEU_TOKEN_AQUI")

# ─── Estados da ConversationHandler ───────────────────────────────────────────
WAITING_PRIVATE_KEY  = 1
WAITING_PROXY_WALLET = 2
WAITING_TRADE_SIZE   = 3
WAITING_MIN_EDGE     = 4
WAITING_WALLET_TYPE  = 5
WAITING_API_KEY      = 6
WAITING_API_SECRET   = 7
WAITING_API_PASS     = 8

# ─── Dados temporários por usuário ────────────────────────────────────────────
_pending_key:         dict[int, str]  = {}
_pending_wallet_type: dict[int, str]  = {}
_pending_opps:        dict[int, list] = {}
_scan_tasks:          dict[int, asyncio.Task] = {}

# ─── Tipos de carteira suportados ─────────────────────────────────────────────
WALLET_TYPES = {
    "metamask": {
        "label":    "🦊 MetaMask / Browser",
        "sig_type": 3,    # POLY_GNOSIS_SAFE / EIP-1271
        "desc": (
            "Carteira conectada via extensão do browser.\n\n"
            "📋 *Como obter sua Private Key no MetaMask:*\n"
            "1. Abra o MetaMask\n"
            "2. Clique nos 3 pontos → _Account Details_\n"
            "3. _Export Private Key_\n"
            "4. Digite sua senha e copie a chave\n\n"
            "📋 *Como obter seu Proxy Wallet:*\n"
            "1. Acesse polymarket.com\n"
            "2. Conecte sua carteira\n"
            "3. Vá em _Profile_ → copie o endereço exibido"
        ),
    },
    "magic": {
        "label":    "✉️ Magic / Email",
        "sig_type": 1,    # POLY_PROXY
        "desc": (
            "Carteira criada com e-mail na Polymarket.\n\n"
            "📋 *Como exportar sua Private Key do Magic:*\n"
            "1. Acesse polymarket.com e faça login\n"
            "2. Clique no seu avatar → _Settings_\n"
            "3. _Export Wallet_ → confirme por e-mail\n"
            "4. Copie a private key exibida\n\n"
            "📋 *Proxy Wallet:*\n"
            "Será derivado automaticamente da sua private key ✅"
        ),
    },
    "gnosis": {
        "label":    "🔐 Gnosis Safe",
        "sig_type": 2,    # GNOSIS_SAFE
        "desc": (
            "Smart contract wallet (Safe multisig).\n\n"
            "📋 *O que você precisa:*\n"
            "1. Private key do EOA _signer_ do Safe\n"
            "2. Endereço do contrato Safe (0x...)\n\n"
            "⚠️ _Recomendado apenas para usuários avançados._\n"
            "Certifique-se de que o Safe tem permissão de trading na Polymarket."
        ),
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# MENU PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

def main_menu_keyboard(connected: bool) -> InlineKeyboardMarkup:
    if connected:
        buttons = [
            [InlineKeyboardButton("📊 Status da Conta",    callback_data="status")],
            [InlineKeyboardButton("🔍 Escanear Agora",     callback_data="scan_now")],
            [InlineKeyboardButton("⚙️ Configurações",      callback_data="settings")],
            [InlineKeyboardButton("📜 Histórico de Trades",callback_data="history")],
            [InlineKeyboardButton("🔌 Desconectar",        callback_data="disconnect")],
        ]
    else:
        buttons = [
            [InlineKeyboardButton("🔗 Conectar Conta Polymarket", callback_data="connect")],
            [InlineKeyboardButton("ℹ️ Como funciona",             callback_data="howto")],
        ]
    return InlineKeyboardMarkup(buttons)


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    log.info(f"[Start] recebido de chat_id={chat_id}")
    db.init_db()
    user = db.get_user(chat_id)

    text = (
        "🤖 *PolyWeather Bot*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "Bot de trading automático para mercados de clima na Polymarket.\n\n"
        "Estratégia: *Forecast vs Mercado* — compra quando o preço está "
        "abaixo do que o modelo meteorológico prevê.\n"
    )

    if user:
        text += f"\n✅ Conta conectada: `{user['proxy_wallet'][:8]}...`"
    else:
        text += "\n⚠️ Nenhuma conta conectada."

    await update.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_menu_keyboard(connected=bool(user)),
    )


async def start_alias(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Responde quando o usuário digita 'start' sem barra."""
    if update.message and update.message.text and update.message.text.strip().lower() == "start":
        await start(update, ctx)


async def ping(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong ✅ bot online")


def ensure_scan_task(chat_id: int, app):
    existing = _scan_tasks.get(chat_id)
    if existing and not existing.done():
        return
    _scan_tasks[chat_id] = asyncio.create_task(scan_loop(chat_id, app))


# ═══════════════════════════════════════════════════════════════════════════════
# CONEXÃO DA CONTA — FLUXO COMPLETO NATIVO TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════════

def _wallet_type_keyboard() -> InlineKeyboardMarkup:
    """Menu de seleção do tipo de carteira."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🦊 MetaMask / Browser", callback_data="wtype_metamask")],
        [InlineKeyboardButton("✉️  Magic / Email",      callback_data="wtype_magic")],
        [InlineKeyboardButton("🔐 Gnosis Safe",         callback_data="wtype_gnosis")],
        [InlineKeyboardButton("❌ Cancelar",            callback_data="wtype_cancel")],
    ])


async def connect_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Passo 1 — Apresenta aviso de segurança e escolha de carteira."""
    query = update.callback_query
    await query.answer()

    await query.message.reply_text(
        "🔐 *Conectar Conta Polymarket*\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Antes de continuar, leia:\n\n"
        "🛡️ Sua private key é *criptografada localmente* com AES-256 e nunca "
        "é transmitida para servidores externos.\n\n"
        "⚠️ *Use uma carteira dedicada* — não use sua carteira principal. "
        "Deposite apenas o capital que deseja operar.\n\n"
        "Qual tipo de carteira você usa na Polymarket?",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=_wallet_type_keyboard(),
    )
    return WAITING_WALLET_TYPE


async def handle_wallet_type(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Passo 2 — Usuário selecionou o tipo de carteira."""
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    data = query.data  # "wtype_metamask" / "wtype_magic" / etc.

    if data == "wtype_cancel":
        await query.message.edit_text("❌ Conexão cancelada.")
        return ConversationHandler.END

    wtype = data.replace("wtype_", "")
    wallet_info = WALLET_TYPES.get(wtype)
    if not wallet_info:
        await query.message.edit_text("❌ Tipo inválido.")
        return ConversationHandler.END

    _pending_wallet_type[chat_id] = wtype

    # Mostra instruções específicas para o tipo de carteira
    await query.message.reply_text(
        f"{wallet_info['label']}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{wallet_info['desc']}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 Cole sua *Private Key* abaixo.\n"
        f"_A mensagem será deletada automaticamente após recebida._\n\n"
        f"/cancelar para abortar.",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Cancelar", callback_data="wtype_cancel"),
        ]]),
    )
    return WAITING_PRIVATE_KEY


async def receive_private_key(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Passo 3 — Recebe e valida a private key, deleta a mensagem."""
    chat_id = update.effective_chat.id
    key = update.message.text.strip()

    # Deleta imediatamente por segurança
    try:
        await update.message.delete()
    except Exception:
        pass

    # Normaliza: adiciona 0x se necessário
    if not key.startswith("0x"):
        key = "0x" + key

    if len(key) != 66:
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=(
                "❌ *Private key inválida.*\n"
                "Deve ter 64 caracteres hex (com ou sem `0x`).\n\n"
                "Tente novamente ou /cancelar."
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_PRIVATE_KEY

    _pending_key[chat_id] = key
    wtype = _pending_wallet_type.get(chat_id, "metamask")

    # Magic: proxy wallet é derivado automaticamente — pula essa etapa
    if wtype == "magic":
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=(
                "✅ *Chave recebida e criptografada.*\n\n"
                "⏳ Derivando endereço da carteira automaticamente..."
            ),
            parse_mode=ParseMode.MARKDOWN,
        )
        return await _finalize_connection(chat_id, key, proxy_wallet=None, ctx=ctx)

    # MetaMask / Gnosis: precisa do proxy wallet
    label = "endereço do Safe (contrato)" if wtype == "gnosis" else "Proxy Wallet"
    await ctx.bot.send_message(
        chat_id=chat_id,
        text=(
            "✅ *Chave recebida e criptografada.*\n\n"
            f"📋 Agora cole o *{label}*.\n"
            f"_(começa com `0x`, 42 caracteres)_\n\n"
            "/cancelar para abortar."
        ),
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_PROXY_WALLET


async def receive_proxy_wallet(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Passo 4 — Recebe o endereço proxy/Safe."""
    chat_id = update.effective_chat.id
    wallet = update.message.text.strip()

    if not (wallet.startswith("0x") and len(wallet) == 42):
        await update.message.reply_text(
            "❌ Endereço inválido.\n"
            "Deve começar com `0x` e ter 42 caracteres.\n\n"
            "Tente novamente:",
            parse_mode=ParseMode.MARKDOWN,
        )
        return WAITING_PROXY_WALLET

    private_key = _pending_key.get(chat_id)
    if not private_key:
        await update.message.reply_text("❌ Sessão expirada. Use /start.")
        return ConversationHandler.END

    await update.message.reply_text("⏳ Verificando credenciais na Polymarket...")
    return await _finalize_connection(chat_id, private_key, proxy_wallet=wallet, ctx=ctx)


async def _finalize_connection(chat_id: int, private_key: str,
                                proxy_wallet: str | None,
                                ctx: ContextTypes.DEFAULT_TYPE):
    """Passo final — Autentica, salva e inicia o scanner."""
    try:
        wtype     = _pending_wallet_type.pop(chat_id, "metamask")
        sig_type  = WALLET_TYPES[wtype]["sig_type"]
        wallet_label = WALLET_TYPES[wtype]["label"]

        # Se proxy não foi informado, tenta derivar
        if proxy_wallet is None:
            proxy_wallet = _derive_proxy_wallet(private_key)
            if not proxy_wallet:
                await ctx.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "❌ Não foi possível derivar o endereço automaticamente.\n"
                        "Por favor, informe seu Proxy Wallet manualmente (0x...):"
                    ),
                )
                _pending_key[chat_id] = private_key
                return WAITING_PROXY_WALLET

        # Autentica
        ex = exe.PolymarketExecutor(private_key, proxy_wallet, sig_type=sig_type)
        ok = await ex.authenticate()

        _pending_key.pop(chat_id, None)

        if not ok:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=(
                    "❌ *Falha na autenticação.*\n\n"
                    "Possíveis causas:\n"
                    "• Private key incorreta\n"
                    "• Proxy Wallet não corresponde à chave\n"
                    "• Carteira sem fundos na Polymarket\n\n"
                    "Use /start para tentar novamente."
                ),
                parse_mode=ParseMode.MARKDOWN,
            )
            return ConversationHandler.END

        balance = await ex.get_balance()
        balance_text = f"${balance:.2f} USDC" if balance is not None else "indisponível no momento"

        db.save_user(chat_id, private_key, proxy_wallet)
        exe._executor_cache[chat_id] = ex

        short_wallet = f"{proxy_wallet[:6]}...{proxy_wallet[-4:]}"

        await ctx.bot.send_message(
            chat_id=chat_id,
            text=(
                "✅ *Conta conectada com sucesso!*\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔑 Tipo:    `{wallet_label}`\n"
                f"👛 Wallet:  `{short_wallet}`\n"
                f"💰 Saldo:   `{balance_text}`\n\n"
                "📡 *Scanner ativo* — você receberá alertas a cada 5 min "
                "quando houver oportunidade com edge ≥ 15%.\n\n"
                "Use as configurações para ajustar tamanho e edge mínimo."
            ),
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=main_menu_keyboard(connected=True),
        )

        ensure_scan_task(chat_id, ctx.application)
        return ConversationHandler.END
    except Exception as e:
        log.exception(f"[FinalizeConnection] erro chat_id={chat_id}: {e}")
        await ctx.bot.send_message(
            chat_id=chat_id,
            text=(
                "❌ Ocorreu um erro ao finalizar a conexão.\n"
                "Tente novamente com /start."
            ),
        )
        return ConversationHandler.END


def _derive_proxy_wallet(private_key: str) -> str | None:
    """Deriva o endereço EOA a partir da private key (para Magic/email wallets)."""
    try:
        from eth_account import Account
        account = Account.from_key(private_key)
        return account.address
    except Exception:
        return None


async def cancel_connect(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    _pending_key.pop(chat_id, None)
    _pending_wallet_type.pop(chat_id, None)

    text = "❌ Conexão cancelada.\n\nUse /start para tentar novamente."
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.edit_text(text)
    else:
        await update.message.reply_text(text)
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════════
# STATUS DA CONTA
# ═══════════════════════════════════════════════════════════════════════════════

async def show_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    user = db.get_user(chat_id)
    if not user:
        await query.message.reply_text("⚠️ Conta não conectada. Use /start.")
        return

    ex = exe.get_executor(chat_id, user["private_key"], user["proxy_wallet"])
    balance = await ex.get_balance()
    balance_text = f"${balance:.2f} USDC" if balance is not None else "indisponível no momento"
    trades  = db.get_user_trades(chat_id, limit=5)

    executed = [t for t in trades if t["status"] == "executed"]
    pending  = [t for t in trades if t["status"] == "pending"]

    text = (
        f"📊 *Status da Conta*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Saldo:        `{balance_text}`\n"
        f"✅ Trades exec.: `{len(executed)}`\n"
        f"⏳ Pendentes:   `{len(pending)}`\n"
        f"⚙️ Tamanho/trade: `${user['trade_size']:.2f}`\n"
        f"📉 Edge mínimo:  `{user['min_edge']*100:.0f}%`\n"
        f"🛡️ Conf. mínima: `{user.get('min_confidence', 0.55)*100:.0f}%`\n"
        f"📦 Limite/dia:   `{int(user.get('max_daily_trades', 8))}` trades\n"
        f"💵 Exposição/dia:`${float(user.get('max_daily_exposure', 100.0)):.0f}`\n"
        f"🔄 Scanner:      `{'Ativo' if user['active'] else 'Pausado'}`\n"
    )

    buttons = [[
        InlineKeyboardButton("⏸ Pausar Scanner" if user["active"] else "▶️ Retomar Scanner",
                             callback_data="toggle_scanner"),
        InlineKeyboardButton("🔙 Menu", callback_data="menu"),
    ]]

    await query.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÕES
# ═══════════════════════════════════════════════════════════════════════════════

async def show_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    user = db.get_user(chat_id)

    text = (
        f"⚙️ *Configurações*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💵 Tamanho por trade: `${user['trade_size']:.2f}`\n"
        f"📉 Edge mínimo para alertar: `{user['min_edge']*100:.0f}%`\n"
        f"🛡️ Confiança mínima: `{user.get('min_confidence', 0.55)*100:.0f}%`\n"
        f"📦 Máx. trades/dia: `{int(user.get('max_daily_trades', 8))}`\n"
        f"💵 Máx. exposição/dia: `${float(user.get('max_daily_exposure', 100.0)):.0f}`\n"
    )

    buttons = [
        [InlineKeyboardButton("💵 Alterar tamanho/trade", callback_data="set_size")],
        [InlineKeyboardButton("📉 Alterar edge mínimo",   callback_data="set_edge")],
        [InlineKeyboardButton("🔙 Menu",                  callback_data="menu")],
    ]

    await query.message.reply_text(
        text,
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def set_size_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "💵 Digite o novo tamanho por trade em USDC (ex: `25`):",
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_TRADE_SIZE


async def receive_trade_size(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        size = float(update.message.text.strip())
        if size < 1:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Valor inválido. Digite um número >= 1.")
        return WAITING_TRADE_SIZE

    db.update_user_settings(chat_id, trade_size=size)
    await update.message.reply_text(
        f"✅ Tamanho atualizado para `${size:.2f}` por trade.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END


async def set_edge_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "📉 Digite o edge mínimo em % para receber alertas (ex: `15` para 15%):",
        parse_mode=ParseMode.MARKDOWN,
    )
    return WAITING_MIN_EDGE


async def receive_min_edge(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    try:
        edge = float(update.message.text.strip()) / 100
        if not (0.05 <= edge <= 0.95):
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Valor inválido. Digite entre 5 e 95.")
        return WAITING_MIN_EDGE

    db.update_user_settings(chat_id, min_edge=edge)
    await update.message.reply_text(
        f"✅ Edge mínimo atualizado para `{edge*100:.0f}%`.",
        parse_mode=ParseMode.MARKDOWN,
    )
    return ConversationHandler.END


# ═══════════════════════════════════════════════════════════════════════════════
# SCAN MANUAL
# ═══════════════════════════════════════════════════════════════════════════════

async def scan_now(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    user = db.get_user(chat_id)
    if not user:
        await query.message.reply_text("⚠️ Conta não conectada.")
        return

    msg = await query.message.reply_text("🔍 Escaneando mercados de clima...")

    opps = await scanner.scan_opportunities(min_edge=user["min_edge"])

    if not opps:
        await msg.edit_text("😴 Nenhuma oportunidade encontrada agora. Tente mais tarde.")
        return

    await msg.edit_text(f"✅ {len(opps)} oportunidade(s) encontrada(s)!")
    await send_opportunities(chat_id, opps, user, ctx.application)


# ═══════════════════════════════════════════════════════════════════════════════
# ENVIO DE OPORTUNIDADES PARA APROVAÇÃO
# ═══════════════════════════════════════════════════════════════════════════════

async def send_opportunities(chat_id: int, opps: list, user: dict, app):
    """Envia cada oportunidade com botões Executar / Ignorar."""
    _pending_opps[chat_id] = {i: opp for i, opp in enumerate(opps[:5])}

    for i, opp in enumerate(opps[:5]):
        text = scanner.format_opportunity(opp)
        text += f"\n\n💵 *Tamanho:* `${user['trade_size']:.2f} USDC`"

        buttons = [[
            InlineKeyboardButton("✅ EXECUTAR",  callback_data=f"exec_{i}"),
            InlineKeyboardButton("❌ IGNORAR",  callback_data=f"skip_{i}"),
        ]]

        await app.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=InlineKeyboardMarkup(buttons),
        )


async def handle_trade_decision(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Processa aprovação ou rejeição de um trade."""
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    data = query.data  # "exec_0" ou "skip_0"
    action, idx_str = data.split("_", 1)
    idx = int(idx_str)

    user_opps = _pending_opps.get(chat_id, {})
    opp = user_opps.get(idx)

    if opp is None:
        await query.message.edit_text("⚠️ Oportunidade expirada.")
        return

    if action == "skip":
        await query.message.edit_text(
            f"❌ *Ignorado*\n_{opp.question[:60]}..._",
            parse_mode=ParseMode.MARKDOWN,
        )
        user_opps.pop(idx, None)
        return

    # EXECUTAR
    user = db.get_user(chat_id)
    if not user:
        await query.message.edit_text("⚠️ Sessão expirada. Use /start.")
        return

    stats = db.get_today_trade_stats(chat_id)
    daily_count = int(user.get("max_daily_trades", 8) or 8)
    daily_exposure = float(user.get("max_daily_exposure", 100.0) or 100.0)
    min_conf = float(user.get("min_confidence", 0.55) or 0.55)

    if opp.confidence < min_conf:
        await query.message.edit_text(
            f"🛡️ *Trade bloqueado por confiança*\n"
            f"Confiança: `{opp.confidence*100:.0f}%` | Mínimo: `{min_conf*100:.0f}%`",
            parse_mode=ParseMode.MARKDOWN,
        )
        user_opps.pop(idx, None)
        return

    if stats["count"] >= daily_count:
        await query.message.edit_text(
            f"🛑 *Limite diário de trades atingido*\n"
            f"Executados hoje: `{stats['count']}` / `{daily_count}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        user_opps.pop(idx, None)
        return

    projected = stats["exposure"] + float(user["trade_size"])
    if projected > daily_exposure:
        await query.message.edit_text(
            f"🛑 *Limite diário de exposição atingido*\n"
            f"Atual: `${stats['exposure']:.2f}` | Após trade: `${projected:.2f}`\n"
            f"Limite: `${daily_exposure:.2f}`",
            parse_mode=ParseMode.MARKDOWN,
        )
        user_opps.pop(idx, None)
        return

    await query.message.edit_text(
        f"⏳ *Executando ordem...*\n"
        f"🌡️ {opp.city} | {opp.side} | ${user['trade_size']:.2f}",
        parse_mode=ParseMode.MARKDOWN,
    )

    ex = exe.get_executor(chat_id, user["private_key"], user["proxy_wallet"])
    result = await ex.place_order(
        token_id=opp.token_id,
        side=opp.side,
        price=opp.market_price,
        size=user["trade_size"],
    )

    trade_id = db.log_trade(
        chat_id=chat_id,
        market_id=opp.market_id,
        question=opp.question,
        side=opp.side,
        price=opp.market_price,
        size=user["trade_size"],
        model_prob=opp.model_prob,
        edge=opp.edge,
        status="executed" if result.success else "failed",
        order_id=result.order_id,
    )

    if result.success:
        await query.message.edit_text(
            f"✅ *Ordem executada!*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🌡️ {opp.city} | `{opp.side}`\n"
            f"💵 Tamanho: `${result.size_filled:.2f}`\n"
            f"💹 Preço médio: `{result.price_avg:.3f}`\n"
            f"🆔 Order ID: `{result.order_id}`",
            parse_mode=ParseMode.MARKDOWN,
        )
    else:
        await query.message.edit_text(
            f"❌ *Ordem falhou*\n"
            f"Erro: `{result.error}`",
            parse_mode=ParseMode.MARKDOWN,
        )

    user_opps.pop(idx, None)


# ═══════════════════════════════════════════════════════════════════════════════
# HISTÓRICO
# ═══════════════════════════════════════════════════════════════════════════════

async def show_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    trades = db.get_user_trades(chat_id, limit=10)
    if not trades:
        await query.message.reply_text("📜 Nenhum trade registrado ainda.")
        return

    lines = ["📜 *Últimos 10 Trades*\n━━━━━━━━━━━━━━━━━━━━"]
    for t in trades:
        icon = "✅" if t["status"] == "executed" else "❌"
        lines.append(
            f"{icon} `{t['side']}` | ${t['size']:.0f} | edge {t['edge']*100:.0f}%\n"
            f"   _{t['question'][:50]}..._"
        )

    await query.message.reply_text(
        "\n".join(lines),
        parse_mode=ParseMode.MARKDOWN,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# LOOP DE SCAN AUTOMÁTICO
# ═══════════════════════════════════════════════════════════════════════════════

SCAN_INTERVAL = 5 * 60  # 5 minutos

async def scan_loop(chat_id: int, app):
    """Loop contínuo de scan para um usuário específico."""
    log.info(f"[ScanLoop] Iniciado para chat_id={chat_id}")

    while True:
        await asyncio.sleep(SCAN_INTERVAL)

        user = db.get_user(chat_id)
        if not user or not user["active"]:
            log.info(f"[ScanLoop] Pausado para {chat_id}")
            continue

        try:
            opps = await scanner.scan_opportunities(min_edge=user["min_edge"])
            if opps:
                log.info(f"[ScanLoop] {len(opps)} oportunidades para {chat_id}")
                await send_opportunities(chat_id, opps, user, app)
        except Exception as e:
            log.error(f"[ScanLoop] Erro para {chat_id}: {e}")


async def toggle_scanner(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    user = db.get_user(chat_id)
    new_state = not bool(user["active"])
    db.set_user_active(chat_id, new_state)

    label = "retomado ▶️" if new_state else "pausado ⏸"
    await query.message.reply_text(f"Scanner {label}.")

    if new_state:
        ensure_scan_task(chat_id, ctx.application)


async def disconnect(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id

    db.delete_user(chat_id, delete_trades=False)
    exe.clear_executor(chat_id)
    task = _scan_tasks.pop(chat_id, None)
    if task and not task.done():
        task.cancel()
    _pending_key.pop(chat_id, None)
    _pending_wallet_type.pop(chat_id, None)
    _pending_opps.pop(chat_id, None)

    await query.message.reply_text(
        "🔌 Conta desconectada com sucesso. Use /start para conectar novamente.",
        reply_markup=main_menu_keyboard(connected=False),
    )


async def show_howto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.message.reply_text(
        "ℹ️ *Como funciona*\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "1️⃣ Conecte sua carteira Polymarket\n"
        "2️⃣ O bot escaneia mercados de clima a cada 5 minutos\n"
        "3️⃣ Quando detecta um preço errado (edge > mínimo), te avisa\n"
        "4️⃣ Você aprova ou ignora — o bot executa a ordem\n\n"
        "📡 *Fontes:* NWS (EUA) + Open-Meteo (global)\n"
        "🧮 *Estratégia:* Forecast vs Mercado\n"
        "⚡ *Edge mínimo padrão:* 15%",
        parse_mode=ParseMode.MARKDOWN,
    )


async def back_to_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = update.effective_chat.id
    user = db.get_user(chat_id)
    await query.message.reply_text(
        "🤖 *Menu Principal*",
        parse_mode=ParseMode.MARKDOWN,
        reply_markup=main_menu_keyboard(connected=bool(user and user.get("active", 0))),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# INICIALIZAÇÃO
# ═══════════════════════════════════════════════════════════════════════════════

async def post_init(app):
    """Reinicia loops de scan para usuários ativos ao reiniciar o bot."""
    db.init_db()
    users = db.get_all_active_users()
    for user in users:
        ensure_scan_task(user["chat_id"], app)
    log.info(f"[Init] {len(users)} usuário(s) ativo(s) com scanner iniciado.")


async def on_error(update: object, ctx: ContextTypes.DEFAULT_TYPE):
    log.exception("[BotError] Exceção não tratada", exc_info=ctx.error)


def main():
    db.init_db()

    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()

    # ConversationHandler para conexão da conta
    connect_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(connect_start, pattern="^connect$")],
        states={
            WAITING_WALLET_TYPE: [
                CallbackQueryHandler(handle_wallet_type, pattern="^wtype_"),
            ],
            WAITING_PRIVATE_KEY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_private_key),
                CallbackQueryHandler(cancel_connect, pattern="^wtype_cancel$"),
            ],
            WAITING_PROXY_WALLET: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_proxy_wallet),
            ],
        },
        fallbacks=[
            CommandHandler("cancelar", cancel_connect),
            CallbackQueryHandler(cancel_connect, pattern="^wtype_cancel$"),
        ],
        allow_reentry=True,
    )

    # ConversationHandler para configurações
    settings_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(set_size_start, pattern="^set_size$"),
            CallbackQueryHandler(set_edge_start, pattern="^set_edge$"),
        ],
        states={
            WAITING_TRADE_SIZE: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_trade_size)],
            WAITING_MIN_EDGE:   [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_min_edge)],
        },
        fallbacks=[CommandHandler("cancelar", cancel_connect)],
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("ping", ping))
    app.add_handler(MessageHandler(filters.Regex(r"(?i)^start$"), start_alias))
    app.add_handler(connect_conv)
    app.add_handler(settings_conv)

    app.add_handler(CallbackQueryHandler(show_status,        pattern="^status$"))
    app.add_handler(CallbackQueryHandler(scan_now,           pattern="^scan_now$"))
    app.add_handler(CallbackQueryHandler(show_settings,      pattern="^settings$"))
    app.add_handler(CallbackQueryHandler(show_history,       pattern="^history$"))
    app.add_handler(CallbackQueryHandler(toggle_scanner,     pattern="^toggle_scanner$"))
    app.add_handler(CallbackQueryHandler(disconnect,         pattern="^disconnect$"))
    app.add_handler(CallbackQueryHandler(show_howto,         pattern="^howto$"))
    app.add_handler(CallbackQueryHandler(back_to_menu,       pattern="^menu$"))
    app.add_handler(CallbackQueryHandler(handle_trade_decision, pattern="^(exec|skip)_\\d+$"))
    app.add_error_handler(on_error)

    log.info("🤖 PolyWeather Bot iniciado.")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
