"""
db.py — Gerenciamento de sessões de usuário (SQLite)
Armazena: private key criptografada, proxy wallet, configurações por usuário
"""

import sqlite3
import os
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv()

def _build_secure_db_path() -> str:
    """Cria diretório privado para dados sensíveis e retorna caminho do SQLite."""
    explicit_db_path = os.getenv("APP_DB_PATH", "").strip()

    if explicit_db_path:
        db_path = os.path.abspath(os.path.expanduser(explicit_db_path))
        data_dir = os.path.dirname(db_path)
    else:
        # Default fora do repositório para reduzir risco de versionamento/acesso acidental.
        data_dir = os.path.abspath(os.path.expanduser(
            os.getenv("APP_DATA_DIR", "~/.local/share/polyweather_bot")
        ))
        db_file = os.getenv("APP_DB_FILE", "vault.db").strip() or "vault.db"
        db_path = os.path.join(data_dir, db_file)

    os.makedirs(data_dir, mode=0o700, exist_ok=True)
    try:
        os.chmod(data_dir, 0o700)
    except OSError:
        # Em alguns ambientes (ex.: FS gerenciado), chmod pode não ser permitido.
        pass
    return db_path


DB_PATH = _build_secure_db_path()

# Chave obrigatória para manter criptografia consistente entre reinícios/deploy.
ENCRYPT_KEY = os.getenv("ENCRYPT_KEY", "").strip()
if not ENCRYPT_KEY:
    raise RuntimeError("ENCRYPT_KEY não definida. Configure em variável de ambiente ou no arquivo .env.")

fernet = Fernet(ENCRYPT_KEY.encode() if isinstance(ENCRYPT_KEY, str) else ENCRYPT_KEY)


def _open_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA secure_delete = ON")
    try:
        os.chmod(DB_PATH, 0o600)
    except OSError:
        pass
    return conn


def _ensure_user_columns(conn: sqlite3.Connection):
    """Garante colunas novas sem quebrar bancos já existentes."""
    c = conn.cursor()
    c.execute("PRAGMA table_info(users)")
    existing = {row[1] for row in c.fetchall()}

    if "max_daily_trades" not in existing:
        c.execute("ALTER TABLE users ADD COLUMN max_daily_trades INTEGER DEFAULT 8")
    if "max_daily_exposure" not in existing:
        c.execute("ALTER TABLE users ADD COLUMN max_daily_exposure REAL DEFAULT 100.0")
    if "min_confidence" not in existing:
        c.execute("ALTER TABLE users ADD COLUMN min_confidence REAL DEFAULT 0.55")
    if "sig_type" not in existing:
        c.execute("ALTER TABLE users ADD COLUMN sig_type INTEGER DEFAULT 1")

    # Usuários que ficaram no corte antigo de 15% voltam para 10%.
    c.execute("UPDATE users SET min_edge = 0.10 WHERE min_edge = 0.15")


def init_db():
    conn = _open_conn()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id       INTEGER PRIMARY KEY,
            private_key   TEXT,
            proxy_wallet  TEXT,
            sig_type      INTEGER DEFAULT 1,
            trade_size    REAL    DEFAULT 10.0,
            min_edge      REAL    DEFAULT 0.10,
            max_daily_trades   INTEGER DEFAULT 8,
            max_daily_exposure REAL    DEFAULT 100.0,
            min_confidence     REAL    DEFAULT 0.55,
            active        INTEGER DEFAULT 1,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id       INTEGER,
            market_id     TEXT,
            question      TEXT,
            side          TEXT,
            price         REAL,
            size          REAL,
            model_prob    REAL,
            edge          REAL,
            status        TEXT DEFAULT 'pending',
            order_id      TEXT,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    _ensure_user_columns(conn)
    conn.commit()
    conn.close()


def save_user(chat_id: int, private_key: str, proxy_wallet: str, sig_type: int = 1):
    encrypted_key = fernet.encrypt(private_key.encode()).decode()
    conn = _open_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO users (chat_id, private_key, proxy_wallet, sig_type)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(chat_id) DO UPDATE SET
            private_key=excluded.private_key,
            proxy_wallet=excluded.proxy_wallet,
            sig_type=excluded.sig_type,
            active=1
    """, (chat_id, encrypted_key, proxy_wallet, int(sig_type)))
    conn.commit()
    conn.close()


def get_user(chat_id: int) -> dict | None:
    conn = _open_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return None
    keys = [
        "chat_id", "private_key", "proxy_wallet", "sig_type", "trade_size", "min_edge",
        "max_daily_trades", "max_daily_exposure", "min_confidence",
        "active", "created_at",
    ]
    user = dict(zip(keys, row))
    user["private_key"] = fernet.decrypt(user["private_key"].encode()).decode()
    return user


def update_user_settings(
    chat_id: int,
    trade_size: float = None,
    min_edge: float = None,
    max_daily_trades: int = None,
    max_daily_exposure: float = None,
    min_confidence: float = None,
):
    conn = _open_conn()
    c = conn.cursor()
    if trade_size is not None:
        c.execute("UPDATE users SET trade_size=? WHERE chat_id=?", (trade_size, chat_id))
    if min_edge is not None:
        c.execute("UPDATE users SET min_edge=? WHERE chat_id=?", (min_edge, chat_id))
    if max_daily_trades is not None:
        c.execute("UPDATE users SET max_daily_trades=? WHERE chat_id=?", (max_daily_trades, chat_id))
    if max_daily_exposure is not None:
        c.execute("UPDATE users SET max_daily_exposure=? WHERE chat_id=?", (max_daily_exposure, chat_id))
    if min_confidence is not None:
        c.execute("UPDATE users SET min_confidence=? WHERE chat_id=?", (min_confidence, chat_id))
    conn.commit()
    conn.close()


def set_user_active(chat_id: int, active: bool):
    conn = _open_conn()
    c = conn.cursor()
    c.execute("UPDATE users SET active=? WHERE chat_id=?", (int(active), chat_id))
    conn.commit()
    conn.close()


def delete_user(chat_id: int, delete_trades: bool = False):
    """Remove sessão do usuário. Opcionalmente remove histórico de trades."""
    conn = _open_conn()
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE chat_id=?", (chat_id,))
    if delete_trades:
        c.execute("DELETE FROM trades WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()


def get_all_active_users() -> list[dict]:
    conn = _open_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE active=1")
    rows = c.fetchall()
    conn.close()
    keys = [
        "chat_id", "private_key", "proxy_wallet", "sig_type", "trade_size", "min_edge",
        "max_daily_trades", "max_daily_exposure", "min_confidence",
        "active", "created_at",
    ]
    users = []
    for row in rows:
        u = dict(zip(keys, row))
        try:
            u["private_key"] = fernet.decrypt(u["private_key"].encode()).decode()
        except Exception:
            continue
        users.append(u)
    return users


def log_trade(chat_id: int, market_id: str, question: str, side: str,
              price: float, size: float, model_prob: float, edge: float,
              status: str = "pending", order_id: str = None) -> int:
    conn = _open_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO trades (chat_id, market_id, question, side, price, size, model_prob, edge, status, order_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (chat_id, market_id, question, side, price, size, model_prob, edge, status, order_id))
    trade_id = c.lastrowid
    conn.commit()
    conn.close()
    return trade_id


def update_trade_status(trade_id: int, status: str, order_id: str = None):
    conn = _open_conn()
    c = conn.cursor()
    c.execute("UPDATE trades SET status=?, order_id=? WHERE id=?", (status, order_id, trade_id))
    conn.commit()
    conn.close()


def get_user_trades(chat_id: int, limit: int = 10) -> list[dict]:
    conn = _open_conn()
    c = conn.cursor()
    c.execute("""
        SELECT * FROM trades WHERE chat_id=? ORDER BY created_at DESC LIMIT ?
    """, (chat_id, limit))
    rows = c.fetchall()
    conn.close()
    keys = ["id", "chat_id", "market_id", "question", "side", "price",
            "size", "model_prob", "edge", "status", "order_id", "created_at"]
    return [dict(zip(keys, row)) for row in rows]


def get_today_trade_stats(chat_id: int) -> dict:
    """Retorna estatísticas de execução do dia atual (UTC) para controle de risco."""
    conn = _open_conn()
    c = conn.cursor()
    c.execute(
        """
        SELECT COUNT(*), COALESCE(SUM(size), 0)
        FROM trades
        WHERE chat_id=? AND status='executed' AND date(created_at)=date('now')
        """,
        (chat_id,),
    )
    row = c.fetchone() or (0, 0)
    conn.close()
    return {
        "count": int(row[0] or 0),
        "exposure": float(row[1] or 0.0),
    }
