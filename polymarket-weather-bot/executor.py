"""
executor.py — Integração com a CLOB API da Polymarket
Autenticação L1/L2, criação e envio de ordens
"""

import asyncio
import aiohttp
import hashlib
import hmac
import time
import json
import os
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from dataclasses import dataclass

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID  = 137  # Polygon


def _get_order_retry_max_attempts() -> int:
    """Retorna quantidade de tentativas de envio de ordem (mínimo 1)."""
    raw = os.getenv("ORDER_RETRY_MAX_ATTEMPTS", "3")
    try:
        return max(1, int(raw))
    except ValueError:
        return 3


def _is_order_version_mismatch_error(err: object) -> bool:
    """Detecta rejeição transitória de versão de ordem da CLOB."""
    text = str(err).lower()
    return "order_version_mismatch" in text


def _retry_backoff_seconds(attempt: int) -> float:
    """Backoff curto e progressivo para reenvio de ordens."""
    return min(1.0, 0.25 * attempt)


@dataclass
class OrderResult:
    success:   bool
    order_id:  Optional[str]
    error:     Optional[str]
    size_filled: float = 0.0
    price_avg:   float = 0.0


class PolymarketExecutor:
    """
    Gerencia autenticação e execução de ordens na Polymarket.
    Cada usuário tem sua própria instância com suas credenciais.
    """

    def __init__(self, private_key: str, proxy_wallet: str, sig_type: int = 1):
        self.private_key   = private_key
        self.proxy_wallet  = proxy_wallet
        self.sig_type      = sig_type   # 1=Magic/Proxy, 2=Gnosis, 3=EIP-1271
        self._api_key      = None
        self._api_secret   = None
        self._api_passphrase = None
        self._authenticated = False

    async def authenticate(self) -> bool:
        """
        Realiza autenticação L1 (EIP-712) e obtém credenciais L2.
        Usa py-clob-client em thread separada para não bloquear o event loop.
        """
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._sync_authenticate
            )
            return result
        except Exception as e:
            print(f"[Executor] Erro de autenticação: {e}")
            return False

    def _sync_authenticate(self) -> bool:
        """Autenticação síncrona via py-clob-client."""
        try:
            from py_clob_client.client import ClobClient
            client = ClobClient(
                host=CLOB_HOST,
                chain_id=CHAIN_ID,
                key=self.private_key,
                signature_type=self.sig_type,
                funder=self.proxy_wallet,
            )
            creds = client.create_or_derive_api_creds()
            # Garante credenciais L2 ativas para chamadas de saldo/ordem.
            client.set_api_creds(creds)
            self._api_key        = creds.api_key
            self._api_secret     = creds.api_secret
            self._api_passphrase = creds.api_passphrase
            self._clob_client    = client
            self._authenticated  = True
            return True
        except ImportError:
            # Modo simulação quando py-clob-client não está instalado
            self._authenticated = True
            self._simulation_mode = True
            return True
        except Exception as e:
            print(f"[Executor] _sync_authenticate falhou: {e}")
            return False

    async def get_balance(self) -> Optional[float]:
        """Retorna saldo em USDC da carteira."""
        if not self._authenticated:
            ok = await self.authenticate()
            if not ok:
                return None

        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._sync_get_balance
            )
            return result
        except Exception:
            return None

    def _sync_get_balance(self) -> Optional[float]:
        try:
            if getattr(self, "_simulation_mode", False):
                return 1000.0  # saldo simulado

            from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

            resp = self._clob_client.get_balance_allowance(
                params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )

            # A API pode retornar estruturas diferentes dependendo da versão.
            # Tentamos os campos mais comuns antes de desistir.
            candidates = [
                resp.get("balance") if isinstance(resp, dict) else None,
                resp.get("availableBalance") if isinstance(resp, dict) else None,
                resp.get("balanceAllowance") if isinstance(resp, dict) else None,
            ]

            for raw in candidates:
                if raw is None:
                    continue
                try:
                    value = float(raw)
                    return value / 1e6 if value > 10_000 else value
                except (ValueError, TypeError):
                    continue

            # Se veio dict sem campos conhecidos, não quebra o fluxo do bot.
            print(f"[Executor] Saldo indisponível no formato retornado: {resp}")
            return None
        except Exception as e:
            print(f"[Executor] Erro ao buscar saldo: {e}")
            return None

    async def place_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
    ) -> OrderResult:
        """
        Envia uma ordem FOK (Fill or Kill) na Polymarket.
        FOK garante que ou executa completa ou cancela — sem ordens parciais pendentes.
        """
        if not self._authenticated:
            ok = await self.authenticate()
            if not ok:
                return OrderResult(success=False, order_id=None,
                                   error="Falha na autenticação")

        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self._sync_place_order(token_id, side, price, size)
            )
            return result
        except Exception as e:
            return OrderResult(
                success=False,
                order_id=None,
                error=(
                    f"{type(e).__name__}: {e} "
                    f"(side={side}, token={token_id}, price={price}, size={size})"
                ),
            )

    def _sync_place_order(self, token_id: str, side: str,
                          price: float, size: float) -> OrderResult:
        """Criação síncrona da ordem."""
        try:
            if getattr(self, "_simulation_mode", False):
                # Modo simulação — não executa trade real
                fake_id = f"SIM_{int(time.time())}"
                return OrderResult(
                    success=True,
                    order_id=fake_id,
                    error=None,
                    size_filled=size,
                    price_avg=price,
                )

            from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import BUY

            # Operação long-only: YES/NO significa qual token comprar.
            order_side = BUY

            if not token_id:
                return OrderResult(
                    success=False,
                    order_id=None,
                    error="Token inválido para execução",
                )

            # CLOB usa tick de 0.01; normaliza evitando erros numéricos de float.
            norm_price = float(
                Decimal(str(price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            )
            norm_price = max(0.01, min(0.99, norm_price))
            norm_size = float(
                Decimal(str(size)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            )

            order_args = OrderArgs(
                token_id=token_id,
                price=norm_price,
                size=norm_size,
                side=order_side,
            )

            # FOK: Fill or Kill — executa tudo ou cancela
            options = PartialCreateOrderOptions(
                tick_size="0.01",
                neg_risk=False,
            )

            attempts = _get_order_retry_max_attempts()
            for attempt in range(1, attempts + 1):
                try:
                    # Recria a ordem em cada tentativa para obter versão/timestamp atualizados.
                    signed_order = self._clob_client.create_order(order_args, options)
                    resp = self._clob_client.post_order(signed_order, "FOK")
                except Exception as e:
                    if _is_order_version_mismatch_error(e) and attempt < attempts:
                        # Em mismatch recorrente, renovar sessão ajuda a alinhar versão/nonce.
                        self._sync_refresh_order_session()
                        time.sleep(_retry_backoff_seconds(attempt))
                        continue
                    return OrderResult(
                        success=False,
                        order_id=None,
                        error=f"{type(e).__name__}: {e}",
                    )

                if resp and resp.get("success"):
                    return OrderResult(
                        success=True,
                        order_id=resp.get("orderID", ""),
                        error=None,
                        size_filled=float(resp.get("sizeFilled", size)),
                        price_avg=float(resp.get("price", price)),
                    )

                if isinstance(resp, dict):
                    err_msg = resp.get("errorMsg") or resp.get("error") or "Ordem rejeitada"
                else:
                    err_msg = f"Resposta inválida da CLOB: {resp}"

                if _is_order_version_mismatch_error(err_msg) and attempt < attempts:
                    self._sync_refresh_order_session()
                    time.sleep(_retry_backoff_seconds(attempt))
                    continue

                return OrderResult(
                    success=False,
                    order_id=None,
                    error=(
                        f"{err_msg} "
                        f"(side={side}, token={token_id}, price={norm_price}, size={norm_size})"
                    ),
                )

            return OrderResult(
                success=False,
                order_id=None,
                error=(
                    "Ordem rejeitada apos retries por order_version_mismatch "
                    f"(side={side}, token={token_id}, price={norm_price}, size={norm_size})"
                ),
            )

        except Exception as e:
            return OrderResult(
                success=False,
                order_id=None,
                error=f"{type(e).__name__}: {e}",
            )

    def _sync_refresh_order_session(self) -> None:
        """Renova cliente/credenciais L2 para reduzir falhas transitórias de versão."""
        try:
            self._sync_authenticate()
        except Exception:
            # Melhor esforço: o fluxo principal ainda decide sucesso/falha da ordem.
            return

    async def get_positions(self) -> list[dict]:
        """Retorna posições abertas do usuário."""
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._sync_get_positions
            )
            return result
        except Exception:
            return []

    def _sync_get_positions(self) -> list[dict]:
        try:
            if getattr(self, "_simulation_mode", False):
                return []
            resp = self._clob_client.get_positions()
            return resp if isinstance(resp, list) else []
        except Exception:
            return []


# Cache de executores por usuário (evita re-autenticar a cada scan)
_executor_cache: dict[int, PolymarketExecutor] = {}


def get_executor(chat_id: int, private_key: str, proxy_wallet: str) -> PolymarketExecutor:
    """Retorna executor cacheado ou cria um novo."""
    if chat_id not in _executor_cache:
        _executor_cache[chat_id] = PolymarketExecutor(private_key, proxy_wallet)
    return _executor_cache[chat_id]


def clear_executor(chat_id: int):
    """Remove executor do cache (ex: ao desconectar)."""
    _executor_cache.pop(chat_id, None)
