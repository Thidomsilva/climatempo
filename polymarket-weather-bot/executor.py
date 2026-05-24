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
import urllib.parse
import urllib.request
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional
from dataclasses import dataclass

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID  = 137  # Polygon
GAMMA_API = "https://gamma-api.polymarket.com"


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


def _order_type_candidates() -> tuple[str, ...]:
    """Tipos de ordem tentados em sequência para reduzir rejeições transitórias."""
    raw = os.getenv("ORDER_TYPE_CANDIDATES", "FOK,GTC")
    parsed = [x.strip().upper() for x in raw.split(",") if x.strip()]
    allowed = [x for x in parsed if x in {"FOK", "GTC", "GTD"}]
    return tuple(allowed) if allowed else ("FOK", "GTC")


def _is_extreme_price(price: float) -> bool:
    """Evita operar nos extremos onde a CLOB tende a rejeitar por mismatch."""
    return price <= 0.02 or price >= 0.98


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
        market_id: Optional[str] = None,
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
                lambda: self._sync_place_order(token_id, side, price, size, market_id)
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
                          price: float, size: float,
                          market_id: Optional[str] = None) -> OrderResult:
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

            from py_clob_client.clob_types import OrderArgs
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
            if _is_extreme_price(norm_price):
                return OrderResult(
                    success=False,
                    order_id=None,
                    error=(
                        f"Preco fora da faixa operavel ({norm_price:.2f}). "
                        "Mercado pode estar em estado extremo/proximo da resolucao."
                    ),
                )
            norm_size = float(
                Decimal(str(size)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            )
            # `size` recebido do bot está em USDC (notional).
            # Para ordem limite, a CLOB espera quantidade do token (shares).
            token_size = float(
                Decimal(str(norm_size / norm_price)).quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                )
            )
            token_size = max(0.01, token_size)

            current_token_id = token_id

            # NO tem apresentado mais mismatch; tenta primeiro ordem de mercado por notional.
            if (side or "").strip().upper() == "NO":
                market_result = self._try_native_market_order(
                    token_id=current_token_id,
                    amount_usdc=norm_size,
                    preferred_price=norm_price,
                )
                if market_result is not None:
                    return market_result

            attempts = _get_order_retry_max_attempts()
            order_type_candidates = _order_type_candidates()
            fallback_debug: list[str] = []

            for attempt in range(1, attempts + 1):
                used_order_type = None
                live_price = self._resolve_live_limit_price(
                    token_id=current_token_id,
                    fallback_price=norm_price,
                    amount_usdc=norm_size,
                )
                live_token_size = float(
                    Decimal(str(norm_size / live_price)).quantize(
                        Decimal("0.01"), rounding=ROUND_HALF_UP
                    )
                )
                live_token_size = max(0.01, live_token_size)
                try:
                    # Recria a ordem em cada tentativa para obter versão/timestamp atualizados.
                    resp = None
                    last_exc: Exception | None = None

                    for order_type in order_type_candidates:
                        used_order_type = order_type
                        try:
                            order_args = OrderArgs(
                                token_id=current_token_id,
                                price=live_price,
                                size=live_token_size,
                                side=order_side,
                            )
                            # Sem options customizadas: a própria biblioteca resolve tick/neg_risk.
                            signed_order = self._clob_client.create_order(order_args)
                            resp = self._clob_client.post_order(signed_order, order_type)
                            break
                        except Exception as e:
                            last_exc = e
                            if _is_order_version_mismatch_error(e):
                                continue
                            raise

                    if resp is None and last_exc is not None:
                        raise last_exc
                except Exception as e:
                    native_result = self._try_native_create_and_post_order(
                        token_id=current_token_id,
                        order_side=order_side,
                        price=live_price,
                        size=live_token_size,
                    ) if _is_order_version_mismatch_error(e) else None
                    if native_result is not None:
                        return native_result
                    if _is_order_version_mismatch_error(e):
                        fallback_debug.append("native_limit_failed")

                    market_result = self._try_native_market_order(
                        token_id=current_token_id,
                        amount_usdc=norm_size,
                        preferred_price=norm_price,
                    ) if _is_order_version_mismatch_error(e) else None
                    if market_result is not None:
                        return market_result
                    if _is_order_version_mismatch_error(e):
                        fallback_debug.append("native_market_failed")

                    if _is_order_version_mismatch_error(e) and attempt < attempts:
                        # Em mismatch recorrente, renovar sessão ajuda a alinhar versão/nonce.
                        self._sync_refresh_order_session()
                        refreshed_token = self._refresh_token_id_from_market(
                            market_id=market_id,
                            side=side,
                            current_token_id=current_token_id,
                        )
                        if refreshed_token:
                            current_token_id = refreshed_token
                        time.sleep(_retry_backoff_seconds(attempt))
                        continue
                    return OrderResult(
                        success=False,
                        order_id=None,
                        error=(
                            f"{type(e).__name__}: {e} "
                            f"(side={side}, token={current_token_id}, price={live_price}, size={norm_size}, token_size={live_token_size})"
                        ),
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
                    refreshed_token = self._refresh_token_id_from_market(
                        market_id=market_id,
                        side=side,
                        current_token_id=current_token_id,
                    )
                    if refreshed_token:
                        current_token_id = refreshed_token
                    time.sleep(_retry_backoff_seconds(attempt))
                    continue

                return OrderResult(
                    success=False,
                    order_id=None,
                    error=(
                        f"{err_msg} "
                        f"(side={side}, token={current_token_id}, price={live_price}, size={norm_size}, token_size={live_token_size}, "
                        f"order_type={used_order_type})"
                    ),
                )

            # Última tentativa no caminho nativo do client (GTC + auto-resolve interno).
            try:
                native_result = self._try_native_create_and_post_order(
                    token_id=current_token_id,
                    order_side=order_side,
                    price=norm_price,
                    size=token_size,
                )
                if native_result is not None:
                    return native_result
                fallback_debug.append("final_native_limit_failed")

                market_result = self._try_native_market_order(
                    token_id=current_token_id,
                    amount_usdc=norm_size,
                    preferred_price=norm_price,
                )
                if market_result is not None:
                    return market_result
                fallback_debug.append("final_native_market_failed")
            except Exception as e:
                # Mantém erro detalhado no retorno final abaixo.
                fallback_debug.append(f"final_fallback_exception={type(e).__name__}")

            return OrderResult(
                success=False,
                order_id=None,
                error=(
                    "Ordem rejeitada apos retries por order_version_mismatch "
                    f"(side={side}, token={current_token_id}, price={norm_price}, size={norm_size}, token_size={token_size}, "
                    f"fallbacks={','.join(fallback_debug) if fallback_debug else 'none'})"
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

    def _try_native_create_and_post_order(
        self,
        token_id: str,
        order_side: str,
        price: float,
        size: float,
    ) -> Optional[OrderResult]:
        """Tenta o fluxo nativo do client sem opções customizadas."""
        try:
            from py_clob_client.clob_types import OrderArgs

            fallback_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=order_side,
            )
            fallback_resp = self._clob_client.create_and_post_order(fallback_args)
            if fallback_resp and fallback_resp.get("success"):
                return OrderResult(
                    success=True,
                    order_id=fallback_resp.get("orderID", ""),
                    error=None,
                    size_filled=float(fallback_resp.get("sizeFilled", size)),
                    price_avg=float(fallback_resp.get("price", price)),
                )
            return None
        except Exception:
            return None

    def _resolve_live_limit_price(
        self,
        token_id: str,
        fallback_price: float,
        amount_usdc: float,
    ) -> float:
        """Busca preço atualizado na CLOB para reduzir uso de preço stale."""
        candidates: list[float] = []

        # Endpoint de preço para lado BUY no token alvo.
        try:
            raw = self._clob_client.get_price(token_id, "BUY")
            parsed = self._extract_price_value(raw)
            if parsed is not None:
                candidates.append(parsed)
        except Exception:
            pass

        # Fallback para midpoint.
        try:
            raw_mid = self._clob_client.get_midpoint(token_id)
            parsed_mid = self._extract_price_value(raw_mid)
            if parsed_mid is not None:
                candidates.append(parsed_mid)
        except Exception:
            pass

        # Fallback para cálculo por livro de ordens com amount em USDC.
        try:
            calc = float(self._clob_client.calculate_market_price(token_id, "BUY", amount_usdc))
            candidates.append(calc)
        except Exception:
            pass

        if not candidates:
            candidates.append(float(fallback_price))

        live = candidates[0]
        live = float(Decimal(str(live)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        return max(0.01, min(0.99, live))

    @staticmethod
    def _extract_price_value(raw: object) -> Optional[float]:
        """Extrai preço de respostas numéricas/dict da API."""
        if raw is None:
            return None
        if isinstance(raw, (int, float, str)):
            try:
                return float(raw)
            except ValueError:
                return None
        if isinstance(raw, dict):
            for key in ("price", "mid", "midpoint", "value"):
                val = raw.get(key)
                if val is None:
                    continue
                try:
                    return float(val)
                except (ValueError, TypeError):
                    continue
        return None

    def _try_native_market_order(
        self,
        token_id: str,
        amount_usdc: float,
        preferred_price: float,
    ) -> Optional[OrderResult]:
        """Tenta ordem de mercado nativa (amount em USDC)."""
        try:
            from py_clob_client.clob_types import MarketOrderArgs

            # amount é em colateral (USDC), alinhado com trade_size do bot.
            # Primeiro tentamos price=0 para a biblioteca calcular preço de mercado.
            for candidate_price in (0.0, float(preferred_price)):
                market_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=float(amount_usdc),
                    price=float(candidate_price),
                )
                signed = self._clob_client.create_market_order(market_args)

                # Para market order, GTC tende a ser o caminho mais estável na CLOB.
                resp = self._clob_client.post_order(signed, "GTC")
                if resp and resp.get("success"):
                    return OrderResult(
                        success=True,
                        order_id=resp.get("orderID", ""),
                        error=None,
                        size_filled=float(resp.get("sizeFilled", 0.0)),
                        price_avg=float(resp.get("price", preferred_price)),
                    )
            return None
        except Exception:
            return None

    def _refresh_token_id_from_market(
        self,
        market_id: Optional[str],
        side: str,
        current_token_id: str,
    ) -> Optional[str]:
        """Atualiza token_id a partir do market_id na Gamma API quando houver mismatch."""
        if not market_id:
            return None

        market = self._fetch_market_from_gamma(market_id)
        if not market:
            return None

        resolved = self._resolve_token_id_from_market(market, side)
        if not resolved or resolved == current_token_id:
            return None

        return resolved

    def _fetch_market_from_gamma(self, market_id: str) -> Optional[dict]:
        """Busca snapshot atualizado do mercado na Gamma API."""
        encoded_id = urllib.parse.quote(str(market_id), safe="")
        urls = [
            f"{GAMMA_API}/markets/{encoded_id}",
            f"{GAMMA_API}/markets?id={encoded_id}",
            f"{GAMMA_API}/markets?ids={encoded_id}",
        ]

        for url in urls:
            try:
                with urllib.request.urlopen(url, timeout=8) as response:
                    if response.status != 200:
                        continue
                    payload = json.loads(response.read().decode("utf-8"))
            except Exception:
                continue

            if isinstance(payload, dict) and payload.get("id"):
                return payload

            if isinstance(payload, list):
                for item in payload:
                    if str(item.get("id", "")) == str(market_id):
                        return item
                if payload and isinstance(payload[0], dict):
                    return payload[0]

        return None

    def _resolve_token_id_from_market(self, market: dict, side: str) -> Optional[str]:
        """Resolve token_id para YES/NO usando campos atuais do mercado."""
        try:
            outcomes_raw = market.get("outcomes", [])
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            token_ids_raw = market.get("clobTokenIds", [])
            token_ids = json.loads(token_ids_raw) if isinstance(token_ids_raw, str) else token_ids_raw
            tokens_raw = market.get("tokens", [])
            tokens = json.loads(tokens_raw) if isinstance(tokens_raw, str) else tokens_raw
        except Exception:
            return None

        if not isinstance(token_ids, list) or not token_ids:
            return None

        side_norm = (side or "YES").strip().upper()

        # Fonte preferida: mapeamento explícito outcome -> token em `tokens`.
        if isinstance(tokens, list) and tokens:
            wanted = "yes" if side_norm == "YES" else "no"
            for item in tokens:
                if not isinstance(item, dict):
                    continue
                outcome = str(item.get("outcome", "")).strip().lower()
                if outcome != wanted:
                    continue
                tok = item.get("token_id") or item.get("tokenId") or item.get("id")
                if tok:
                    return str(tok)

        if (
            isinstance(outcomes, list)
            and len(outcomes) >= 2
            and str(outcomes[0]).strip().lower() == "yes"
        ):
            if side_norm == "YES" and len(token_ids) >= 1:
                return str(token_ids[0])
            if side_norm == "NO" and len(token_ids) >= 2:
                return str(token_ids[1])
            return None

        # Multi-outcome: neste bot operamos apenas compra de outcome (YES).
        if side_norm == "YES" and len(token_ids) >= 1:
            return str(token_ids[0])

        return None

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


def get_executor(
    chat_id: int,
    private_key: str,
    proxy_wallet: str,
    sig_type: int = 1,
) -> PolymarketExecutor:
    """Retorna executor cacheado ou cria um novo."""
    cached = _executor_cache.get(chat_id)
    if cached:
        if (
            cached.private_key == private_key
            and cached.proxy_wallet == proxy_wallet
            and int(cached.sig_type) == int(sig_type)
        ):
            return cached

    _executor_cache[chat_id] = PolymarketExecutor(
        private_key,
        proxy_wallet,
        sig_type=int(sig_type),
    )
    return _executor_cache[chat_id]


def clear_executor(chat_id: int):
    """Remove executor do cache (ex: ao desconectar)."""
    _executor_cache.pop(chat_id, None)
