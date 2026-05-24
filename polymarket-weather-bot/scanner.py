"""
scanner.py — Motor de detecção de edge (v2 — dinâmico)

Mudanças vs v1:
- Descobre cidades diretamente dos títulos dos mercados da Gamma API
- Não usa lista fixa de cidades — monitora TUDO que a Polymarket oferece
- Fonte de resolução correta: Wunderground (não NWS/NOAA)
- Mapeamento ICAO completo para as ~40 cidades ativas
- Suporte a High Temp e Low Temp
- Filtros de liquidez e tempo até resolução
"""

import asyncio
import aiohttp
import re
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Optional

# ─── Fonte de resolução real: Wunderground ─────────────────────────────────────
# A Polymarket usa SEMPRE o Wunderground como fonte oficial.
# Cada cidade aponta para uma estação ICAO específica nessa URL:
# https://www.wunderground.com/history/daily/{país}/{cidade}/{ICAO}
#
# Mapeamento completo baseado nas regras dos mercados ativos:
CITY_STATION_MAP: dict[str, dict] = {
    # ── Ásia ──────────────────────────────────────────────────────────────────
    "tokyo":        {"icao": "RJTT", "lat": 35.5494, "lon": 139.7798, "unit": "C"},
    "seoul":        {"icao": "RKSI", "lat": 37.4691, "lon": 126.4510, "unit": "C"},
    "hong kong":    {"icao": "VHHH", "lat": 22.3080, "lon": 113.9185, "unit": "C"},
    "shanghai":     {"icao": "ZSPD", "lat": 31.1443, "lon": 121.8083, "unit": "C"},
    "taipei":       {"icao": "RCSS", "lat": 25.0694, "lon": 121.5522, "unit": "C"},
    "singapore":    {"icao": "WSSS", "lat":  1.3644, "lon": 103.9915, "unit": "C"},
    "beijing":      {"icao": "ZBAA", "lat": 40.0799, "lon": 116.5844, "unit": "C"},
    "shenzhen":     {"icao": "ZGSZ", "lat": 22.6393, "lon": 113.8107, "unit": "C"},
    "qingdao":      {"icao": "ZSQD", "lat": 36.2661, "lon": 120.3744, "unit": "C"},
    "wuhan":        {"icao": "ZHHH", "lat": 30.7838, "lon": 114.2081, "unit": "C"},
    "bangkok":      {"icao": "VTBS", "lat": 13.6811, "lon": 100.7475, "unit": "C"},
    "osaka":        {"icao": "RJBB", "lat": 34.4347, "lon": 135.2440, "unit": "C"},

    # ── Europa ────────────────────────────────────────────────────────────────
    "london":       {"icao": "EGLL", "lat": 51.4775, "lon": -0.46139, "unit": "C"},
    "paris":        {"icao": "LFPB", "lat": 48.9694, "lon":  2.44139, "unit": "C"},
    "warsaw":       {"icao": "EPWA", "lat": 52.1657, "lon": 20.9671,  "unit": "C"},
    "milan":        {"icao": "LIMC", "lat": 45.6306, "lon":  8.72811, "unit": "C"},
    "munich":       {"icao": "EDDM", "lat": 48.3537, "lon": 11.7750,  "unit": "C"},
    "moscow":       {"icao": "UUWW", "lat": 55.5914, "lon": 37.2614,  "unit": "C"},
    "amsterdam":    {"icao": "EHAM", "lat": 52.3086, "lon":  4.76389, "unit": "C"},
    "madrid":       {"icao": "LEMD", "lat": 40.4719, "lon": -3.56264, "unit": "C"},
    "rome":         {"icao": "LIRF", "lat": 41.8003, "lon": 12.2389,  "unit": "C"},
    "berlin":       {"icao": "EDDB", "lat": 52.3667, "lon": 13.5033,  "unit": "C"},
    "istanbul":     {"icao": "LTBA", "lat": 40.9769, "lon": 28.8146,  "unit": "C"},

    # ── Oriente Médio ─────────────────────────────────────────────────────────
    "tel aviv":     {"icao": "LLBG", "lat": 32.0114, "lon": 34.8867,  "unit": "C"},
    "dubai":        {"icao": "OMDB", "lat": 25.2528, "lon": 55.3644,  "unit": "C"},

    # ── América do Norte ──────────────────────────────────────────────────────
    "new york":     {"icao": "KLGA", "lat": 40.7769, "lon": -73.8740, "unit": "F"},
    "los angeles":  {"icao": "KLAX", "lat": 33.9425, "lon": -118.408, "unit": "F"},
    "chicago":      {"icao": "KORD", "lat": 41.9742, "lon": -87.9073, "unit": "F"},
    "dallas":       {"icao": "KDAL", "lat": 32.8471, "lon": -96.8518, "unit": "F"},
    "miami":        {"icao": "KMIA", "lat": 25.7959, "lon": -80.2870, "unit": "F"},
    "atlanta":      {"icao": "KATL", "lat": 33.6407, "lon": -84.4277, "unit": "F"},
    "seattle":      {"icao": "KSEA", "lat": 47.4502, "lon": -122.309, "unit": "F"},
    "san francisco":{"icao": "KSFO", "lat": 37.6213, "lon": -122.379, "unit": "F"},
    "philadelphia": {"icao": "KPHL", "lat": 39.8719, "lon": -75.2411, "unit": "F"},
    "boston":       {"icao": "KBOS", "lat": 42.3631, "lon": -71.0064, "unit": "F"},
    "washington":   {"icao": "KDCA", "lat": 38.8521, "lon": -77.0377, "unit": "F"},
    "minneapolis":  {"icao": "KMSP", "lat": 44.8848, "lon": -93.2223, "unit": "F"},
    "jacksonville": {"icao": "KJAX", "lat": 30.4941, "lon": -81.6879, "unit": "F"},
    "san antonio":  {"icao": "KSAT", "lat": 29.5337, "lon": -98.4698, "unit": "F"},
    "toronto":      {"icao": "CYYZ", "lat": 43.6772, "lon": -79.6306, "unit": "C"},
    "montreal":     {"icao": "CYUL", "lat": 45.4706, "lon": -73.7408, "unit": "C"},
    "vancouver":    {"icao": "CYVR", "lat": 49.1947, "lon": -123.184, "unit": "C"},

    # ── América do Sul ────────────────────────────────────────────────────────
    "são paulo":    {"icao": "SBGR", "lat": -23.435, "lon": -46.4731, "unit": "C"},
    "sao paulo":    {"icao": "SBGR", "lat": -23.435, "lon": -46.4731, "unit": "C"},
    "buenos aires": {"icao": "SAEZ", "lat": -34.822, "lon": -58.5358, "unit": "C"},

    # ── Oceania / África ──────────────────────────────────────────────────────
    "sydney":       {"icao": "YSSY", "lat": -33.946, "lon": 151.1772, "unit": "C"},
    "wellington":   {"icao": "NZWN", "lat": -41.327, "lon": 174.8050, "unit": "C"},
    "cape town":    {"icao": "FACT", "lat": -33.969, "lon": 18.59972, "unit": "C"},

    # ── América Central ───────────────────────────────────────────────────────
    "panama city":  {"icao": "MPMG", "lat":  8.9794, "lon": -79.5558, "unit": "C"},
}

GAMMA_API  = "https://gamma-api.polymarket.com"
METEO_API  = "https://api.open-meteo.com/v1/forecast"
WU_HISTORY = "https://api.weather.com/v1/location/{icao}:9:US/observations/historical.json"

# Filtros de qualidade
MIN_LIQUIDITY_USD  = 200    # liquidez mínima no order book
MIN_HOURS_TO_CLOSE = float(os.getenv("MIN_HOURS_TO_CLOSE", "0.5"))
MAX_HOURS_TO_CLOSE = float(os.getenv("MAX_HOURS_TO_CLOSE", "48"))

# Custos e margem operacional (fee + spread + slippage), abatidos do edge bruto.
TRADING_COST_BUFFER = float(os.getenv("TRADING_COST_BUFFER", "0.03"))
FORECAST_MODELS = [
    m.strip() for m in os.getenv("FORECAST_MODELS", "gfs_seamless").split(",")
    if m.strip()
]

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclass
class Opportunity:
    city:         str
    question:     str
    market_id:    str
    token_id:     str
    market_type:  str          # "high" ou "low"
    side:         str          # "YES" ou "NO"
    market_price: float
    model_prob:   float
    edge:         float  # edge líquido (já descontado custo)
    unit:         str
    bucket_low:   Optional[float]
    bucket_high:  Optional[float]
    liquidity:    float = 0.0
    hours_to_close: float = 0.0
    icao:         str = ""
    gross_edge:   float = 0.0
    confidence:   float = 0.0


@dataclass
class ForecastEstimate:
    temp: float
    spread: float
    model_count: int


# ─── Extração de cidade do título ─────────────────────────────────────────────

def extract_city_from_question(question: str) -> Optional[tuple[str, str, dict]]:
    """
    Extrai cidade, tipo (high/low) e dados da estação a partir do título.
    Ex: "Highest temperature in Hong Kong on May 24?" → ("hong kong", "high", {...})
    """
    q = question.lower()

    # Detecta tipo
    if "highest temperature" in q or "high temp" in q:
        mtype = "high"
    elif "lowest temperature" in q or "low temp" in q:
        mtype = "low"
    else:
        return None

    # Extrai cidade — tenta do mais longo para o mais curto (evita match parcial)
    cities_sorted = sorted(CITY_STATION_MAP.keys(), key=len, reverse=True)
    for city_key in cities_sorted:
        if city_key in q:
            return city_key, mtype, CITY_STATION_MAP[city_key]

    return None


# ─── Previsão meteorológica via Open-Meteo ────────────────────────────────────

async def fetch_forecast(
    session: aiohttp.ClientSession,
    lat: float,
    lon: float,
    unit: str,
    mtype: str,
    target_date: Optional[date] = None,
) -> Optional[ForecastEstimate]:
    """
    Busca previsão para a data-alvo usando um pequeno ensemble de modelos da Open-Meteo.
    Retorna temperatura média e dispersão entre modelos (incerteza).
    """
    temp_unit = "fahrenheit" if unit == "F" else "celsius"
    field = "temperature_2m_max" if mtype == "high" else "temperature_2m_min"

    target_iso = target_date.isoformat() if target_date else None
    model_values: list[float] = []

    for model in FORECAST_MODELS:
        params = {
            "latitude":          lat,
            "longitude":         lon,
            "daily":             field,
            "temperature_unit":  temp_unit,
            "forecast_days":     10,
            "timezone":          "auto",
            "models":            model,
        }

        try:
            async with session.get(
                METEO_API,
                params=params,
                timeout=aiohttp.ClientTimeout(total=12)
            ) as r:
                if r.status != 200:
                    continue
                data = await r.json()
                daily = data.get("daily", {})
                temps = daily.get(field, [])
                days = daily.get("time", [])
                if not temps:
                    continue

                selected: Optional[float] = None
                if target_iso and days and target_iso in days:
                    idx = days.index(target_iso)
                    if idx < len(temps):
                        selected = float(temps[idx])
                if selected is None:
                    selected = float(temps[0])

                model_values.append(selected)
        except Exception:
            continue

    if not model_values:
        return None

    if len(model_values) == 1:
        spread = 0.6 if unit == "C" else 1.0
    else:
        spread = (max(model_values) - min(model_values)) / 2.0

    mean_temp = sum(model_values) / len(model_values)
    return ForecastEstimate(temp=mean_temp, spread=spread, model_count=len(model_values))


def extract_target_date(question: str, end_dt_str: Optional[str]) -> Optional[date]:
    """Extrai data do título (ex: on May 24). Fallback: data de fechamento do mercado."""
    q = (question or "").lower()
    m = re.search(r"on\s+([a-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?", q)
    if m:
        month_name = m.group(1)
        day = int(m.group(2))
        year_raw = m.group(3)
        month = MONTHS.get(month_name)
        if month:
            now_utc = datetime.now(timezone.utc)
            year = int(year_raw) if year_raw else now_utc.year
            try:
                candidate = date(year, month, day)
                if not year_raw and candidate < now_utc.date():
                    candidate = date(year + 1, month, day)
                return candidate
            except ValueError:
                pass

    if end_dt_str:
        try:
            end_dt = datetime.fromisoformat(end_dt_str.replace("Z", "+00:00"))
            return end_dt.date()
        except Exception:
            return None

    return None


# ─── Cálculo de probabilidade por bucket ─────────────────────────────────────

def bucket_probability(
    forecast: float,
    low: Optional[float],
    high: Optional[float],
    unit: str,
    mtype: str,
    extra_sigma: float = 0.0,
) -> float:
    """
    Estima P(temperatura cair no bucket) com distribuição normal.
    Sigma calibrado por tipo: high temp tem mais variância que low temp.
    """
    from scipy.stats import norm

    # Sigma em graus — calibrado empiricamente
    # High temp: ±2°C / ±3.6°F  |  Low temp: ±1.5°C / ±2.7°F
    if unit == "C":
        sigma = 2.0 if mtype == "high" else 1.5
    else:
        sigma = 3.6 if mtype == "high" else 2.7
    sigma = max(0.8, sigma + extra_sigma)

    if low is not None and high is not None:
        return norm.cdf(high, forecast, sigma) - norm.cdf(low, forecast, sigma)
    elif low is None and high is not None:
        return norm.cdf(high, forecast, sigma)
    elif low is not None and high is None:
        return 1 - norm.cdf(low, forecast, sigma)
    return 0.5


# ─── Parse de bucket de temperatura do título ────────────────────────────────

def parse_bucket(outcome_label: str, unit: str) -> tuple[Optional[float], Optional[float]]:
    """
    Extrai range de temperatura do label de cada outcome.
    Formatos suportados:
      "24°C"          → exato (±0.5)
      "25°C or higher"→ (25, None)
      "23°C or lower" → (None, 23)
      "68–70°F"       → (68, 70)
    """
    label = outcome_label.strip()

    # Formato: "X or higher" / "X or more"
    m = re.search(r'([\d.]+)\s*(?:°[CF])?\s*or\s+(?:higher|more|above)', label, re.I)
    if m:
        return float(m.group(1)), None

    # Formato: "X or lower" / "X or less" / "below X"
    m = re.search(r'([\d.]+)\s*(?:°[CF])?\s*or\s+(?:lower|less|below)', label, re.I)
    if m:
        return None, float(m.group(1))
    m = re.search(r'below\s+([\d.]+)', label, re.I)
    if m:
        return None, float(m.group(1))

    # Formato: "X–Y" ou "X-Y" com unidades opcionais
    m = re.search(r'([\d.]+)\s*(?:°[CF])?\s*[-–]\s*([\d.]+)', label, re.I)
    if m:
        return float(m.group(1)), float(m.group(2))

    # Formato: exato "24°C" ou "24" — trata como bucket de ±0.5
    m = re.search(r'^([\d.]+)\s*(?:°[CF])?$', label.strip())
    if m:
        v = float(m.group(1))
        return v - 0.5, v + 0.5

    return None, None


def confidence_score(liquidity: float, hours_left: float, gross_edge: float) -> float:
    """
    Score de confiança de 0 a 1 combinando:
    - liquidez (quanto maior, melhor)
    - proximidade da resolução (evita horizonte longo)
    - edge bruto (sinal mais forte)
    """
    liq_norm = max(0.0, min(1.0, (liquidity - MIN_LIQUIDITY_USD) / 1500.0))
    edge_norm = max(0.0, min(1.0, gross_edge / 0.30))
    # Quanto mais próximo do fechamento (mas >= mínimo), maior confiança.
    time_window = max(1.0, MAX_HOURS_TO_CLOSE - MIN_HOURS_TO_CLOSE)
    time_norm = 1.0 - max(0.0, min(1.0, (hours_left - MIN_HOURS_TO_CLOSE) / time_window))

    return 0.40 * edge_norm + 0.35 * liq_norm + 0.25 * time_norm


# ─── Busca de mercados na Gamma API ──────────────────────────────────────────

async def fetch_all_weather_markets(session: aiohttp.ClientSession) -> list[dict]:
    """
    Busca mercados de temperatura ativos a partir dos eventos de clima.
    A API de markets não está filtrando corretamente por tag em todos os casos,
    então usamos events + tag_slug=weather e extraímos os markets embutidos.
    """
    markets: list[dict] = []
    seen_ids: set[str] = set()

    offset = 0
    while True:
        params = {
            "tag_slug": "weather",
            "active": "true",
            "closed": "false",
            "limit": 100,
            "offset": offset,
        }

        try:
            async with session.get(
                f"{GAMMA_API}/events",
                params=params,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as r:
                if r.status != 200:
                    break
                events = await r.json()
                if not events:
                    break

                for event in events:
                    title = (event.get("title") or "").lower()
                    if (
                        "highest temperature in" not in title
                        and "lowest temperature in" not in title
                    ):
                        continue

                    for m in (event.get("markets") or []):
                        if not m.get("active", False) or m.get("closed", False):
                            continue
                        mid = m.get("id", "")
                        if not mid or mid in seen_ids:
                            continue
                        seen_ids.add(mid)
                        markets.append(m)

                if len(events) < 100:
                    break
                offset += 100
        except Exception:
            break

    return markets


def parse_bucket_from_question(question: str, unit: str) -> tuple[Optional[float], Optional[float]]:
    """Extrai bucket de mercados binários (Yes/No) a partir do texto da pergunta."""
    q = question.strip().lower()

    # be 29°C or higher
    m = re.search(r"be\s+(-?[\d.]+)\s*°?[cf]?\s*or\s*(?:higher|above|more)", q)
    if m:
        return float(m.group(1)), None

    # be 15°C or below/lower
    m = re.search(r"be\s+(-?[\d.]+)\s*°?[cf]?\s*or\s*(?:below|lower|less)", q)
    if m:
        return None, float(m.group(1))

    # be 16°C on ...  -> exato (±0.5)
    m = re.search(r"be\s+(-?[\d.]+)\s*°?[cf]?\s+on", q)
    if m:
        v = float(m.group(1))
        return v - 0.5, v + 0.5

    return None, None


def extract_yes_price(market: dict, outcomes: list, prices_raw) -> Optional[float]:
    """Retorna preço de YES usando outcomePrices ou fallback de book/último trade."""
    # Formato clássico
    if prices_raw and isinstance(prices_raw, list) and len(prices_raw) > 0:
        try:
            p = float(prices_raw[0])
            if 0.0 <= p <= 1.0:
                return p
        except (ValueError, TypeError):
            pass

    # Fallback para formato novo
    for key in ("yesPrice", "bestAsk", "bestBid", "lastTradePrice", "price"):
        raw = market.get(key)
        if raw is None:
            continue
        try:
            p = float(raw)
            if 0.0 <= p <= 1.0:
                return p
        except (ValueError, TypeError):
            continue

    return None


# ─── Filtros de qualidade ────────────────────────────────────────────────────

def passes_quality_filters(market: dict) -> tuple[bool, float, float]:
    """
    Verifica liquidez e janela de tempo até resolução.
    Retorna (passa, liquidez_usd, horas_até_fechamento).
    """
    # Liquidez
    try:
        liquidity = float(market.get("liquidity", 0) or 0)
    except (ValueError, TypeError):
        liquidity = 0.0

    if liquidity < MIN_LIQUIDITY_USD:
        return False, liquidity, 0.0

    # Tempo até fechamento
    end_dt_str = market.get("endDate") or market.get("end_date_iso")
    if not end_dt_str:
        return False, liquidity, 0.0

    try:
        end_dt = datetime.fromisoformat(end_dt_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        hours_left = (end_dt - now).total_seconds() / 3600
    except Exception:
        return False, liquidity, 0.0

    if hours_left < MIN_HOURS_TO_CLOSE or hours_left > MAX_HOURS_TO_CLOSE:
        return False, liquidity, hours_left

    return True, liquidity, hours_left


# ─── Engine principal ────────────────────────────────────────────────────────

async def scan_opportunities(min_edge: float = 0.15) -> list["Opportunity"]:
    """
    Varre todos os mercados de clima na Polymarket dinamicamente.
    Retorna oportunidades ordenadas por edge decrescente.
    """
    opportunities: list[Opportunity] = []

    async with aiohttp.ClientSession() as session:
        # 1. Busca todos os mercados de clima
        markets = await fetch_all_weather_markets(session)

        # 2. Agrupa mercados por cidade para buscar forecast uma vez por cidade
        city_markets: dict[str, list[dict]] = {}
        city_meta:    dict[str, tuple]       = {}  # key -> (mtype, station_data, target_date)

        for market in markets:
            question = market.get("question", "")
            parsed = extract_city_from_question(question)
            if not parsed:
                continue
            city_key, mtype, station = parsed
            target_date = extract_target_date(
                question,
                market.get("endDate") or market.get("end_date_iso"),
            )
            date_key = target_date.isoformat() if target_date else "unknown"
            key = f"{city_key}|{mtype}|{date_key}"
            city_markets.setdefault(key, []).append(market)
            city_meta[key] = (mtype, station, target_date)

        # 3. Busca forecasts em paralelo para todas as cidades únicas
        forecast_tasks = {}
        for key, (mtype, station, target_date) in city_meta.items():
            task = asyncio.create_task(
                fetch_forecast(session, station["lat"], station["lon"],
                               station["unit"], mtype, target_date)
            )
            forecast_tasks[key] = task

        forecasts = {k: await t for k, t in forecast_tasks.items()}

        # 4. Para cada mercado, calcula edge por outcome
        for key, mlist in city_markets.items():
            mtype, station, _ = city_meta[key]
            forecast_est = forecasts.get(key)
            if forecast_est is None:
                continue
            forecast_temp = forecast_est.temp
            extra_sigma = forecast_est.spread * 0.5

            for market in mlist:
                # Filtros de qualidade
                ok, liquidity, hours_left = passes_quality_filters(market)
                if not ok:
                    continue

                question = market.get("question", "")
                market_id = market.get("id", "")
                outcomes_raw = market.get("outcomes", [])
                prices_raw = market.get("outcomePrices", [])

                # outcomes pode vir como JSON string.
                if isinstance(outcomes_raw, str):
                    try:
                        outcomes = json.loads(outcomes_raw)
                    except Exception:
                        outcomes = []
                else:
                    outcomes = outcomes_raw

                if not outcomes:
                    continue

                # Token IDs para execução
                try:
                    token_ids_raw = market.get("clobTokenIds", "[]")
                    token_ids = (
                        json.loads(token_ids_raw)
                        if isinstance(token_ids_raw, str)
                        else token_ids_raw
                    )
                except Exception:
                    token_ids = []

                is_binary_yes_no = len(outcomes) == 2 and str(outcomes[0]).lower() == "yes"

                if is_binary_yes_no:
                    yes_price = extract_yes_price(market, outcomes, prices_raw)
                    if yes_price is None:
                        continue

                    low, high = parse_bucket_from_question(question, station["unit"])
                    if low is None and high is None:
                        continue

                    model_prob = bucket_probability(
                        forecast_temp, low, high, station["unit"], mtype, extra_sigma=extra_sigma
                    )

                    edge = model_prob - yes_price
                    abs_edge = abs(edge)
                    net_edge = abs_edge - TRADING_COST_BUFFER

                    if net_edge < min_edge:
                        continue

                    side = "YES" if edge > 0 else "NO"
                    trade_price = yes_price if side == "YES" else (1 - yes_price)

                    # Em mercado binário, NO usa o segundo token (quando disponível).
                    if side == "YES":
                        token_id = token_ids[0] if len(token_ids) >= 1 else ""
                    else:
                        token_id = token_ids[1] if len(token_ids) >= 2 else ""
                    if not token_id:
                        continue

                    city_display = key.split("|")[0].title()

                    opportunities.append(Opportunity(
                        city=city_display,
                        question=question,
                        market_id=market_id,
                        token_id=token_id,
                        market_type=mtype,
                        side=side,
                        market_price=trade_price,
                        model_prob=model_prob,
                        edge=net_edge,
                        unit=station["unit"],
                        bucket_low=low,
                        bucket_high=high,
                        liquidity=liquidity,
                        hours_to_close=hours_left,
                        icao=station["icao"],
                        gross_edge=abs_edge,
                        confidence=confidence_score(liquidity, hours_left, abs_edge),
                    ))
                    continue

                # Formato tradicional de buckets em outcomes
                for i, outcome_label in enumerate(outcomes):
                    try:
                        price = float(prices_raw[i])
                    except (IndexError, ValueError, TypeError):
                        continue

                    low, high = parse_bucket(outcome_label, station["unit"])
                    if low is None and high is None:
                        continue

                    model_prob = bucket_probability(
                        forecast_temp, low, high, station["unit"], mtype, extra_sigma=extra_sigma
                    )

                    edge = model_prob - price
                    abs_edge = abs(edge)
                    net_edge = abs_edge - TRADING_COST_BUFFER

                    if net_edge < min_edge:
                        continue

                    # Token ID deste outcome
                    token_id = token_ids[i] if i < len(token_ids) else ""

                    # Em mercados multi-outcome operamos apenas compra do outcome (long-only).
                    if edge <= 0:
                        continue
                    side = "YES"
                    trade_price = price

                    city_display = key.split("|")[0].title()

                    opportunities.append(Opportunity(
                        city=city_display,
                        question=question,
                        market_id=market_id,
                        token_id=token_id,
                        market_type=mtype,
                        side=side,
                        market_price=trade_price,
                        model_prob=model_prob,
                        edge=net_edge,
                        unit=station["unit"],
                        bucket_low=low,
                        bucket_high=high,
                        liquidity=liquidity,
                        hours_to_close=hours_left,
                        icao=station["icao"],
                        gross_edge=abs_edge,
                        confidence=confidence_score(liquidity, hours_left, abs_edge),
                    ))

    # Ordena: edge desc, depois liquidez desc
    opportunities.sort(key=lambda o: (o.edge, o.liquidity), reverse=True)
    return opportunities


async def get_market_monitoring_snapshot(limit_questions: int = 8) -> dict:
    """
    Retorna um resumo dos mercados que o bot está monitorando no momento.
    Útil para dar transparência quando nenhuma oportunidade for encontrada.
    """
    snapshot = {
        "total_markets": 0,
        "parseable_markets": 0,
        "monitorable_markets": 0,
        "cities": [],
        "dates": [],
        "sample_questions": [],
    }

    async with aiohttp.ClientSession() as session:
        markets = await fetch_all_weather_markets(session)
        snapshot["total_markets"] = len(markets)

        city_counts: dict[str, int] = {}
        date_counts: dict[str, int] = {}
        samples: list[tuple[float, str, str]] = []

        for market in markets:
            question = (market.get("question") or "").strip()
            parsed = extract_city_from_question(question)
            if not parsed:
                continue

            snapshot["parseable_markets"] += 1

            city_key, _, _ = parsed
            city_display = city_key.title()
            city_counts[city_display] = city_counts.get(city_display, 0) + 1

            ok, _, hours_left = passes_quality_filters(market)
            if ok:
                snapshot["monitorable_markets"] += 1
                target_date = extract_target_date(
                    question,
                    market.get("endDate") or market.get("end_date_iso"),
                )
                if target_date:
                    date_key = target_date.isoformat()
                    date_counts[date_key] = date_counts.get(date_key, 0) + 1
                if target_date:
                    samples.append((hours_left, target_date.isoformat(), question))
                else:
                    samples.append((hours_left, "n/a", question))

        top_cities = sorted(city_counts.items(), key=lambda it: it[1], reverse=True)
        snapshot["cities"] = top_cities[:8]

        # Datas com mais mercados (normalmente hoje e próximos dias).
        top_dates = sorted(date_counts.items(), key=lambda it: (it[0], -it[1]))
        snapshot["dates"] = top_dates[:8]

        samples.sort(key=lambda it: it[0])
        snapshot["sample_questions"] = [
            {"target_date": d, "question": q}
            for _, d, q in samples[:limit_questions]
        ]

    return snapshot


def format_market_snapshot(snapshot: dict) -> str:
    """Formata snapshot de monitoramento para texto amigável no Telegram."""
    lines = [
        "Mercados monitorados agora:",
        f"- Total ativos (API): {snapshot.get('total_markets', 0)}",
        f"- Reconhecidos pelo bot: {snapshot.get('parseable_markets', 0)}",
        f"- Dentro dos filtros (liq/tempo): {snapshot.get('monitorable_markets', 0)}",
    ]

    cities = snapshot.get("cities") or []
    if cities:
        city_text = ", ".join([f"{city} ({count})" for city, count in cities[:6]])
        lines.append(f"- Cidades mais frequentes: {city_text}")

    dates = snapshot.get("dates") or []
    if dates:
        dates_text = ", ".join([f"{d} ({count})" for d, count in dates[:6]])
        lines.append(f"- Datas encontradas: {dates_text}")

    sample_questions = snapshot.get("sample_questions") or []
    if sample_questions:
        lines.append("")
        lines.append("Exemplos de mercados ativos:")
        for item in sample_questions[:5]:
            if isinstance(item, dict):
                q = (item.get("question") or "")[:90]
                d = item.get("target_date") or "n/a"
                lines.append(f"- [{d}] {q}")
            else:
                lines.append(f"- {str(item)[:90]}")

    return "\n".join(lines)


# ─── Formatação para Telegram ─────────────────────────────────────────────────

def format_opportunity(opp: Opportunity) -> str:
    type_icon = "🌡️" if opp.market_type == "high" else "🌙"
    type_label = "Temp Máx" if opp.market_type == "high" else "Temp Mín"

    if opp.bucket_low is not None and opp.bucket_high is not None:
        bucket = f"{opp.bucket_low}–{opp.bucket_high}°{opp.unit}"
    elif opp.bucket_low is not None:
        bucket = f"≥ {opp.bucket_low}°{opp.unit}"
    else:
        bucket = f"≤ {opp.bucket_high}°{opp.unit}"

    hours = f"{opp.hours_to_close:.1f}h"
    liq   = f"${opp.liquidity:,.0f}"

    return (
        f"{type_icon} *{opp.city}* — {type_label}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 Bucket:    `{bucket}`\n"
        f"📊 Modelo:    `{opp.model_prob*100:.1f}%`\n"
        f"💹 Mercado:   `{opp.market_price*100:.1f}%`\n"
        f"⚡ Edge líq.: `{opp.edge*100:.1f}%`\n"
        f"🧪 Edge bruto:`{opp.gross_edge*100:.1f}%`\n"
        f"🛡️ Confiança: `{opp.confidence*100:.0f}%`\n"
        f"🎲 Lado:      `{opp.side}`\n"
        f"💧 Liquidez:  `{liq}`\n"
        f"⏱️ Fecha em:  `{hours}`\n"
        f"📡 Estação:   `{opp.icao}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"_{opp.question[:90]}_"
    )
