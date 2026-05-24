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
from datetime import datetime, timezone
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
MIN_HOURS_TO_CLOSE = 2      # horas mínimas até resolução
MAX_HOURS_TO_CLOSE = 96     # não operar mercados muito distantes

# Custos e margem operacional (fee + spread + slippage), abatidos do edge bruto.
TRADING_COST_BUFFER = float(os.getenv("TRADING_COST_BUFFER", "0.03"))


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
) -> Optional[float]:
    """
    Busca temperatura máxima ou mínima prevista via Open-Meteo.
    Open-Meteo usa ensemble GFS de 51 membros — mais preciso que NWS para cobertura global.
    """
    temp_unit = "fahrenheit" if unit == "F" else "celsius"
    field = "temperature_2m_max" if mtype == "high" else "temperature_2m_min"

    params = {
        "latitude":          lat,
        "longitude":         lon,
        "daily":             field,
        "temperature_unit":  temp_unit,
        "forecast_days":     5,
        "timezone":          "auto",
        "models":            "gfs_seamless",   # GFS global — consistente com Wunderground
    }

    try:
        async with session.get(
            METEO_API, params=params,
            timeout=aiohttp.ClientTimeout(total=12)
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
            temps = data["daily"].get(field, [])
            return float(temps[0]) if temps else None
    except Exception:
        return None


# ─── Cálculo de probabilidade por bucket ─────────────────────────────────────

def bucket_probability(
    forecast: float,
    low: Optional[float],
    high: Optional[float],
    unit: str,
    mtype: str,
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
    Busca todos os mercados de clima ativos, paginando a Gamma API.
    Filtra por tag 'weather' e 'daily-temperature'.
    """
    markets = []
    tags = ["weather", "daily-temperature", "high-temperature", "low-temperature"]

    seen_ids: set[str] = set()

    for tag in tags:
        offset = 0
        while True:
            params = {
                "tag_slug": tag,
                "active":   "true",
                "closed":   "false",
                "limit":    100,
                "offset":   offset,
            }
            try:
                async with session.get(
                    f"{GAMMA_API}/markets",
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as r:
                    if r.status != 200:
                        break
                    data = await r.json()
                    if not data:
                        break
                    for m in data:
                        mid = m.get("id", "")
                        if mid and mid not in seen_ids:
                            seen_ids.add(mid)
                            markets.append(m)
                    if len(data) < 100:
                        break
                    offset += 100
            except Exception:
                break

    return markets


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
        city_meta:    dict[str, tuple]       = {}  # city_key → (mtype, station_data)

        for market in markets:
            question = market.get("question", "")
            parsed = extract_city_from_question(question)
            if not parsed:
                continue
            city_key, mtype, station = parsed
            key = f"{city_key}|{mtype}"
            city_markets.setdefault(key, []).append(market)
            city_meta[key] = (mtype, station)

        # 3. Busca forecasts em paralelo para todas as cidades únicas
        forecast_tasks = {}
        for key, (mtype, station) in city_meta.items():
            task = asyncio.create_task(
                fetch_forecast(session, station["lat"], station["lon"],
                               station["unit"], mtype)
            )
            forecast_tasks[key] = task

        forecasts = {k: await t for k, t in forecast_tasks.items()}

        # 4. Para cada mercado, calcula edge por outcome
        for key, mlist in city_markets.items():
            mtype, station = city_meta[key]
            forecast_temp = forecasts.get(key)
            if forecast_temp is None:
                continue

            for market in mlist:
                # Filtros de qualidade
                ok, liquidity, hours_left = passes_quality_filters(market)
                if not ok:
                    continue

                question = market.get("question", "")
                market_id = market.get("id", "")
                outcomes  = market.get("outcomes", [])
                prices_raw = market.get("outcomePrices", [])

                if not outcomes or not prices_raw:
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

                # Avalia cada outcome (bucket)
                for i, outcome_label in enumerate(outcomes):
                    try:
                        price = float(prices_raw[i])
                    except (IndexError, ValueError, TypeError):
                        continue

                    low, high = parse_bucket(outcome_label, station["unit"])
                    if low is None and high is None:
                        continue

                    model_prob = bucket_probability(
                        forecast_temp, low, high, station["unit"], mtype
                    )

                    edge = model_prob - price
                    abs_edge = abs(edge)
                    net_edge = abs_edge - TRADING_COST_BUFFER

                    if net_edge < min_edge:
                        continue

                    # Token ID deste outcome
                    token_id = token_ids[i] if i < len(token_ids) else ""

                    # Lado: compra YES se modelo > mercado, NO se modelo < mercado
                    side = "YES" if edge > 0 else "NO"
                    trade_price = price if side == "YES" else (1 - price)

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
