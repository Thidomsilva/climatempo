# 🤖 PolyWeather Bot

Bot de Telegram para trading automático de mercados de clima na Polymarket.
Estratégia: **Forecast vs Mercado** — detecta quando o preço está abaixo da probabilidade real prevista pelos modelos meteorológicos e notifica o usuário para aprovação.

---

## Arquitetura

```
bot.py          → Bot Telegram (menus, aprovação de trades)
scanner.py      → Motor de detecção de edge (NWS + Open-Meteo vs Polymarket)
executor.py     → Integração CLOB API da Polymarket (autenticação + ordens)
db.py           → Banco de dados SQLite (usuários + histórico de trades)
requirements.txt
.env.example
```

---

## Instalação

### 1. Clone e instale dependências

```bash
git clone <seu-repo>
cd polymarket-weather-bot
pip install -r requirements.txt
```

### 2. Configure as variáveis de ambiente

```bash
cp .env.example .env
```

Edite `.env`:

```env
TELEGRAM_TOKEN=seu_token_do_botfather
ENCRYPT_KEY=sua_chave_fernet
APP_DATA_DIR=.secure_data
```

Gere a ENCRYPT_KEY com:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Crie seu bot no Telegram

1. Abra @BotFather no Telegram
2. `/newbot` → dê um nome e username
3. Copie o token para o `.env`

### 4. Rode

```bash
python bot.py
```

---

## Uso pelo usuário no Telegram

1. `/start` → abre o menu principal
2. **Conectar Conta** → cola a private key + endereço proxy wallet
3. Bot autentica e inicia o scanner automaticamente
4. A cada 5 minutos, escaneia mercados de clima
5. Quando detecta edge ≥ 15%, envia alerta com botões:
   - ✅ **EXECUTAR** → envia ordem FOK na Polymarket
   - ❌ **IGNORAR** → descarta a oportunidade

---

## Cidades monitoradas (12 ao total)

| Cidade        | Estação | Unidade |
|---------------|---------|---------|
| New York      | KLGA    | °F      |
| Chicago       | KORD    | °F      |
| Miami         | KMIA    | °F      |
| Dallas        | KDAL    | °F      |
| Los Angeles   | KLAX    | °F      |
| Seattle       | KSEA    | °F      |
| Atlanta       | KATL    | °F      |
| London        | EGLL    | °C      |
| Tokyo         | RJTT    | °C      |
| São Paulo     | SBGR    | °C      |
| Buenos Aires  | SAEZ    | °C      |
| Cape Town     | FACT    | °C      |

> **Importante:** cada cidade usa a estação de aeroporto correta que a Polymarket usa para resolução — não o centro da cidade.

---

## Deploy em produção (recomendado)

### Railway (mais simples)
```bash
railway init
railway up
```

### DigitalOcean VPS
```bash
# Instale Python 3.11+, clone o repo, configure .env
# Use PM2 ou systemd para manter o processo ativo
pm2 start bot.py --interpreter python3
```

> **Dica:** Para menor latência, use VPS em Nova York (Vultr/DO NYC1) — mais próximo dos servidores da Polymarket.

---

## Segurança

- Private keys são criptografadas com Fernet (AES-128-CBC) antes de salvar no SQLite
- A mensagem com a private key é deletada automaticamente do Telegram
- Use uma carteira dedicada — nunca a sua carteira principal
- A ENCRYPT_KEY deve ser mantida em segredo e em variável de ambiente
- O diretório de dados do bot (`APP_DATA_DIR`) deve ficar fora de versionamento e com permissão restrita
- Nunca comite `.env`; use secrets do provedor de deploy (Railway/Render/GitHub Actions)

---

## Configurações por usuário

| Parâmetro       | Padrão | Descrição                              |
|-----------------|--------|----------------------------------------|
| trade_size      | $10    | Tamanho de cada ordem em USDC          |
| min_edge        | 15%    | Edge mínimo para receber alertas       |
| active          | true   | Ativa/pausa o scanner                  |

---

## Fontes de dados

| Fonte       | Cobertura   | API Key |
|-------------|-------------|---------|
| NWS/NOAA    | EUA         | ❌ Gratuito |
| Open-Meteo  | Global      | ❌ Gratuito |
| Gamma API   | Polymarket  | ❌ Público  |
| CLOB API    | Polymarket  | ✅ Via L1 auth |

---

## Aviso de risco

Este software é fornecido apenas para fins educacionais. Trading em mercados de predição envolve risco substancial de perda. Use apenas capital que você pode perder. Verifique as leis da sua jurisdição antes de usar.
