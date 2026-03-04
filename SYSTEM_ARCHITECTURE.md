# Egreja Investment AI PRO - Arquitetura do Sistema

## 🎯 Visão Geral

Sistema **adaptativo de trading com aprendizado contínuo** que melhora a cada trade realizado. Combina:
- **5 Fontes de Dados** (agregação)
- **10+ Indicadores Técnicos** (análise em tempo real)
- **Análise Fundamental** (valuation, ratings)
- **Machine Learning** (aprende com trades)
- **Backtesting Contínuo** (otimiza estratégia)

---

## 📊 Arquitetura em Camadas

```
┌─────────────────────────────────────┐
│         Dashboard (Vercel)          │
│     www.egreja.com (Frontend)       │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│    Railway Daemon (Backend Python)   │
│     intelligent_daemon.py (main)     │
└──────────────┬──────────────────────┘
               │
    ┌──────────┴──────────┬──────────────┬──────────────┐
    │                     │              │              │
┌───▼────┐    ┌───────────▼┐  ┌────────▼─┐  ┌────────▼─────┐
│Data    │    │ Technical  │  │Machine   │  │Performance   │
│Agg.    │    │ Analysis   │  │Learning  │  │Analyzer      │
└────────┘    └────────────┘  └──────────┘  └──────────────┘
    │              │              │              │
    ├─ yfinance    ├─ RSI         ├─ Feedback   ├─ Backtesting
    ├─ Alpha Van.  ├─ MACD        ├─ Tree Rnd   ├─ Optimization
    ├─ Finnhub     ├─ Bollinger   ├─ Weights    ├─ Sensitivity
    ├─ CoinGecko   ├─ Stochastic  ├─ Weights    ├─ Daily Report
    └─ Investing   ├─ ADX         └─ Model      └─ Metrics
                   ├─ CCI
                   ├─ EMA
                   ├─ Momentum
                   └─ Volatility
```

---

## 🔄 Fluxo de Execução Diário

### 1️⃣ Coleta de Dados (Trade Signals)

```python
# Agregação de 5+ fontes
data_agg.aggregate_all_sources('PETR4.SA')
# ├─ yfinance: OHLCV histórico
# ├─ Alpha Vantage: SMA, RSI, MACD (se houver chave)
# ├─ Finnhub: Preço atual + mudança
# ├─ CoinGecko: Criptos (se aplicável)
# └─ Investing.com: Sentiment + Fear Index
```

**Resultado:** Dados consolidados de múltiplas fontes

---

### 2️⃣ Análise Técnica (10+ Indicadores)

```python
technical.analyze_symbol('PETR4.SA', price_history, current_price)
# ├─ EMA 9, 21, 50 (tendência)
# ├─ RSI 14 (sobrecompra/venda)
# ├─ MACD (momentum)
# ├─ Bollinger Bands (volatilidade)
# ├─ Stochastic (força)
# ├─ ADX (força da tendência)
# ├─ CCI (canal)
# ├─ Momentum (mudança de preço)
# └─ Volatilidade
#
# Output: Score técnico 0-100 + Recomendação (BUY/SELL/HOLD)
```

**Indicadores Convergentes:**
- Precisa de 3/5 sinais concordarem para gerar sinal
- Filtro de tendência: EMA21 vs EMA50 confirma direção

---

### 3️⃣ Análise Fundamental

```python
fundamental.combined_score('PETR4.SA', technical_score)
# ├─ P/E Ratio (valuation)
# ├─ ROE (rentabilidade)
# ├─ Debt/Equity (endividamento)
# ├─ Profit Margin (lucratividade)
# └─ PB Ratio (preço/lucro)
#
# Output: Score fundamental 0-100 + Rating (Subavaliada/Normal/Cara)
```

**Scoring:**
- P/E < 15: +20 pts
- ROE > 15%: +15 pts
- Debt/Equity < 0.5: +10 pts
- Resultado final: Score 0-100

---

### 4️⃣ Geração de Sinal Combinado

```python
trade_signals.generate_signal(symbol, price_history, current_price)
# ├─ Técnico: 50% do score final
# └─ Fundamental: 50% do score final
#
# Score = (técnico * 0.50) + (fundamental * 0.50)
#
# Recomendação Final:
# ├─ Score >= 70 + BUY técnico → 🟢 COMPRA FORTE
# ├─ Score >= 60 + BUY técnico → 🟢 COMPRA
# ├─ 40-60 → 🟡 MANTER
# ├─ Score <= 50 + SELL técnico → 🔴 VENDA
# └─ Score <= 40 + SELL técnico → 🔴 VENDA FORTE
```

---

### 5️⃣ Machine Learning (Sistema Adaptativo)

```python
ml_engine.record_trade(symbol, trade, result)
# ├─ Input: Sinal gerado, Indicadores, Resultado real
# ├─ Processamento:
# │  ├─ Atualizar pesos dos indicadores
# │  │  └─ Sinais vencedores: +5% peso
# │  │  └─ Sinais perdedores: -3% peso
# │  └─ Treinar modelo Random Forest
# │     └─ Features: RSI, MACD, Momentum, CCI
# │     └─ Target: Win (1) / Loss (0)
# └─ Output: Modelo treinado por símbolo
```

**Aprendizado Contínuo:**
1. **Feedback Loop**: Cada trade registrado atualiza o modelo
2. **Pesos Adaptativos**: Indicadores ganham peso se acertam
3. **Predição de Sucesso**: ML prediz prob. de ganho (0-100%)
4. **Performance Dinâmica**: Score final ajustado por histórico

---

### 6️⃣ Performance & Backtesting

```python
analyzer.backtest_strategy(symbol, price_history, strategy_func)
# ├─ Simula execução da estratégia no histórico
# ├─ Calcula:
# │  ├─ Win Rate %
# │  ├─ Profit Factor
# │  ├─ Return %
# │  ├─ Max Drawdown
# │  ├─ Sharpe Ratio
# │  └─ Avg Profit/Loss
# └─ Output: Métricas reais de desempenho
```

**Otimização Contínua:**
- Backtesting de novas combinações de parâmetros
- Identificação de padrões: qual estratégia vence?
- Sensibilidade: robusto ou frágil o sinal?

---

## 🧠 Como o Sistema Aprende

### Cenário 1: Trade Vencedor
```
Entrada: Sinal técnico RSI=35 (sobrevenda) → COMPRA
Sinal gerado: 🟢 COMPRA FORTE (Score: 75)
Resultado real: +$500 lucro em 4 horas

Aprendizado:
├─ Peso do RSI aumenta 5% (acertou!)
├─ Peso do MACD aumenta 3% (concordou)
├─ Modelo RF aprende: RSI=35 + MACD>0 = Ganhar (prob 75%)
└─ Próximas compras similares: confiança aumenta
```

### Cenário 2: Trade Perdedor
```
Entrada: Sinal técnico EMA9>EMA21 → COMPRA
Sinal gerado: 🟢 COMPRA (Score: 65)
Resultado real: -$200 perda em 2 horas

Aprendizado:
├─ Peso do EMA diminui 3% (errou!)
├─ Peso do RSI diminui 1% (não confirmou)
├─ Modelo RF aprende: EMA9>EMA21 sozinho NÃO é suficiente
└─ Próximas sinais EMA: exige confirmação adicional (RSI < 70)
```

---

## 📈 Métricas de Sucesso

Cada símbolo tem seus próprios KPIs:

### Por Ativo (symbol)
```
symbol: 'PETR4.SA'
├─ Trades Totais: 47
├─ Vencedores: 29 (61%)
├─ Perdedores: 18 (39%)
├─ Win Rate: 61.7%
├─ Profit Factor: 2.8
├─ Lucro Total: $2,847
├─ Lucro Médio: $60.57
├─ Max Drawdown: 8.2%
└─ Sharpe Ratio: 1.45
```

### Portfólio Total
```
├─ Símbolos Monitorados: 40
├─ Sinais Gerados: 287
├─ Trades Executados: 164
├─ P&L Total: +$34,775
├─ Return Anualizado: +3.48%
└─ Símbolos Rentáveis: 35 (87.5%)
```

---

## 🔐 Segurança & Limites

### Proteções Integradas
```python
# 1. Position Sizing
├─ Máximo 10% do capital por trade
├─ Stop loss automático: -2% do entry
└─ Take profit automático: +3% do entry

# 2. Risk Management
├─ Max 3 posições abertas simultaneamente
├─ Max drawdown diário: 5%
└─ Pausa automática se losing streak > 3 trades

# 3. Validação de Sinal
├─ Mínimo 3/5 indicadores concordarem
├─ Mínimo score 60 para BUY
├─ Mínimo score 40 para SELL
└─ Sensibilidade check: sinal é robusto?
```

---

## 📂 Estrutura de Arquivos

```
egreja-daemon/
├─ intelligent_daemon.py      (Main entry point)
├─ market_data.py             (Data collection)
├─ technical_analysis.py      (10+ indicators)
├─ fundamental_analysis.py    (P/E, ROE, etc)
├─ trade_signals.py           (Sinal combinado)
├─ data_aggregator.py         (5+ fontes)
├─ machine_learning_engine.py (ML + feedback)
├─ performance_analyzer.py    (Backtesting)
├─ requirements.txt           (Dependencies)
├─ Procfile                   (Railway config)
├─ railway.json               (Railway deploy)
├─ .env.example               (Variables)
├─ DEPLOYMENT.md              (Como deployar)
├─ README.md                  (Documentação)
└─ SYSTEM_ARCHITECTURE.md     (Este arquivo)

data/
├─ trades_PETR4_202603.json   (Histórico de trades)
├─ model_PETR4.pkl            (Modelo ML treinado)
└─ performance_2026-03-04.json (Relatório diário)
```

---

## 🚀 Próximos Passos (Roadmap)

### Phase 1 (Agora) ✅
- [x] 5+ fontes de dados
- [x] 10+ indicadores técnicos
- [x] ML com feedback loop
- [x] Backtesting contínuo
- [x] 20 B3 + 20 NYSE monitorados

### Phase 2 (Próxima semana)
- [ ] Deep Learning (LSTM) para previsão
- [ ] Análise de correlação entre ativos
- [ ] Execution Engine (Binance/OKX API)
- [ ] Real-time WebSocket trades
- [ ] Risk Manager avançado

### Phase 3 (Mês que vem)
- [ ] Reinforcement Learning (Q-Learning)
- [ ] Ensemble de modelos
- [ ] Multi-timeframe analysis
- [ ] News sentiment analysis
- [ ] Portfolio optimization (Markowitz)

### Phase 4 (Long-term)
- [ ] GPU acceleration (cupy)
- [ ] Distributed processing
- [ ] HFT strategies
- [ ] Options trading
- [ ] Crypto derivatives

---

## 💰 Resultados Esperados

**Depois de 30 dias de aprendizado:**
- Win Rate: 55%+ (vs 50% aleatório)
- Profit Factor: 2.0+
- Sharpe Ratio: 1.2+
- Max Drawdown: < 10%

**Depois de 90 dias:**
- Win Rate: 60%+
- Profit Factor: 2.5+
- Sharpe Ratio: 1.5+
- Sistema adaptado a cada ativo

---

## 📞 Contato & Support

Problemas ou sugestões?
- GitHub: https://github.com/betoegreja-ship-it/egreja-daemon
- Issues: Create GitHub issue com logs
- Email: beto@egreja.com

---

**Última atualização:** 2026-03-04 12:01
**Status:** ✅ Em produção no Railway
**Próxima revisão:** 2026-03-11
