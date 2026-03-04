# 🚀 ArbitrageAI Pro - Guia Rápido de Uso

## ⚡ Início Rápido (5 minutos)

### 1. Acessar Dashboard

```
URL: https://3000-idhremnk8pz28mn4jx32e-b56aab35.us2.manus.computer
```

### 2. Navegar pelas Abas

| Aba | Função | Atualização |
|-----|--------|-------------|
| **Visão Geral** | Métricas principais | Tempo real |
| **Operações Abertas** | Trades em andamento | 5 segundos |
| **Mercados** | 16 ativos com cotações | 2 segundos |
| **Gráficos** | Visualizações de performance | 1 minuto |
| **Insights** | Análises de Sofia IA | 30 segundos |
| **Histórico** | Todos os trades fechados | 1 minuto |

### 3. Interpretar Métricas

**Capital Atual:** Capital inicial ($1.000.000) + P&L total  
**P&L Total:** Soma de lucros e prejuízos de todos os trades  
**Taxa de Acerto:** % de trades lucrativos  
**Operações Abertas:** Número de trades em andamento  
**Confiança Sofia:** Nível de confiança da IA (0-100%)  

---

## 🤖 Executar Sistema Autônomo

### Opção 1: Execução Manual (Teste)

```bash
cd /home/ubuntu/arbitrage-dashboard
python3 autonomous_trading_system.py
```

### Opção 2: Daemon em Background

```bash
cd /home/ubuntu/arbitrage-dashboard
nohup python3 autonomous_trading_system.py > daemon.log 2>&1 &

# Verificar se está rodando
ps aux | grep autonomous

# Ver logs
tail -f daemon.log
```

### Opção 3: Scheduler Automático (Recomendado)

```bash
cd /home/ubuntu/arbitrage-dashboard
python3 auto_scheduler.py
```

---

## 📊 Executar Aprendizado Contínuo

```bash
cd /home/ubuntu/arbitrage-dashboard
python3 improved_learning_system.py
```

**O que faz:**
- Analisa todos os trades fechados
- Identifica melhores horários para trading
- Ajusta pesos dos indicadores
- Executa backtesting em 3 ativos
- Seleciona melhor estratégia

**Frequência recomendada:** 1x por semana

---

## 🎯 Ajustar Parâmetros

### Arquivo: `autonomous_trading_system.py`

```python
# Linha 31-42: Configurações principais
self.config = {
    'analysis_interval': 3600,      # Intervalo entre análises (segundos)
    'min_score_to_trade': 70,       # Score mínimo para executar trade
    'max_open_trades': 3,           # Máximo de trades abertos
    'max_position_size': 0.30,      # % do capital por trade
    'take_profit_pct': 0.03,        # Take-profit (3%)
    'stop_loss_pct': 0.02,          # Stop-loss (2%)
    'trailing_stop_pct': 0.01,      # Trailing stop (1%)
    'max_trade_duration': 7200,     # Timeout (2 horas)
    'circuit_breaker_loss': 0.05,   # Pausa se perder 5% em 24h
}
```

### Arquivo: `improved_learning_system.py`

```python
# Linha 23-32: Configurações de aprendizado
self.config = {
    'min_score': 70,                # Score mínimo
    'take_profit_pct': 0.03,        # Take-profit
    'stop_loss_pct': 0.02,          # Stop-loss
    'min_trend_strength': 25,       # ADX mínimo
    'require_multi_timeframe_confirmation': True,  # Confirmar em múltiplos timeframes
}
```

---

## 🔍 Monitorar Performance

### Ver Trades Abertos

```bash
# Via tRPC
curl -s "http://localhost:3000/api/trpc/sofia.getOpenTrades" | jq

# Via banco de dados
mysql -h <host> -u <user> -p<password> -D <database> -e "SELECT * FROM trades WHERE status='OPEN';"
```

### Ver Métricas Diárias

```bash
curl -s "http://localhost:3000/api/trpc/sofia.getDailyStats" | jq
```

### Ver Análises de Sofia

```bash
curl -s "http://localhost:3000/api/trpc/sofia.getSofiaAnalyses?input={\"limit\":10}" | jq
```

---

## ⚠️ Situações de Emergência

### Parar Todos os Trades

```bash
# Parar daemon
pkill -f autonomous_trading_system

# Fechar trades manualmente via banco
mysql -h <host> -u <user> -p<password> -D <database> -e "
UPDATE trades 
SET status='CLOSED', exit_price=entry_price, pnl=0, close_reason='MANUAL_STOP' 
WHERE status='OPEN';
"
```

### Circuit Breaker Ativado

**O que aconteceu:** Sistema perdeu 5% do capital em 24 horas  
**Ação automática:** Trading pausado por 6 horas  
**O que fazer:** Aguardar ou investigar causa da perda  

```bash
# Ver trades das últimas 24h
mysql -h <host> -u <user> -p<password> -D <database> -e "
SELECT symbol, pnl, close_reason, closed_at 
FROM trades 
WHERE status='CLOSED' AND closed_at >= NOW() - INTERVAL 24 HOUR 
ORDER BY closed_at DESC;
"
```

### Dashboard Não Carrega

```bash
# Verificar servidor
curl http://localhost:3000/health

# Reiniciar servidor
cd /home/ubuntu/arbitrage-dashboard
pnpm dev

# Verificar logs
tail -f .manus-logs/devserver.log
```

---

## 📈 Interpretar Sinais de Sofia IA

### Score (0-100)

- **70-100:** BUY forte (confiança 70-95%)
- **55-70:** BUY moderado (confiança 55-70%)
- **45-55:** HOLD (confiança 40-50%)
- **30-45:** SELL moderado (confiança 55-70%)
- **0-30:** SELL forte (confiança 70-95%)

### Recomendações

- **BUY:** Comprar ativo (tendência de alta)
- **SELL:** Vender ativo (tendência de baixa)
- **HOLD:** Aguardar (mercado lateral)

### Badge "EXECUTADO"

Indica que a análise resultou em um trade real

---

## 🎓 Dicas de Uso

### 1. Deixe o Sistema Aprender

- Não interrompa trades manualmente
- Aguarde pelo menos 50 trades antes de avaliar performance
- Execute aprendizado contínuo semanalmente

### 2. Monitore Regularmente

- Acesse dashboard diariamente
- Verifique taxa de acerto (ideal >55%)
- Acompanhe P&L total

### 3. Ajuste Gradualmente

- Faça mudanças pequenas nos parâmetros
- Teste com backtesting antes de aplicar em produção
- Documente todas as alterações

### 4. Gestão de Risco

- Nunca desative circuit breaker
- Mantenha stop-loss sempre ativo
- Não aumente tamanho de posição acima de 30%

### 5. Diversificação

- Sistema já opera 15 criptomoedas + ouro
- Não force trades em um único ativo
- Confie na seleção automática

---

## 📞 Comandos Úteis

```bash
# Ver status do sistema
ps aux | grep -E "autonomous|sofia|scheduler"

# Ver logs em tempo real
tail -f .manus-logs/devserver.log
tail -f daemon.log

# Reiniciar banco de dados
pnpm db:push

# Atualizar dependências
pnpm install
pip3 install -r requirements.txt

# Backup do banco
mysqldump -h <host> -u <user> -p<password> <database> > backup.sql

# Restaurar backup
mysql -h <host> -u <user> -p<password> <database> < backup.sql
```

---

## 🎯 Checklist de Uso Diário

- [ ] Acessar dashboard
- [ ] Verificar capital atual e P&L
- [ ] Ver operações abertas
- [ ] Conferir taxa de acerto
- [ ] Ler insights de Sofia IA
- [ ] Verificar se daemon está rodando
- [ ] Revisar trades fechados do dia

---

## 📚 Documentação Completa

- **DOCUMENTACAO_EXECUTIVA_FINAL.md** - Documentação completa do sistema
- **ARQUITETURA_SISTEMA_UNIFICADO.md** - Arquitetura técnica
- **README.md** - Template do projeto
- **todo.md** - Lista de tarefas

---

**🚀 Sucesso nos seus trades!**
