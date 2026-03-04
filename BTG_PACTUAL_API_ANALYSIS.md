# Análise de Compatibilidade: API BTG Pactual Home Broker

**Data:** 23 de fevereiro de 2026  
**Sistema:** ArbitrageAI - Dashboard Profissional de Trading  
**Objetivo:** Avaliar integração da API do BTG Pactual com sistema existente

---

## 📋 Resumo Executivo

O BTG Pactual oferece **duas opções principais** para integração:

1. **BTG Pactual Empresas API** (Oficial) - Foco em banking e pagamentos
2. **BTG Home Broker API** (Não oficial) - Foco em trading de ações B3

**Recomendação:** A API não oficial do Home Broker é **compatível e viável** para integração com o ArbitrageAI, permitindo trading automatizado de ações B3 através da sua conta BTG.

---

## 🔍 Opção 1: BTG Pactual Empresas API (Oficial)

### Características
- **Portal:** https://developer.btgpactual.com/
- **Documentação:** https://developers.empresas.btgpactual.com/
- **Status:** Oficial e suportado pelo BTG
- **Foco:** Banking, pagamentos, Pix, boletos, transferências

### Produtos Disponíveis
✅ Account, Balance and Reconciliation  
✅ Depositary Bank  
✅ Know Your Customer (KYC)  
✅ Pix (envio e recebimento)  
✅ Payment Slip (Boleto)  
✅ Money Transfers (TEF/TED)  
❌ **Trading de ações** (NÃO disponível)  
❌ **Ordens de compra/venda** (NÃO disponível)

### Requisitos
- Criar conta BTG Pactual Empresas
- Plano Avançado para produção
- Registrar como desenvolvedor
- Criar aplicativo via Developer Console
- Autenticação via BTG Id (OAuth 2.0)

### Compatibilidade com ArbitrageAI
❌ **NÃO COMPATÍVEL** - Não oferece APIs de trading/home broker

---

## 🔍 Opção 2: BTG Home Broker API (Não Oficial)

### Características
- **Repositório:** https://github.com/mygs/btg-hb-api
- **Status:** Não oficial, desenvolvido pela comunidade
- **Linguagem:** Python
- **Protocolo:** WebSocket
- **Foco:** Trading de ações B3 em tempo real

### Funcionalidades Disponíveis

#### ✅ Dados de Mercado em Tempo Real
- **Limit Order Book (LOB)** - Livro de ofertas agregado
  - Bid/Ask prices e quantidades
  - Profundidade do book
  - Spread bid/ask
  - Book imbalance
  
- **Trade Feed** - Fluxo de negócios
  - Preço, quantidade, horário
  - Identificação de comprador/vendedor
  - Agressor (iniciador da operação)

- **Broker Ranking** - Ranking de corretoras
  - Volume por corretora
  - Posição líquida (NET)
  - Quantidade de operações

#### ✅ Análises Avançadas
- Weighted Price (preço ponderado)
- Middle Price (preço médio)
- Book Imbalance (desequilíbrio do livro)
- Balance Bid/Ask percentual

#### ❓ Execução de Ordens
**Status:** Não documentado explicitamente no repositório  
**Provável:** Somente leitura de dados (sem execução)

### Configuração Necessária

```json
{
  "SYMBOLS": ["PETR4", "VALE3", "ITUB4"],
  "ACCOUNT": "sua_conta_btg",
  "TKNWF": "token_web_feeder",
  "ENDPOINT": "wss://webfeeder.btgpactual.com/ws?reconnect="
}
```

### Como Obter o Token (TKNWF)
1. Fazer login no BTG Home Broker web
2. Abrir DevTools do navegador (F12)
3. Ir para aba Network/Rede
4. Filtrar por "ws" (WebSocket)
5. Copiar o token da URL de conexão

### Compatibilidade com ArbitrageAI

| Funcionalidade | ArbitrageAI Atual | BTG HB API | Compatível |
|----------------|-------------------|------------|------------|
| Cotações em tempo real | ✅ Binance/Yahoo | ✅ WebSocket | ✅ SIM |
| Ações B3 | ✅ Yahoo Finance | ✅ Nativo | ✅ SIM |
| Limit Order Book | ❌ Não | ✅ Sim | ✅ UPGRADE |
| Broker Ranking | ❌ Não | ✅ Sim | ✅ UPGRADE |
| Execução de ordens | ❌ Simulado | ❓ Não confirmado | ⚠️ LIMITADO |
| Multi-mercado | ✅ Crypto+B3+NYSE | ⚠️ Apenas B3 | ⚠️ PARCIAL |

---

## 🎯 Plano de Integração Recomendado

### Fase 1: Integração de Dados (2-3 dias)
**Objetivo:** Substituir Yahoo Finance por BTG HB API para ações B3

**Tarefas:**
1. Clonar repositório `btg-hb-api`
2. Obter token TKNWF da sua conta BTG
3. Criar módulo `btg_connector.py` no ArbitrageAI
4. Implementar WebSocket client para receber dados em tempo real
5. Mapear símbolos B3 (PETR4, VALE3, ITUB4, WEGE3, etc.)
6. Atualizar `market_data.py` para usar BTG como fonte primária

**Benefícios:**
- ✅ Dados em tempo real (vs. 15min delay do Yahoo)
- ✅ Limit Order Book completo
- ✅ Informações de liquidez e spread
- ✅ Broker ranking para análise de fluxo

### Fase 2: Análises Avançadas (3-5 dias)
**Objetivo:** Usar dados do LOB para melhorar estratégias

**Tarefas:**
1. Implementar cálculo de Book Imbalance
2. Adicionar análise de liquidez (bid/ask depth)
3. Criar indicador de pressão compradora/vendedora
4. Integrar com ML Predictor para usar LOB features
5. Adicionar visualização de LOB no dashboard

**Benefícios:**
- ✅ Melhor timing de entrada/saída
- ✅ Detecção de movimentos de grandes players
- ✅ Redução de slippage

### Fase 3: Execução de Ordens (Investigação)
**Objetivo:** Verificar possibilidade de executar ordens via API

**Tarefas:**
1. Analisar código-fonte completo do `btg-hb-api`
2. Fazer engenharia reversa do protocolo WebSocket
3. Testar envio de ordens em ambiente de sandbox
4. Documentar endpoints e payloads necessários

**Alternativas se não for possível:**
- Continuar com execução manual via interface BTG
- Usar notificações para alertar quando abrir/fechar trades
- Integrar com outra corretora que tenha API oficial (XP, Clear, etc.)

---

## ⚠️ Riscos e Considerações

### Riscos Técnicos
1. **API não oficial** - Pode ser descontinuada ou bloqueada pelo BTG
2. **Token expira** - Necessário renovar TKNWF periodicamente
3. **Rate limiting** - Possíveis limites de requisições não documentados
4. **Breaking changes** - BTG pode alterar protocolo sem aviso

### Riscos Regulatórios
1. **Termos de uso** - Verificar se automação é permitida
2. **Compliance** - Garantir que uso está dentro das regras da CVM
3. **Responsabilidade** - Trades automatizados são de sua responsabilidade

### Mitigações
- ✅ Manter Yahoo Finance como fallback
- ✅ Implementar monitoramento de conexão
- ✅ Logs detalhados de todas as operações
- ✅ Circuit breaker para parar em caso de erro
- ✅ Notificações de falha de conexão

---

## 💰 Custos

### BTG Pactual Empresas API (Oficial)
- Plano Avançado necessário para produção
- Custo não divulgado publicamente
- Contato com equipe comercial necessário

### BTG Home Broker API (Não Oficial)
- ✅ **GRATUITO** - Código open source
- ✅ Usa sua conta BTG existente
- ❌ Sem suporte oficial
- ❌ Sem garantias de SLA

---

## 📊 Comparação com Alternativas

| Corretora | API Oficial | Trading B3 | Custo | Documentação |
|-----------|-------------|------------|-------|--------------|
| **BTG Pactual** | ❌ (só banking) | ✅ (não oficial) | Grátis | Comunidade |
| **XP Investimentos** | ✅ | ✅ | Grátis | Oficial |
| **Clear (XP)** | ✅ | ✅ | Grátis | Oficial |
| **Rico (XP)** | ✅ | ✅ | Grátis | Oficial |
| **Modalmais** | ✅ | ✅ | Grátis | Oficial |
| **Interactive Brokers** | ✅ | ✅ | Pago | Excelente |

---

## ✅ Recomendação Final

### Para Dados de Mercado (Curto Prazo)
**✅ IMPLEMENTAR** integração com BTG HB API não oficial

**Justificativa:**
- Você já tem conta BTG
- Dados em tempo real gratuitos
- Limit Order Book completo
- Implementação rápida (2-3 dias)
- Fallback para Yahoo Finance

### Para Execução de Ordens (Médio Prazo)
**⚠️ INVESTIGAR** possibilidade de execução via API não oficial

**Se não for viável:**
- Considerar abrir conta em corretora com API oficial (XP, Clear, Modal)
- Manter BTG para dados e outra corretora para execução
- Ou continuar com execução manual no BTG

---

## 📝 Próximos Passos Imediatos

1. **Obter token TKNWF** da sua conta BTG (5 minutos)
2. **Testar conexão** com WebSocket do BTG (30 minutos)
3. **Validar dados** recebidos vs. Yahoo Finance (1 hora)
4. **Decisão:** Prosseguir com integração completa?

---

## 📚 Referências

- [BTG Developer Portal](https://developer.btgpactual.com/)
- [BTG Empresas Developers](https://developers.empresas.btgpactual.com/)
- [BTG HB API GitHub](https://github.com/mygs/btg-hb-api)
- [Manual de Integração BTG](https://github.com/mygs/btg-hb-api/tree/main/reference)

---

**Conclusão:** A API não oficial do BTG Home Broker é **viável e recomendada** para melhorar a qualidade dos dados de ações B3 no ArbitrageAI, mas para execução automatizada de ordens, considere também corretoras com APIs oficiais.
