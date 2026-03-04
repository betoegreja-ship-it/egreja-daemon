# Análise do Protocolo BTG EFS (Nova API)

## Descoberta Inicial - 2026-02-24

### Mudança de Arquitetura

O BTG Pactual migrou sua infraestrutura de WebSocket:

**Antiga API (btg-hb-api):**
- Endpoint: `wss://webfeeder.btgpactual.com/ws?reconnect=TOKEN`
- Autenticação: Token TKNWF na URL
- Protocolo: Documentado no repositório mygs/btg-hb-api

**Nova API (EFS - Electronic Financial Services):**
- Endpoint: `wss://efs.btgpactual.com/trader/socket-XXXXX`
- Autenticação: Session-based (cookie/header)
- Protocolo: Proprietário, não documentado publicamente

### Conexão WebSocket Capturada

```
URL: wss://efs.btgpactual.com/trader/socket-VlnJiReeD
Estado: 101 Switching Protocols
Origin: https://app.btgpactual.com

Headers de Requisição:
- Cache-Control: no-cache
- Origin: https://app.btgpactual.com
- Sec-WebSocket-Version: 13
- Sec-WebSocket-Extensions: permessage-deflate

Headers de Resposta:
- Connection: upgrade
- Upgrade: websocket
- Sec-WebSocket-Accept: ODlwSdwUA0Ddps+SaapNgrsvRPs=
```

### Mensagens Observadas

Das capturas de tela, identificamos os seguintes tipos de mensagens:

#### 1. Heartbeat
```json
{
  "msgSeqNum": "645",
  "msgType": "info",
  "topic": "heartbeat",
  "data": "1771962914329",
  "sendingTime": "1771962914329"
}
```

#### 2. Order Updates (order-v2)
```json
{
  "msgType": "subscribe",
  "topic": "order-v2",
  "filters": {
    "account": "005871112",
    "interval": "400",
    "format": "eventTime,orderStatus,requestStatus"
  },
  "topic": "order-v2",
  "isPush": false,
  "offset": "0",
  "onlyUpdates": true,
  "strategy": "P",
  "subscriptionId": "bcce70ac921f6739d1a1e67717ad3dfe7e9dc71"
}
```

#### 3. Position Summary
```json
{
  "msgType": "unsubscribe",
  "topic": "position-summary-v2",
  "filters": {
    "account": "005871112"
  },
  "topic": "position-summary-v2",
  "format": "eventTime,profitPercent,D,volume",
  "positionType": "ALL",
  "subscriptionId": "b338296c8d974ee8f727f6e0c550f80bb9856f0a"
}
```

#### 4. Account Summary
```json
{
  "msgType": "unsubscribe",
  "topic": "account-summary",
  "filters": {
    "account": "005871112"
  },
  "format": "balanceOnline,dAvailableLimit,dtOpenOrders,dtPositionCount,lostOpenOrders,totalWarranty",
  "interval": "1000",
  "subscriptionId": "16050eeb0f44adac25a045867a189de9d2d43913"
}
```

#### 5. Book Price
```json
{
  "msgType": "unsubscribe",
  "topic": "book-price",
  "filters": {
    "symbol": ""
  },
  "interval": "1000",
  "format": "",
  "topic": "book-price",
  "subscriptionId": "74ab9039805742002bed2e0703089fac2e92902"
}
```

### Estrutura de Mensagens

**Padrão Geral:**
```json
{
  "msgSeqNum": "string",      // Número sequencial da mensagem
  "msgType": "string",         // Tipo: "info", "subscribe", "unsubscribe"
  "topic": "string",           // Tópico: "heartbeat", "order-v2", "book-price", etc.
  "data": "string|object",     // Dados da mensagem
  "sendingTime": "timestamp",  // Timestamp Unix em milissegundos
  "filters": {},               // Filtros de subscrição
  "subscriptionId": "string"   // ID único da subscrição
}
```

### Tópicos Identificados

1. **heartbeat** - Keep-alive da conexão
2. **order-v2** - Atualizações de ordens
3. **position-summary-v2** - Resumo de posições
4. **account-summary** - Resumo da conta
5. **book-price** - Livro de ofertas (Order Book)
6. **financial** - Informações financeiras

### Autenticação

A nova API **não usa token na URL**. A autenticação é feita via:

1. **Cookies de sessão** do navegador
2. **Headers HTTP** (Origin, User-Agent)
3. **Socket ID dinâmico** na URL (`socket-VlnJiReeD`)

**Desafio:** Replicar autenticação fora do navegador requer:
- Extrair cookies de sessão válidos
- Ou fazer login programático via API REST primeiro
- Ou usar browser automation (Selenium/Playwright)

### Próximos Passos

#### Opção 1: Engenharia Reversa Completa
- [ ] Capturar mais mensagens de diferentes tópicos
- [ ] Identificar formato de subscrição para cotações
- [ ] Descobrir como obter socket ID válido
- [ ] Implementar autenticação programática

#### Opção 2: Browser Automation
- [ ] Usar Playwright/Selenium para controlar navegador
- [ ] Reutilizar sessão autenticada do navegador
- [ ] Interceptar mensagens WebSocket
- [ ] Mais confiável mas mais pesado

#### Opção 3: Híbrido (Recomendado)
- [ ] Usuário faz login manual no navegador
- [ ] Script extrai cookies de sessão
- [ ] Conecta WebSocket com cookies válidos
- [ ] Renova sessão periodicamente

### Comparação com API Antiga

| Aspecto | API Antiga (webfeeder) | API Nova (EFS) |
|---------|------------------------|----------------|
| Endpoint | webfeeder.btgpactual.com | efs.btgpactual.com |
| Autenticação | Token na URL | Cookies/Session |
| Protocolo | Simples, documentado | Complexo, proprietário |
| Socket ID | Fixo (token) | Dinâmico (gerado) |
| Mensagens | Estruturadas (classes Python) | JSON puro |
| Dificuldade | Fácil | Média-Alta |

### Recomendação

Para integração imediata e confiável:

1. **Continuar com Yahoo Finance** para ações B3 (já funciona)
2. **Manter código BTG EFS** como feature experimental
3. **Implementar Opção 3** (híbrido) quando necessário

Para dados em tempo real críticos:
- Considerar **API oficial BTG** (se disponível para clientes)
- Ou usar **provedores de dados** (Bloomberg, Refinitiv)

### Código de Exemplo (Conceito)

```python
import websocket
import json
import requests

class BTGEFSConnector:
    def __init__(self, cookies):
        self.cookies = cookies
        self.socket_id = self._get_socket_id()
        self.ws_url = f"wss://efs.btgpactual.com/trader/{self.socket_id}"
    
    def _get_socket_id(self):
        # TODO: Descobrir como gerar/obter socket ID válido
        # Pode exigir chamada REST API primeiro
        pass
    
    def connect(self):
        self.ws = websocket.WebSocketApp(
            self.ws_url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            cookie=self.cookies  # Passar cookies de sessão
        )
        self.ws.run_forever()
    
    def subscribe_book_price(self, symbol):
        msg = {
            "msgType": "subscribe",
            "topic": "book-price",
            "filters": {"symbol": symbol},
            "interval": "1000"
        }
        self.ws.send(json.dumps(msg))
    
    def _on_message(self, ws, message):
        data = json.loads(message)
        topic = data.get('topic')
        
        if topic == 'book-price':
            self._process_book_price(data)
        elif topic == 'heartbeat':
            # Responder heartbeat
            pass
```

### Conclusão

A nova API BTG EFS é significativamente mais complexa que a anterior. Requer:
- Engenharia reversa adicional
- Gerenciamento de sessão/cookies
- Possível violação de ToS (Terms of Service)

**Recomendação:** Manter Yahoo Finance como fonte primária e considerar BTG EFS apenas se houver necessidade crítica de dados em tempo real sub-segundo.

---

**Status:** Investigação em andamento
**Última atualização:** 2026-02-24
