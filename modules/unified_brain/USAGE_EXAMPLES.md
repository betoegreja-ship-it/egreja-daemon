# Unified Brain - Usage Examples

## Quick Start

### 1. Initialize the Module

```python
from flask import Flask
from modules.unified_brain import create_unified_brain_blueprint
import mysql.connector

# Database connection factory
def get_db():
    return mysql.connector.connect(
        host="localhost",
        user="egreja_user",
        password="secure_password",
        database="egreja_trading"
    )

app = Flask(__name__)

# Create and register blueprint
brain_bp = create_unified_brain_blueprint(
    db_fn=get_db,
    log=app.logger
)
app.register_blueprint(brain_bp)

if __name__ == '__main__':
    app.run(debug=True, port=5000)
```

### 2. Access the API Endpoints

#### Get Brain System State
```bash
curl http://localhost:5000/brain/system-state
```

Response:
```json
{
  "status": "success",
  "brain_state": {
    "timestamp": "2026-04-04T19:51:00",
    "brain_status": "operational",
    "brain_score": 34.7,
    "phase": "early_learning",
    "market_regime": "WEEKEND_STANDBY",
    "modules_count": 5,
    "lessons_learned": 45,
    "patterns_detected": 12,
    "active_patterns": 12,
    "correlations_tracked": 8
  }
}
```

#### Get Daily Digest
```bash
curl http://localhost:5000/brain/digest
```

Response:
```json
{
  "status": "success",
  "digest": {
    "date": "2026-04-04",
    "report_time": "2026-04-04T19:51:00",
    "summary": "Relatório de Inteligência: Cérebro Egreja",
    "modules_status": {
      "Arbitrage": "preparado (spreads em range histórico)",
      "Crypto": "ativo (BTC volatilidade normal)",
      "Stocks": "fechado (fim de semana)",
      "Derivatives": "fechado (fim de semana)",
      "Long_Horizon": "monitorando rebalance"
    },
    "key_insights": [
      "Mercado em WEEKEND_STANDBY — próxima sessão segunda-feira",
      "Correlações mantêm padrão: PETR4-VALE3 em 0.87",
      "Nenhum alerta crítico detectado",
      "Próxima oportunidade de arbi esperada segunda-feira"
    ],
    "alerts": [
      {
        "severity": "info",
        "message": "Fim de semana — monitoramento passivo ativo"
      }
    ]
  }
}
```

## Advanced Usage Examples

### 3. Programmatic Access to Engines

```python
from modules.unified_brain.learning_engine import LearningEngine
from modules.unified_brain.regime_detector import RegimeDetector
from modules.unified_brain.decision_engine import DecisionEngine
from modules.unified_brain.correlation_engine import CorrelationEngine

# Initialize engines
learning = LearningEngine(db_fn=get_db, log=logger)
regime = RegimeDetector(db_fn=get_db, log=logger)
decisions = DecisionEngine(db_fn=get_db, log=logger)
correlations = CorrelationEngine(db_fn=get_db, log=logger)

# Get current market regime
current_regime = regime.get_current_regime()
print(f"Market regime: {current_regime['regime_type']}")
print(f"Confidence: {current_regime['confidence']}")

# Get recommendations based on regime
recommendation = regime.get_regime_recommendation()
for action in recommendation['recommendation']['actions']:
    print(f"- {action}")

# Get all lessons learned
lessons = learning.get_lessons_summary()
print(f"Total lessons: {lessons['total_lessons']}")
print(f"Average confidence: {lessons['average_confidence']}")
```

### 4. Filter Lessons by Module

```bash
# Get only Arbitrage lessons
curl "http://localhost:5000/brain/lessons?module=Arbitrage&min_confidence=75"

# Get only high-confidence Derivatives lessons
curl "http://localhost:5000/brain/lessons?module=Derivatives&min_confidence=85"
```

Response:
```json
{
  "status": "success",
  "total_lessons": 8,
  "lessons": [
    {
      "module": "Arbitrage",
      "lesson_type": "Pattern",
      "description": "PETR4-VALE3 spread compression em 47% dos casos antecede queda de 2-3% em 48h",
      "impact_score": 8.2,
      "confidence": 76,
      "learned_at": "2025-11-15"
    },
    ...
  ]
}
```

### 5. Analyze Cross-Module Patterns

```bash
curl "http://localhost:5000/brain/patterns?min_confidence=80"
```

Response:
```json
{
  "status": "success",
  "total_patterns": 12,
  "active_patterns": 12,
  "patterns": [
    {
      "pattern_type": "Volatility_Clustering",
      "description": "Alta volatilidade em 1 módulo → próximos 1-3d volatilidade sobe em todos",
      "modules_involved": ["Crypto", "Stocks", "Derivatives", "Arbitrage"],
      "correlation": 0.71,
      "confidence": 89,
      "occurrences": 47,
      "active": true
    }
  ]
}
```

### 6. Monitor Risk Across All Modules

```bash
curl http://localhost:5000/brain/risk-radar
```

Response:
```json
{
  "status": "success",
  "risk_data": {
    "timestamp": "2026-04-04T19:51:00",
    "overall_risk_level": "moderate",
    "risk_by_module": {
      "Arbitrage": "low_spread_risk",
      "Crypto": "moderate_volatility",
      "Stocks": "neutral_weekend",
      "Derivatives": "vega_monitoring",
      "Long_Horizon": "normal_drawdown"
    },
    "alerts": [
      {
        "type": "VEGA_RISK",
        "level": "warning",
        "message": "Portfolio Vega > 7.5k monitore"
      }
    ]
  }
}
```

### 7. Get AI Recommendations

```bash
# Get all urgent decisions
curl "http://localhost:5000/brain/decisions?filter=urgent"

# Get only opportunities
curl "http://localhost:5000/brain/decisions?filter=opportunities"

# Get decisions for specific module
curl "http://localhost:5000/brain/decisions?module=Derivatives"
```

Response (opportunities):
```json
{
  "status": "success",
  "decisions": {
    "opportunities_count": 4,
    "opportunities": [
      {
        "decision_id": "DEC_20260404_001",
        "decision_type": "STRONG_BUY",
        "asset": "PETR4",
        "module": "Multi-module",
        "recommendation": "PETR4 FORTE COMPRA: Score 78 + Arbi oportunidade + Momentum RSI",
        "confidence": 86,
        "risk_assessment": {
          "best_case_gain": 8.2,
          "worst_case_loss": -3.5,
          "risk_reward": 2.34
        },
        "status": "active"
      }
    ]
  }
}
```

### 8. Analyze Market Regime with History

```bash
curl "http://localhost:5000/brain/regime?include_history=true&days=60"
```

Response:
```json
{
  "status": "success",
  "regime_data": {
    "current": {
      "regime_type": "WEEKEND_STANDBY",
      "confidence": 98,
      "indicators": {
        "ibovespa_trend": "closed",
        "rsi_stocks": 50.0,
        "iv_options": 22.5,
        "arbi_spreads_bps": 0.0
      }
    },
    "probabilities": {
      "BULL": 0.20,
      "BEAR": 0.15,
      "SIDEWAYS": 0.25,
      "VOLATILE": 0.30,
      "CRISIS": 0.10
    },
    "recommendation": {
      "strategy": "MONITORING",
      "actions": [
        "Monitora notícias macro overnight",
        "Crypto trading continues normalmente",
        "Preparar ordens para segunda-feira"
      ]
    },
    "history": [
      {
        "date": "2026-03-20",
        "regime_type": "BULL",
        "confidence": 0.87,
        "duration_days": 15
      }
    ]
  }
}
```

### 9. View Brain Evolution

```bash
curl http://localhost:5000/brain/evolution
```

Response:
```json
{
  "status": "success",
  "evolution": {
    "current_score": 34.7,
    "phase": "early_learning",
    "accuracy_pct": 65.0,
    "total_lessons": 45,
    "patterns_active": 12,
    "decision_accuracy": 0.8667,
    "evolution_history": [
      {
        "date": "2025-10-20",
        "evolution_score": 20.1,
        "total_lessons": 5,
        "accuracy_pct": 55.0,
        "patterns_active": 3
      },
      {
        "date": "2026-04-04",
        "evolution_score": 34.7,
        "total_lessons": 45,
        "accuracy_pct": 65.0,
        "patterns_active": 12
      }
    ]
  }
}
```

### 10. Check Correlations

```bash
# Module correlations (default)
curl http://localhost:5000/brain/correlations

# Asset correlations
curl "http://localhost:5000/brain/correlations?type=assets&asset=PETR4"

# Strategy correlations
curl "http://localhost:5000/brain/correlations?type=strategies"

# Macro factor correlations
curl "http://localhost:5000/brain/correlations?type=macro"
```

Response (modules):
```json
{
  "status": "success",
  "correlation_type": "modules",
  "data": {
    "correlation_matrix": {
      "Stocks": {
        "Derivatives": 0.76,
        "Crypto": 0.42,
        "Arbitrage": 0.65,
        "Long_Horizon": 0.82
      },
      "Derivatives": {
        "Arbitrage": 0.89,
        "Crypto": 0.38
      }
    },
    "strongest_link": {
      "module_a": "Arbitrage",
      "module_b": "Derivatives",
      "correlation": 0.89
    },
    "tightest_coupling": {
      "module_a": "Crypto",
      "module_b": "Derivatives",
      "correlation": 0.38
    }
  }
}
```

### 11. Module-Specific Feed

```bash
# Get Arbitrage module feed
curl http://localhost:5000/brain/module-feed/Arbitrage

# Get Crypto module feed
curl http://localhost:5000/brain/module-feed/Crypto

# Get Derivatives module feed
curl http://localhost:5000/brain/module-feed/Derivatives
```

Response:
```json
{
  "status": "success",
  "module": "Arbitrage",
  "decisions": {
    "count": 2,
    "decisions": [
      {
        "decision_type": "TIMING_WINDOW",
        "recommendation": "BTC pump detected → execute PETR4-VALE3 arbi in 45-90min",
        "confidence": 79,
        "status": "expired"
      }
    ]
  },
  "lessons_count": 9,
  "lessons_sample": [
    {
      "lesson_type": "Pattern",
      "description": "Melhor janela temporal para arbi: 09:35-10:15 (85% mais oportunidades)",
      "impact_score": 7.9,
      "confidence": 82
    }
  ]
}
```

### 12. Cross-Module Insights

```bash
curl http://localhost:5000/brain/cross-insights
```

Response:
```json
{
  "status": "success",
  "insights": {
    "cross_module_correlations": {
      "Arbitrage-Derivatives": 0.89,
      "Stocks-Long_Horizon": 0.82,
      "Stocks-Derivatives": 0.76
    },
    "strongest_link": {
      "module_a": "Arbitrage",
      "module_b": "Derivatives",
      "description": "Execução excelente para estratégias combinadas"
    },
    "cross_domain_patterns": 8,
    "synergy_opportunities": 4,
    "summary": "Arbitrage-Derivatives coupling 0.89 ideal para PCP+FST simultaneamente"
  }
}
```

## Integration with Other Modules

### Using Regime Detection in Arbitrage Module

```python
from modules.unified_brain.regime_detector import RegimeDetector

detector = RegimeDetector(db_fn=get_db, log=logger)

def execute_arbi_trade(spread_opportunity):
    regime = detector.get_current_regime()

    if regime['regime_type'] == 'SIDEWAYS':
        # In sideways, spreads are normal, execute with standard parameters
        position_size = 100000  # Standard
    elif regime['regime_type'] == 'VOLATILE':
        # In volatile, reduce position size by 20%
        position_size = 80000
    elif regime['regime_type'] == 'CRISIS':
        # In crisis, don't execute
        return None
    else:
        position_size = 100000

    return execute_trade(spread_opportunity, position_size)
```

### Using Decision Engine in Long_Horizon Module

```python
from modules.unified_brain.decision_engine import DecisionEngine

decisions = DecisionEngine(db_fn=get_db, log=logger)

def rebalance_portfolio():
    # Get all active decisions
    all_decisions = decisions.get_all_decisions(status='active')

    # Look for sector rotation signals
    rotation_decisions = [
        d for d in all_decisions['decisions']
        if d['decision_type'] == 'SECTOR_ROTATION'
    ]

    if rotation_decisions:
        for decision in rotation_decisions:
            # Apply sector rotation to portfolio
            apply_sector_rotation(decision['recommendation'])
```

### Using Correlation Engine in Derivatives Module

```python
from modules.unified_brain.correlation_engine import CorrelationEngine

correlations = CorrelationEngine(db_fn=get_db, log=logger)

def select_strategies():
    # Get strategy synergy analysis
    synergy = correlations.get_strategy_correlations()

    # Find low-correlation strategy pairs
    high_synergy = synergy['high_synergy_pairs']

    # Execute complementary strategies
    for pair in high_synergy:
        if pair['synergy_potential'] == 'high':
            execute_combined_strategy(pair['strategy_a'], pair['strategy_b'])
```

## Monitoring & Alerts

### Set Up Custom Monitoring

```python
def monitor_brain_health():
    import requests

    response = requests.get('http://localhost:5000/brain/health')
    health = response.json()

    if health['status'] != 'healthy':
        send_alert(f"Brain health degraded: {health}")

    if health['brain_score'] < 30:
        send_alert(f"Brain learning phase slow, score: {health['brain_score']}")

    return health

# Run every 15 minutes
schedule.every(15).minutes.do(monitor_brain_health)
```

### Track Decision Accuracy

```python
def track_decision_outcomes():
    decisions = requests.get(
        'http://localhost:5000/brain/decisions?filter=all'
    ).json()

    active_decisions = [
        d for d in decisions['decisions']
        if d['status'] == 'active'
    ]

    correct = sum(
        1 for d in active_decisions
        if d.get('outcome') == 'success'
    )

    accuracy = correct / len(active_decisions) if active_decisions else 0
    print(f"Decision accuracy: {accuracy:.1%}")

    return accuracy
```

## Dashboard Integration

```javascript
// Frontend JavaScript to fetch and display brain state
async function updateBrainDashboard() {
    const response = await fetch('/brain/system-state');
    const data = await response.json();

    // Update brain score
    document.getElementById('brain-score').textContent =
        data.brain_state.brain_score.toFixed(1);

    // Update regime
    document.getElementById('market-regime').textContent =
        data.brain_state.market_regime;

    // Update lessons count
    document.getElementById('lessons-count').textContent =
        data.brain_state.lessons_learned;

    // Update active patterns
    document.getElementById('patterns-count').textContent =
        data.brain_state.active_patterns;
}

// Refresh every 30 seconds
setInterval(updateBrainDashboard, 30000);
```

---

**For more details, see README.md**
