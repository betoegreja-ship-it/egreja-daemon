# Unified Brain - Integration Guide

## Quick Integration

### 1. Add to Main Flask Application

In your main `app.py` or `main.py`:

```python
from flask import Flask
from modules.unified_brain import create_unified_brain_blueprint
import mysql.connector

# Initialize Flask app
app = Flask(__name__)

# Database connection factory
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="egreja_user",
        password="your_password",
        database="egreja_trading",
        autocommit=False
    )

# Create and register all module blueprints
from modules.arbitrage import create_arbitrage_blueprint
from modules.crypto import create_crypto_blueprint
from modules.stocks import create_stocks_blueprint
from modules.derivatives import create_strategies_blueprint
from modules.long_horizon import create_long_horizon_blueprint
from modules.unified_brain import create_unified_brain_blueprint

# Register blueprints
app.register_blueprint(create_arbitrage_blueprint(get_db_connection, app.logger))
app.register_blueprint(create_crypto_blueprint(get_db_connection, app.logger))
app.register_blueprint(create_stocks_blueprint(get_db_connection, app.logger))
app.register_blueprint(create_strategies_blueprint(get_db_connection, app.logger, None, {}))
app.register_blueprint(create_long_horizon_blueprint(get_db_connection, app.logger))

# Register UNIFIED BRAIN LAST (it reads from all others)
app.register_blueprint(create_unified_brain_blueprint(get_db_connection, app.logger))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
```

### 2. Initialize Database Tables

Run this during app startup to create the Unified Brain tables:

```python
from modules.unified_brain import create_unified_brain_tables

def initialize_databases():
    """Initialize all database schemas"""
    conn = get_db_connection()

    try:
        # Initialize all module tables
        create_unified_brain_tables(conn)

        conn.commit()
        print("✓ All database tables initialized successfully")
    except Exception as e:
        conn.rollback()
        print(f"✗ Database initialization failed: {e}")
        raise
    finally:
        conn.close()

# Call during app startup
if __name__ == '__main__':
    initialize_databases()
    app.run(...)
```

## Module-to-Module Integration Examples

### Arbitrage Module Using Unified Brain

```python
# In modules/arbitrage/execution.py

from modules.unified_brain.regime_detector import RegimeDetector
from modules.unified_brain.learning_engine import LearningEngine
from datetime import date

class ArbitrageExecutor:
    def __init__(self, db_fn, logger):
        self.db_fn = db_fn
        self.logger = logger
        self.regime_detector = RegimeDetector(db_fn=db_fn, log=logger)
        self.learning_engine = LearningEngine(db_fn=db_fn, log=logger)

    def execute_spread_trade(self, pair_a, pair_b, spread_bps):
        """Execute arbitrage trade with brain guidance"""

        # Get current market regime
        regime = self.regime_detector.get_current_regime()
        self.logger.info(f"Market regime: {regime['regime_type']}")

        # Get lessons learned about this pair
        lessons = self.learning_engine.get_lessons_summary()
        pair_lessons = [l for l in lessons['lessons']
                       if pair_a in l['description'] or pair_b in l['description']]

        if pair_lessons:
            self.logger.info(f"Found {len(pair_lessons)} lessons for {pair_a}-{pair_b}")
            best_lesson = max(pair_lessons, key=lambda x: x['confidence'])
            self.logger.info(f"Best guidance: {best_lesson['description']}")

        # Adjust position size based on regime
        if regime['regime_type'] == 'VOLATILE':
            position_size = 80000  # 20% reduction in volatile
        elif regime['regime_type'] == 'SIDEWAYS':
            position_size = 120000  # 20% increase in sideways
        else:
            position_size = 100000  # Standard

        # Execute trade
        return self._execute_internal(pair_a, pair_b, spread_bps, position_size)
```

### Crypto Module Using Unified Brain

```python
# In modules/crypto/signal_generator.py

from modules.unified_brain.correlation_engine import CorrelationEngine
from modules.unified_brain.decision_engine import DecisionEngine

class CryptoSignalGenerator:
    def __init__(self, db_fn, logger):
        self.db_fn = db_fn
        self.logger = logger
        self.correlation_engine = CorrelationEngine(db_fn=db_fn, log=logger)
        self.decision_engine = DecisionEngine(db_fn=db_fn, log=logger)

    def generate_signals(self, btc_data, eth_data):
        """Generate crypto signals informed by brain"""

        # Check for strong cross-module signals
        decisions = self.decision_engine.get_opportunity_decisions()

        crypto_decisions = [
            d for d in decisions['opportunities']
            if 'Crypto' in d.get('module', '') or 'BTC' in d.get('asset', '')
        ]

        if crypto_decisions:
            for decision in crypto_decisions:
                self.logger.info(
                    f"Brain signal: {decision['recommendation']} "
                    f"(confidence: {decision['confidence']}%)"
                )

        # Get crypto-specific correlations
        correlations = self.correlation_engine.get_asset_correlations('BTC')

        self.logger.info(f"BTC correlations: {len(correlations['correlations'])} tracked")

        # Generate signals informed by brain knowledge
        return self._generate_signals_internal(btc_data, eth_data)
```

### Stocks Module Using Unified Brain

```python
# In modules/stocks/scoring.py

from modules.unified_brain.learning_engine import LearningEngine
from modules.unified_brain.regime_detector import RegimeDetector

class StockScorer:
    def __init__(self, db_fn, logger):
        self.db_fn = db_fn
        self.logger = logger
        self.learning_engine = LearningEngine(db_fn=db_fn, log=logger)
        self.regime_detector = RegimeDetector(db_fn=db_fn, log=logger)

    def score_stock(self, ticker, technical_data, fundamental_data):
        """Score a stock with brain-informed insights"""

        # Check for relevant lessons
        lessons = self.learning_engine.get_lessons_summary()
        relevant_lessons = [
            l for l in lessons['lessons']
            if l['module'] == 'Stocks' and l['confidence'] > 80
        ]

        self.logger.info(f"Using {len(relevant_lessons)} high-confidence stock lessons")

        # Get current regime for context
        regime = self.regime_detector.get_current_regime()

        # Adjust scoring based on regime
        base_score = self._calculate_base_score(ticker, technical_data, fundamental_data)

        if regime['regime_type'] == 'BULL':
            # Favor momentum in bull market
            momentum_weight = 1.2
        elif regime['regime_type'] == 'BEAR':
            # Favor quality/dividends in bear market
            momentum_weight = 0.8
        else:
            momentum_weight = 1.0

        # Apply lessons to scoring
        for lesson in relevant_lessons:
            if 'RSI' in lesson['description'] and lesson['impact_score'] > 8:
                # Apply RSI lesson with high impact
                pass  # Adjust scoring accordingly

        return base_score * momentum_weight
```

### Derivatives Module Using Unified Brain

```python
# In modules/derivatives/strategy_selector.py

from modules.unified_brain.correlation_engine import CorrelationEngine
from modules.unified_brain.regime_detector import RegimeDetector

class StrategySelector:
    def __init__(self, db_fn, logger):
        self.db_fn = db_fn
        self.logger = logger
        self.correlation_engine = CorrelationEngine(db_fn=db_fn, log=logger)
        self.regime_detector = RegimeDetector(db_fn=db_fn, log=logger)

    def select_strategies(self):
        """Select optimal strategies based on regime and synergies"""

        # Get current regime
        regime = self.regime_detector.get_current_regime()
        self.logger.info(f"Selecting strategies for {regime['regime_type']} regime")

        # Get strategy correlations
        synergies = self.correlation_engine.get_strategy_correlations()
        high_synergy_pairs = synergies['high_synergy_pairs']

        strategies = []

        # Select based on regime
        if regime['regime_type'] == 'SIDEWAYS':
            # FST and SKEW_ARB excel in sideways
            strategies.extend(['FST', 'SKEW_ARB'])

            # Check if we can add complementary strategies
            for pair in high_synergy_pairs:
                if pair['strategy_a'] == 'FST' and pair['synergy_potential'] == 'high':
                    strategies.append(pair['strategy_b'])

        elif regime['regime_type'] == 'VOLATILE':
            # Vol arb strategies in volatile
            strategies.extend(['VOL_ARB', 'SKEW_ARB'])

        elif regime['regime_type'] == 'BULL':
            # PCP and roll arb in bull
            strategies.extend(['PCP', 'ROLL_ARB'])

        self.logger.info(f"Selected strategies: {strategies}")
        return strategies
```

### Long Horizon Module Using Unified Brain

```python
# In modules/long_horizon/portfolio_manager.py

from modules.unified_brain.decision_engine import DecisionEngine
from modules.unified_brain.regime_detector import RegimeDetector

class PortfolioManager:
    def __init__(self, db_fn, logger):
        self.db_fn = db_fn
        self.logger = logger
        self.decision_engine = DecisionEngine(db_fn=db_fn, log=logger)
        self.regime_detector = RegimeDetector(db_fn=db_fn, log=logger)

    def rebalance_portfolio(self, current_positions):
        """Rebalance portfolio with brain guidance"""

        # Check for urgent risk decisions
        risk_decisions = self.decision_engine.get_risk_decisions()

        if risk_decisions['alerts']:
            self.logger.warning(
                f"⚠️  {len(risk_decisions['alerts'])} risk alerts - "
                "increasing defensiveness"
            )
            # Reduce overall leverage
            pass

        # Check for sector rotation signals
        all_decisions = self.decision_engine.get_all_decisions()
        rotation_decisions = [
            d for d in all_decisions['decisions']
            if d['decision_type'] == 'SECTOR_ROTATION'
        ]

        if rotation_decisions:
            for decision in rotation_decisions:
                self.logger.info(f"Sector rotation detected: {decision['recommendation']}")
                # Apply sector rotation to portfolio
                pass

        # Get regime recommendation
        regime = self.regime_detector.get_current_regime()
        recommendation = regime.get('recommendation', {})

        self.logger.info(f"Regime strategy: {recommendation.get('strategy', 'NEUTRAL')}")

        # Rebalance accordingly
        return self._rebalance_internal(current_positions)
```

## Setting Up Monitoring & Alerts

### Health Check Endpoint

```python
# In your monitoring service

import requests
import time
from datetime import datetime

class BrainHealthMonitor:
    def __init__(self, brain_url='http://localhost:5000/brain'):
        self.brain_url = brain_url
        self.last_score = None

    def check_health(self):
        """Check brain health and alert if degradation"""
        try:
            response = requests.get(f'{self.brain_url}/health', timeout=5)
            health = response.json()

            # Check operational status
            if health.get('status') != 'healthy':
                alert(f"⚠️ Brain health DEGRADED: {health}")
                return False

            # Check brain score trend
            current_score = health.get('brain_score', 0)
            if self.last_score and current_score < self.last_score - 5:
                alert(f"⚠️ Brain score dropped from {self.last_score} to {current_score}")

            self.last_score = current_score

            # Check for urgent decisions
            response = requests.get(f'{self.brain_url}/decisions?filter=urgent')
            decisions = response.json()

            if decisions.get('decisions', {}).get('urgent_count', 0) > 0:
                alert(f"🚨 {decisions['decisions']['urgent_count']} URGENT decisions")

            return True

        except Exception as e:
            alert(f"❌ Brain health check failed: {e}")
            return False

    def run_continuous(self, interval_seconds=300):
        """Run health checks continuously"""
        while True:
            self.check_health()
            time.sleep(interval_seconds)

def alert(message):
    """Send alert (email, Slack, etc.)"""
    print(f"[{datetime.now().isoformat()}] {message}")
    # TODO: Implement email/Slack/SMS alerting
```

### Decision Tracking

```python
# In your monitoring service

class DecisionTracker:
    def __init__(self, brain_url='http://localhost:5000/brain'):
        self.brain_url = brain_url
        self.decisions_log = []

    def track_decisions(self):
        """Track decision accuracy over time"""
        response = requests.get(f'{self.brain_url}/decisions')
        decisions_data = response.json()

        active_decisions = [
            d for d in decisions_data.get('decisions', [])
            if d.get('status') == 'active'
        ]

        self.decisions_log.extend(active_decisions)

        # Calculate accuracy
        total = len(self.decisions_log)
        correct = sum(1 for d in self.decisions_log if d.get('outcome') == 'success')

        accuracy = (correct / total * 100) if total > 0 else 0

        print(f"Decision accuracy: {accuracy:.1f}% ({correct}/{total})")

        # Target: > 75%
        if accuracy < 75:
            alert(f"⚠️ Decision accuracy below target: {accuracy:.1f}%")

        return accuracy
```

## Dashboard Integration

### React/Vue Frontend Example

```javascript
// In your dashboard component

import React, { useEffect, useState } from 'react';

export function BrainDashboard() {
  const [brainState, setBrainState] = useState(null);
  const [digest, setDigest] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Fetch brain system state
    Promise.all([
      fetch('/brain/system-state').then(r => r.json()),
      fetch('/brain/digest').then(r => r.json()),
    ]).then(([state, digest]) => {
      setBrainState(state.brain_state);
      setDigest(digest.digest);
      setLoading(false);
    });

    // Refresh every 30 seconds
    const interval = setInterval(() => {
      fetch('/brain/system-state').then(r => r.json()).then(data => {
        setBrainState(data.brain_state);
      });
    }, 30000);

    return () => clearInterval(interval);
  }, []);

  if (loading) return <div>Loading Brain...</div>;

  return (
    <div className="brain-dashboard">
      <div className="brain-score">
        <h2>Brain Score</h2>
        <div className="score-display">
          {brainState.brain_score.toFixed(1)}/100
        </div>
        <p className="phase">{brainState.phase}</p>
      </div>

      <div className="market-regime">
        <h3>Market Regime</h3>
        <div className="regime-badge">
          {brainState.market_regime}
        </div>
      </div>

      <div className="key-metrics">
        <div className="metric">
          <span>Lessons Learned</span>
          <strong>{brainState.lessons_learned}</strong>
        </div>
        <div className="metric">
          <span>Patterns Active</span>
          <strong>{brainState.active_patterns}</strong>
        </div>
        <div className="metric">
          <span>Decisions Active</span>
          <strong>{brainState.modules_count}</strong>
        </div>
      </div>

      <div className="digest">
        <h3>Today's Digest</h3>
        <div className="insights">
          {digest.key_insights.map((insight, i) => (
            <p key={i}>✓ {insight}</p>
          ))}
        </div>
      </div>
    </div>
  );
}
```

## Testing

### Unit Tests Example

```python
# In tests/test_unified_brain.py

import pytest
from modules.unified_brain.learning_engine import LearningEngine
from modules.unified_brain.regime_detector import RegimeDetector
from modules.unified_brain.decision_engine import DecisionEngine

def test_learning_engine_initialization():
    engine = LearningEngine()
    assert engine is not None
    assert len(engine._demo_data['lessons']) > 0

def test_lessons_summary():
    engine = LearningEngine()
    summary = engine.get_lessons_summary()

    assert summary['total_lessons'] == 45
    assert summary['average_confidence'] > 70
    assert len(summary['by_module']) == 5

def test_regime_detection():
    detector = RegimeDetector()
    regime = detector.get_current_regime()

    assert regime['regime_type'] in ['BULL', 'BEAR', 'SIDEWAYS', 'VOLATILE', 'CRISIS', 'WEEKEND_STANDBY']
    assert 0 <= regime['confidence'] <= 100

def test_decision_engine():
    engine = DecisionEngine()
    decisions = engine.get_all_decisions()

    assert len(decisions['decisions']) > 0
    assert all(0 <= d['confidence'] <= 100 for d in decisions['decisions'])

@pytest.mark.integration
def test_flask_endpoints(client):
    """Test Flask endpoints"""

    # Test system state
    response = client.get('/brain/system-state')
    assert response.status_code == 200
    data = response.get_json()
    assert 'brain_state' in data

    # Test digest
    response = client.get('/brain/digest')
    assert response.status_code == 200

    # Test lessons
    response = client.get('/brain/lessons?min_confidence=80')
    assert response.status_code == 200

    # Test decisions
    response = client.get('/brain/decisions?filter=urgent')
    assert response.status_code == 200
```

---

## Summary

The Unified Brain integrates seamlessly with all 5 Egreja modules:

1. **Arbitrage** — Gets regime guidance & pair lessons
2. **Crypto** — Gets cross-module signals & correlations
3. **Stocks** — Gets regime context & high-confidence lessons
4. **Derivatives** — Gets strategy synergies & regime recommendations
5. **Long Horizon** — Gets risk alerts & sector rotation signals

All modules feed data INTO the brain, which synthesizes intelligence and broadcasts recommendations back OUT to all modules.

**The result**: An integrated, learning system where intelligence flows bidirectionally, enabling smarter decisions across the entire trading platform.
