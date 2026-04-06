/**
 * Egreja Investment AI — Real-Time Ticker Tape
 *
 * Professional scrolling price bar showing all 111+ stocks and crypto prices.
 * Auto-refreshes every 15 seconds. Green/red color coding with arrows.
 *
 * Usage: Include this script, then call initTickerTape(apiBase) where apiBase
 * is the backend URL (e.g., 'https://diligent-spirit-production.up.railway.app')
 */

(function() {
    'use strict';

    const REFRESH_INTERVAL = 15000; // 15 seconds
    let _apiBase = '';
    let _animDuration = 120; // seconds for full scroll cycle
    let _tape = null;
    let _track = null;
    let _timer = null;

    function injectStyles() {
        if (document.getElementById('ticker-tape-styles')) return;
        const style = document.createElement('style');
        style.id = 'ticker-tape-styles';
        style.textContent = `
            /* ─── Ticker Tape Bar ──────────────────────────── */
            .ticker-tape-container {
                width: 100%;
                background: #070B12;
                border-bottom: 1px solid #1A2332;
                overflow: hidden;
                position: relative;
                height: 36px;
                font-family: 'JetBrains Mono', 'Courier New', monospace;
                font-size: 12px;
                user-select: none;
                z-index: 99;
            }
            .ticker-tape-container:hover .ticker-tape-track {
                animation-play-state: paused;
            }
            .ticker-tape-track {
                display: flex;
                align-items: center;
                height: 100%;
                white-space: nowrap;
                will-change: transform;
                animation: tickerScroll var(--tape-duration, 120s) linear infinite;
            }
            @keyframes tickerScroll {
                0%   { transform: translateX(0); }
                100% { transform: translateX(-50%); }
            }
            .ticker-tape-item {
                display: inline-flex;
                align-items: center;
                gap: 6px;
                padding: 0 14px;
                height: 100%;
                border-right: 1px solid #111820;
                cursor: default;
                transition: background 0.15s;
            }
            .ticker-tape-item:hover {
                background: #111820;
            }
            .ticker-tape-item .tt-symbol {
                font-weight: 600;
                color: #E2E8F0;
                letter-spacing: 0.3px;
            }
            .ticker-tape-item .tt-price {
                color: #94A3B8;
                font-weight: 400;
            }
            .ticker-tape-item .tt-change {
                font-weight: 600;
                font-size: 11px;
                padding: 1px 5px;
                border-radius: 3px;
                letter-spacing: 0.2px;
            }
            .ticker-tape-item .tt-change.up {
                color: #22C55E;
                background: rgba(34, 197, 94, 0.1);
            }
            .ticker-tape-item .tt-change.down {
                color: #EF4444;
                background: rgba(239, 68, 68, 0.1);
            }
            .ticker-tape-item .tt-change.flat {
                color: #64748B;
                background: rgba(100, 116, 139, 0.1);
            }
            .ticker-tape-item .tt-market {
                font-size: 9px;
                color: #475569;
                font-weight: 400;
                letter-spacing: 0.5px;
            }
            .ticker-tape-item .tt-market.b3 { color: #C9A84C; }
            .ticker-tape-item .tt-market.nyse { color: #60A5FA; }
            .ticker-tape-item .tt-market.crypto { color: #FB923C; }

            /* Gradient fade edges */
            .ticker-tape-container::before,
            .ticker-tape-container::after {
                content: '';
                position: absolute;
                top: 0;
                bottom: 0;
                width: 40px;
                z-index: 2;
                pointer-events: none;
            }
            .ticker-tape-container::before {
                left: 0;
                background: linear-gradient(to right, #070B12, transparent);
            }
            .ticker-tape-container::after {
                right: 0;
                background: linear-gradient(to left, #070B12, transparent);
            }

            /* Loading state */
            .ticker-tape-loading {
                display: flex;
                align-items: center;
                justify-content: center;
                height: 100%;
                color: #475569;
                font-size: 11px;
                gap: 8px;
            }
            .ticker-tape-loading .dot {
                width: 4px; height: 4px; border-radius: 50%;
                background: #475569;
                animation: tapePulse 1.2s infinite ease-in-out;
            }
            .ticker-tape-loading .dot:nth-child(2) { animation-delay: 0.2s; }
            .ticker-tape-loading .dot:nth-child(3) { animation-delay: 0.4s; }
            @keyframes tapePulse {
                0%, 80%, 100% { opacity: 0.3; }
                40% { opacity: 1; }
            }
        `;
        document.head.appendChild(style);
    }

    function createContainer() {
        _tape = document.createElement('div');
        _tape.className = 'ticker-tape-container';
        _tape.innerHTML = `
            <div class="ticker-tape-loading">
                <div class="dot"></div><div class="dot"></div><div class="dot"></div>
                <span>Carregando preços...</span>
            </div>
        `;
        // Insert after header or at top of body
        const header = document.querySelector('header');
        if (header && header.nextSibling) {
            header.parentNode.insertBefore(_tape, header.nextSibling);
        } else {
            document.body.prepend(_tape);
        }
    }

    function formatPrice(price, currency) {
        if (price >= 1000) return price.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        if (price >= 1) return price.toFixed(2);
        return price.toFixed(4);
    }

    function buildTapeHTML(items) {
        if (!items || items.length === 0) return '';

        let html = '';
        for (const item of items) {
            const dir = item.c > 0.01 ? 'up' : (item.c < -0.01 ? 'down' : 'flat');
            const arrow = dir === 'up' ? '▲' : (dir === 'down' ? '▼' : '─');
            const sign = item.c > 0 ? '+' : '';
            const mktClass = item.m === 'B3' ? 'b3' : (item.m === 'CRYPTO' ? 'crypto' : 'nyse');
            const cur = item.cur === 'BRL' ? 'R$' : '$';

            html += `<div class="ticker-tape-item">
                <span class="tt-market ${mktClass}">${item.m}</span>
                <span class="tt-symbol">${item.t}</span>
                <span class="tt-price">${cur}${formatPrice(item.p, item.cur)}</span>
                <span class="tt-change ${dir}">${arrow} ${sign}${item.c.toFixed(2)}%</span>
            </div>`;
        }
        // Duplicate for seamless loop
        return html + html;
    }

    async function fetchPrices() {
        try {
            const res = await fetch(_apiBase + '/api/ticker-tape');
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            const data = await res.json();
            return data.items || [];
        } catch (err) {
            console.warn('[TickerTape] Fetch error:', err.message);
            return null;
        }
    }

    let _initialized = false;

    async function updateTape() {
        const items = await fetchPrices();
        if (!items || items.length === 0) return;

        // Fast scroll: 0.35s per item so 120 items = ~42s full cycle
        _animDuration = Math.max(30, items.length * 0.35);

        if (!_initialized) {
            // First load: build full HTML
            _tape.style.setProperty('--tape-duration', _animDuration + 's');
            _tape.innerHTML = `<div class="ticker-tape-track">${buildTapeHTML(items)}</div>`;
            _track = _tape.querySelector('.ticker-tape-track');
            _initialized = true;
        } else {
            // Subsequent loads: update prices in-place WITHOUT resetting scroll position
            const allItems = _track.querySelectorAll('.ticker-tape-item');
            const itemMap = {};
            for (const item of items) {
                itemMap[item.t] = item;
            }
            allItems.forEach(el => {
                const symEl = el.querySelector('.tt-symbol');
                if (!symEl) return;
                const ticker = symEl.textContent.trim();
                const data = itemMap[ticker];
                if (!data) return;

                // Update price
                const priceEl = el.querySelector('.tt-price');
                if (priceEl) {
                    const cur = data.cur === 'BRL' ? 'R$' : '$';
                    priceEl.textContent = `${cur}${formatPrice(data.p, data.cur)}`;
                }

                // Update change
                const changeEl = el.querySelector('.tt-change');
                if (changeEl) {
                    const dir = data.c > 0.01 ? 'up' : (data.c < -0.01 ? 'down' : 'flat');
                    const arrow = dir === 'up' ? '▲' : (dir === 'down' ? '▼' : '─');
                    const sign = data.c > 0 ? '+' : '';
                    changeEl.className = `tt-change ${dir}`;
                    changeEl.textContent = `${arrow} ${sign}${data.c.toFixed(2)}%`;
                }
            });
        }
    }

    /**
     * Initialize the ticker tape.
     * @param {string} apiBase - Backend API base URL
     * @param {object} opts - Optional: { position: 'top'|'bottom' }
     */
    window.initTickerTape = function(apiBase, opts) {
        _apiBase = apiBase || '';
        opts = opts || {};

        injectStyles();
        createContainer();

        // Initial load
        updateTape();

        // Auto-refresh
        _timer = setInterval(updateTape, REFRESH_INTERVAL);

        // Pause on visibility hidden
        document.addEventListener('visibilitychange', function() {
            if (document.hidden) {
                clearInterval(_timer);
            } else {
                updateTape();
                _timer = setInterval(updateTape, REFRESH_INTERVAL);
            }
        });
    };
})();
