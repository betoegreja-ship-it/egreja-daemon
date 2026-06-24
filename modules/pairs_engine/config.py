"""Configuracao dos 15 pares stat arbi B3."""

PAIRS_CONFIG = [
    # ─────────── TIER 1: Holding x Operacional ───────────
    {
        'id': 'ITUB4-ITSA4', 'name': 'Itau / Itausa',
        'leg_a': 'ITUB4', 'leg_b': 'ITSA4',
        'pair_type': 'HOLDING',
        'expected_spread_pct': 22.0,   # ITSA negocia ~22% abaixo do equiv ITUB
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'A',          # ambas top 5 IBOV
        'beta_a_to_b': 1.0,             # hedge ratio inicial estatico
        'enabled': True,
    },
    {
        'id': 'VALE3-BRAP4', 'name': 'Vale / Bradespar',
        'leg_a': 'VALE3', 'leg_b': 'BRAP4',
        'pair_type': 'HOLDING',
        'expected_spread_pct': 30.0,
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'B',          # BRAP tem liquidez menor
        'beta_a_to_b': 1.0,
        'enabled': True,
    },

    # ─────────── TIER 1: Classes ON/PN ───────────
    {
        'id': 'PETR4-PETR3', 'name': 'Petrobras PN/ON',
        'leg_a': 'PETR4', 'leg_b': 'PETR3',
        'pair_type': 'CLASSES',
        'expected_spread_pct': 3.0,     # PN trade c/ premium ~3% sobre ON
        'z_entry': 2.0, 'z_exit': 0.3, 'z_stop': 3.5,
        'liquidity_tier': 'A',
        'beta_a_to_b': 1.0,
        'enabled': True,
    },
    {
        'id': 'BBDC4-BBDC3', 'name': 'Bradesco PN/ON',
        'leg_a': 'BBDC4', 'leg_b': 'BBDC3',
        'pair_type': 'CLASSES',
        'expected_spread_pct': 4.0,
        'z_entry': 2.0, 'z_exit': 0.3, 'z_stop': 3.5,
        'liquidity_tier': 'A',
        'beta_a_to_b': 1.0,
        'enabled': True,
    },
    {
        'id': 'GGBR4-GGBR3', 'name': 'Gerdau PN/ON',
        'leg_a': 'GGBR4', 'leg_b': 'GGBR3',
        'pair_type': 'CLASSES',
        'expected_spread_pct': 5.0,
        'z_entry': 2.0, 'z_exit': 0.3, 'z_stop': 3.5,
        'liquidity_tier': 'B',
        'beta_a_to_b': 1.0,
        'enabled': True,
    },
    {
        'id': 'ELET3-ELET6', 'name': 'Eletrobras ON/PNB',
        'leg_a': 'ELET3', 'leg_b': 'ELET6',
        'pair_type': 'CLASSES',
        'expected_spread_pct': 8.0,
        'z_entry': 2.0, 'z_exit': 0.3, 'z_stop': 3.5,
        'liquidity_tier': 'B',
        'beta_a_to_b': 1.0,
        'enabled': True,
    },

    # ─────────── TIER 2: Setoriais ───────────
    {
        'id': 'SUZB3-KLBN11', 'name': 'Suzano / Klabin',
        'leg_a': 'SUZB3', 'leg_b': 'KLBN11',
        'pair_type': 'SECTORIAL', 'sector': 'paper_cellulose',
        'expected_spread_pct': 0.0,    # spread de RATIO, nao % absoluto
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'A',
        'beta_a_to_b': 1.2,            # Suzano mais volatil
        'enabled': True,
    },
    {
        'id': 'CPFE3-CMIG4', 'name': 'CPFL / Cemig',
        'leg_a': 'CPFE3', 'leg_b': 'CMIG4',
        'pair_type': 'SECTORIAL', 'sector': 'electric_utilities',
        'expected_spread_pct': 0.0,
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'B',
        'beta_a_to_b': 0.8,            # CPFL mais defensiva
        'enabled': True,
    },
    {
        'id': 'SBSP3-CSMG3', 'name': 'Sabesp / Copasa',
        'leg_a': 'SBSP3', 'leg_b': 'CSMG3',
        'pair_type': 'SECTORIAL', 'sector': 'sanitation',
        'expected_spread_pct': 0.0,
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'B',
        'beta_a_to_b': 1.1,
        'enabled': True,
    },
    {
        'id': 'EQTL3-ENGI11', 'name': 'Equatorial / Energisa',
        'leg_a': 'EQTL3', 'leg_b': 'ENGI11',
        'pair_type': 'SECTORIAL', 'sector': 'electric_distribution',
        'expected_spread_pct': 0.0,
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'B',
        'beta_a_to_b': 1.0,
        'enabled': True,
    },
    {
        'id': 'AZUL4-GOLL4', 'name': 'Azul / Gol',
        'leg_a': 'AZUL4', 'leg_b': 'GOLL4',
        'pair_type': 'SECTORIAL', 'sector': 'airlines',
        'expected_spread_pct': 0.0,
        'z_entry': 2.5, 'z_exit': 0.5, 'z_stop': 4.0,   # mais conservador (volatil)
        'liquidity_tier': 'C',          # baixa liquidez
        'beta_a_to_b': 1.0,
        'enabled': True,
    },
    {
        'id': 'B3SA3-XPBR31', 'name': 'B3 / XP',
        'leg_a': 'B3SA3', 'leg_b': 'XPBR31',
        'pair_type': 'SECTORIAL', 'sector': 'exchanges_brokers',
        'expected_spread_pct': 0.0,
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'B',
        'beta_a_to_b': 1.0,
        'enabled': True,
    },
    {
        'id': 'RDOR3-HAPV3', 'name': 'Rede DOr / Hapvida',
        'leg_a': 'RDOR3', 'leg_b': 'HAPV3',
        'pair_type': 'SECTORIAL', 'sector': 'healthcare',
        'expected_spread_pct': 0.0,
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'B',
        'beta_a_to_b': 0.9,
        'enabled': True,
    },
    {
        'id': 'CIEL3-STNE3', 'name': 'Cielo / StoneCo',
        'leg_a': 'CIEL3', 'leg_b': 'STNE3',
        'pair_type': 'SECTORIAL', 'sector': 'payments',
        'expected_spread_pct': 0.0,
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'C',          # STNE listada nos EUA tem complicacao
        'beta_a_to_b': 1.2,
        'enabled': False,               # disabled inicial — STNE eh ADR nos EUA
    },
    {
        'id': 'BBSE3-BBAS3', 'name': 'BB Seguridade / Banco do Brasil',
        'leg_a': 'BBSE3', 'leg_b': 'BBAS3',
        'pair_type': 'HOLDING_PARENT',  # BB controla BBSE
        'sector': 'banking_insurance',
        'expected_spread_pct': 0.0,
        'z_entry': 2.0, 'z_exit': 0.4, 'z_stop': 3.5,
        'liquidity_tier': 'A',
        'beta_a_to_b': 0.7,
        'enabled': True,
    },
]

PAIRS_LIST = [p['id'] for p in PAIRS_CONFIG if p.get('enabled', True)]


def get_pair(pair_id):
    """Retorna config do par pelo ID, ou None."""
    for p in PAIRS_CONFIG:
        if p['id'] == pair_id:
            return p
    return None


def all_symbols():
    """Retorna conjunto de todos os simbolos B3 usados nos pares."""
    syms = set()
    for p in PAIRS_CONFIG:
        if not p.get('enabled', True):
            continue
        syms.add(p['leg_a'])
        syms.add(p['leg_b'])
    return syms
