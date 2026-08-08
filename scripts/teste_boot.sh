#!/bin/bash
# TESTE DE BOOT: importa o api_server (executa TODO o codigo de modulo,
# inclusive os blocos de bootstrap) sem subir thread — o guard __main__ protege.
export MYSQLHOST=gondola.proxy.rlwy.net MYSQLPORT=47196 MYSQLUSER=root
export MYSQLPASSWORD=wIDTWUbXlyVhDMzswQVugAsvGoNKIrCX MYSQLDATABASE=railway
export SERVICE_ROLE=core
python3 - <<'PY'
import sys, traceback
try:
    import api_server as S
    print('BOOT OK — modulo importado sem excecao')
    print('  _pairs_engine_loaded :', getattr(S, '_pairs_engine_loaded', '?'))
    print('  _PAIRS_LIST          :', len(getattr(S, '_PAIRS_LIST', [])), 'pares ->',
          getattr(S, '_PAIRS_LIST', []))
    for n in ('_PAIRS_CFG','_pairs_open','_pairs_closed','_pairs_spreads',
              '_PAIRS_CAPITAL','_PAIRS_MAX_POSITIONS'):
        print(f'  {n:22}: {"OK" if hasattr(S, n) else "AUSENTE <<< PROBLEMA"}')
except Exception as e:
    print('BOOT FALHOU:', type(e).__name__, e)
    traceback.print_exc()
    sys.exit(1)
PY
