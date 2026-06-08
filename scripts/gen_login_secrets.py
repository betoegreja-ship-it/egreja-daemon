#!/usr/bin/env python3
"""
Gerador de credenciais para o WEB LOGIN do Egreja Investment AI.

Uso interativo:
    python3 scripts/gen_login_secrets.py

Uso direto (senha como argumento):
    python3 scripts/gen_login_secrets.py 'MinhaSenhaForte123!'

Resultado: 4 valores que voce copia para o Railway (Settings > Variables).
"""
import hashlib
import secrets
import sys
import getpass

SALT_DEFAULT = 'egreja-2026'

def gen_hash(password: str, salt: str = SALT_DEFAULT) -> str:
    return hashlib.sha256((salt + password).encode('utf-8')).hexdigest()

def main():
    print("\n=== Egreja Investment AI — Gerador de Credenciais Web ===\n")

    # Username
    user = input("Username (padrao: admin): ").strip() or 'admin'

    # Senha
    if len(sys.argv) > 1:
        pwd = sys.argv[1]
        pwd2 = pwd
    else:
        pwd = getpass.getpass("Senha (oculto): ")
        pwd2 = getpass.getpass("Confirme a senha: ")
    if pwd != pwd2:
        print("\nERRO: senhas nao conferem.")
        sys.exit(1)
    if len(pwd) < 8:
        print("\nERRO: senha precisa ter pelo menos 8 caracteres.")
        sys.exit(1)

    # Salt (manter o default e' OK, mas oferecemos opcao)
    use_random_salt = input("Usar salt aleatorio para maior seguranca? [s/N]: ").strip().lower() == 's'
    salt = secrets.token_hex(16) if use_random_salt else SALT_DEFAULT

    # Hash
    h = gen_hash(pwd, salt)

    # Flask secret
    flask_secret = secrets.token_hex(32)

    print("\n" + "="*70)
    print("COPIE ESTAS VARIAVEIS PARA O RAILWAY (Settings > Variables)")
    print("="*70)
    print(f"WEB_USERNAME={user}")
    print(f"WEB_PASSWORD_HASH={h}")
    if use_random_salt:
        print(f"WEB_PASSWORD_SALT={salt}")
    print(f"FLASK_SECRET_KEY={flask_secret}")
    print(f"WEB_SESSION_DAYS=7")
    print("="*70)
    print("\nApos colar no Railway:")
    print("  1. Salve as variaveis")
    print("  2. Railway vai redeploy automaticamente")
    print("  3. Acesse a URL e teste o login\n")
    print("Para mudar a senha depois: rode este script de novo e atualize")
    print("WEB_PASSWORD_HASH no Railway. O FLASK_SECRET_KEY nao precisa mudar.\n")

if __name__ == '__main__':
    main()
