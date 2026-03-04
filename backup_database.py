#!/usr/bin/env python3
"""
Backup Automatizado do Banco de Dados ArbitrageAI
Exporta dump completo das tabelas para S3 com retenção de 30 dias
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do banco de dados
DB_HOST = os.getenv('DB_HOST', 'gateway01.us-east-1.prod.aws.tidbcloud.com')
DB_PORT = os.getenv('DB_PORT', '4000')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME', 'arbitrage_ai')

# Configurações S3
S3_BUCKET = os.getenv('S3_BUCKET', 'arbitrage-ai-backups')
S3_PREFIX = 'database-backups/'

# Configurações de retenção
RETENTION_DAYS = 30

# Configurações de notificação
NOTIFICATION_EMAIL = os.getenv('OWNER_EMAIL', 'owner@example.com')
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER')
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD')

def log(message):
    """Log com timestamp"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def send_notification(subject, body, is_error=False):
    """Envia notificação por email"""
    try:
        if not SMTP_USER or not SMTP_PASSWORD:
            log("⚠️  Credenciais SMTP não configuradas, pulando notificação")
            return
        
        msg = MIMEMultipart()
        msg['From'] = SMTP_USER
        msg['To'] = NOTIFICATION_EMAIL
        msg['Subject'] = f"[ArbitrageAI Backup] {subject}"
        
        msg.attach(MIMEText(body, 'plain'))
        
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        
        log(f"✅ Notificação enviada: {subject}")
    
    except Exception as e:
        log(f"❌ Erro ao enviar notificação: {e}")

def create_backup():
    """Cria backup do banco de dados"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"arbitrage_ai_backup_{timestamp}.sql"
        backup_path = f"/tmp/{backup_filename}"
        
        log(f"📦 Criando backup: {backup_filename}")
        
        # Comando mysqldump
        dump_command = [
            'mysqldump',
            f'--host={DB_HOST}',
            f'--port={DB_PORT}',
            f'--user={DB_USER}',
            f'--password={DB_PASSWORD}',
            '--ssl-mode=REQUIRED',
            '--single-transaction',
            '--routines',
            '--triggers',
            '--events',
            DB_NAME,
            '--result-file=' + backup_path
        ]
        
        # Executar dump
        result = subprocess.run(
            dump_command,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos
        )
        
        if result.returncode != 0:
            raise Exception(f"mysqldump falhou: {result.stderr}")
        
        # Verificar se arquivo foi criado
        if not os.path.exists(backup_path):
            raise Exception("Arquivo de backup não foi criado")
        
        file_size_mb = os.path.getsize(backup_path) / (1024 * 1024)
        log(f"✅ Backup criado: {file_size_mb:.2f} MB")
        
        return backup_path, backup_filename
    
    except subprocess.TimeoutExpired:
        raise Exception("Timeout ao criar backup (>5 minutos)")
    except Exception as e:
        raise Exception(f"Erro ao criar backup: {e}")

def upload_to_s3(local_path, filename):
    """Upload do backup para S3"""
    try:
        log(f"☁️  Fazendo upload para S3: s3://{S3_BUCKET}/{S3_PREFIX}{filename}")
        
        s3_client = boto3.client('s3')
        
        # Upload com metadata
        s3_client.upload_file(
            local_path,
            S3_BUCKET,
            S3_PREFIX + filename,
            ExtraArgs={
                'Metadata': {
                    'backup-date': datetime.now().isoformat(),
                    'database': DB_NAME,
                    'retention-days': str(RETENTION_DAYS)
                }
            }
        )
        
        log(f"✅ Upload concluído: {filename}")
        
        # Deletar arquivo local
        os.remove(local_path)
        log(f"🗑️  Arquivo local removido: {local_path}")
        
        return True
    
    except ClientError as e:
        raise Exception(f"Erro ao fazer upload para S3: {e}")
    except Exception as e:
        raise Exception(f"Erro inesperado no upload: {e}")

def cleanup_old_backups():
    """Remove backups com mais de RETENTION_DAYS dias"""
    try:
        log(f"🧹 Limpando backups com mais de {RETENTION_DAYS} dias...")
        
        s3_client = boto3.client('s3')
        
        # Listar todos os backups
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=S3_PREFIX
        )
        
        if 'Contents' not in response:
            log("ℹ️  Nenhum backup encontrado no S3")
            return
        
        cutoff_date = datetime.now() - timedelta(days=RETENTION_DAYS)
        deleted_count = 0
        
        for obj in response['Contents']:
            key = obj['Key']
            last_modified = obj['LastModified'].replace(tzinfo=None)
            
            if last_modified < cutoff_date:
                log(f"🗑️  Deletando backup antigo: {key} (idade: {(datetime.now() - last_modified).days} dias)")
                s3_client.delete_object(Bucket=S3_BUCKET, Key=key)
                deleted_count += 1
        
        if deleted_count > 0:
            log(f"✅ {deleted_count} backup(s) antigo(s) removido(s)")
        else:
            log("ℹ️  Nenhum backup antigo para remover")
    
    except ClientError as e:
        log(f"⚠️  Erro ao limpar backups antigos: {e}")
    except Exception as e:
        log(f"⚠️  Erro inesperado na limpeza: {e}")

def main():
    """Função principal"""
    log("="*60)
    log("🚀 Iniciando backup automatizado do banco de dados")
    log("="*60)
    
    try:
        # Validar credenciais
        if not DB_USER or not DB_PASSWORD:
            raise Exception("Credenciais do banco de dados não configuradas (.env)")
        
        # 1. Criar backup
        backup_path, backup_filename = create_backup()
        
        # 2. Upload para S3
        upload_to_s3(backup_path, backup_filename)
        
        # 3. Limpar backups antigos
        cleanup_old_backups()
        
        # 4. Notificar sucesso
        success_message = f"""
Backup do banco de dados concluído com sucesso!

Arquivo: {backup_filename}
Bucket: s3://{S3_BUCKET}/{S3_PREFIX}
Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Retenção: {RETENTION_DAYS} dias

O backup foi armazenado com segurança no S3.
        """
        
        send_notification("✅ Backup Concluído", success_message)
        
        log("="*60)
        log("✅ Backup automatizado concluído com sucesso!")
        log("="*60)
        
        return 0
    
    except Exception as e:
        error_message = f"""
ERRO ao realizar backup do banco de dados!

Erro: {str(e)}
Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Por favor, verifique os logs e corrija o problema.
        """
        
        log(f"❌ ERRO: {e}")
        send_notification("❌ Falha no Backup", error_message, is_error=True)
        
        log("="*60)
        log("❌ Backup automatizado falhou!")
        log("="*60)
        
        return 1

if __name__ == "__main__":
    sys.exit(main())
