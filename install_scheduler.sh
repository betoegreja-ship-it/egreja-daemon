#!/bin/bash

# Script de instalação do Auto Scheduler como serviço systemd
# Executa Sofia IA automaticamente em horário de trading

echo "🚀 Instalando Auto Scheduler..."

# Criar arquivo de serviço systemd
sudo tee /etc/systemd/system/sofia-scheduler.service > /dev/null <<EOF
[Unit]
Description=Sofia IA Auto Scheduler
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/arbitrage-dashboard
Environment="DATABASE_URL=$DATABASE_URL"
Environment="PATH=/usr/local/bin:/usr/bin:/bin"
ExecStart=/usr/bin/python3.11 /home/ubuntu/arbitrage-dashboard/auto_scheduler.py
Restart=always
RestartSec=30
StandardOutput=append:/home/ubuntu/arbitrage-dashboard/logs/scheduler/service.log
StandardError=append:/home/ubuntu/arbitrage-dashboard/logs/scheduler/service_error.log

[Install]
WantedBy=multi-user.target
EOF

# Recarregar systemd
echo "🔄 Recarregando systemd..."
sudo systemctl daemon-reload

# Habilitar serviço para iniciar no boot
echo "✅ Habilitando serviço..."
sudo systemctl enable sofia-scheduler.service

# Iniciar serviço
echo "🚀 Iniciando serviço..."
sudo systemctl start sofia-scheduler.service

# Verificar status
echo ""
echo "📊 Status do serviço:"
sudo systemctl status sofia-scheduler.service --no-pager

echo ""
echo "✅ Instalação concluída!"
echo ""
echo "📝 Comandos úteis:"
echo "   sudo systemctl status sofia-scheduler    # Ver status"
echo "   sudo systemctl stop sofia-scheduler      # Parar"
echo "   sudo systemctl start sofia-scheduler     # Iniciar"
echo "   sudo systemctl restart sofia-scheduler   # Reiniciar"
echo "   sudo journalctl -u sofia-scheduler -f    # Ver logs em tempo real"
echo ""
