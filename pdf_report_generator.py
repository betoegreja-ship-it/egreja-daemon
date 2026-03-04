#!/usr/bin/env python3
"""
PDF Report Generator para Egreja Investment AI
Gera relatório diário com análises, gráficos e estatísticas
"""

import os
from datetime import datetime, timedelta
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from reportlab.lib.pagesizes import letter, A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.pdfgen import canvas
import io

class ReportGenerator:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('MYSQLHOST', 'localhost'),
            'user': os.getenv('MYSQLUSER', 'root'),
            'password': os.getenv('MYSQLPASSWORD', ''),
            'database': os.getenv('MYSQLDATABASE', 'railway'),
            'port': int(os.getenv('MYSQLPORT', 3306))
        }
        self.output_dir = '/tmp/egreja_reports'
        os.makedirs(self.output_dir, exist_ok=True)
    
    def get_signals_from_db(self):
        """Busca últimos sinais do MySQL"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            
            query = """
            SELECT symbol, market_type, price, score, signal, rsi, ema9, ema21, ema50, created_at
            FROM market_signals
            ORDER BY created_at DESC
            LIMIT 40
            """
            cursor.execute(query)
            signals = cursor.fetchall()
            cursor.close()
            conn.close()
            
            return signals
        except Exception as e:
            print(f"❌ Erro ao buscar sinais: {e}")
            return []
    
    def get_statistics(self):
        """Busca estatísticas gerais"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            
            # Total de sinais hoje
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute(f"""
            SELECT COUNT(*) as total FROM market_signals 
            WHERE DATE(created_at) = '{today}'
            """)
            today_total = cursor.fetchone()['total']
            
            # Sinais de compra hoje
            cursor.execute(f"""
            SELECT COUNT(*) as total FROM market_signals 
            WHERE DATE(created_at) = '{today}' AND signal LIKE '%COMPRA%'
            """)
            buy_signals = cursor.fetchone()['total']
            
            # Sinais de venda hoje
            cursor.execute(f"""
            SELECT COUNT(*) as total FROM market_signals 
            WHERE DATE(created_at) = '{today}' AND signal LIKE '%VENDA%'
            """)
            sell_signals = cursor.fetchone()['total']
            
            # Score médio
            cursor.execute("""
            SELECT AVG(score) as avg_score FROM market_signals
            WHERE DATE(created_at) = %s
            """, (today,))
            avg_score = cursor.fetchone()['avg_score'] or 0
            
            cursor.close()
            conn.close()
            
            return {
                'total': today_total,
                'buy_signals': buy_signals,
                'sell_signals': sell_signals,
                'avg_score': round(avg_score, 2)
            }
        except Exception as e:
            print(f"❌ Erro ao buscar estatísticas: {e}")
            return {}
    
    def create_chart(self, title, data_labels, data_values, filename):
        """Cria gráfico e salva como PNG"""
        try:
            plt.figure(figsize=(10, 6))
            colors_list = ['#00ff00' if v > 50 else '#ff0000' for v in data_values]
            
            plt.bar(data_labels, data_values, color=colors_list, alpha=0.7, edgecolor='black')
            plt.title(title, fontsize=16, fontweight='bold')
            plt.ylabel('Score', fontsize=12)
            plt.ylim(0, 100)
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            plt.grid(axis='y', alpha=0.3)
            
            filepath = os.path.join(self.output_dir, filename)
            plt.savefig(filepath, dpi=100, bbox_inches='tight')
            plt.close()
            
            return filepath
        except Exception as e:
            print(f"❌ Erro ao criar gráfico: {e}")
            return None
    
    def create_performance_chart(self):
        """Cria gráfico de performance do último mês"""
        try:
            conn = mysql.connector.connect(**self.db_config)
            cursor = conn.cursor(dictionary=True)
            
            # Buscar P&L diário do último mês
            query = """
            SELECT DATE(created_at) as date, AVG(score) as avg_score, COUNT(*) as num_signals
            FROM market_signals
            WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY DATE(created_at)
            ORDER BY date
            """
            cursor.execute(query)
            data = cursor.fetchall()
            cursor.close()
            conn.close()
            
            if not data:
                return None
            
            dates = [d['date'].strftime('%d/%m') for d in data]
            scores = [d['avg_score'] for d in data]
            
            plt.figure(figsize=(12, 6))
            plt.plot(dates, scores, marker='o', linewidth=2, markersize=6, color='#0066ff')
            plt.title('Performance - Últimos 30 Dias', fontsize=16, fontweight='bold')
            plt.ylabel('Score Médio', fontsize=12)
            plt.xlabel('Data', fontsize=12)
            plt.ylim(0, 100)
            plt.grid(alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            filepath = os.path.join(self.output_dir, 'performance_chart.png')
            plt.savefig(filepath, dpi=100)
            plt.close()
            
            return filepath
        except Exception as e:
            print(f"❌ Erro ao criar gráfico de performance: {e}")
            return None
    
    def generate_pdf(self):
        """Gera relatório PDF completo"""
        try:
            # Buscar dados
            signals = self.get_signals_from_db()
            stats = self.get_statistics()
            
            if not signals:
                print("❌ Nenhum sinal encontrado")
                return None
            
            # Criar gráficos
            top_signals = signals[:10]
            top_labels = [s['symbol'] for s in top_signals]
            top_scores = [s['score'] for s in top_signals]
            
            chart1 = self.create_chart(
                'Top 10 Oportunidades',
                top_labels,
                top_scores,
                'top_signals.png'
            )
            
            chart2 = self.create_performance_chart()
            
            # Criar PDF
            filename = os.path.join(
                self.output_dir, 
                f"relatorio_egreja_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.pdf"
            )
            
            doc = SimpleDocTemplate(filename, pagesize=letter)
            story = []
            styles = getSampleStyleSheet()
            
            # Título
            title_style = ParagraphStyle(
                'CustomTitle',
                parent=styles['Heading1'],
                fontSize=24,
                textColor=colors.HexColor('#0066ff'),
                spaceAfter=30,
                alignment=1
            )
            story.append(Paragraph('📊 Egreja Investment AI - Relatório Diário', title_style))
            story.append(Paragraph(f'Data: {datetime.now().strftime("%d/%m/%Y %H:%M")}', styles['Normal']))
            story.append(Spacer(1, 0.3*inch))
            
            # Estatísticas
            stats_data = [
                ['Métrica', 'Valor'],
                ['Total de Sinais', str(stats.get('total', 0))],
                ['Sinais de Compra', str(stats.get('buy_signals', 0))],
                ['Sinais de Venda', str(stats.get('sell_signals', 0))],
                ['Score Médio', f"{stats.get('avg_score', 0):.1f}/100"],
            ]
            
            stats_table = Table(stats_data, colWidths=[3*inch, 2*inch])
            stats_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066ff')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            story.append(stats_table)
            story.append(Spacer(1, 0.3*inch))
            
            # Gráfico 1
            if chart1:
                story.append(Paragraph('Principais Oportunidades', styles['Heading2']))
                story.append(Image(chart1, width=6*inch, height=3.5*inch))
                story.append(Spacer(1, 0.2*inch))
            
            # Gráfico 2
            if chart2:
                story.append(PageBreak())
                story.append(Paragraph('Performance - Últimos 30 Dias', styles['Heading2']))
                story.append(Image(chart2, width=6.5*inch, height=3.5*inch))
                story.append(Spacer(1, 0.3*inch))
            
            # Tabela de sinais
            story.append(PageBreak())
            story.append(Paragraph('Últimos 40 Sinais', styles['Heading2']))
            
            table_data = [['Symbol', 'Market', 'Preço', 'Score', 'Signal', 'RSI']]
            for signal in signals[:40]:
                table_data.append([
                    signal['symbol'],
                    signal['market_type'],
                    f"${signal['price']:.2f}" if signal['price'] < 1000 else f"R${signal['price']:.2f}",
                    str(signal['score']),
                    signal['signal'][:20],
                    f"{signal['rsi']:.1f}" if signal['rsi'] else 'N/A',
                ])
            
            signals_table = Table(table_data, colWidths=[1*inch, 0.8*inch, 1*inch, 0.7*inch, 1.2*inch, 0.7*inch])
            signals_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0066ff')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightgrey),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black),
            ]))
            story.append(signals_table)
            
            # Build PDF
            doc.build(story)
            
            print(f"✅ PDF gerado: {filename}")
            return filename
        
        except Exception as e:
            print(f"❌ Erro ao gerar PDF: {e}")
            return None

if __name__ == '__main__':
    generator = ReportGenerator()
    pdf_path = generator.generate_pdf()
    
    if pdf_path:
        print(f"✅ Relatório disponível em: {pdf_path}")
    else:
        print("❌ Falha ao gerar relatório")
