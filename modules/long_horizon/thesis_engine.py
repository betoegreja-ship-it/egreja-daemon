"""
Investment Thesis Generation Engine for Long Horizon Module.

Generates explainable, investment-grade theses in Portuguese for each asset.
Includes conviction level, key drivers, risks, hedge suggestions, and horizon.
Supports 87+ stocks via dynamic thesis generation with sector templates.
"""

import logging
from datetime import date
import json

logger = logging.getLogger(__name__)


# Sector database: ticker -> sector classification and characteristics
SECTOR_DATABASE = {
    # B3 Blue Chips (Industrial/Diversified)
    'WEGE3': {'sector': 'Blue Chips', 'market': 'BR', 'industry': 'Industrial', 'profile': 'Multinational engineer and constructor'},
    'SUZB3': {'sector': 'Blue Chips', 'market': 'BR', 'industry': 'Pulp & Paper', 'profile': 'Global pulp and paper producer'},
    'EMBR3': {'sector': 'Blue Chips', 'market': 'BR', 'industry': 'Aerospace', 'profile': 'Regional aircraft manufacturer'},

    # B3 Utilities
    'CMIG4': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Energy Distribution', 'profile': 'Electricity distributor'},
    'CPLE6': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Energy Distribution', 'profile': 'Electricity distributor'},
    'EQTL3': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Energy', 'profile': 'Power generation and transmission'},
    'TAEE11': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Energy Transmission', 'profile': 'Electricity transmission'},
    'CPFE3': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Energy Distribution', 'profile': 'São Paulo energy distributor'},
    'EGIE3': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Energy', 'profile': 'Power generation and distribution'},
    'ENEV3': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Energy', 'profile': 'Renewable energy developer'},
    'SBSP3': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Water/Sanitation', 'profile': 'São Paulo water and sanitation'},
    'AESB3': {'sector': 'Utilities', 'market': 'BR', 'industry': 'Energy Distribution', 'profile': 'Electricity distributor'},

    # B3 Banks/Finance
    'BPAC11': {'sector': 'Banks', 'market': 'BR', 'industry': 'Banking', 'profile': 'Digital banking platform'},
    'BBDC3': {'sector': 'Banks', 'market': 'BR', 'industry': 'Banking', 'profile': 'Large Brazilian bank'},
    'BBSE3': {'sector': 'Banks', 'market': 'BR', 'industry': 'Banking', 'profile': 'Regional bank'},
    'CXSE3': {'sector': 'Banks', 'market': 'BR', 'industry': 'Investment Bank', 'profile': 'Brokerage and investment bank'},

    # B3 Food/Beverages (Note: ABEV3 is in hardcoded, others here)
    'BRFS3': {'sector': 'Food', 'market': 'BR', 'industry': 'Food Processing', 'profile': 'Meat processor and exporter'},
    'MRFG3': {'sector': 'Food', 'market': 'BR', 'industry': 'Food Processing', 'profile': 'Meat processor'},
    'JBSS3': {'sector': 'Food', 'market': 'BR', 'industry': 'Food Processing', 'profile': 'Meat processor'},
    'MDIA3': {'sector': 'Food', 'market': 'BR', 'industry': 'Media/Publishing', 'profile': 'Media and publishing'},

    # B3 Commodities (Mining/Steel)
    'GGBR4': {'sector': 'Mining', 'market': 'BR', 'industry': 'Steel', 'profile': 'Steel producer'},
    'CSNA3': {'sector': 'Mining', 'market': 'BR', 'industry': 'Steel', 'profile': 'Integrated steel producer'},
    'GOAU4': {'sector': 'Mining', 'market': 'BR', 'industry': 'Steel', 'profile': 'Steel manufacturer'},
    'USIM5': {'sector': 'Mining', 'market': 'BR', 'industry': 'Steel', 'profile': 'Steel producer'},
    'CMIN3': {'sector': 'Mining', 'market': 'BR', 'industry': 'Mining', 'profile': 'Coal mining'},
    'KLBN11': {'sector': 'Mining', 'market': 'BR', 'industry': 'Forestry/Pulp', 'profile': 'Forestry company'},

    # B3 Retail/E-commerce
    'MGLU3': {'sector': 'Retail', 'market': 'BR', 'industry': 'E-commerce', 'profile': 'Online retailer'},
    'CASH3': {'sector': 'Retail', 'market': 'BR', 'industry': 'Retail', 'profile': 'Department store'},
    'AMER3': {'sector': 'Retail', 'market': 'BR', 'industry': 'Retail', 'profile': 'Department store'},
    'LREN3': {'sector': 'Retail', 'market': 'BR', 'industry': 'Fashion/Retail', 'profile': 'Fashion and apparel'},
    'TOTS3': {'sector': 'Retail', 'market': 'BR', 'industry': 'Footwear/Retail', 'profile': 'Footwear retailer'},

    # B3 Healthcare/Pharma
    'HAPV3': {'sector': 'Healthcare', 'market': 'BR', 'industry': 'Medical Devices', 'profile': 'Medical technology'},
    'RDOR3': {'sector': 'Healthcare', 'market': 'BR', 'industry': 'Diagnostics', 'profile': 'Laboratory services'},
    'HYPE3': {'sector': 'Healthcare', 'market': 'BR', 'industry': 'Hypertension Care', 'profile': 'Healthcare provider'},
    'RADL3': {'sector': 'Healthcare', 'market': 'BR', 'industry': 'Pharmacy', 'profile': 'Pharmacy chain'},

    # B3 Education
    'COGN3': {'sector': 'Education', 'market': 'BR', 'industry': 'Education', 'profile': 'Online education platform'},
    'YDUQ3': {'sector': 'Education', 'market': 'BR', 'industry': 'Education', 'profile': 'Educational services'},

    # B3 Transport
    'AZUL4': {'sector': 'Transport', 'market': 'BR', 'industry': 'Airlines', 'profile': 'Budget airline'},
    'CCRO3': {'sector': 'Transport', 'market': 'BR', 'industry': 'Transportation', 'profile': 'Transportation services'},

    # B3 Fuel/Oil & Gas
    'VBBR3': {'sector': 'Oil & Gas', 'market': 'BR', 'industry': 'Oil & Gas', 'profile': 'Fuel distributor'},
    'UGPA3': {'sector': 'Oil & Gas', 'market': 'BR', 'industry': 'Oil & Gas', 'profile': 'Oil products'},
    'CSAN3': {'sector': 'Oil & Gas', 'market': 'BR', 'industry': 'Oil & Gas', 'profile': 'Oil distributor'},
    'PRIO3': {'sector': 'Oil & Gas', 'market': 'BR', 'industry': 'Oil & Gas', 'profile': 'Oil producer'},
    'RECV3': {'sector': 'Oil & Gas', 'market': 'BR', 'industry': 'Oil & Gas', 'profile': 'Oil producer'},

    # B3 Shopping Centers/Services
    'ALOS3': {'sector': 'Services', 'market': 'BR', 'industry': 'Shopping Centers', 'profile': 'Shopping center operator'},
    'MULT3': {'sector': 'Services', 'market': 'BR', 'industry': 'Shopping Centers', 'profile': 'Shopping center operator'},
    'SMFT3': {'sector': 'Services', 'market': 'BR', 'industry': 'Shopping Centers', 'profile': 'Shopping center operator'},
    'RENT3': {'sector': 'Services', 'market': 'BR', 'industry': 'Services', 'profile': 'Services and logistics'},

    # B3 Consumer Goods
    'ALPA4': {'sector': 'Consumer', 'market': 'BR', 'industry': 'Consumer Goods', 'profile': 'Household products'},
    'POMO4': {'sector': 'Consumer', 'market': 'BR', 'industry': 'Consumer Goods', 'profile': 'Consumer products'},
    'NTCO3': {'sector': 'Consumer', 'market': 'BR', 'industry': 'Consumer Goods', 'profile': 'Paper products'},

    # B3 Telecom
    'VIVT3': {'sector': 'Telecom', 'market': 'BR', 'industry': 'Telecom', 'profile': 'Telecommunications'},

    # US Mega-Tech
    'AAPL': {'sector': 'Mega-Tech', 'market': 'US', 'industry': 'Technology', 'profile': 'Consumer electronics and software'},
    'MSFT': {'sector': 'Mega-Tech', 'market': 'US', 'industry': 'Software', 'profile': 'Enterprise software and cloud'},
    'GOOGL': {'sector': 'Mega-Tech', 'market': 'US', 'industry': 'Internet', 'profile': 'Search and digital advertising'},
    'AMZN': {'sector': 'Mega-Tech', 'market': 'US', 'industry': 'E-commerce', 'profile': 'E-commerce and cloud'},
    'META': {'sector': 'Mega-Tech', 'market': 'US', 'industry': 'Social Media', 'profile': 'Social media and advertising'},
    'NVDA': {'sector': 'Mega-Tech', 'market': 'US', 'industry': 'Semiconductors', 'profile': 'AI and GPU semiconductors'},

    # US Streaming/Media
    'NFLX': {'sector': 'Streaming', 'market': 'US', 'industry': 'Streaming', 'profile': 'Video streaming service'},
    'DIS': {'sector': 'Streaming', 'market': 'US', 'industry': 'Media', 'profile': 'Media and entertainment'},
    'SPOT': {'sector': 'Streaming', 'market': 'US', 'industry': 'Streaming', 'profile': 'Music streaming'},
    'TME': {'sector': 'Streaming', 'market': 'US', 'industry': 'Streaming', 'profile': 'Music streaming China'},

    # US Semiconductors
    'TSM': {'sector': 'Semiconductors', 'market': 'US', 'industry': 'Semiconductors', 'profile': 'Chip foundry'},
    'AVGO': {'sector': 'Semiconductors', 'market': 'US', 'industry': 'Semiconductors', 'profile': 'Broadcom chips'},
    'MU': {'sector': 'Semiconductors', 'market': 'US', 'industry': 'Semiconductors', 'profile': 'Memory chips'},
    'ARM': {'sector': 'Semiconductors', 'market': 'US', 'industry': 'Semiconductors', 'profile': 'Chip design'},
    'SMCI': {'sector': 'Semiconductors', 'market': 'US', 'industry': 'Hardware', 'profile': 'Server hardware'},
    'AMD': {'sector': 'Semiconductors', 'market': 'US', 'industry': 'Semiconductors', 'profile': 'Processors and GPUs'},
    'INTC': {'sector': 'Semiconductors', 'market': 'US', 'industry': 'Semiconductors', 'profile': 'Processor manufacturer'},

    # US Banks
    'JPM': {'sector': 'Banks', 'market': 'US', 'industry': 'Banking', 'profile': 'Diversified bank'},
    'BAC': {'sector': 'Banks', 'market': 'US', 'industry': 'Banking', 'profile': 'Large bank'},
    'GS': {'sector': 'Banks', 'market': 'US', 'industry': 'Investment Banking', 'profile': 'Investment bank'},
    'MS': {'sector': 'Banks', 'market': 'US', 'industry': 'Investment Banking', 'profile': 'Investment bank'},

    # US Payments
    'V': {'sector': 'Payments', 'market': 'US', 'industry': 'Payment Networks', 'profile': 'Visa payment network'},
    'MA': {'sector': 'Payments', 'market': 'US', 'industry': 'Payment Networks', 'profile': 'Mastercard network'},

    # US Healthcare
    'JNJ': {'sector': 'Healthcare', 'market': 'US', 'industry': 'Pharma/Devices', 'profile': 'Diversified healthcare'},
    'PFE': {'sector': 'Healthcare', 'market': 'US', 'industry': 'Pharma', 'profile': 'Pharmaceutical company'},
    'UNH': {'sector': 'Healthcare', 'market': 'US', 'industry': 'Insurance', 'profile': 'Health insurance and services'},
    'LLY': {'sector': 'Healthcare', 'market': 'US', 'industry': 'Pharma', 'profile': 'Pharmaceutical company'},

    # US Energy
    'XOM': {'sector': 'Energy', 'market': 'US', 'industry': 'Oil & Gas', 'profile': 'Integrated energy company'},
    'CVX': {'sector': 'Energy', 'market': 'US', 'industry': 'Oil & Gas', 'profile': 'Oil and gas producer'},
    'COP': {'sector': 'Energy', 'market': 'US', 'industry': 'Oil & Gas', 'profile': 'Oil and gas producer'},

    # US SaaS
    'ADBE': {'sector': 'SaaS', 'market': 'US', 'industry': 'Software', 'profile': 'Design and creativity software'},
    'CRM': {'sector': 'SaaS', 'market': 'US', 'industry': 'Enterprise Software', 'profile': 'CRM and cloud services'},
    'NOW': {'sector': 'SaaS', 'market': 'US', 'industry': 'Enterprise Software', 'profile': 'Workflow automation platform'},
    'ORCL': {'sector': 'SaaS', 'market': 'US', 'industry': 'Database/Cloud', 'profile': 'Enterprise database and cloud'},
    'SNOW': {'sector': 'SaaS', 'market': 'US', 'industry': 'Cloud Data', 'profile': 'Cloud data platform'},

    # US E-commerce/Fintech
    'SHOP': {'sector': 'E-commerce', 'market': 'US', 'industry': 'E-commerce', 'profile': 'E-commerce platform'},
    'MELI': {'sector': 'E-commerce', 'market': 'US', 'industry': 'E-commerce', 'profile': 'LatAm e-commerce'},
    'HOOD': {'sector': 'Fintech', 'market': 'US', 'industry': 'Fintech', 'profile': 'Retail trading platform'},
    'HUBS': {'sector': 'SaaS', 'market': 'US', 'industry': 'SaaS', 'profile': 'Customer platform'},
    'TCOM': {'sector': 'E-commerce', 'market': 'US', 'industry': 'Travel', 'profile': 'Chinese travel booking'},

    # US ETFs
    'SPY': {'sector': 'ETFs', 'market': 'US', 'industry': 'Index Fund', 'profile': 'S&P 500 ETF'},
    'QQQ': {'sector': 'ETFs', 'market': 'US', 'industry': 'Index Fund', 'profile': 'Nasdaq-100 ETF'},
    'IWM': {'sector': 'ETFs', 'market': 'US', 'industry': 'Index Fund', 'profile': 'Russell 2000 ETF'},

    # US Transport
    'UBER': {'sector': 'Transport', 'market': 'US', 'industry': 'Mobility', 'profile': 'Ride-hailing and delivery'},
    'LYFT': {'sector': 'Transport', 'market': 'US', 'industry': 'Mobility', 'profile': 'Ride-hailing'},
    'TSLA': {'sector': 'Auto/Tech', 'market': 'US', 'industry': 'Electric Vehicles', 'profile': 'EV manufacturer and energy'},

    # US Crypto
    'COIN': {'sector': 'Crypto', 'market': 'US', 'industry': 'Cryptocurrency', 'profile': 'Crypto exchange platform'},

    # US Other
    'BABA': {'sector': 'E-commerce', 'market': 'US', 'industry': 'E-commerce', 'profile': 'Chinese e-commerce'},
    'PLTR': {'sector': 'Software', 'market': 'US', 'industry': 'Data Analytics', 'profile': 'Data intelligence platform'},
    'OKLO': {'sector': 'Energy', 'market': 'US', 'industry': 'Nuclear Energy', 'profile': 'Advanced nuclear energy'},
    'TGT': {'sector': 'Retail', 'market': 'US', 'industry': 'Retail', 'profile': 'Department store chain'},
}

# Sector templates for dynamic thesis generation
SECTOR_TEMPLATES = {
    'Mega-Tech': {
        'thesis_template': """
{ticker} é líder global em tecnologia com posicionamento defensivo e múltiplos caminhos
de crescimento. Plataforma de {profile} oferece moat competitivo duradouro baseado em
rede, switching costs e vantagens de escala. Fluxo de caixa robusto, retorno ao acionista
previsível e exposição a tendências estruturais de longo prazo (IA, cloud computing,
transformação digital). Valuation premium justificado por qualidade, crescimento e
previsibilidade. Posição core para investidor de horizonte 5+ anos com foco em crescimento
de longo prazo.
        """,
        'key_drivers': [
            'Crescimento de receita organicamente',
            'Margem operacional e fluxo de caixa livre',
            'Market share em categorias-chave',
            'Investimento em P&D e inovação',
            'Retorno de capital ao acionista',
        ],
        'risks': """
(1) Competição intensa em mercados-chave reduz participação; (2) Pressão regulatória
(antitrust, privacidade de dados) limita modelo de negócio; (3) Obsolescência tecnológica
em produtos-chave; (4) Risco de macro/recessão reduz gastos de clientes; (5) Exposição a
ciclos de capex substanciais.
        """,
        'hedge_suggestion': 'Put spread protetor ou posição em defensivos tech',
        'recommended_horizon': '5+ years',
        'conviction_base': 78,
    },
    'Semiconductors': {
        'thesis_template': """
{ticker} é produtor crítico de semicondutores com exposição a ciclos de capex e demanda
de computação. Posição de {profile} oferece vantagem competitiva em tecnologia e custo.
Demanda multidecadal de eletrônica, IA e computação cloud sustenta crescimento. Margens
EBITDA elevadas quando em ciclo positivo. Valuation reflete volatilidade cíclica típica
do setor. Apropriado para investidor com tolerância a volatilidade e visão de 3-5 anos
no ciclo tecnológico.
        """,
        'key_drivers': [
            'Ciclo de capex global e demanda de chips',
            'Market share em produtos-chave',
            'Margem operacional e volume',
            'Avanço tecnológico (nós menores)',
            'Preço médio de venda (ASP)',
        ],
        'risks': """
(1) Ciclo descendente de capex reduz demanda dramaticamente; (2) Obsolescência
tecnológica de processadores; (3) Competição global (China) em custos; (4) Recessão
econômica reduz demanda de eletrônicos; (5) Concentração de clientes (Apple, Amazon, etc.).
        """,
        'hedge_suggestion': 'Put spread ciclista ou posição defensiva durante picos de capex',
        'recommended_horizon': '3-5 years',
        'conviction_base': 72,
    },
    'Streaming': {
        'thesis_template': """
{ticker} é provedor de {profile} com modelo de negócio baseado em assinatura recorrente.
Posição defensiva em entretenimento digital com catálogo diversificado e base de
assinantes global. Margem operacional melhora com alavancagem de conteúdo. Risco de
churn e competição, mas categoria de streaming consolidando-se globalmente. Posição
apropriada para investidor com horizonte de 3-5 anos em transformação de mídia.
        """,
        'key_drivers': [
            'Crescimento de assinantes e ARPU',
            'Margens operacionais',
            'Custo de aquisição de conteúdo',
            'Preço de assinatura e retenção',
            'Penetração geográfica',
        ],
        'risks': """
(1) Churn elevado de assinantes em competição acirrada; (2) Aumento de custos de
conteúdo original; (3) Saturação de mercado em países desenvolvidos; (4) Pirataria
online; (5) Pressão de margens em mercados emergentes com menor ARPU.
        """,
        'hedge_suggestion': 'Put spread contra fadiga de conteúdo ou saturação',
        'recommended_horizon': '3-5 years',
        'conviction_base': 70,
    },
    'Utilities': {
        'thesis_template': """
{ticker} é empresa de utilidade essencial com receitas previsíveis e caráter defensivo.
Modelo de negócio de {profile} oferece fluxo de caixa estável, previsível e com piso
de demanda. Regulação garante retorno sobre capital investido. Exposição a crescimento
de demanda de energia/água de longo prazo. Dividend yield elevado (5-8% a.a.) atrai
yield hunters. Posição defensiva para investidor com horizonte 5+ anos buscando renda
consistente e proteção contra inflação.
        """,
        'key_drivers': [
            'Volume de demanda e consumo',
            'Tarifa regulada e reajustes',
            'Eficiência operacional',
            'Investimento em infraestrutura',
            'Política de dividendos',
        ],
        'risks': """
(1) Mudança regulatória comprime margens (redução de tarifa); (2) Pressão de taxa de
juros reduz retorno sobre capex; (3) Transição energética reduz demanda (energia solar);
(4) Aumento de inadimplência em cenário recessivo; (5) Investimento de capex intensivo
com retorno de longo prazo.
        """,
        'hedge_suggestion': 'Proteção natural via previsibilidade; considerar put spread contra queda de taxa',
        'recommended_horizon': '5+ years',
        'conviction_base': 75,
    },
    'Banks': {
        'thesis_template': """
{ticker} é instituição financeira com {profile} diversificado. Modelo de negócio
baseado em intermediação financeira, taxa de spreads e receitas de serviços. Posição
apropriada em ambiente de juros mais altos. Margem de intermediação (NII) sensível a
ciclos de taxa e crescimento econômico. Dividend yield moderado (3-5% a.a.) com
potencial de crescimento. Horizonte apropriado de 4-5 anos com visão em ciclo econômico.
        """,
        'key_drivers': [
            'Nível de taxa de juros',
            'Spread de crédito',
            'Volume de crédito',
            'Inadimplência e cobertura',
            'Eficiência operacional',
        ],
        'risks': """
(1) Queda de taxa de juros reduz NII; (2) Aumento de inadimplência em recessão;
(3) Competição de fintechs reduz market share; (4) Pressão regulatória em capital
mínimo e provisões; (5) Exposição a ciclos econômicos.
        """,
        'hedge_suggestion': 'Put spread contra queda de taxa de juros',
        'recommended_horizon': '4-5 years',
        'conviction_base': 73,
    },
    'Food': {
        'thesis_template': """
{ticker} é produtor/processador de alimentos com exposição a commodity prices e demanda
de proteína. Posição de {profile} oferece exposição a exportações globais e ciclos de
preço de commodities (soja, milho, energia). Margem EBITDA sensível a custos de
insumos. Dividend policy agressivo em ciclos positivos. Horizonte de 3-5 anos
apropriado para investidor com visão em ciclos de commodities.
        """,
        'key_drivers': [
            'Preço de commodities de insumo',
            'Volume de produção e exportação',
            'Margem EBITDA e câmbio',
            'Eficiência operacional',
            'Política de dividendos',
        ],
        'risks': """
(1) Queda de preço de commodities comprime EBITDA; (2) Surtos de doença animal reduzem
produção; (3) Câmbio forte reduz competitividade de exportação; (4) Pressão ambiental e
regulatória; (5) Consolidação de mercado comprime preços.
        """,
        'hedge_suggestion': 'Posição em commodity hedges; put spread contra queda de preço',
        'recommended_horizon': '3-5 years',
        'conviction_base': 68,
    },
    'Mining': {
        'thesis_template': """
{ticker} é mineradora com exposição cíclica a preços de commodities (minério de ferro,
aço, carvão). Posição de {profile} oferece leverage a ciclos de demanda global, especialmente
China. Margem operacional muito sensível a preço spot. Capex substancial requerido.
Dividend yield elevado em ciclos positivos, comprometido em ciclos baixos. Horizonte
de 2-4 anos apropriado para investidor com visão em ciclos de commodities.
        """,
        'key_drivers': [
            'Preço spot de commodity',
            'Demanda chinesa e estímulos',
            'Custo operacional de produção',
            'Volume de produção',
            'Investimento de capex',
        ],
        'risks': """
(1) Ciclo descendente de preço reduz EBITDA dramaticamente; (2) Desaceleração chinesa
reduz demanda; (3) Risco ambiental/regulatório retarda projetos; (4) Competição global
em custos; (5) Alavancagem elevada em ciclos baixos.
        """,
        'hedge_suggestion': 'Put spread contra queda de commodity ou posição defensiva',
        'recommended_horizon': '2-4 years',
        'conviction_base': 70,
    },
    'Retail': {
        'thesis_template': """
{ticker} é varejista com exposição a consumo doméstico e demanda de consumidor. Posição
de {profile} oferece leverage a ciclos econômicos e renda de consumidor. Margem bruta
sensível a ticket médio e volume. Pressão competitiva e transformação digital aumentam
complexidade. Horizon apropriado de 2-4 anos com visão em ciclo de consumo.
        """,
        'key_drivers': [
            'Volume de vendas e ticket médio',
            'Margem bruta e alavancagem operacional',
            'Fluxo de clientes e retenção',
            'Transformação e presença omnichannel',
            'Eficiência operacional',
        ],
        'risks': """
(1) Desaceleração econômica reduz consumo; (2) Pressão de margem de competição;
(3) E-commerce reduz margens; (4) Aumento de custo de trabalho e aluguel;
(5) Obsolescência de modelo de negócio.
        """,
        'hedge_suggestion': 'Put spread contra recessão ou queda de consumo',
        'recommended_horizon': '2-4 years',
        'conviction_base': 65,
    },
    'Healthcare': {
        'thesis_template': """
{ticker} é empresa de healthcare com exposição a demanda defensiva de serviços/produtos
de saúde. Posição de {profile} oferece crescimento secular e defensibilidade. Exposição
a envelhecimento populacional e aumento de gastos de saúde. Margem operacional estável
com pressão regulatória. Horizonte apropriado de 5+ anos para crescimento defensivo.
        """,
        'key_drivers': [
            'Volume de pacientes e ARPU',
            'Margem operacional e eficiência',
            'Inovação de produtos/serviços',
            'Conformidade regulatória',
            'Retorno de capital',
        ],
        'risks': """
(1) Pressão regulatória em preços; (2) Competição de players alternativos;
(3) Risco de litigância; (4) Mudanças em políticas de saúde; (5) Obsolescência
de produtos/tratamentos.
        """,
        'hedge_suggestion': 'Proteção natural em crescimento defensivo',
        'recommended_horizon': '5+ years',
        'conviction_base': 74,
    },
    'SaaS': {
        'thesis_template': """
{ticker} é provedor de software empresarial com modelo de negócio de assinatura recorrente
(SaaS). Posição de {profile} oferece margem operacional elevada, previsibilidade de
receita e leverage a transformação digital de empresas. Crescimento de ARR impulsionado
por customer expansion e net dollar retention. Posição de crescimento para investidor
com horizonte 5+ anos em transformação digital corporativa.
        """,
        'key_drivers': [
            'Crescimento de ARR e customer count',
            'Net dollar retention e churn',
            'Margem operacional e FCF',
            'Investimento em R&D',
            'Alavancagem de margem',
        ],
        'risks': """
(1) Churn de clientes em recessão econômica; (2) Pressão de preço e competição;
(3) Aumento de custo de aquisição (CAC); (4) Mudança tecnológica substitui produto;
(5) Pressão de macro reduz capex de clientes.
        """,
        'hedge_suggestion': 'Put spread contra recessão ou mudança tecnológica',
        'recommended_horizon': '5+ years',
        'conviction_base': 76,
    },
    'Energy': {
        'thesis_template': """
{ticker} é produtor de energia com exposição a preços de commodity (petróleo, gás).
Posição de {profile} oferece cash flow robusto em ciclos altos com dividend yield
elevado. Exposição a ciclos cíclicos de energia e demanda global. Sensível a preços
spot, câmbio e política energética. Horizonte apropriado de 3-5 anos com visão em
ciclos de energia.
        """,
        'key_drivers': [
            'Preço de petróleo/gás',
            'Volume de produção',
            'Custo operacional',
            'Política de retorno de capital',
            'Câmbio e riscos políticos',
        ],
        'risks': """
(1) Ciclo descendente de preço reduz cash flow; (2) Transição energética reduz
demanda de longo prazo; (3) Pressão regulatória (carbono, ESG); (4) Risco político
em jurisdições; (5) Volatilidade de preço cria incerteza.
        """,
        'hedge_suggestion': 'Put spread contra queda de preço de energia',
        'recommended_horizon': '3-5 years',
        'conviction_base': 68,
    },
    'Payments': {
        'thesis_template': """
{ticker} é operador de rede de pagamentos com modelo de negócio de taxa recorrente.
Posição de {profile} oferece crescimento secular em volumes de pagamento global,
margens operacionais elevadas e previsibilidade. Exposição a crescimento de cashless
society e e-commerce. Baixa capex, alto FCF. Posição core para investidor com horizonte
5+ anos em crescimento defensivo.
        """,
        'key_drivers': [
            'Volume de transações processadas',
            'Taxa por transação (basis points)',
            'Margem operacional e leverage',
            'Crescimento em mercados emergentes',
            'Retorno de capital',
        ],
        'risks': """
(1) Competição de fintech e novas redes; (2) Pressão de taxa (regulação, competição);
(3) Câmbio em receitas internacionais; (4) Risco de recessão reduz volume;
(5) Concentração em clientes-chave.
        """,
        'hedge_suggestion': 'Proteção leve contra recessão; posição defensiva natural',
        'recommended_horizon': '5+ years',
        'conviction_base': 77,
    },
    'E-commerce': {
        'thesis_template': """
{ticker} é plataforma de e-commerce com exposição a crescimento de varejo online.
Posição de {profile} oferece exposição a transformação digital do varejo em mercados
emergentes e desenvolvidos. Modelos variam (marketplace, varejo próprio, logistics).
Margem sensível a competição e investimento de growth. Horizonte de 3-5 anos apropriado
para investidor com visão em crescimento digital.
        """,
        'key_drivers': [
            'Volume de GMV e crescimento',
            'Margem operacional e alavancagem',
            'Crescimento de usuários e ARPU',
            'Retenção e frequência de compra',
            'Eficiência de marketing',
        ],
        'risks': """
(1) Competição acirrada comprime margens; (2) Pressão de custo de fulfillment e logistics;
(3) Churn de vendedores em marketplace; (4) Saturação de mercado em regiões desenvolvidas;
(5) Recessão reduz consumo.
        """,
        'hedge_suggestion': 'Put spread contra recessão ou pressão de margin',
        'recommended_horizon': '3-5 years',
        'conviction_base': 70,
    },
    'ETFs': {
        'thesis_template': """
{ticker} é ETF passivo que replica índice {profile}. Exposição diversificada e de
baixo custo a mercado acionário. Apropriado para investidor que deseja exposure ao
mercado sem stock picking. Baixíssima taxa de administração (~0.03-0.10% a.a.), alta
liquidez. Posição defensiva vs ações isoladas. Horizonte apropriado 5+ anos para
investor com visão buy-and-hold.
        """,
        'key_drivers': [
            'Rentabilidade agregada de empresas',
            'Nível de taxa de juros',
            'Fluxo de capital ao mercado',
            'Câmbio (para ETFs internacionais)',
            'Crescimento econômico',
        ],
        'risks': """
(1) Recessão econômica reduz lucros agregados; (2) Ciclo de alta taxa de juros
pressiona valuation; (3) Volatilidade de mercado; (4) Saída de capital estrangeiro;
(5) Concentração setorial em índice.
        """,
        'hedge_suggestion': 'Posição natural em diversificação',
        'recommended_horizon': '5+ years',
        'conviction_base': 72,
    },
    'Fintech': {
        'thesis_template': """
{ticker} é plataforma fintech com modelo de negócio disruptivo em serviços financeiros.
Posição de {profile} oferece leverage a crescimento de bancos digitais e acesso
democratizado a investimentos/crédito. Modelo geralmente baseado em taxa/spread ou
volume. Crescimento de usuários e engagement crítico. Pressão de competição e regulação.
Horizonte de 3-5 anos apropriado para crescimento agressivo.
        """,
        'key_drivers': [
            'Crescimento de usuários ativos',
            'Valor de transação processado',
            'Margem por transação',
            'Retenção e engagement',
            'Monetização',
        ],
        'risks': """
(1) Competição acirrada de fintechs globais; (2) Pressão regulatória (compliance);
(3) Churn alto de usuários; (4) Redução de spreads; (5) Necessidade de capex para
crescimento.
        """,
        'hedge_suggestion': 'Put spread contra recessão ou pressão regulatória',
        'recommended_horizon': '3-5 years',
        'conviction_base': 67,
    },
    'Telecom': {
        'thesis_template': """
{ticker} é operadora de telecomunicações com receitas de assinatura previsível.
Posição de {profile} oferece fluxo de caixa estável, exposição defensiva a
comunicação essencial. Capex elevado em infraestrutura (5G, fibra) requerido para
competir. Margens pressionadas por competição e regulação. Dividend yield moderado-
elevado. Horizonte de 4-5 anos apropriado para crescimento defensivo.
        """,
        'key_drivers': [
            'Volume de assinantes',
            'ARPU e mix de serviços',
            'Eficiência operacional',
            'Investimento em infraestrutura (capex)',
            'Política de dividendos',
        ],
        'risks': """
(1) Competição de operadores alternativos reduz ARPU; (2) Pressão regulatória em
tarifas; (3) Obsolescência de tecnologia (desuso 4G); (4) Churn de clientes;
(5) Aumento de custo de capex de 5G.
        """,
        'hedge_suggestion': 'Proteção contra pressão regulatória',
        'recommended_horizon': '4-5 years',
        'conviction_base': 71,
    },
    'Blue Chips': {
        'thesis_template': """
{ticker} é uma das empresas líderes do mercado brasileiro com histórico comprovado de
excelência operacional e geração de valor. {profile} com posicionamento competitivo
forte e barreira de entrada significativa. Gestão profissional focada em retorno ao
acionista via dividendos e recompras. Empresa com track record de crescimento acima do
PIB e resiliência em ciclos adversos. Ideal para posição core de longo prazo em carteira
diversificada.
        """,
        'key_drivers': [
            'Crescimento de receita e margem operacional',
            'Posição competitiva e market share',
            'Retorno ao acionista (dividendos + recompras)',
            'Eficiência operacional e inovação',
            'Governança corporativa',
        ],
        'risks': """
Principais riscos: (1) Desaceleração econômica brasileira impactando demanda;
(2) Concorrência crescente de players globais; (3) Risco cambial em exportações;
(4) Pressão regulatória setorial; (5) Risco de execução em estratégia de expansão.
        """.strip(),
        'hedge_suggestion': 'Diversificação setorial e geográfica dentro da carteira',
        'recommended_horizon': '3-5 years',
        'conviction_base': 76,
    },
    'Consumer': {
        'thesis_template': """
{ticker} opera no setor de consumo brasileiro com exposição direta ao ciclo econômico
doméstico. {profile} com marca reconhecida e canal de distribuição estabelecido.
Recuperação do poder de compra das famílias e queda de juros beneficiam o setor.
Risco de execução e competição com players internacionais são fatores monitorados.
Posição adequada para exposição tática ao ciclo de consumo brasileiro.
        """,
        'key_drivers': [
            'Ciclo econômico e confiança do consumidor',
            'Nível de emprego e renda real',
            'Taxa de juros e crédito ao consumidor',
            'Força da marca e canal de distribuição',
            'Eficiência operacional',
        ],
        'risks': """
Principais riscos: (1) Recessão ou desaceleração econômica; (2) Inflação pressionando
margens; (3) Concorrência de importados e e-commerce; (4) Mudanças nos hábitos de consumo;
(5) Risco de crédito ao consumidor.
        """.strip(),
        'hedge_suggestion': 'Posição defensiva em utilities ou ETF de renda fixa',
        'recommended_horizon': '2-4 years',
        'conviction_base': 62,
    },
    'Education': {
        'thesis_template': """
{ticker} atua no setor de educação brasileira com escala relevante em ensino superior.
{profile} com potencial de consolidação setorial mas desafios regulatórios significativos.
Tendência de digitalização (EAD) oferece oportunidade de redução de custos e expansão
geográfica. Setor sensível a política governamental (FIES, PROUNI) e poder aquisitivo
das famílias. Investimento especulativo com potencial de turnaround.
        """,
        'key_drivers': [
            'Política educacional (FIES, PROUNI)',
            'Penetração de EAD (ensino à distância)',
            'Consolidação setorial e M&A',
            'Ticket médio e taxa de evasão',
            'Regulação do MEC',
        ],
        'risks': """
Principais riscos: (1) Mudanças em políticas de financiamento estudantil; (2) Alta evasão
e inadimplência; (3) Competição crescente de EdTech; (4) Risco regulatório do MEC;
(5) Dependência de crédito estudantil governamental.
        """.strip(),
        'hedge_suggestion': 'Posição reduzida com stop loss definido; hedge via short ETF educação',
        'recommended_horizon': '2-3 years',
        'conviction_base': 50,
    },
    'Transport': {
        'thesis_template': """
{ticker} opera no setor de transporte e infraestrutura com exposição ao ciclo econômico.
{profile} com ativos estratégicos mas sensível a preço de combustível, câmbio e demanda.
Setor com alta alavancagem operacional — recovery em período de alta demanda pode gerar
retornos expressivos, mas downturn amplifica perdas. Monitoramento ativo de métricas
operacionais e financeiras é essencial.
        """,
        'key_drivers': [
            'Demanda de passageiros/carga',
            'Preço de combustível (querosene/diesel)',
            'Taxa de câmbio (custos dolarizados)',
            'Yield e load factor operacional',
            'Concessões e regulação setorial',
        ],
        'risks': """
Principais riscos: (1) Volatilidade de combustível impactando margens; (2) Risco cambial
em custos dolarizados; (3) Alta alavancagem financeira; (4) Competição acirrada de preço;
(5) Risco regulatório e de concessão.
        """.strip(),
        'hedge_suggestion': 'Hedge de combustível e câmbio; posição pequena com stop loss',
        'recommended_horizon': '2-4 years',
        'conviction_base': 55,
    },
    'Oil & Gas': {
        'thesis_template': """
{ticker} opera na cadeia de petróleo e gás com exposição direta ao preço internacional
do barril. {profile} com ativos produtivos e potencial de geração de caixa significativa
em cenário de preços sustentados. Setor cíclico que demanda disciplina de capital e
eficiência operacional. Dividendos atrativos em fase madura do ciclo. Transição
energética é risco de longo prazo monitorado.
        """,
        'key_drivers': [
            'Preço internacional do petróleo (Brent)',
            'Produção e reservas provadas',
            'Custo de extração (lifting cost)',
            'Política de dividendos',
            'Transição energética',
        ],
        'risks': """
Principais riscos: (1) Colapso do preço do petróleo; (2) Risco geopolítico e OPEC+;
(3) Transição energética acelerada; (4) Regulação ambiental crescente;
(5) Risco de execução em novos projetos.
        """.strip(),
        'hedge_suggestion': 'Put spread em petróleo ou posição inversa em ETF de energia limpa',
        'recommended_horizon': '3-5 years',
        'conviction_base': 70,
    },
    'Services': {
        'thesis_template': """
{ticker} atua no setor de serviços com modelo de negócio asset-light e recorrência
de receita. {profile} com escalabilidade e potencial de expansão geográfica.
Setor beneficiado por tendência de outsourcing e digitalização de serviços.
Margens podem expandir com ganho de escala. Monitorar churn, ticket médio e
satisfação do cliente como indicadores de saúde do negócio.
        """,
        'key_drivers': [
            'Expansão de base de clientes',
            'Ticket médio e recorrência',
            'Margens operacionais e escalabilidade',
            'Digitalização e eficiência',
            'Competição e diferenciação',
        ],
        'risks': """
Principais riscos: (1) Competição crescente pressionando preços; (2) Churn de clientes;
(3) Risco de execução em expansão; (4) Dependência de mão de obra qualificada;
(5) Ciclo econômico afetando demanda.
        """.strip(),
        'hedge_suggestion': 'Diversificação setorial; posição moderada',
        'recommended_horizon': '3-5 years',
        'conviction_base': 65,
    },
    'Software': {
        'thesis_template': """
{ticker} é player de software/tecnologia com modelo de receita recorrente (SaaS/licenças).
{profile} com altas margens brutas e potencial de crescimento exponencial. Setor beneficiado
por transformação digital e adoção de IA. Valuation premium justificado por crescimento
e previsibilidade de receita. Risco de disruption tecnológica e competição de big techs.
        """,
        'key_drivers': [
            'Crescimento de ARR/MRR (receita recorrente)',
            'Margem bruta e net retention rate',
            'TAM (mercado endereçável total)',
            'Inovação e roadmap de produto',
            'Adoção de IA e automação',
        ],
        'risks': """
Principais riscos: (1) Valuation elevado sujeito a correção; (2) Competição de big techs;
(3) Disruption tecnológica; (4) Dificuldade de retenção de talentos;
(5) Desaceleração de gastos com TI em recessão.
        """.strip(),
        'hedge_suggestion': 'Diversificação entre SaaS growth e value; collar em posições grandes',
        'recommended_horizon': '3-5 years',
        'conviction_base': 72,
    },
    'Crypto': {
        'thesis_template': """
{ticker} tem exposição direta ao ecossistema de criptoativos e blockchain. {profile}
com modelo de negócio vinculado à adoção institucional de cripto e DeFi. Setor de
altíssima volatilidade com potencial assimétrico de retorno. Regulação em evolução
pode ser catalisador positivo (legitimação) ou negativo (restrições). Posição
especulativa que requer sizing disciplinado e gestão ativa de risco.
        """,
        'key_drivers': [
            'Preço do Bitcoin e volume de trading',
            'Adoção institucional de cripto',
            'Regulação (SEC, CFTC, Banco Central)',
            'Inovação em DeFi e blockchain',
            'Sentimento de mercado e risk-on/risk-off',
        ],
        'risks': """
Principais riscos: (1) Volatilidade extrema de criptoativos; (2) Risco regulatório severo;
(3) Hack ou falha de segurança; (4) Competição de exchanges tradicionais entrando no mercado;
(5) Bear market prolongado em cripto.
        """.strip(),
        'hedge_suggestion': 'Posição pequena (2-5% do portfólio max); stop loss agressivo',
        'recommended_horizon': '1-3 years',
        'conviction_base': 52,
    },
    'Auto/Tech': {
        'thesis_template': """
{ticker} combina tecnologia com mobilidade/transporte em modelo disruptivo. {profile}
com potencial de redefinir o setor mas execution risk significativo. Margens sob
pressão de competição e investimento em P&D. Valuation reflete expectativas futuras
mais que resultados presentes. Posição para investidor com alta tolerância a volatilidade
e convicção na tese de longo prazo.
        """,
        'key_drivers': [
            'Inovação tecnológica e roadmap',
            'Market share e crescimento de receita',
            'Margens e caminho para lucratividade',
            'Regulação do setor',
            'Competição global',
        ],
        'risks': """
Principais riscos: (1) Valuation desconectado de fundamentos; (2) Execution risk alto;
(3) Competição intensa de incumbentes; (4) Risco regulatório; (5) Dependência de
capital externo para crescimento.
        """.strip(),
        'hedge_suggestion': 'Put de proteção ou collar; sizing conservador',
        'recommended_horizon': '3-5 years',
        'conviction_base': 58,
    },
}


def get_investment_theses() -> dict:
    """
    Generate realistic investment theses for MVP assets in Portuguese.

    Returns:
        {ticker: {
            'thesis_text': str,
            'key_drivers': list,
            'risks': str,
            'hedge_suggestion': str,
            'recommended_horizon': str,
            'conviction_level': float
        }}
    """
    return {
        'PETR4': {
            'thesis_text': """
Petrobras apresenta oportunidade atrativa em valuation com P/E deprimido e dividend yield
superior a 10% a.a. A recuperação gradual dos preços do petróleo (cenário base $80-90/bbl)
e o programa de desinvestimento em ativos não-core devem impulsionar fluxo de caixa livre
e retorno de capital aos acionistas. A produção pré-sal está em ramp-up, com custos
declinantes, enquanto a alavancagem caminha para níveis confortáveis. O risco de interferência
governamental persiste, mas o novo conselho mira em maximizar valor ao acionista.
Acumulação em preços deprimidos (R$25-30) oferece bom risco/retorno para horizonte de 3-5 anos.
            """.strip(),
            'key_drivers': [
                'Preço do Petróleo (commodity ciclical)',
                'Cash flow e retorno de capital',
                'Alavancagem declinante',
                'Geração pré-sal (WACC-beating)',
                'Política de dividendos',
            ],
            'risks': """
Principais riscos: (1) Reversão de preços de petróleo para $60/bbl ou inferior impactaria
P&L e dividendos; (2) Interferência governamental em preços domésticos de combustível;
(3) Transição energética de longo prazo reduz demanda; (4) Desgaste de ativos fixos requer
capex sustentado; (5) Risco cambial em receita dolarizada com custos parcialmente locais.
            """.strip(),
            'hedge_suggestion': 'Put spread (R$25/R$20) ou posição reduzida em petróleo sintético',
            'recommended_horizon': '3-5 years',
            'conviction_level': 76,
        },
        'VALE3': {
            'thesis_text': """
Vale é empresa de classe mundial com ativos de minério de ferro de custo operacional ultra-baixo
(AllIn Sustaining Cost ~$15/ton). Apesar de exposição cíclica aos preços de commodities, a
posição de custos estruturalmente inferior (vs peers globais) e disciplina de capital recente
oferecem proteção relativa. China representa 60% da demanda; estímulos econômicos lá tendem
a suportar preços. Carteira de projetos em Moçambique e Canadá expandem escala de baixo custo.
Dividend policy agressivo com payout ratio ~40-50%. Acúmulo em correções é estratégia viável
para investidores com visão de 2-4 anos em commodities.
            """.strip(),
            'key_drivers': [
                'Preço do minério de ferro global',
                'Demanda chinesa e estímulos econômicos',
                'Custo operacional e EBITDA',
                'Política de retorno de capital',
                'Exploração e ramp-up de novos projetos',
            ],
            'risks': """
Riscos significativos incluem: (1) Desaceleração econômica chinesa que reduz consumo de aço;
(2) Preços de Fe2O3 podem cair para $70-80/ton, impactando drasticamente o EBITDA;
(3) Risco regulatório e ambiental (Minas-IronQual) pode retardar projetos; (4) Câmbio forte
reduz receita em dólares; (5) Competição de fontes alternativas (e.g. sucata, minério indiano).
            """.strip(),
            'hedge_suggestion': 'Put spread em preço de ferro ou posição parcial defensiva',
            'recommended_horizon': '2-4 years',
            'conviction_level': 73,
        },
        'ITUB4': {
            'thesis_text': """
Itaú Unibanco é instituição financeira líder na América Latina com ROE consistentemente
acima de 15% a.a., modelo de negócio diversificado (varejo, atacado, tesouraria, seguros).
Carteira de clientes de alta renda, custos operacionais eficientes e forte gestão de risco
diferenciam em relação a peers. Ambiente de juros mais altos no Brasil favorece NII.
Margem de crédito adequada, cobertura de prejuízos robusta. Valuation em ~0.9x P/B oferece
margem de segurança. Dividend yield de 4-5% a.a. com crescimento discreto. Posição de
longo prazo para investor com horizonte 5+ anos.
            """.strip(),
            'key_drivers': [
                'Nível de taxa Selic e curva de juros',
                'Eficiência operacional (índice de custo)',
                'Volume e spread de crédito',
                'Inadimplência e cobertura de riscos',
                'Capex e retorno ao acionista',
            ],
            'risks': """
Exposição a ciclos de crédito: (1) Recessão econômica pode elevar inadimplência;
(2) Queda de juros reduz margem de intermediação; (3) Competição de fintechs em
varejo de baixo risco; (4) Risco regulatório (capital mínimo, provisões dinâmicas);
(5) Exposição cambial em operações internacionais.
            """.strip(),
            'hedge_suggestion': 'Put spread em taxa de juros (proteção contra queda de Selic)',
            'recommended_horizon': '5+ years',
            'conviction_level': 81,
        },
        'BBDC4': {
            'thesis_text': """
Bradesco é banco sistêmico com carteira de varejo massificado, rede de agências e
infraestrutura de TI robusta. Eficiência operacional, reduzida exposição ao atacado e
modelo de insurance subsidiary geram fluxo de caixa consistente. Dividend policy conservador
mas previsível (~30-40% payout). Valuation atrativo em ~0.85x P/B com yield de 4-5% a.a.
Posição defensiva em portfólio diversificado por foco em crédito de varejo de menor volatilidade.
            """.strip(),
            'key_drivers': [
                'Nível de taxa Selic',
                'Volume de crédito de varejo',
                'Spread de crédito (NII)',
                'Eficiência operacional',
                'Renda de seguros',
            ],
            'risks': """
(1) Pressão de taxa de juros (Selic em trajetória descendente impactaria NII);
(2) Aumento de inadimplência em cenário de desemprego elevado;
(3) Competição de fintechs em crédito pessoa física;
(4) Risco de perda de market share em seguros;
(5) Exposição a rating risk (rebaixamento de classificação).
            """.strip(),
            'hedge_suggestion': 'Proteção contra queda de spread de crédito',
            'recommended_horizon': '4-5 years',
            'conviction_level': 78,
        },
        'BBAS3': {
            'thesis_text': """
Banco do Brasil é instituição estatal sistêmica com carteira diversificada e presença
nacional. Eficiência operacional melhorada sob gestão recente, com foco em digitalização
e redução de custos. Dividend yield elevado (~4-5% com bônus) atrai yield hunters.
Valuation é desconto a peers por risco governamental e menor governança corporativa.
Posição especulativa para investidores confortáveis com exposição à política econômica
brasileira em horizonte de 2-3 anos.
            """.strip(),
            'key_drivers': [
                'Pressão política em taxas de crédito',
                'Programa de reestruturação e eficiência',
                'Volume de crédito (pessoa física e jurídica)',
                'Nível de Selic e NII',
                'Capex em TI e transformação digital',
            ],
            'risks': """
(1) Risco político: pressão para manutenção artificial de spreads baixos prejudica
rentabilidade; (2) Troca de liderança pode reverter agenda de eficiência;
(3) Inadimplência elevada em varejo de baixa renda; (4) Exposição a títulos
governamentais em ambiente de alta inflação; (5) Lower ROE e ROA vs peers privados.
            """.strip(),
            'hedge_suggestion': 'Proteção em posição (Put spread contra risco político)',
            'recommended_horizon': '2-3 years',
            'conviction_level': 68,
        },
        'ABEV3': {
            'thesis_text': """
Ambev é líder global em bebidas com presença em 70+ países, porém com exposição
significativa à América Latina. Portfólio diversificado (cerveja, água, refrigerante,
café, suco) oferece hedge contra ciclos. Margem EBITDA robusta (~40%+), brand power
internacionalmente reconhecido (Brahma, Skol, Corona, Modelo). Preço reduzido recentemente
oferece oportunidade; dividend yield acima de média do setor (~3-4%). Posição defensiva
para portfólio com visão de 3-5 anos em consumo.
            """.strip(),
            'key_drivers': [
                'Volume de vendas e mix de portfólio',
                'Margem EBITDA (commodity + pricing)',
                'Preço de commodities (malte, lúpulo, alumínio)',
                'Taxa de câmbio (receita em USD)',
                'Despesas SG&A',
            ],
            'risks': """
(1) Pressão de commodity prices (malte, lúpulo, alumínio) aumenta COGS;
(2) Desaceleração econômica reduz volume, especialmente em LatAm;
(3) Câmbio forte impacta tradução de receitas; (4) Transição de bebidas alcóolicas
para água/bebidas saudáveis reduz margens; (5) Impostos específicos em bebidas
açucaradas em múltiplas jurisdições.
            """.strip(),
            'hedge_suggestion': 'Posição defensiva (commodity hedges via contratos futuros)',
            'recommended_horizon': '3-5 years',
            'conviction_level': 76,
        },
        'B3SA3': {
            'thesis_text': """
B3 é operadora de bolsa de valores, derivativos e câmbio com monopólio regulatório
efetivo no Brasil. Modelo de receita altamente previsível (taxa de negociação + taxas
de listagem + serviços de pós-negociação). Alavancagem operacional alta: crescimento
de volume fluxo direto para lucro incremental. Margens EBITDA >70%, FCF >60% do
lucro líquido. Governance excelente, sem interferência estatal. Posição core para
investidor de longo prazo (5+ anos) buscando crescimento moderado e estável.
            """.strip(),
            'key_drivers': [
                'Volume de negociação (ações, futuros, câmbio)',
                'Número de IPOs e listagens',
                'Alavancagem de receita fixa em múltiplos volumes',
                'Margens operacionais e de EBITDA',
                'Investimentos em tecnologia e infraestrutura',
            ],
            'risks': """
(1) Desaceleração de mercado reduz volume de negociação e receita;
(2) Competição internacional (plataformas de trading, exchanges globais)
pode capturar segmentos; (3) Risco regulatório (impostos sobre transações);
(4) Concentração em Brasil como mercado único; (5) Investimentos em infraestrutura
(data center, sistemas) requerem capex contínuo.
            """.strip(),
            'hedge_suggestion': 'Proteção natural via diversificação de receitas',
            'recommended_horizon': '5+ years',
            'conviction_level': 79,
        },
        'BOVA11': {
            'thesis_text': """
BOVA11 é ETF que replica o índice Ibovespa, oferecendo exposição diversificada a
75+ ações de maior liquidez do Brasil. Posição estratégica para investidor que deseja
exposure ao Brasil macro sem stock picking. Baixo fee (~0.06% a.a.), alta liquidez e
fungibilidade (cria/resgata versus cesta). Apropriado para horizonte de 5+ anos em
contexto de incerteza política. Dividend yield próximo ao índice (~3-4% a.a.).
Posição defensiva vs. ações isoladas.
            """.strip(),
            'key_drivers': [
                'Crescimento econômico do Brasil (PIB)',
                'Nível de taxa de juros (Selic)',
                'Risco-país (EMBI+ Brazil)',
                'Câmbio real vs USD',
                'Fluxo de capital estrangeiro',
            ],
            'risks': """
(1) Recessão econômica brasileira reduz lucros agregados de empresas;
(2) Crise política ou fiscal (aumento de spreads soberanos);
(3) Inflação elevada e Selic em patamar alto reduzem valuations;
(4) Saída de capital estrangeiro pressiona câmbio;
(5) Exposição concentrada em commodities (petróleo, minério) que são procíclicas.
            """.strip(),
            'hedge_suggestion': 'Proteção via posição em dólar ou ouro (diversificação)',
            'recommended_horizon': '5+ years',
            'conviction_level': 71,
        },

    }

def generate_dynamic_thesis(ticker: str, scores: dict = None) -> dict:
    """
    Generate a dynamic thesis for any ticker using sector templates.

    Args:
        ticker: Stock ticker symbol
        scores: Optional dict with scoring/conviction adjustments

    Returns:
        dict with thesis information or None if ticker not found in database
    """
    if ticker not in SECTOR_DATABASE:
        return None

    ticker_info = SECTOR_DATABASE[ticker]
    sector = ticker_info['sector']

    # Find the matching template (map sector names to template keys)
    template_key = sector
    if sector not in SECTOR_TEMPLATES:
        # Fallback to similar sector or generic
        return None

    template = SECTOR_TEMPLATES[template_key]

    # Generate thesis text from template
    thesis_text = template['thesis_template'].format(
        ticker=ticker,
        profile=ticker_info['profile']
    ).strip()

    # Adjust conviction based on scores if provided
    conviction = template['conviction_base']
    if scores and 'overall_score' in scores:
        # Adjust conviction level based on provided scores
        adjustment = (scores.get('overall_score', 0) - 50) * 0.5  # Range from -25 to +25
        conviction = min(95, max(30, conviction + adjustment))

    return {
        'thesis_text': thesis_text,
        'key_drivers': template['key_drivers'],
        'risks': template['risks'].strip(),
        'hedge_suggestion': template['hedge_suggestion'],
        'recommended_horizon': template['recommended_horizon'],
        'conviction_level': int(conviction),
    }


def generate_thesis_for_ticker(ticker: str, scores: dict = None) -> dict:
    """
    Generate thesis for a specific ticker.
    First checks hardcoded theses, then falls back to dynamic generation.

    Args:
        ticker: Stock ticker symbol
        scores: Optional dict with scoring/conviction adjustments

    Returns:
        dict with thesis information or None if not found
    """
    # First try hardcoded theses for MVP assets
    theses = get_investment_theses()
    if ticker in theses:
        thesis_data = theses[ticker]
        return {
            'ticker': ticker,
            'thesis_date': date.today().isoformat(),
            'thesis_text': thesis_data['thesis_text'],
            'key_drivers': json.dumps(thesis_data['key_drivers']),
            'risks': thesis_data['risks'],
            'hedge_suggestion': thesis_data['hedge_suggestion'],
            'recommended_horizon': thesis_data['recommended_horizon'],
            'conviction_level': thesis_data['conviction_level'],
            'model_version': 'v1.0-hardcoded',
        }

    # Fall back to dynamic thesis generation
    dynamic_thesis = generate_dynamic_thesis(ticker, scores)
    if dynamic_thesis is None:
        return None

    return {
        'ticker': ticker,
        'thesis_date': date.today().isoformat(),
        'thesis_text': dynamic_thesis['thesis_text'],
        'key_drivers': json.dumps(dynamic_thesis['key_drivers']),
        'risks': dynamic_thesis['risks'],
        'hedge_suggestion': dynamic_thesis['hedge_suggestion'],
        'recommended_horizon': dynamic_thesis['recommended_horizon'],
        'conviction_level': dynamic_thesis['conviction_level'],
        'model_version': 'v2.0-dynamic',
    }


if __name__ == '__main__':
    # Demo: Print all hardcoded theses and sample dynamic theses
    print("\n=== Long Horizon Investment Theses (Portuguese) ===\n")
    theses = get_investment_theses()
    for ticker, thesis in theses.items():
        print(f"\n{'='*60}")
        print(f"TICKER: {ticker} | Conviction: {thesis['conviction_level']}")
        print(f"{'='*60}")
        print(f"Thesis:\n{thesis['thesis_text']}\n")
        print(f"Horizon: {thesis['recommended_horizon']}")

    print(f"\n{'='*80}\n")
    print("=== Sample Dynamic Theses (Sector-based) ===\n")

    # Sample some dynamic theses
    sample_tickers = ['WEGE3', 'NFLX', 'NVDA', 'MGLU3', 'TSLA', 'CMIG4', 'JPM']
    for ticker in sample_tickers:
        thesis = generate_thesis_for_ticker(ticker)
        if thesis:
            print(f"\n{'='*60}")
            print(f"TICKER: {ticker} | Conviction: {thesis['conviction_level']} | Version: {thesis['model_version']}")
            print(f"{'='*60}")
            print(f"Thesis:\n{thesis['thesis_text']}\n")
            print(f"Horizon: {thesis['recommended_horizon']}")
        else:
            print(f"\nTICKER: {ticker} - Not found in database")
