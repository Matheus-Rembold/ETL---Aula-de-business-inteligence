# -*- coding: utf-8 -*-

import pandas as pd
import glob
from sqlalchemy import create_engine


# 1. EXTRACT


arquivos = glob.glob("data/*.csv")

if not arquivos:
    raise FileNotFoundError("Nenhum arquivo CSV encontrado em data/")

dados = pd.concat(
    [pd.read_csv(a, encoding="utf-8", sep=';') for a in arquivos],
    ignore_index=True
)

print(f"[ETL] {len(arquivos)} arquivo(s) carregado(s) — {len(dados)} linhas brutas.")


# 2. TRANSFORM


#  Datas 
dados['Data de Compra'] = pd.to_datetime(dados['Data de Compra'], format='%d/%m/%Y')
dados['ano']       = dados['Data de Compra'].dt.year
dados['mes']       = dados['Data de Compra'].dt.month
dados['trimestre'] = dados['Data de Compra'].dt.quarter


_dias = {
    'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira',
    'Wednesday': 'Quarta-feira', 'Thursday': 'Quinta-feira',
    'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
}
dados['dia_semana'] = dados['Data de Compra'].dt.day_name().map(_dias)

#Campos numéricos

for col in ['Valor (em US$)', 'Cotação (em R$)', 'Valor (em R$)']:
    dados[col] = (
        dados[col]
        .astype(str)
        .str.replace(',', '.', regex=False)
        .str.strip()
    )
    dados[col] = pd.to_numeric(dados[col], errors='coerce')

dados['Final do Cartão'] = pd.to_numeric(dados['Final do Cartão'], errors='coerce').astype('Int64')

# Parcelas
dados['num_parcela']    = pd.to_numeric(dados['Parcela'].str.split('/').str[0], errors='coerce')
dados['total_parcelas'] = pd.to_numeric(dados['Parcela'].str.split('/').str[1], errors='coerce')

# Compras à vista: "Única" → 1/1
mask_unica = dados['Parcela'].str.strip().str.lower() == 'única'
dados.loc[mask_unica, ['num_parcela', 'total_parcelas']] = 1

dados['num_parcela']    = dados['num_parcela'].fillna(1).astype(int)
dados['total_parcelas'] = dados['total_parcelas'].fillna(1).astype(int)

# Categoria 

dados['Categoria'] = dados['Categoria'].astype(str).str.strip()
dados.loc[dados['Categoria'].isin(['-', '', 'nan']), 'Categoria'] = 'Não categorizado'

# --- 2.5 Remoção de pagamentos de fatura ---
dados = dados[
    ~dados['Descrição']
    .str.replace(r'\s+', ' ', regex=True)
    .str.strip()
    .str.contains('Inclusao de Pagamento', case=False, na=False)
].reset_index(drop=True)
print(f"[ETL] {len(dados)} linhas após remoção de pagamentos de fatura.")


# 3. DIMENSÕES


# DIM_DATA
dim_data = (
    dados[['Data de Compra', 'ano', 'mes', 'trimestre', 'dia_semana']]
    .copy()
    .assign(dia=dados['Data de Compra'].dt.day)
    .drop_duplicates()
    .reset_index(drop=True)
)
dim_data['id_data'] = dim_data.index + 1

# DIM_TITULAR
dim_titular = (
    dados[['Nome no Cartão', 'Final do Cartão']]
    .drop_duplicates()
    .reset_index(drop=True)
)
dim_titular['id_titular'] = dim_titular.index + 1

# DIM_CATEGORIA
dim_categoria = (
    dados[['Categoria']]
    .drop_duplicates()
    .reset_index(drop=True)
)
dim_categoria['id_categoria'] = dim_categoria.index + 1

# DIM_ESTABELECIMENTO
dim_estabelecimento = (
    dados[['Descrição']]
    .drop_duplicates()
    .reset_index(drop=True)
)
dim_estabelecimento['id_estabelecimento'] = dim_estabelecimento.index + 1


# 4. FATO

fato = (
    dados
    .merge(dim_data,           on=['Data de Compra', 'ano', 'mes', 'trimestre', 'dia_semana'], how='left')
    .merge(dim_titular,        on=['Nome no Cartão', 'Final do Cartão'],                       how='left')
    .merge(dim_categoria,      on=['Categoria'],                                                how='left')
    .merge(dim_estabelecimento, on=['Descrição'],                                              how='left')
)

fato_transacao = fato[[
    'id_data', 'id_titular', 'id_categoria', 'id_estabelecimento',
    'Valor (em R$)', 'Valor (em US$)', 'Cotação (em R$)',
    'Parcela', 'num_parcela', 'total_parcelas'
]].copy()

fato_transacao.columns = [
    'id_data', 'id_titular', 'id_categoria', 'id_estabelecimento',
    'valor_brl', 'valor_usd', 'cotacao',
    'parcela_texto', 'num_parcela', 'total_parcelas'
]


# 5. LOAD

engine = create_engine('postgresql+psycopg2://postgres:masterkey@localhost:5432/dw')

dim_data.to_sql('dim_data',               engine, index=False, if_exists='replace')
dim_titular.to_sql('dim_titular',         engine, index=False, if_exists='replace')
dim_categoria.to_sql('dim_categoria',     engine, index=False, if_exists='replace')
dim_estabelecimento.to_sql('dim_estabelecimento', engine, index=False, if_exists='replace')
fato_transacao.to_sql('fato_transacao',   engine, index=False, if_exists='replace')

print("[ETL] Carga concluída com sucesso!")
print(f"  dim_data:            {len(dim_data)} registros")
print(f"  dim_titular:         {len(dim_titular)} registros")
print(f"  dim_categoria:       {len(dim_categoria)} registros")
print(f"  dim_estabelecimento: {len(dim_estabelecimento)} registros")
print(f"  fato_transacao:      {len(fato_transacao)} registros")