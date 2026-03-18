import pandas as pd
import glob
from sqlalchemy import create_engine

arquivos = glob.glob("data/*.csv")

dados = pd.concat(
    [pd.read_csv(a, sep=';', encoding='utf-8') for a in arquivos],
    ignore_index=True
)

#Datas: converter “DD/MM/AAAA” para tipo date; derivar ano, mês, trimestre,dia da semana.


dados['Data de Compra'] = pd.to_datetime(dados['Data de Compra'], format='%d/%m/%Y')

dados['ano'] = dados['Data de Compra'].dt.year
dados['mes'] = dados['Data de Compra'].dt.month
dados['trimestre'] = dados['Data de Compra'].dt.quarter
dados['dia_semana'] = dados['Data de Compra'].dt.day_name(locale='pt_BR')


# garantir tipo numérico; tratar vírgula/ponto e campos vazios.

dados['Final do Cartão'] = dados['Final do Cartão'].astype(int)
dados['Valor (em US$)'] = dados['Valor (em US$)'].astype(float)
dados['Cotação (em R$)'] = dados['Cotação (em R$)'].astype(float)
dados['Valor (em R$)'] = dados['Valor (em R$)'].astype(float)


# parcelas

dados['num_parcela'] = dados['Parcela'].str.split('/').str[0]
dados['num_parcela'] = pd.to_numeric(dados['num_parcela'], errors='coerce')

dados['total_parcelas'] = dados['Parcela'].str.split('/').str[1]
dados['total_parcelas'] = pd.to_numeric(dados['total_parcelas'], errors='coerce')

dados.loc[dados['Parcela'] == 'Única', ['num_parcela', 'total_parcelas']] = 1

dados['total_parcelas'] = dados['total_parcelas'].astype(int)
dados['num_parcela'] = dados['num_parcela'].astype(int)


#Dimensoes 

# dim data

dim_data = dados[['Data de Compra', 'ano', 'mes', 'trimestre', 'dia_semana']].copy()

dim_data['dia'] = dim_data['Data de Compra'].dt.day

dim_data = dim_data.drop_duplicates().reset_index(drop=True)

dim_data['id_data'] = dim_data.index + 1




# dim titular

dim_titular = dados[['Nome no Cartão', 'Final do Cartão']].copy()

dim_titular = dim_titular.drop_duplicates().reset_index(drop=True)

dim_titular['id_titular'] = dim_titular.index + 1



# dim categoria 

dim_categoria = dados[['Categoria']].copy()

dim_categoria['Categoria'] = dim_categoria['Categoria'].replace('-', 'Não categorizado')

dim_categoria = dim_categoria.drop_duplicates().reset_index(drop=True)

dim_categoria['id_categoria'] = dim_categoria.index + 1


# dim estabelecimento

dim_estabelecimento = dados[['Descrição']].copy()

dim_estabelecimento = dim_estabelecimento.drop_duplicates().reset_index(drop=True)

dim_estabelecimento['id_estabelecimento'] = dim_estabelecimento.index + 1

# fato 



fato = dados.merge(
    dim_data,
    on=['Data de Compra', 'ano', 'mes', 'trimestre', 'dia_semana'],
    how='left'
)

fato = fato.merge(
    dim_titular,
    on=['Nome no Cartão', 'Final do Cartão'],
    how='left'
)

fato = fato.merge(
    dim_categoria,
    on='Categoria',
    how='left'
)

fato = fato.merge(
    dim_estabelecimento,
    on='Descrição',
    how='left'
)

fato_transacao = fato[[
    'id_data',
    'id_titular',
    'id_categoria',
    'id_estabelecimento',
    'Valor (em R$)',
    'Valor (em US$)',
    'Cotação (em R$)',
    'Parcela',
    'num_parcela',
    'total_parcelas'
]].copy()

fato_transacao.columns = [
    'id_data',
    'id_titular',
    'id_categoria',
    'id_estabelecimento',
    'valor_brl',
    'valor_usd',
    'cotacao',
    'parcela_texto',
    'num_parcela',
    'total_parcelas'
]


#Criar banco de dados

engine = create_engine('postgresql://usuario:senha@localhost:5432/dw')

dim_data.to_sql('dim_data', engine, index=False, if_exists='replace')
dim_titular.to_sql('dim_titular', engine, index=False, if_exists='replace')
dim_categoria.to_sql('dim_categoria', engine, index=False, if_exists='replace')
dim_estabelecimento.to_sql('dim_estabelecimento', engine, index=False, if_exists='replace')
fato_transacao.to_sql('fato_transacao', engine, index=False, if_exists='replace')