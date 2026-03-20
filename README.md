# Projeto de BI e data warehouse sobre fatura de cartão de crédito
O objetivo deste trabalho pratico é estudar e compreender os conceitos basicos sobre data warehouse, ETL e BI


## Contexto 

### Cenário
Os dados disponibilizados para a realização da análise consistem em registros de faturas de um cartão de crédito, contendo informações sobre os gastos realizados entre meses de 2025. Esses dados permitem analisar o comportamento de consumo ao longo do período, possibilitando identificar padrões de gastos, categorias mais utilizadas e a distribuição das despesas ao longo do tempo. Para que isso seja possivel deve-se criar um data warehouse aliementado por etl para poder fazar a analize e tomar as melhores deciosoes a partir desses gastos.

### Objetivos 
Ao final do projeto, espera-se ser capaz de:
- Modelar um data warehouse (modelo dimensional) a partir de fontes transacionais (CSV).
- Implementar um pipeline ETL (Extract, Transform, Load) com limpeza e padronização de dados.
- Carregar dados em um repositório analítico (ex.: banco relacional ou ferramenta de DW).
- Explorar os dados com consultas analíticas (agregações, séries temporais, comparações).
- Desenvolver ou acoplar ferramentas de análise (relatórios, dashboards, visualizações).

### Proposta de Valor

Atualmente, os dados das faturas do cartão de crédito estão armazenados em arquivos CSV individuais, contendo registros das compras realizadas em determinados períodos. Nesse formato, a análise das informações tende a ser feita de maneira manual, exigindo a abertura dos arquivos e a organização dos dados em planilhas para que seja possível realizar comparações ou identificar padrões de gastos.

Esse processo pode se tornar demorado e pouco eficiente, principalmente quando o volume de dados aumenta ou quando há necessidade de realizar análises mais detalhadas, como a comparação de gastos entre categorias, períodos ou cartões diferentes.
Benefícios de aplicar BI

A aplicação de uma solução de Business Intelligence (BI) permite integrar e organizar os dados das faturas em um único ambiente estruturado, facilitando a visualização e a análise das informações. Com os dados organizados, torna-se possível criar relatórios e dashboards que apresentam os gastos de forma clara e dinâmica.
Entre os principais benefícios estão a possibilidade de visualizar rapidamente os gastos por categoria, acompanhar a evolução das despesas ao longo do tempo e identificar padrões de consumo. Além disso, dashboards interativos permitem explorar os dados de diferentes formas, possibilitando análises por período, categoria de compra ou cartão utilizado.

Com isso, a análise deixa de ser um processo manual e passa a ser mais ágil, permitindo obter respostas rápidas para perguntas de negócio e apoiar uma melhor compreensão sobre o comportamento de gastos.

## Escopo

Para a realização da análise dos dados, serão utilizados alguns campos específicos presentes nas faturas do cartão de crédito. Os dados selecionados para compor o escopo da análise são: 
- data da compra (DD/MM/AAAA)
- nome no cartão (text)
- final do cartão (int)
- categoria (text)
- número da parcela
- descricao (text)
- pacela(text)
- valor em u$ (float)
- Cotacao em real (float)
- valor da compra em reais(R$) (float)

Esses campos foram escolhidos por permitirem compreender melhor o comportamento de consumo ao longo do período analisado, possibilitando identificar padrões de gastos, distribuição das despesas e categorias mais relevantes.


### Perguntas de negócio 

Com os dados selecionados e organizados, a análise busca responder algumas perguntas de negócio que ajudam a entender melhor os padrões de gastos presentes nas faturas. Entre as principais questões que o trabalho pretende esclarecer estão:

- Gasto total por titular no período e por mês.

- Gasto por categoria (top 10 categorias em valor).

- Evolução mensal do total gasto (série temporal).

- Comparativo entre titulares (valor médio por transação, quantidade de transações).

- Principais estabelecimentos por valor (top N).

- Comportamento de parcelamento: quantidade de compras à vista vs parceladas.

- Dia da semana com mais transações ou maior volume.

- Estornos e créditos: total e impacto por titular/categoria.



## Projeto para o data warehouse

Modelagem dimensional

Tabela fato: FATO_TRANSACAO

- id_data (FK para DIM_DATA)
- id_titular (FK para DIM_TITULAR)
- id_categoria (FK para DIM_CATEGORIA)
- id_estabelecimento (FK para DIM_ESTABELECIMENTO)
- valor_brl (valor em R$)
- valor_usd (valor em US$)
- cotacao
- parcela_texto (ex.: “Única”, “1/3”)
- num_parcela (inteiro, extraído quando “x/y”)
- total_parcelas (inteiro, extraído quando “x/y”)


DIM_DATA: id_data, data (date), dia, mês, trimestre, ano, dia_semana (ou
nome do dia).

DIM_TITULAR: id_titular, nome_titular, final_cartao.

DIM_CATEGORIA: id_categoria, nome_categoria.

DIM_ESTABELECIMENTO: id_estabelecimento, nome_estabelecimento
(descrição da coluna “Descrição”)