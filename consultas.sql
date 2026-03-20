
-- Gasto total por titular no período e por mês
SELECT 
    t."Nome no Cartão",
    d.ano,
    d.mes,
    SUM(f.valor_brl) AS total_gasto
FROM fato_transacao f
JOIN dim_titular t ON f.id_titular = t.id_titular
JOIN dim_data d ON f.id_data = d.id_data
GROUP BY t."Nome no Cartão", d.ano, d.mes
ORDER BY t."Nome no Cartão", d.ano, d.mes;

-- Gasto por categoria (top 10 categorias em valor)

SELECT 
    c."Categoria",
    SUM(f.valor_brl) AS total_gasto
FROM fato_transacao f
JOIN dim_categoria c ON f.id_categoria = c.id_categoria
GROUP BY c."Categoria"
ORDER BY total_gasto DESC
LIMIT 10;


-- Evolução mensal do total gasto (série temporal).


SELECT 
    d.ano,
    d.mes,
    SUM(f.valor_brl) AS total_gasto
FROM fato_transacao f
JOIN dim_data d ON f.id_data = d.id_data
GROUP BY d.ano, d.mes
ORDER BY d.ano, d.mes;


-- Comparativo entre titulares (valor médio por transação, quantidade de transações

SELECT 
    t."Nome no Cartão",
    COUNT(*) AS qtd_transacoes,
    AVG(f.valor_brl) AS valor_medio,
    SUM(f.valor_brl) AS total_gasto
FROM fato_transacao f
JOIN dim_titular t ON f.id_titular = t.id_titular
GROUP BY t."Nome no Cartão"
ORDER BY total_gasto DESC;


-- Principais estabelecimentos por valor (top N).



SELECT 
    e."Descrição",
    SUM(f.valor_brl) AS total_gasto
FROM fato_transacao f
JOIN dim_estabelecimento e ON f.id_estabelecimento = e.id_estabelecimento
GROUP BY e."Descrição"
ORDER BY total_gasto DESC
LIMIT 7;


-- Comportamento de parcelamento: quantidade de compras à vista vs parceladas

SELECT 
    CASE 
        WHEN f.total_parcelas = 1 THEN 'À vista'
        ELSE 'Parcelado'
    END AS tipo_compra,
    COUNT(*) AS quantidade,
    SUM(f.valor_brl) AS total_gasto
FROM fato_transacao f
GROUP BY tipo_compra;


-- Dia da semana com mais transações ou maior volume


SELECT 
    CASE 
        WHEN f.total_parcelas = 1 THEN 'À vista'
        ELSE 'Parcelado'
    END AS tipo_compra,
    COUNT(*) AS quantidade,
    SUM(f.valor_brl) AS total_gasto
FROM fato_transacao f
GROUP BY tipo_compra;

SELECT 
    d.dia_semana,
    COUNT(*) AS qtd_transacoes
FROM fato_transacao f
JOIN dim_data d ON f.id_data = d.id_data
GROUP BY d.dia_semana
ORDER BY qtd_transacoes DESC;


-- Estornos e créditos: total e impacto por titular/categoria

SELECT 
    SUM(f.valor_brl) AS total_estornos
FROM fato_transacao f
WHERE f.valor_brl < 0;

SELECT 
    t."Nome no Cartão",
    SUM(f.valor_brl) AS total_estornos
FROM fato_transacao f
JOIN dim_titular t ON f.id_titular = t.id_titular
WHERE f.valor_brl < 0
GROUP BY t."Nome no Cartão"
ORDER BY total_estornos;

SELECT 
    c."Categoria",
    SUM(f.valor_brl) AS total_estornos
FROM fato_transacao f
JOIN dim_categoria c ON f.id_categoria = c.id_categoria
WHERE f.valor_brl < 0
GROUP BY c."Categoria"
ORDER BY total_estornos;