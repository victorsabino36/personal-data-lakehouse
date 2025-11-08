{{ config(
    materialized='incremental',  
    alias='monthly_summary', 
    unique_key=['sigla_empresa', 'data_inicio_mes'] 
) }}

SELECT
    sigla_empresa,
    -- Agrupamento e Aliases
    EXTRACT(YEAR FROM data_pregao) AS ano,
    EXTRACT(MONTH FROM data_pregao) AS mes,
    DATE_TRUNC(data_pregao, MONTH) AS data_inicio_mes, -- <-- Coluna de Filtro
    
    -- Métricas
    SUM(volume_negociado) AS volume_total_mensal,
    AVG(valor_fechamento) AS preco_medio_fechamento_mensal,
    MAX(valor_maximo) AS maximo_mensal,
    MIN(valor_minimo) AS minimo_mensal,
    
   

FROM
    {{ ref('daily_stocks') }} -- Fonte é a Camada Silver

--FILTRO INCREMENTAL: SÓ PROCESSA OS DADOS QUE ESTÃO NOVO MÊS
{% if is_incremental() %}
WHERE
    -- Compara a data de início do mês do dado Silver (data_pregao)
    -- com a data máxima que já existe na Gold (data_inicio_mes)
    DATE_TRUNC(data_pregao, MONTH) >= (SELECT MAX(data_inicio_mes) FROM {{ this }})
{% endif %}

GROUP BY
    sigla_empresa,
    ano,
    mes,
    data_inicio_mes
ORDER BY
    sigla_empresa, ano, mes