{{ config(
    materialized='incremental',
    alias='daily_stocks',
    unique_key=['sigla_empresa', 'data_pregao'], 
    on_schema_change='fail'
) }}
SELECT 
    CAST(date AS DATE) AS data_pregao,
    ticker AS sigla_empresa,
    open AS valor_abertura,
    high AS valor_maximo,
    low AS valor_minimo,
    close AS valor_fechamento,
    volume AS volume_negociado,

    -- Enriquecimento Simples
    (close - open) AS variacao_dia_abs,
    SAFE_DIVIDE((close - open), open) AS variacao_dia_perc,
    
    -- Rastreabilidade
    CURRENT_TIMESTAMP() as data_ingestao_silver
FROM 
    {{ source('stock_bronze', 'raw_stock_daily') }}
  
WHERE 
    volume > 0 
    AND close > 0
    --  Lógica que SÓ RODA no modo INCREMENTAL (sem o --full-refresh)
    {% if is_incremental() %}
    -- Compara a data de pregao do dado BRONZE com a data máxima já existente no SILVER ({{ this }})
    AND CAST(date AS DATE) >= (SELECT MAX(data_pregao) FROM {{ this }})
    {% endif %}


