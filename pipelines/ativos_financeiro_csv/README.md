# 📈 Projeto Data Lakehouse: Análise de Portfólio de Investimentos

Plataforma completa de Data Engineering para ingestão, transformação e análise de dados de ativos financeiros (Renda Fixa, Variável e Cripto), resultando em um dashboard interativo para tomada de decisão e monitoramento de desempenho.

## 🎯 Objetivo

Criar uma solução Data Lakehouse escalável para processar grandes volumes de dados de mercado, unificando métricas e fornecendo uma fonte única da verdade para a análise de risco e retorno de investimentos.

## 🛠️ Stack Tecnológico

| Categoria | Ferramenta | Descrição |
| :--- | :--- | :--- |
| Ingestão e ELT | **Python** (Pandas, Requests) | Extração de dados via APIs e pré-processamento para carga inicial. |
| Armazenamento | **Google BigQuery** | Data Warehouse para o processamento de dados em escala, utilizando SQL para transformações. |
| Visualização | **Power BI** | Criação de dashboards dinâmicos para a camada de consumo (Gold Layer). |
| Workflow | **Git/GitHub** | Controle de versão e colaboração. |

## ⚙️ Arquitetura do Processo (ETL/ELT)

A pipeline de dados segue o seguinte fluxo:

1.  **Extração (Python):** Coleta dados de APIs de mercado (IPCA, SELIC, Criptomoedas, Ações).
2.  **Carga (BigQuery - Staging):** Os dados brutos são carregados no BigQuery (camada Bronze/Staging).
3.  **Transformação (BigQuery - SQL):** Aplicação de regras de negócio, cálculos de rentabilidade, consolidação de portfólio e criação de tabelas dimensionais/fatos (Camada Silver e Gold).

## 📊 Resultado Final: Dashboard de Portfólio

O painel de controle (desenvolvido no Power BI) oferece as seguintes funcionalidades:
* Visualização do Valor Total Investido.
* Distribuição percentual do portfólio por ativo.
* Comparação do desempenho (ROI) entre diferentes classes de ativos (Renda Fixa, Ações, Cripto).

**![Dashboard de Análise de Portfólio](dashboards/dashboard_ativos_financeiro.png)**

---
**Desenvolvido por:** [Victor Sabino]
