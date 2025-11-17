
# 1. Base: Usamos uma imagem Python leve
FROM python:3.11-slim

# Metadados: Pode ser útil para rastreamento
LABEL maintainer="Victor Sabino"
LABEL description="DBT Project Container for Cloud Run Execution"

# 2. Instalação: Instala o dbt-bigquery
RUN pip install dbt-bigquery

# 3. Contexto: Define o diretório de trabalho dentro do container
WORKDIR /usr/app

# 4. Copia: Copia todos os arquivos do seu projeto dbt
# O '.' representa o conteúdo da pasta dbt/
COPY . /usr/app

# 5. Entrypoint: Define o ponto de entrada. O Cloud Run irá passar o comando (run, test)
ENTRYPOINT [ "dbt" ]