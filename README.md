# Projeto de Ciência de Dados - Cilia Tecnologia

## Visão Geral

Este repositório contém as soluções e análises para o desafio de ciência de dados fornecido pela Cilia Tecnologia. O projeto está organizado em duas partes principais com base em dois conjuntos de dados e inclui notebooks Jupyter para análise de dados, consultas SQL e uma configuração do Airflow para pipelines de dados.

## Introdução

### Pré-requisitos

- Docker e Docker Compose instalados em sua máquina.
- Jupyter Notebook ou Google Colab para executar os notebooks de análise.

### Preparação dos Dados

Para executar os notebooks, é necessário descompactar os arquivos de dados:

- Para o **Dataset 1**, descompacte `src/data/Dataset-1.7z` no diretório `src/data/Dataset-1`.
- Para o **Dataset 2**, descompacte `src/data/Dataset-2.7z` no diretório `src/data/Dataset-2`.

### Executando o Projeto

#### Configuração do Airflow

1. Navegue até o diretório `airflow`:

    ```sh
    cd airflow
    ```

2. Inicie os serviços do Airflow usando Docker Compose:

    ```sh
    docker-compose up
    ```

3. Acesse a interface web do Airflow em `http://localhost:8080`.

### Notebooks

- A análise para o Dataset 1 pode ser encontrada nos seguintes notebooks:
  - `src/notebooks/analysis-dataset-1.ipynb`
  - `src/notebooks/anl_dataset_1_google_colab.ipynb` (Google Colab)
  - [Notebook Google Colab](https://colab.research.google.com/drive/1bxm7izkbt8qt3293fT7Z2fthqYoLYRbe?usp=sharing)
- A análise para o Dataset 2 está em `src/notebooks/analysis-dataset-2.ipynb`.

## DAGs do Airflow

### Descrição das DAGs

- **local_dag**: Executa uma sequência de tarefas envolvendo extração, transformação e carregamento (ETL) de dados localmente usando scripts Python.
- **remote_dag**: Consulta dados de um banco de dados PostgreSQL remoto e registra os resultados.

### Executando as DAGs

Para executar as DAGs, certifique-se de que o Airflow está em execução e acesse a interface web do Airflow. Acione as DAGs manualmente a partir da interface.

## Perguntas do Desafio e Soluções

### Dataset 1

1. **Explicação das Tabelas e Relações**: Análise detalhada e explicação fornecida no notebook `analysis-dataset-1.ipynb`.
2. **Consulta SQL - Top 3 Produtos Mais Vendidos**:

- No SQLite:

    ```sql
    SELECT 
    Sales.ProductKey AS ProductKey,
    Product.ProductName AS Product,
    SUM(OrderQuantity) AS TotalQuantity
    FROM Sales
    INNER JOIN Product ON Product.ProductKey = Sales.ProductKey
    GROUP BY Sales.ProductKey, Product.ProductName 
    ORDER BY TotalQuantity DESC
    LIMIT 3;
    ```

- No PostgreSQL:

    ```sql
    SELECT
    Sales."ProductKey" AS ProductKey,
    Product."ProductName" AS Product,
    SUM("OrderQuantity") AS TotalQuantity
    FROM Sales
    INNER JOIN Product ON Product."ProductKey" = Sales."ProductKey"
    GROUP BY Sales."ProductKey", Product."ProductName"
    ORDER BY TotalQuantity DESC
    LIMIT 3;
    ```

3. **Consulta SQL - Lucro Total por Região**:

Segue o código para calcular o lucro total por região:

- No SQLite:

    ```sql
    SELECT 
    Sales.ProductKey AS ProductKey,
    Product.ProductName AS Product,
    SUM(OrderQuantity) AS TotalQuantity
    FROM Sales
    INNER JOIN Product ON Product.ProductKey = Sales.ProductKey
    GROUP BY Sales.ProductKey, Product.ProductName 
    ORDER BY TotalQuantity DESC
    LIMIT 3;
    ```

- No PostgreSQL:

    ```sql
    SELECT
        t."Region",
        to_char(SUM(s."SalesAmount" - s."TotalProductCost"), 'FM999999999.00') AS TotalProfit
    FROM
        sales s
    JOIN
        territory t ON s."SalesTerritoryKey" = t."SalesTerritoryKey"
    GROUP BY
        t."Region"
    ORDER BY
        SUM(s."SalesAmount" - s."TotalProductCost") DESC;
    ```

4. **Implementação do Processo ETL**: Descrito no notebook com uma explicação passo a passo e código Python.

5. **Etapas de Pré-processamento**: Detalhadas no notebook com código Python para cada etapa.

6. **Total de Vendas e Lucro por Ano e Região**: Código Python detalhado no notebook e consulta SQL abaixo:

- No PostgreSQL:

    ```sql
    SELECT 
        DATE_PART('year', TO_TIMESTAMP("OrderDate", 'MM/DD/YY HH24:MI')) AS Year,
        "SalesTerritoryKey" AS Region,
        SUM("SalesAmount") AS TotalSales,
        SUM("SalesAmount" - "TotalProductCost") AS Profit
    FROM 
        sales
    GROUP BY 
        Year, Region
    ORDER BY 
        Year, Region;
    ```

7. **Segmentação de Clientes**: Adicionada uma nova coluna na tabela `Sales` baseada em segmentos de lucratividade.

8. **Impacto da Redução do Desconto**: Análise e resultados fornecidos no notebook.

9. **Modelagem Preditiva**: Previsões de lucratividade futura usando modelos de machine learning detalhadas no notebook.

### Dataset 2

10. **DAGs do Airflow para Pipeline de Dados**: Implementadas e descritas em `airflow\dags\local_dag.py` e `airflow\dags\remote_dag.py`.

11. **Top 20 Variações Diárias nos Casos de COVID**: Análise e ranking fornecidos em `analysis-dataset-2.ipynb`.

12. **Análise Exploratória dos Dados de COVID**: Insights e visualizações incluídos no notebook.

13. **Estratégias para Anotação de Dados**: Discussões no notebook, focando em minimizar erros em projetos de deep learning.

## Conclusão

Este projeto demonstra a aplicação de técnicas de ciência e engenharia de dados para resolver problemas do mundo real. As análises, consultas SQL e DAGs do Airflow mostram a capacidade de lidar e processar grandes conjuntos de dados de forma eficaz. Desenvolvi utilizando Jupyter Notebook, Python, SQL (SQLite e PostgreSQL), Airflow, Bash e Docker.

Para quaisquer dúvidas ou mais informações, por favor, entre em contato pelo e-mail [datasageanalytics@gmail.com] ou pelo LinkedIn [Erick Bryan Cubas](https://www.linkedin.com/in/the-bryan/)
