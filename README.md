# Pipeline Lakehouse com Apache Airflow e PySpark

> Pipeline completo de Engenharia de Dados implementando arquitetura
> Medallion (Lakehouse), orquestrado com Apache Airflow e processado com PySpark.

## Sobre o Projeto

Este projeto simula um pipeline de dados real de um e-commerce,
evoluindo dados brutos em JSON até um dataset analítico final,
passando pelas camadas **Landing → Bronze → Silver → Gold**.

## Arquitetura

`JSON bruto → Landing → Bronze → Silver → Gold`

```|     Camadas     |    Formato   |                         O que fazem                  |
_________________________________________________________________________________________
|     Landing     |     JSON     |   Zona de entrada dos dados, chegam sem alterações   |
_________________________________________________________________________________________
|     Bronze      |    Parquet   |     Converte JSON para Parquet sem transformações    |
_________________________________________________________________________________________
|     Silver      |    Parquet   |     Remove prefixo das colunas (customer_id → id)    |
_________________________________________________________________________________________
|      Gold       |    Parquet   |     JOIN + agregação: pedidos por cidade e estado    |
```

## Tecnologias usadas

![Python](https://img.shields.io/badge/Python-3.8+-blue?logo=python)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9-red?logo=apacheairflow)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.4-orange?logo=apachespark)
![Linux](https://img.shields.io/badge/Linux-Ubuntu-orange?logo=linux)

---

## Como Executar

### Pré-requisitos
- Linux Ubuntu 20.04+
- Python 3.8+
- Java JDK 11+

### Instalação
```bash
# Criar ambiente virtual
python3 -m venv ~/airflow_env
source ~/airflow_env/bin/activate

# Instalar dependências
pip install "apache-airflow==2.9.1"
pip install pyspark

# Inicializar Airflow
airflow db init
```

### Executar o Pipeline
```bash
# Terminal 1
airflow webserver --port 8080

# Terminal 2
airflow scheduler
```

Acesse **http://localhost:8080** e execute o DAG `pipeline_lakehouse_completo`.

---

## Resultado Final (Gold)

| city | state | quantidade_pedidos | valor_total_pedidos |
|------|-------|--------------------|---------------------|
| Curitiba | PR | 1 | 1200.00 |
| Rio de Janeiro | RJ | 1 | 820.50 |
| São Paulo | SP | 2 | 439.90 |

---

*Projeto desenvolvido como exercício prático do Programa de Talentos: Estágio em Engenharia de Dados — Bulk Consulting*
