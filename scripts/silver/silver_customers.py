import os
from pyspark.sql import SparkSession

BRONZE = os.path.expanduser("~/lakehouse/bronze")
SILVER = os.path.expanduser("~/lakehouse/silver")

PREFIXO = "customer_"

def main():
    spark = SparkSession.builder \
        .appName("Silver - Customers") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 50)
    print("🥈 SILVER - customers")
    print("=" * 50)

    caminho_entrada = os.path.join(BRONZE, "customers")
    df = spark.read.parquet(caminho_entrada)

    print("📋 Colunas ANTES:")
    print(df.columns)

    # Remove o prefixo "customer_" de cada coluna
    for coluna in df.columns:
        novo_nome = coluna.replace(PREFIXO, "")
        df = df.withColumnRenamed(coluna, novo_nome)

    print("📋 Colunas DEPOIS:")
    print(df.columns)
    df.show()

    caminho_saida = os.path.join(SILVER, "customers")
    df.write.mode("overwrite").parquet(caminho_saida)

    print(f"✅ Salvo em: {caminho_saida}")
    spark.stop()

if __name__ == "__main__":
    main()
