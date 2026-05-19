import os
from pyspark.sql import SparkSession

# ── Configuração de caminhos ──────────────────────────────
LANDING = os.path.expanduser("~/lakehouse/landing")
BRONZE  = os.path.expanduser("~/lakehouse/bronze")

def main():
    spark = SparkSession.builder \
        .appName("Bronze - Customers") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")  # Só mostra erros, sem spam

    print("=" * 50)
    print("🥉 BRONZE - customers")
    print("=" * 50)

    # Lê o JSON da Landing
    caminho_entrada = os.path.join(LANDING, "customers.json")
    df = spark.read.option("multiline", "true").json(caminho_entrada)

    print(f"📖 Lido: {caminho_entrada}")
    print(f"📊 Linhas: {df.count()}")
    df.printSchema()
    df.show()

    # Salva como Parquet na Bronze
    caminho_saida = os.path.join(BRONZE, "customers")
    df.write.mode("overwrite").parquet(caminho_saida)

    print(f"✅ Salvo em: {caminho_saida}")
    spark.stop()

if __name__ == "__main__":
    main()
