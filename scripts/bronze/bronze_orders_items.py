import os
from pyspark.sql import SparkSession

LANDING = os.path.expanduser("~/lakehouse/landing")
BRONZE  = os.path.expanduser("~/lakehouse/bronze")

def main():
    spark = SparkSession.builder \
        .appName("Bronze - Orders Items") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 50)
    print("🥉 BRONZE - orders_items")
    print("=" * 50)

    caminho_entrada = os.path.join(LANDING, "orders_items.json")
    df = spark.read.option("multiline", "true").json(caminho_entrada)

    print(f"📖 Lido: {caminho_entrada}")
    print(f"📊 Linhas: {df.count()}")
    df.printSchema()
    df.show()

    caminho_saida = os.path.join(BRONZE, "orders_items")
    df.write.mode("overwrite").parquet(caminho_saida)

    print(f"✅ Salvo em: {caminho_saida}")
    spark.stop()

if __name__ == "__main__":
    main()
