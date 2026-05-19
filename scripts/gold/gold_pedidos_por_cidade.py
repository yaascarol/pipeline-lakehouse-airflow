import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = os.path.expanduser("~/lakehouse/silver")
GOLD   = os.path.expanduser("~/lakehouse/gold")

def main():
    spark = SparkSession.builder \
        .appName("Gold - Pedidos por Cidade") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 50)
    print("🥇 GOLD - pedidos_por_cidade")
    print("=" * 50)

    # ── 1. Lê customers e orders da Silver ─────────────────
    customers = spark.read.parquet(os.path.join(SILVER, "customers"))
    orders    = spark.read.parquet(os.path.join(SILVER, "orders"))

    print("📋 Customers (Silver):")
    customers.show()

    print("📋 Orders (Silver):")
    orders.show()

    # ── 2. JOIN: liga orders aos customers pelo id ─────────
    # customers tem: id, name, city, state
    # orders tem:    id, customer_id, total, date
    # Atenção: após remover prefixos, orders tem coluna "customer_id"?
    # Não! "order_" foi removido de "order_customer_id" → "customer_id"
    # Então fazemos join: orders.customer_id == customers.id

    df_joined = orders.join(
        customers,
        orders["customer_id"] == customers["id"],
        how="inner"
    )

    print("📋 Após JOIN:")
    df_joined.show()

    # ── 3. Agrupa por cidade e estado ──────────────────────
    df_gold = df_joined.groupBy(
        customers["city"],
        customers["state"]
    ).agg(
        F.count(orders["id"]).alias("quantidade_pedidos"),
        F.round(F.sum(orders["total"]), 2).alias("valor_total_pedidos")
    ).orderBy("valor_total_pedidos", ascending=False)

    print("🏆 Dataset GOLD final:")
    df_gold.show()
    print(f"📊 Total de cidades: {df_gold.count()}")

    # ── 4. Salva na Gold ───────────────────────────────────
    caminho_saida = os.path.join(GOLD, "pedidos_por_cidade")
    df_gold.write.mode("overwrite").parquet(caminho_saida)

    print(f"✅ Salvo em: {caminho_saida}")
    spark.stop()

if __name__ == "__main__":
    main()
