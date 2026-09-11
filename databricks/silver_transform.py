# Databricks notebook source
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

BRONZE = "abfss://lakehouse@atliqlakejin.dfs.core.windows.net/bronze"

dbutils.widgets.text("run_date", "")
run_date = dbutils.widgets.get("run_date")

# COMMAND ----------

customers = (
    spark.read.parquet(f"{BRONZE}/customers")
    .withColumn("city", F.initcap(F.trim("city")))
    .withColumn("signup_date", F.to_date("signup_date"))
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
)

customers.write.format("delta").mode("overwrite").saveAsTable("atliq.silver.customers")

# COMMAND ----------

products = (
    spark.read.parquet(f"{BRONZE}/products")
    .dropDuplicates(["product_id"])
    .filter(F.col("product_id").isNotNull())
)

products.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("atliq.silver.products")

# COMMAND ----------

supplier = (
    spark.read.parquet(f"{BRONZE}/supplier_price_list")
    .withColumn("product_id", F.col("product_id").cast("int"))
    .withColumn("supplier_cost", F.col("supplier_cost").cast("decimal(10,2)"))
    .withColumn("effective_date", F.to_date("effective_date"))
    .dropDuplicates(["product_id"])
    .filter(F.col("product_id").isNotNull())
)

supplier.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("atliq.silver.supplier_price_list")

# COMMAND ----------

marketing = (
    spark.read.parquet(f"{BRONZE}/marketing_spend")
    .withColumn("spend_date", F.to_date("spend_date"))
    .withColumn("spend_amount", F.col("spend_amount").cast("decimal(12,2)"))
    .withColumn("clicks", F.col("clicks").cast("int"))
)

marketing.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("atliq.silver.marketing_spend")

# COMMAND ----------

orders_batch = spark.read.parquet(
    f"{BRONZE}/orders/ingest_date={run_date}"
)

w = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

orders_src = (
    orders_batch
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("order_amount", F.col("order_amount").cast("decimal(12,2)"))
    .filter(F.col("order_id").isNotNull())
)

(
    DeltaTable.forName(spark, "atliq.silver.orders")
    .alias("t")
    .merge(
        orders_src.alias("s"),
        "t.order_id = s.order_id"
    )
    .whenMatchedUpdateAll(
        condition="s.updated_at > t.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

(
    DeltaTable.forName(spark, "atliq.silver.orders")
    .alias("t")
    .merge(
        orders_src.alias("s"),
        "t.order_id = s.order_id"
    )
    .whenMatchedUpdateAll(
        condition="s.updated_at > t.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

payments_batch = spark.read.parquet(
    f"{BRONZE}/payments/ingest_date={run_date}"
)

w = Window.partitionBy("payment_id").orderBy(F.col("updated_at").desc())

payments_src = (
    payments_batch
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
    .filter(F.col("payment_id").isNotNull())
)

(
    DeltaTable.createIfNotExists(spark)
    .tableName("atliq.silver.payments")
    .addColumns(payments_src.schema)
    .execute()
)

(
    DeltaTable.forName(spark, "atliq.silver.payments")
    .alias("t")
    .merge(
        payments_src.alias("s"),
        "t.payment_id = s.payment_id"
    )
    .whenMatchedUpdateAll(
        condition="s.updated_at > t.updated_at"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

order_items_batch = spark.read.parquet(
    f"{BRONZE}/order_items/ingest_date={run_date}"
)

order_items_src = (
    order_items_batch
    .dropDuplicates(["order_item_id"])
    .filter(F.col("order_item_id").isNotNull())
)

(
    DeltaTable.createIfNotExists(spark)
    .tableName("atliq.silver.order_items")
    .addColumns(order_items_src.schema)
    .execute()
)

(
    DeltaTable.forName(spark, "atliq.silver.order_items")
    .alias("t")
    .merge(
        order_items_src.alias("s"),
        "t.order_item_id = s.order_item_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)
