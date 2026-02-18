# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

new_data_count = 5
source_table_name = "dbrtry.default.bank_accounts_source"
seq_control_table = "dbrtry.default.id_sequence_control"

# COMMAND ----------

# spark.sql(f"TRUNCATE TABLE {source_table_name}")

# COMMAND ----------

last_id = (
    spark.table(seq_control_table)
    .filter("table_name = 'bank_accounts_source'")
    .select("last_id")
    .collect()[0].last_id
)

print(f"Last ID: {last_id}")

# select * from dbrtry.default.id_sequence_control

# COMMAND ----------

# MAGIC %md
# MAGIC need to ensure the schema is same as souce table 

# COMMAND ----------

# DBTITLE 1,Untitled
df_new = (
    spark.range(1, new_data_count + 1)
    .withColumn(
        "transaction_id",
        F.concat(F.lit("TXN-"), F.lit(last_id) + F.col("id"))
    ).withColumn(
        "account_id",
        F.concat(
            F.lit("ACCT-"),
            F.lpad((F.rand()*100000).cast("int"), 6, "0")
        )
    )
    .withColumn("transaction_date", F.current_date())
    .withColumn("posting_date", F.date_add(F.current_date(), 1))
    .withColumn(
        "amount",
        F.round(F.rand() * 1000, 2).cast("decimal(18,2)")
    )
    .withColumn("description", F.lit("Daily Dummy Load"))
    .drop("id")
)


# COMMAND ----------

df_new.display()

# COMMAND ----------

# overwrite
df_new.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(source_table_name)

print(f"✅ Table overwritten with {new_data_count} fresh daily records.")
display(df_new)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dbrtry.default.bank_accounts_source
# MAGIC order by transaction_id

# COMMAND ----------

new_last_id = last_id + new_data_count

spark.sql(f"""
UPDATE {seq_control_table}
SET last_id = {new_last_id}
WHERE table_name = 'bank_accounts_source'
""")


# COMMAND ----------

# DBTITLE 1,Cell 12
spark.sql(f"SELECT * FROM {seq_control_table}").display()