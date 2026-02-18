# Databricks notebook source
# MAGIC %md
# MAGIC one time load for schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import date

from pyspark.sql.functions import (
    col, lit, current_timestamp, sha2, concat_ws, 
    to_json, struct, when, count
)
import uuid
import time

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS dbrtry.default.bank_accounts_target;

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS dbrtry.default.bank_accounts_target (
#     transaction_id STRING,
#     account_id STRING,
#     transaction_date DATE,
#     posting_date DATE,
#     amount DECIMAL(18, 2),
#     description STRING,
#     -- Audit Columns
#     batch_id STRING,
#     load_timestamp TIMESTAMP
# )
# USING DELTA;

# COMMAND ----------

# Define Table Names
source_table  = "dbrtry.default.bank_accounts_source"
target_table  = "dbrtry.default.bank_accounts_target"
control_table = "dbrtry.default.ETL_BATCH_CONTROL"
reject_table  = "dbrtry.default.ETL_BATCH_REJECTS"


# COMMAND ----------

df_source = spark.sql(f"select * from {source_table}")
df_source.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from dbrtry.default.bank_accounts_target

# COMMAND ----------

# MAGIC %md
# MAGIC Use Json, to change dummy data e.g. amount to None or -10.00 or 0
# MAGIC
# MAGIC Example:
# MAGIC TXN-9998: Had "amount": 0.0 → REJECTED
# MAGIC TXN-9999: Had "amount": 0.0 → REJECTED

# COMMAND ----------

from pyspark.sql.functions import sha2, concat_ws

first_row = df_source.limit(1)
hashed = first_row.withColumn("sha256_hash", sha2(concat_ws("||", *first_row.columns), 256))
display(hashed.select(*first_row.columns, "sha256_hash"))

# COMMAND ----------

# MAGIC %md
# MAGIC have to truncate if same input data 

# COMMAND ----------

# spark.sql("TRUNCATE TABLE dbrtry.default.bank_accounts_target")

# COMMAND ----------

# MAGIC %md
# MAGIC Display the df is only okay for dev not prod 

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC select * from dbrtry.default.bank_accounts_target
# MAGIC order by transaction_id desc

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

df_source = spark.read.table(source_table)
df_source.count()

# COMMAND ----------

# DBTITLE 1,Cell 12
# ==========================================
# 1. SETUP & INFRASTRUCTURE
# ==========================================

from pyspark.sql import DataFrame
from typing import Tuple, Optional
from dataclasses import dataclass

# Ensure Infrastructure Tables Exist (DDL)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {target_table} (
    transaction_id STRING, account_id STRING, transaction_date DATE, amount DECIMAL(18,2), 
    batch_id STRING, load_timestamp TIMESTAMP) USING DELTA PARTITIONED BY (transaction_date)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table} (
    Batch_ID STRING, Dataset STRING, Status STRING, Count_Summary STRING, Hash_Status STRING, Run_Time STRING) USING DELTA""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {reject_table} (
    Batch_ID STRING, Dataset STRING, Business_Key STRING, Error_Reason STRING, Raw_Record STRING, Timestamp TIMESTAMP) USING DELTA""")

print(f"✅ Infrastructure ready. Reading from: {source_table}")

# ==========================================
# 2. ETL CLASS WITH ORGANIZED FUNCTIONS
# ==========================================

@dataclass
class BatchMetrics:
    batch_id: str
    source_count: int
    reject_count: int
    success_count: int
    start_time: str
    end_time: str = None

class StrictETL:
    def __init__(self, source_table: str, target_table: str, control_table: str, reject_table: str):
        self.source_table = source_table
        self.target_table = target_table
        self.control_table = control_table
        self.reject_table = reject_table
        
    def read_source(self) -> Optional[DataFrame]:
        """Read data from source table"""
        try:
            df_source = spark.read.table(self.source_table)
            source_count = df_source.count()
            print(f"📖 Read {source_count} records from source")
            return df_source
        except Exception as e:
            print(f"❌ Error reading source: {e}")
            return None
    
    def add_row_hash(self, df: DataFrame) -> DataFrame:
        """Add data fingerprint hash to each row"""
        return df.withColumn(
            "row_hash", 
            sha2(concat_ws("||", col("transaction_id"), col("amount"), col("transaction_date")), 256)
        )
    
    def validate_records(self, df: DataFrame) -> DataFrame:
        """Apply validation rules and mark records as PASS/REJECT"""
        df_validated = df.withColumn(
            "validation_status",
            when((col("amount").isNull()) | (col("amount") <= 0), "REJECT")
            .when(col("transaction_id").isNull(), "REJECT")
            .otherwise("PASS")
        ).withColumn(
            "error_reason",
            when((col("amount").isNull()) | (col("amount") <= 0), "[CRITICAL] Invalid Amount")
            .when(col("transaction_id").isNull(), "[MISSING_KEY] No Transaction ID")
            .otherwise(None)
        )
        return df_validated
    
    def check_duplicates(self, df: DataFrame) -> DataFrame:
        """Check for duplicate transaction IDs in target table"""
        df_good = df.filter(col("validation_status") == "PASS")
        
        try:
            existing_target = spark.read.table(self.target_table)
            df_duplicates = df_good.join(
                existing_target.select("transaction_id"),
                on="transaction_id",
                how="inner"
            )
            duplicate_count = df_duplicates.count()
            
            if duplicate_count > 0:
                print(f" ❌ ERROR: Found {duplicate_count} duplicate transaction_ids in target table")
                
                duplicate_ids = df_duplicates.select("transaction_id").distinct()
                df = df.withColumn(
                    "validation_status",
                    when(col("transaction_id").isin([row.transaction_id for row in duplicate_ids.collect()]), "REJECT")
                    .otherwise(col("validation_status"))
                ).withColumn(
                    "error_reason",
                    when(
                        col("transaction_id").isin([row.transaction_id for row in duplicate_ids.collect()]) & col("error_reason").isNull(),
                        "[DUPLICATE] Transaction ID already exists in target"
                    ).otherwise(col("error_reason"))
                )
        except Exception as e:
            print(f"ℹ️ Target table empty or not accessible (first load): {e}")
        
        return df
    
    def log_rejects(self, batch_id: str, df_bad: DataFrame) -> None:
        """Write rejected records to reject table"""
        df_rejects_log = df_bad.select(
            lit(batch_id).alias("Batch_ID"),
            lit("bank_accounts").alias("Dataset"),
            col("transaction_id").alias("Business_Key"),
            col("error_reason").alias("Error_Reason"),
            to_json(struct("*")).alias("Raw_Record"),
            current_timestamp().alias("Timestamp")
        )
        df_rejects_log.write.format("delta").mode("append").saveAsTable(self.reject_table)
        print(f"📝 Logged {df_bad.count()} rejected records")
    
    def log_control(self, metrics: BatchMetrics, status: str, hash_status: str) -> None:
        """Write batch execution status to control table"""
        if metrics.end_time:
            run_time = f"{metrics.start_time} - {metrics.end_time}"
        else:
            run_time = f"{metrics.start_time} - FAIL"
        
        count_summary = f"{metrics.source_count} / {metrics.success_count} / {metrics.reject_count}"
        
        control_entry = [(
            metrics.batch_id,
            "bank_accounts",
            status,
            count_summary,
            hash_status,
            run_time
        )]
        
        spark.createDataFrame(
            control_entry,
            ["Batch_ID", "Dataset", "Status", "Count_Summary", "Hash_Status", "Run_Time"]
        ).write.format("delta").mode("append").saveAsTable(self.control_table)
        
        print(f"📊 Logged control entry: {status}")
    
    def write_to_target(self, batch_id: str, df: DataFrame) -> None:
        """Write validated records to target table using Delta merge"""
        df_final = df.drop("validation_status", "error_reason", "row_hash") \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("load_timestamp", current_timestamp()) \
            .withColumn("transaction_date", col("transaction_date").cast("date")) \
            .withColumn("amount", col("amount").cast("decimal(18,2)")) \
            .dropDuplicates(["transaction_id"])
        
        delta_table = DeltaTable.forName(spark, self.target_table)
        (
            delta_table.alias("t")
            .merge(df_final.alias("s"), "t.transaction_id = s.transaction_id")
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute()
        )
        
        print(f"✍️ Merged {df.count()} records to target")
    
    def run_strict_etl(self) -> str:
        """Execute the complete ETL pipeline"""
        # Initialize batch context
        batch_id = str(uuid.uuid4())
        start_time = time.time()
        start_ts_str = time.strftime("%H:%M:%S", time.localtime(start_time))
        print(f"\n🔒 Starting Batch: {batch_id}")
        
        # Step 1: Read source
        df_source = self.read_source()
        if df_source is None:
            return batch_id
        
        source_count = df_source.count()
        
        # Step 2: Add hashing
        df_hashed = self.add_row_hash(df_source)
        
        # Step 3: Validate records
        df_validated = self.validate_records(df_hashed)
        
        # Step 4: Check for duplicates
        df_checked = self.check_duplicates(df_validated)
        
        # Step 5: Split good and bad records
        df_bad = df_checked.filter(col("validation_status") == "REJECT")
        df_good = df_checked.filter(col("validation_status") == "PASS")
        reject_count = df_bad.count()
        success_count = source_count - reject_count
        
        # Create metrics object
        metrics = BatchMetrics(
            batch_id=batch_id,
            source_count=source_count,
            reject_count=reject_count,
            success_count=0,
            start_time=start_ts_str
        )
        
        # Step 6: Decision logic
        if reject_count > 0:
            # FAILURE PATH
            print(f"❌ FAILURE: Found {reject_count} invalid records. Aborting write to Target.")
            
            self.log_rejects(batch_id, df_bad)
            self.log_control(metrics, "FAILURE", "MISMATCH")
            
            return batch_id
        else:
            # SUCCESS PATH
            print(f"✅ SUCCESS: Validation Passed. Writing {source_count} records to Target.")
            
            self.write_to_target(batch_id, df_good)
            
            metrics.success_count = success_count
            metrics.end_time = time.strftime("%H:%M:%S", time.localtime(time.time()))
            self.log_control(metrics, "SUCCESS", "MATCH")
            
            return batch_id

# ==========================================
# 3. EXECUTE PIPELINE
# ==========================================

etl = StrictETL(source_table, target_table, control_table, reject_table)
last_batch_id = etl.run_strict_etl()

# ==========================================
# 4. PRINT RESULTS
# ==========================================
print("\n" + "="*50)
print(f"RESULTS FOR BATCH: {last_batch_id}")
print("="*50)

print("\n--- 1. CONTROL TABLE (Latest Status) ---")
spark.read.table(control_table).filter(col("Batch_ID") == last_batch_id).show(truncate=False)

print("\n--- 2. REJECT TABLE (Why it failed) ---")
spark.read.table(reject_table).filter(col("Batch_ID") == last_batch_id).show(truncate=False)

# COMMAND ----------

print(last_batch_id)

# COMMAND ----------

spark.sql(f"""
SELECT * FROM dbrtry.default.ETL_BATCH_REJECTS
WHERE Batch_ID = '{last_batch_id}'
""").display()

# COMMAND ----------

spark.sql(f"""
SELECT * FROM dbrtry.default.ETL_BATCH_CONTROL
WHERE Batch_ID = '{last_batch_id}'
""").display()

# COMMAND ----------

df_control = spark.read.table(control_table).filter(col("Batch_ID") == last_batch_id)
failed_count = df_control.filter(col("Status") == "FAILURE").count()

# 2. The Kill Switch
if failed_count > 0:
    # A. Optional: Print details for the logs before dying
    print(f"❌ CRITICAL: Found {failed_count} failure records in Control Table.")
    
    # B. Get the error details to show in the exception message
    # (Optional: grab the specific error reason from the Reject table for the error message)
    error_msg = f"ETL Job Aborted. {failed_count} batches failed validation. Check ETL_BATCH_REJECTS for details."
    
    # C. RAISE EXCEPTION -> This turns the Job RED
    raise Exception(error_msg)

else:
    print("✅ Job Verification Passed. No failure flags found.")

# COMMAND ----------

# MAGIC %md
# MAGIC show the taget table as of this batch id 

# COMMAND ----------

spark.sql(f"""
SELECT count(*) FROM dbrtry.default.bank_accounts_target 
WHERE batch_id = '{last_batch_id}'
""").display()

# COMMAND ----------

