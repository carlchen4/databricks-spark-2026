# ==========================================
# 1. SETUP & INFRASTRUCTURE
# ==========================================

# ???? read target instead of df
# get current version - done 
# use function to check acount source_count = df_source.count() - done 
# summary current table and previous - remove backup - like manual flag? 
# check shema of source and target match before hash check - done 
# run time is not toronto time  - done 
# * (optional) validaion key attribute 
# optimiza Control Table
# batch needs to check if unique - done 
# backup needs to be deleted and manually trigger ? 
# do not show filed name in function  ? 
# start_time vs self start_time
# organize the control table and reject table. 
# mask raw record 


import datetime
from datetime import datetime
import uuid
from pyspark.sql import DataFrame
from typing import Tuple, Optional
from dataclasses import dataclass
from pyspark.sql.functions import xxhash64, bit_xor, count, col, when, lit, concat, current_timestamp, to_json, struct, from_utc_timestamp
from zoneinfo import ZoneInfo

# Ensure Infrastructure Tables Exist (DDL)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {target_table} (
    transaction_id STRING, account_id STRING, transaction_date DATE, amount DECIMAL(18,2), 
    batch_id STRING, load_timestamp TIMESTAMP) USING DELTA PARTITIONED BY (transaction_date)""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {control_table} (
    Batch_ID STRING, Dataset STRING, Status STRING, Count_Summary STRING, Hash_Status STRING, Run_Time STRING) USING DELTA""")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {reject_table} (
    Batch_ID STRING, Dataset STRING, Business_Key STRING, Error_Reason STRING, Raw_Record STRING, Timestamp TIMESTAMP) USING DELTA""")

@dataclass
class BatchMetrics:
    batch_id: str
    source_count: int
    reject_count: int
    success_count: int
    start_time: str
    end_time: str = None

class ETL:
    def __init__(self, source_table: str, target_table: str, control_table: str, reject_table: str):
        self.ETL_VERSION = "1.0.0"
        self.source_table = source_table
        self.target_table = target_table
        self.control_table = control_table
        self.reject_table = reject_table
        self.current_version = None  # To track pre-write Delta version for potential rollback
        self.previous_version = None  # To track previous version for rollback
    
    def print_welcome(self):
        """Print welcome banner for the ETL utility"""
        print(f"""
        \033[1;34m
        *******************************************************************
                                                                          
                    🏦   Bank Accounts ETL Pipeline                      
                          Version {self.ETL_VERSION}                              
                                                                       
        *******************************************************************
        \033[0m

        A robust ETL utility for processing bank transactions with built-in 
        data quality checks and validation.

        \033[1mFeatures:\033[0m
        ✅ Schema Validation         ✅ Business Rules Validation
        ✅ Duplicate Detection       ✅ Count Validation
        ✅ Hash Validation           ✅ Automatic Rollback on Failure
        ✅ Full Audit Trail

        \033[1mAuthor:\033[0m Carl Chen
        \033[1mDate:\033[0m   2026-02-19
        """)
        
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

    
    def check_business_rules(self, df: DataFrame) -> DataFrame:
        """Apply validation rules and mark records as PASS/REJECT"""
        print("\n🔍 Starting business rules validation...")
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
        # Check if all records passed validation
        failed_count = df_validated.filter(col("validation_status") == "REJECT").count()
        if failed_count == 0:
            print("✅ Business rules validation passed - all records are valid")
        else:
            print(f"❌ Business rules validation found {failed_count} invalid records")

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
    


    def check_schema(self, source_df: DataFrame, target_df: DataFrame, metrics: BatchMetrics) -> Optional[DataFrame]:
        """
        Validate that source and target DataFrames have matching schemas.
        If schemas do not match, log the failure and return None.
        
        Parameters:
            source_df (DataFrame): Source dataset
            target_df (DataFrame): Target dataset
            metrics (BatchMetrics): The metrics object for the current batch
        
        Returns:
            Optional[DataFrame]: Source DataFrame with validation columns if schema is valid, otherwise None
        """
        print("\n🔍 Starting schema validation...")
        
        # Get column names and types
        source_schema = {field.name: str(field.dataType) for field in source_df.schema.fields}
        target_schema = {field.name: str(field.dataType) for field in target_df.schema.fields}
        
        # Exclude audit columns from target (batch_id, load_timestamp)
        audit_columns = ['batch_id', 'load_timestamp']
        target_schema_filtered = {k: v for k, v in target_schema.items() if k not in audit_columns}
        
        # Check if column names match
        source_columns = set(source_schema.keys())
        target_columns = set(target_schema_filtered.keys())
        
        schema_errors = []
        
        if source_columns != target_columns:
            missing_in_target = source_columns - target_columns
            missing_in_source = target_columns - source_columns
            
            if missing_in_target:
                error_msg = f"Columns in source but not in target: {missing_in_target}"
                print(f"❌ {error_msg}")
                schema_errors.append(error_msg)
            if missing_in_source:
                error_msg = f"Columns in target but not in source: {missing_in_source}"
                print(f"❌ {error_msg}")
                schema_errors.append(error_msg)

        # Check if data types match
        type_mismatches = []
        for col_name in source_columns.intersection(target_columns):
            source_type = source_schema[col_name]
            target_type = target_schema_filtered[col_name]
            
            if source_type != target_type:
                mismatch = f"{col_name}: source({source_type}) vs target({target_type})"
                type_mismatches.append(mismatch)
                print(f"❌ Data type mismatch: {mismatch}")
        
        if type_mismatches:
            schema_errors.extend(type_mismatches)
        
        # If errors, log and return None
        if schema_errors:
            error_message = "[SCHEMA_MISMATCH] " + "; ".join(schema_errors)
            print(f"❌ Schema validation failed. Aborting write to Target.")
            
            # Create a DataFrame with the error to log to rejects
            df_rejected = source_df.withColumn("validation_status", lit("REJECT")) \
                                   .withColumn("error_reason", lit(error_message))
            
            metrics.reject_count = source_df.count()
            self.log_rejects(metrics.batch_id, df_rejected)
            self.log_control(metrics, "FAILURE", "SCHEMA_MISMATCH")
            return df_rejected
        else:
            print("✅ Schema validation passed - source and target schemas match")
            return source_df.withColumn("validation_status", lit("PASS")) \
                           .withColumn("error_reason", lit(None).cast("string"))

    def log_rejects(self, batch_id: str, df_bad: DataFrame) -> None:
        """Write rejected records to reject table"""
        df_rejects_log = df_bad.select(
            lit(batch_id).alias("Batch_ID"),
            lit("bank_accounts").alias("Dataset"),
            col("transaction_id").alias("Business_Key"),
            col("error_reason").alias("Error_Reason"),
            to_json(struct("*")).alias("Raw_Record"),
            from_utc_timestamp(current_timestamp(), "America/Toronto").alias("Timestamp")
        )
        df_rejects_log.write.format("delta").mode("append").saveAsTable(self.reject_table)
        print(f"📝 Logged {df_bad.count()} rejected records")
    
    def log_control(self, metrics: BatchMetrics, status: str, hash_status: str) -> None:
        """Write batch execution status to control table"""
        toronto_now = datetime.now(ZoneInfo("America/Toronto"))
        if metrics.end_time:
            run_time = f"{toronto_now.date()} {metrics.start_time} - {metrics.end_time}"
        else:
            run_time = f"{toronto_now.date()} {metrics.start_time} - FAIL"
        
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
    
    def get_version(self) -> Optional[int]:
        """Capture the current Delta table version before writing (for rollback reference)"""
        try:
            delta_table = DeltaTable.forName(spark, self.target_table)
            history_df = delta_table.history(1)
            current_version = history_df.select("version").collect()[0][0]
            print(f"📌 Pre-write Delta version: {current_version}")
            return current_version
        except Exception as e:
            print(f"ℹ️ Could not retrieve Delta version (first load): {e}")
            return None

    def write_to_target(self, batch_id: str, df: DataFrame) -> None:
        """Write validated records to target table using Delta merge"""
        # Drop internal ETL columns and add audit columns only
        etl_cols = [c for c in ["validation_status", "error_reason"] if c in df.columns]
        df_final = df.drop(*etl_cols) \
            .withColumn("batch_id", lit(batch_id)) \
            .withColumn("load_timestamp", from_utc_timestamp(current_timestamp(), "America/Toronto")) \
        
        
        delta_table = DeltaTable.forName(spark, self.target_table)
        (
            delta_table.alias("t")
            .merge(df_final.alias("s"), "t.transaction_id = s.transaction_id")
            .whenNotMatchedInsertAll()
            .whenMatchedUpdateAll()
            .execute()
        )
        
        print(f"✍️ Merged {df.count()} records to target")



    def check_hash(
        self,
        source_df: DataFrame,
        target_df: DataFrame,
        partition_column: str
    ) -> DataFrame:
        """
        Validate two DataFrames by partition and return DataFrame with validation status.

        Parameters:
            source_df (DataFrame): Source dataset (e.g. CSV)
            target_df (DataFrame): Target dataset (e.g. Delta)
            partition_column (str): Partition column name
        
        Returns:
            DataFrame: Source DataFrame with validation_status and error_reason columns
        """

        print(f"\n🔍 Starting partition-level validation on column: {partition_column}")

        # Step 1: Aggregate source dataset using all columns
        source_partition_agg = (
            source_df
            .withColumn("row_hash", xxhash64(*[col(c).cast("string") for c in source_df.columns]))
            .groupBy(partition_column)
            .agg(
                bit_xor("row_hash").alias("source_partition_hash"),
                count("*").alias("source_row_count")
            )
        )

        # Step 2: Aggregate target dataset using all columns
        target_partition_agg = (
            target_df
            .withColumn("row_hash", xxhash64(*[col(c).cast("string") for c in target_df.columns]))
            .groupBy(partition_column)
            .agg(
                bit_xor("row_hash").alias("target_partition_hash"),
                count("*").alias("target_row_count")
            )
        )

        # Step 3: Join on partition column
        comparison_result = (
            source_partition_agg
            .join(
                target_partition_agg,
                on=partition_column,
                how="full_outer"
            )
        )

        # Step 4: Detect mismatches
        mismatched_partitions = comparison_result.filter(
            (col("source_partition_hash") != col("target_partition_hash")) |
            (col("source_row_count") != col("target_row_count")) |
            col("source_partition_hash").isNull() |
            col("target_partition_hash").isNull()
        ).select(partition_column) 

        mismatch_count = mismatched_partitions.count()

        if mismatch_count == 0:
            print("✅ All partitions matched successfully.")
            return source_df.withColumn("validation_status", lit("PASS")) \
                            .withColumn("error_reason", lit(None).cast("string"))
        else:
            print(f"❌ Detected {mismatch_count} mismatched partitions.")
            # Display mismatched partitions for debugging
            print("\n--- Mismatched Partitions Detail ---")
            comparison_result.filter(
                (col("source_partition_hash") != col("target_partition_hash")) |
                (col("source_row_count") != col("target_row_count")) |
                col("source_partition_hash").isNull() |
                col("target_partition_hash").isNull()
            ).show(truncate=False)

            

            # Collect mismatched partition values
            mismatched_partition_values = [row[partition_column] for row in mismatched_partitions.collect()]

            # Return source DataFrame with validation status (similar to duplicate check pattern)
            return source_df.withColumn(
                "validation_status",
                when(col(partition_column).isin(mismatched_partition_values), "REJECT")
                .otherwise("PASS")
            ).withColumn(
                "error_reason",
                when(
                    col(partition_column).isin(mismatched_partition_values),
                    concat(lit("[HASH_MISMATCH] Partition "), col(partition_column).cast("string"), lit(" does not match target"))
                ).otherwise(lit(None).cast("string"))
            )



    def check_count(self, source_df: DataFrame, target_df: DataFrame) -> DataFrame:
        """
        Validate that source and target DataFrames have matching record counts.
        Returns source DataFrame with validation_status and error_reason columns.
        
        Parameters:
            source_df (DataFrame): Source dataset
            target_df (DataFrame): Target dataset (filtered to current batch)
        
        Returns:
            DataFrame: Source DataFrame with validation_status and error_reason columns
        """
        print("\n🔍 Starting count validation...")
        
        source_count = source_df.count()
        target_count = target_df.count()
        
        print(f"📊 Source count: {source_count}")
        print(f"📊 Target count: {target_count}")
        
        if source_count == target_count:
            print("✅ Count validation passed - source and target counts match")
            return source_df.withColumn("validation_status", lit("PASS")) \
                           .withColumn("error_reason", lit(None).cast("string"))
        else:
            error_message = f"[COUNT_MISMATCH] Source count ({source_count}) does not match target count ({target_count})"
            print(f"❌ {error_message}")
            # target_table is table name; self.target_table is the DataFrame read from that table
            print(f"Attempting rollback to previous Delta version {target_table} {self.previous_version}")
            spark.sql(f"RESTORE TABLE {target_table} TO VERSION AS OF {self.previous_version}")
            print(f"🔄 Successfully rolled back to Delta version {self.previous_version}")
            return source_df.withColumn("validation_status", lit("REJECT")) \
                           .withColumn("error_reason", lit(error_message))


    def run_etl(self, partition_column: str) -> str:
        """
        Execute the complete ETL pipeline with validation and error handling.
        This method orchestrates the end-to-end ETL process including data extraction,
        validation, transformation, and loading with comprehensive quality checks.
        
        Parameters:
            partition_column (str): Column name to use for partition-level hash validation
        
        Workflow:
            1. Generate unique batch_id (UUID) and verify uniqueness in control/reject tables
            2. Read source data and perform record count
            3. Validate schema compatibility with target table
            4. Apply business rules validation
            5. Check for duplicate records
            6. Split records into valid (PASS) and invalid (REJECT) sets
            7. Execute decision logic:
               - If rejects found: Log to reject table, mark batch as FAILURE
               - If no rejects: Write to target, perform post-write validations:
                 * Count validation (source vs target)
                 * Hash validation (data integrity check)
                 * Rollback to previous Delta version if validation fails
            8. Log batch metrics to control table
        Returns:
            str: The batch_id (UUID) for tracking this ETL run
        Side Effects:
            - Writes valid records to target table (if validation passes)
            - Logs rejected records to reject table (if validation fails)
            - Logs batch metrics to control table
            - May perform Delta table rollback if post-write validation fails
            - Prints status messages and validation results to console
        Raises:
            Exception: Propagates exceptions from read/write operations or table access
        Notes:
            - Uses Toronto timezone (America/Toronto) for timestamps
            - Implements Delta Lake versioning for rollback capability
            - Validates data at multiple checkpoints (schema, business rules, duplicates, count, hash)
            - Batch is marked as FAILURE if any validation fails
        """
        """Execute the complete ETL pipeline"""
        # Initialize batch context
        self.print_welcome()
        print(f"✅ Infrastructure ready. Reading from: {source_table}")
        # Generate batch_id and ensure it's unique
        batch_id = str(uuid.uuid4())
        # Check if batch_id already exists in control or reject tables
        try:
            existing_in_control = spark.read.table(self.control_table).filter(col("Batch_ID") == batch_id).count()
            existing_in_reject = spark.read.table(self.reject_table).filter(col("Batch_ID") == batch_id).count()
            
            # Regenerate if duplicate found (extremely rare with UUID)
            while existing_in_control > 0 or existing_in_reject > 0:
                print(f"⚠️ Duplicate batch_id detected: {batch_id}. Regenerating...")
                batch_id = str(uuid.uuid4())
                existing_in_control = spark.read.table(self.control_table).filter(col("Batch_ID") == batch_id).count()
                existing_in_reject = spark.read.table(self.reject_table).filter(col("Batch_ID") == batch_id).count()
        except Exception as e:
            print(f"ℹ️ Could not check for duplicate batch_id (tables may be empty): {e}")
        start_time = datetime.now(ZoneInfo("America/Toronto"))
        start_ts_str = start_time.strftime("%H:%M:%S")
        self.previous_version = self.get_version()
        print(f"\n🔒 Starting Batch: {batch_id}")
        
        # Read source
        df_source = self.read_source()
        df_target = spark.read.table(self.target_table)
        if df_source is None:
            return batch_id
        
        source_count = df_source.count()
        
        # Create metrics object for logging
        metrics = BatchMetrics(
            batch_id=batch_id,
            source_count=source_count,
            reject_count=0,
            success_count=0,
            start_time=start_ts_str
        )

        # Validate schema
        df_schema_validated = self.check_schema(df_source, df_target, metrics)
        if df_schema_validated is None:
            return batch_id # End ETL if schema validation failed
        
        # Validate records
        df_validated = self.check_business_rules(df_schema_validated)
        
        # Check for duplicates
        df_checked = self.check_duplicates(df_validated)
        
        # Split good and bad records
        df_bad = df_checked.filter(col("validation_status") == "REJECT")
        df_good = df_checked.filter(col("validation_status") == "PASS")
        reject_count = df_bad.count()
        success_count = source_count - reject_count
        
        # Update metrics with reject count
        metrics.reject_count = reject_count
        
        # Step 6: Decision logic
        if reject_count > 0:
            # FAILURE PATH
            print(f"❌ FAILURE: Found {reject_count} invalid records. Aborting write to Target.")     
            self.log_rejects(batch_id, df_bad)
            self.log_control(metrics, "FAILURE", "MISMATCH")
            
            return batch_id
        else:
            # SUCCESS PATH
            self.write_to_target(batch_id, df_good)
            # print current version after write
            self.current_version = self.get_version()

            self.target_table = spark.read.table(self.target_table).filter(col("batch_id") == batch_id)
            # check count of target after write

            # test count check function  
            # df_source = df_source.union(df_source)
            df_check_count = self.check_count(df_source, self.target_table)
            

            # Check if count validation failed
            count_failure_count = df_check_count.filter(col("validation_status") == "REJECT").count()
            if count_failure_count > 0:
                metrics.reject_count = count_failure_count  # ADD THIS LINE
                self.log_rejects(batch_id, df_check_count.filter(col("validation_status") == "REJECT"))
                self.log_control(metrics, "FAILURE", "COUNT_MISMATCH")
                return batch_id
            
            # check hash of source and target match - if not rollback to pre version
            current_batch_target = self.target_table
            current_batch_target = current_batch_target.drop("load_timestamp", "batch_id") 
            df_hash_validated = self.check_hash(df_source, current_batch_target, partition_column)
            hash_failure_count = df_hash_validated.filter(col("validation_status") != "PASS").count()
            if hash_failure_count > 0:
                metrics.reject_count = hash_failure_count  # ADD THIS LINE
                self.log_rejects(batch_id, current_batch_target)
                self.log_control(metrics, "FAILURE", "MISMATCH")
                print(f"❌ Hash validation failed for {hash_failure_count} records. Rolling back to version {pre_version}")        
                # Display source and target data for comparison
                print("\n--- Source Data (df_source) ---")
                df_source.show()
                print("\n--- Current Batch Target Data (current_batch_target) ---")
                current_batch_target.show()
                print("\n--- Hash Validation Results ---")
                df_hash_validated.show()
                spark.sql(f"RESTORE TABLE {self.target_table} TO VERSION AS OF {pre_version}")
                spark.sql(f"""
                          RESTORE TABLE {self.target_table}
                          TO VERSION AS OF {pre_version} """)
                print(f"🔄 Successfully rolled back to Delta version {pre_version}")
                    
            metrics.success_count = success_count
            end_time = datetime.now(ZoneInfo("America/Toronto"))
            metrics.end_time = end_time.strftime("%H:%M:%S")
            self.log_control(metrics, "SUCCESS", "MATCH")
            
            return batch_id

# ==========================================
# 3. EXECUTE PIPELINE
# ==========================================
# test schema target_table_test 
etl = ETL(source_table, target_table, control_table, reject_table)
last_batch_id = etl.run_etl(partition_column="transaction_date")

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