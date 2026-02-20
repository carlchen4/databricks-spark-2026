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
# optimiza Control Table - done 
# batch needs to check if unique - done 
# backup needs to be deleted and manually trigger ? 
# do not show filed name in function  ? 
# start_time vs self start_time ? 
# organize the control table and reject table. - done
# mask raw record ?
# watermark and mode? 
# document all testcases 

import datetime
from datetime import datetime
import uuid
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, DoubleType
from typing import Tuple, Optional
from dataclasses import dataclass
from pyspark.sql.functions import xxhash64, bit_xor, count, col, when, lit, concat, substring, current_timestamp, to_json, struct, from_utc_timestamp, expr, regexp_extract
from zoneinfo import ZoneInfo

# Infrastructure tables are created via the Infrastructure class below

@dataclass
class BatchMetrics:
    run_id: str
    pipeline_name: str
    execution_mode: str
    environment: str
    processing_node: str
    source_system: str
    target_system: str
    watermark_from: str
    watermark_to: str
    data_date: str
    start_ts: datetime
    input_count: int  = 0
    target_count: int = 0
    reject_count: int = 0
    source_hash: str  = None
    target_hash: str  = None
    end_ts: datetime  = None
    run_error: str    = None

    @property
    def total_runtime_min(self) -> Optional[float]:
        if self.end_ts:
            return round((self.end_ts - self.start_ts).total_seconds() / 60, 2)
        return None

    @property
    def run_status(self) -> str:
        if self.run_error:
            return "ERROR"
        if self.reject_count > 0:
            return "FAILURE"
        return "SUCCESS" if self.end_ts else "RUNNING"


class Infrastructure:
    """Manages creation of all ETL infrastructure tables."""

    def __init__(self, source_table: str, target_table: str, control_table: str, reject_table: str):
        self.source_table  = source_table
        self.target_table  = target_table
        self.control_table = control_table
        self.reject_table  = reject_table

    def create_tables(self) -> None:
        """Create target, control, and reject tables if they do not exist."""

        # Derive target DDL dynamically from source schema + audit columns
        source_schema = spark.read.table(self.source_table).schema
        type_map = {
            "StringType()":    "STRING",
            "IntegerType()":   "INT",
            "LongType()":      "LONG",
            "DoubleType()":    "DOUBLE",
            "FloatType()":     "FLOAT",
            "BooleanType()":   "BOOLEAN",
            "DateType()":      "DATE",
            "TimestampType()": "TIMESTAMP",
        }
        source_cols_ddl = ""
        for field in source_schema.fields:
            spark_type = str(field.dataType)
            # Handle DecimalType(p,s) specially
            if spark_type.startswith("DecimalType"):
                precision, scale = field.dataType.precision, field.dataType.scale
                sql_type = f"DECIMAL({precision},{scale})"
            else:
                sql_type = type_map.get(spark_type, "STRING")
            source_cols_ddl += f"\n            {field.name:<20} {sql_type},"

        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.target_table} ({source_cols_ddl}
            -- ETL audit columns
            run_id               STRING,
            load_timestamp       TIMESTAMP
        ) USING DELTA PARTITIONED BY (transaction_date)""")

        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.control_table} (
            run_id            STRING,
            pipeline_name     STRING,
            execution_mode    STRING,
            environment       STRING,
            processing_node   STRING,
            source_system     STRING,
            target_system     STRING,
            watermark_from    STRING,
            watermark_to      STRING,
            data_date         STRING,
            start_ts          TIMESTAMP,
            total_runtime_min DOUBLE,
            end_ts            TIMESTAMP,
            run_status        STRING,
            input_count       LONG,
            target_count      LONG,
            reject_count      LONG,
            source_hash       STRING,
            target_hash       STRING
        ) USING DELTA""")

        # Migrate total_runtime_ms (LONG) → total_runtime_min (DOUBLE) if old schema exists
        control_cols = {f.name for f in spark.read.table(self.control_table).schema.fields}
        if "total_runtime_ms" in control_cols:
            spark.sql(f"ALTER TABLE {self.control_table} RENAME COLUMN total_runtime_ms TO total_runtime_min")
            spark.sql(f"ALTER TABLE {self.control_table} ALTER COLUMN total_runtime_min TYPE DOUBLE")
            print("🔄 Migrated control table: total_runtime_ms (LONG) → total_runtime_min (DOUBLE)")

        spark.sql(f"""CREATE TABLE IF NOT EXISTS {self.reject_table} (
            reject_id      STRING,
            run_id         STRING,
            created_ts     TIMESTAMP,
            business_key   STRING,
            error_category STRING,
            error_message  STRING,
            raw_record     STRING
        ) USING DELTA""")

        print(f"✅ Infrastructure ready.")


# ==========================================
# PII MASKER
# ==========================================

class PIIMasker:
    """
    Masks PII columns in a DataFrame before serialising to raw_record JSON.

    Supported strategies
    --------------------
    "redact"   – replace the entire value with "***"  (default)
    "partial"  – keep the last 4 characters, prefix with "***"  e.g. "***3456"
    "nullify"  – replace with null

    Usage
    -----
    masker = PIIMasker({"amount": "redact", "account_id": "partial"})
    # or shorthand for full-redact on every field:
    masker = PIIMasker(["amount", "account_id"])

    df_safe = masker.mask(df, exclude_cols=["validation_status", "error_reason"])
    """

    _STRATEGIES: dict = {
        "redact":  lambda c: lit("***"),
        "partial": lambda c: concat(lit("***"), substring(c.cast("string"), -4, 4)),
        "nullify": lambda c: lit(None).cast("string"),
    }

    def __init__(self, pii_fields=None):
        """
        pii_fields:
            dict  – {"column_name": "strategy"}  e.g. {"amount": "redact", "account_id": "partial"}
            list  – column names; all will use "redact"
            None  – pass-through, no masking applied
        """
        if pii_fields is None:
            self._rules: dict = {}
        elif isinstance(pii_fields, list):
            self._rules = {c: "redact" for c in pii_fields}
        else:
            self._rules = dict(pii_fields)

    def mask(self, df: DataFrame, exclude_cols: list = None) -> DataFrame:
        """
        Return a new DataFrame where:
          - columns in `exclude_cols` are dropped entirely
          - PII columns are masked and cast to STRING
          - all other columns are retained as-is

        Parameters
        ----------
        df           : source DataFrame
        exclude_cols : column names to drop before building the JSON snapshot
        """
        drop  = set(exclude_cols or [])
        exist = set(df.columns)
        exprs = []

        for field in df.schema.fields:
            name = field.name
            if name in drop:
                continue
            if name in self._rules and name in exist:
                strategy = self._rules[name]
                fn = self._STRATEGIES.get(strategy, self._STRATEGIES["redact"])
                exprs.append(fn(col(name)).alias(name))
            else:
                exprs.append(col(name))

        masked_df = df.select(*exprs)
        print(
            f"🔒 PIIMasker applied — masked: {[k for k in self._rules if k in exist]}, "
            f"dropped: {[c for c in drop if c in exist]}"
        )
        return masked_df

    def masked_struct_expr(self, df: DataFrame, exclude_cols: list = None):
        """
        Return a ``to_json(struct(...))`` Column expression where PII fields are
        masked and ETL-internal columns are excluded.

        Because the expressions are built directly from ``col(name)`` references
        that will be evaluated in the *same* DataFrame passed to ``.select()``,
        the masking is guaranteed to take effect.
        """
        drop  = set(exclude_cols or [])
        exist = set(df.columns)
        exprs = []

        for field in df.schema.fields:
            name = field.name
            if name in drop:
                continue
            strategy = self._rules.get(name)
            if strategy and name in exist:
                fn = self._STRATEGIES.get(strategy, self._STRATEGIES["redact"])
                exprs.append(fn(col(name)).alias(name))
            else:
                exprs.append(col(name))

        masked = [k for k in self._rules if k in exist and k not in drop]
        if masked:
            dropped = [c for c in drop if c in exist]
            print(f"🔒 PIIMasker — masking: {masked}, excluding: {dropped}")
        return to_json(struct(*exprs))


class ETL:
    def __init__(self, source_table: str, target_table: str, control_table: str, reject_table: str,
                 mask_pii: bool = False,
                 pii_fields: dict = None,
                 auto_restore: bool = True):
        self.ETL_VERSION = "1.0.0"
        self.source_table = source_table
        self.target_table = target_table
        self.control_table = control_table
        self.reject_table = reject_table
        self.current_version = None  # To track pre-write Delta version for potential rollback
        self.previous_version = None  # To track previous version for rollback
        self.auto_restore = auto_restore
        # PII masking: active when mask_pii=True, pass-through (no rules) when False
        if not mask_pii:
            self._pii_masker = PIIMasker()          # no rules → all fields written as-is
        else:
            effective_fields = pii_fields
            self._pii_masker = PIIMasker(effective_fields)
        status = "DISABLED — raw values will be logged" if not mask_pii else f"ENABLED — {list(self._pii_masker._rules.keys())}"
        print(f"🔒 PII masking: {status}")
        print(f"🔄 Auto-restore: {'ENABLED — pipeline will rollback automatically on failure' if auto_restore else 'DISABLED — manual restore command will be printed'}")
    
    def _restore_or_prompt(self, reason: str) -> None:
        """Restore target table to previous version, or print the manual command if auto_restore=False."""
        restore_sql = f"RESTORE TABLE {self.target_table} TO VERSION AS OF {self.previous_version}"
        if self.auto_restore:
            print(f"\n🔄 Auto-restoring {self.target_table} to version {self.previous_version} ({reason})...")
            spark.sql(restore_sql)
            print(f"✅ Successfully rolled back to Delta version {self.previous_version}")
        else:
            print(f"\n⚠️  Restore skipped (auto_restore=False). Reason: {reason}")
            print(f"📋  Run this command manually to roll back:")
            print(f"\n    {restore_sql}\n")

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
        ✅ Full Audit Trail.         ✅ Masked PII in Reject Logs

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
        
        # Exclude audit columns from target (run_id, load_timestamp)
        audit_columns = ['run_id', 'load_timestamp']
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
            self.log_rejects(metrics.run_id, df_rejected)
            self.log_control(metrics)
            return None   # signals run_etl to abort — do NOT return df_rejected
        else:
            print("✅ Schema validation passed - source and target schemas match")
            return source_df.withColumn("validation_status", lit("PASS")) \
                           .withColumn("error_reason", lit(None).cast("string"))

    # ETL-internal columns added during validation — excluded from the raw_record snapshot
    _ETL_COLS = ["validation_status", "error_reason"]

    def log_rejects(self, run_id: str, df_bad: DataFrame) -> None:
        """Write rejected records to reject table, masking PII in the raw_record snapshot."""
        # masked_struct_expr builds col() references evaluated against df_bad itself,
        # so masking is applied correctly within the single select() call.
        raw_record_expr = self._pii_masker.masked_struct_expr(df_bad, exclude_cols=self._ETL_COLS)

        df_rejects_log = df_bad.select(
            expr("uuid()").alias("reject_id"),
            lit(run_id).alias("run_id"),
            from_utc_timestamp(current_timestamp(), "America/Toronto").alias("created_ts"),
            col("transaction_id").alias("business_key"),
            regexp_extract(col("error_reason"), r"\[([^\]]+)\]", 1).alias("error_category"),
            col("error_reason").alias("error_message"),
            raw_record_expr.alias("raw_record")
        )
        df_rejects_log.write.format("delta").mode("append").saveAsTable(self.reject_table)
        print(f"📝 Logged {df_bad.count()} rejected records")
    
    def log_control(self, metrics: BatchMetrics) -> None:
        """Write batch execution metrics to control table."""
        control_entry = [(
            metrics.run_id,
            metrics.pipeline_name,
            metrics.execution_mode,
            metrics.environment,
            metrics.processing_node,
            metrics.source_system,
            metrics.target_system,
            metrics.watermark_from,
            metrics.watermark_to,
            metrics.data_date,
            metrics.start_ts,
            metrics.total_runtime_min,
            metrics.end_ts,
            metrics.run_status,
            metrics.input_count,
            metrics.target_count,
            metrics.reject_count,
            metrics.source_hash,
            metrics.target_hash,
        )]

        schema = StructType([
            StructField("run_id",           StringType(),    True),
            StructField("pipeline_name",    StringType(),    True),
            StructField("execution_mode",   StringType(),    True),
            StructField("environment",      StringType(),    True),
            StructField("processing_node",  StringType(),    True),
            StructField("source_system",    StringType(),    True),
            StructField("target_system",    StringType(),    True),
            StructField("watermark_from",   StringType(),    True),
            StructField("watermark_to",     StringType(),    True),
            StructField("data_date",        StringType(),    True),
            StructField("start_ts",         TimestampType(), True),
            StructField("total_runtime_min", DoubleType(),     True),
            StructField("end_ts",           TimestampType(), True),
            StructField("run_status",       StringType(),    True),
            StructField("input_count",      LongType(),      True),
            StructField("target_count",     LongType(),      True),
            StructField("reject_count",     LongType(),      True),
            StructField("source_hash",      StringType(),    True),
            StructField("target_hash",      StringType(),    True),
        ])

        spark.createDataFrame(control_entry, schema) \
             .write.format("delta").mode("append").saveAsTable(self.control_table)
        print(f"📊 Logged control entry: {metrics.run_status}")
    
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

    def write_to_target(self, run_id: str, df: DataFrame) -> None:
        """Write validated records to target table using Delta merge"""
        # Drop internal ETL columns and add audit columns only
        etl_cols = [c for c in ["validation_status", "error_reason"] if c in df.columns]
        df_final = df.drop(*etl_cols) \
            .withColumn("run_id", lit(run_id)) \
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

        print(f"\n🔍 Starting partition-level hash validation on column: {partition_column}")

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

            # ── Row-level diff ────────────────────────────────────────────────
            # The partition hash only tells us *which partition* is dirty.
            # We now drill down to find the *exact rows* that changed.
            #
            # Root-cause of false positives:
            #   Any withColumn(lit(numeric)) on the target can promote a
            #   DecimalType column to DoubleType.  When cast to string,
            #   Decimal preserves trailing zeros ("100.00") but Double
            #   drops them ("100.0"), so logically identical rows hash
            #   differently.  Fix: use SOURCE types for both sides and
            #   sort column names so ordering can never cause mismatches.

            # Canonical column set: intersection sorted alphabetically,
            # typed according to the source schema (ground truth).
            common_cols   = sorted(set(source_df.columns) & set(target_df.columns))
            src_type_map  = {f.name: f.dataType for f in source_df.schema.fields
                             if f.name in common_cols}

            def _row_hash_expr():
                """xxhash64 over common columns cast to source types then to string."""
                return xxhash64(*[
                    col(c).cast(src_type_map[c]).cast("string") for c in common_cols
                ])

            # Step 1: per-row hashes for dirty-partition rows on both sides
            #         — both use _row_hash_expr() so types are identical
            source_dirty_hashed = (
                source_df
                .filter(col(partition_column).isin(mismatched_partition_values))
                .withColumn("_row_hash", _row_hash_expr())
            )
            target_dirty_hashes = (
                target_df
                .filter(col(partition_column).isin(mismatched_partition_values))
                .withColumn("_row_hash", _row_hash_expr())
                .select("_row_hash")
                .distinct()
            )

            # Step 2: source rows absent from target dirty hashes → truly changed rows
            reject_row_hashes = (
                source_dirty_hashed
                .join(target_dirty_hashes, on="_row_hash", how="left_anti")
                .select("_row_hash")
                .distinct()
                .withColumn("_reject_flag", lit(True))
            )

            changed_count = reject_row_hashes.count()
            print(f"   ↳ {changed_count} row(s) actually differ from the target (row-level diff).")

            # Step 3: hash the full source with the same expression, left-join
            #         the small reject set, and guard with the partition filter
            #         to eliminate any theoretical hash-collision false positives.
            return (
                source_df
                .withColumn("_row_hash", _row_hash_expr())
                .join(reject_row_hashes, on="_row_hash", how="left")
                .withColumn(
                    "validation_status",
                    when(
                        (col("_reject_flag") == True) &
                        col(partition_column).isin(mismatched_partition_values),
                        "REJECT"
                    ).otherwise("PASS")
                )
                .withColumn(
                    "error_reason",
                    when(
                        (col("_reject_flag") == True) &
                        col(partition_column).isin(mismatched_partition_values),
                        concat(
                            lit("[HASH_MISMATCH] Partition "),
                            col(partition_column).cast("string"),
                            lit(" — row differs from target")
                        )
                    ).otherwise(lit(None).cast("string"))
                )
                .drop("_row_hash", "_reject_flag")
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
            self._restore_or_prompt(reason="count mismatch")
            return source_df.withColumn("validation_status", lit("REJECT")) \
                           .withColumn("error_reason", lit(error_message))


    def print_queries(self, run_id: str = None) -> None:
        """Print ready-to-run sample queries for the control and reject tables."""
        _run_filter = f"WHERE run_id = '{run_id}'" if run_id else "-- (no run_id filter applied)"
        print(f"""
  SAMPLE QUERIES
  {"=" * 60}

  -- 1. Latest batch status
  SELECT run_id, run_status, input_count, target_count, reject_count,
         total_runtime_min, start_ts, end_ts
  FROM   {self.control_table}
  ORDER BY start_ts DESC
  LIMIT 10;

  -- 2. All runs for a specific date
  SELECT *
  FROM   {self.control_table}
  WHERE  data_date = '2026-02-19'
  ORDER BY start_ts DESC;

  -- 3. Only failed / errored runs
  SELECT run_id, run_status, reject_count, start_ts
  FROM   {self.control_table}
  WHERE  run_status IN ('FAILURE', 'ERROR')
  ORDER BY start_ts DESC;

  -- 4. This run's control record
  SELECT *
  FROM   {self.control_table}
  {_run_filter};

  -- 5. All rejects for this run
  SELECT business_key, error_category, error_message, created_ts
  FROM   {self.reject_table}
  {_run_filter}
  ORDER BY created_ts;

  -- 6. Reject count by category (this run)
  SELECT error_category, COUNT(*) AS reject_count
  FROM   {self.reject_table}
  {_run_filter}
  GROUP BY error_category
  ORDER BY reject_count DESC;

  -- 7. Raw record snapshot for a specific rejected key
  SELECT business_key, error_message, raw_record
  FROM   {self.reject_table}
  WHERE  business_key = '<transaction_id>'
  {("AND run_id = '" + run_id + "'") if run_id else ""};

  -- 8. Hash mismatch runs (source ≠ target hash)
  SELECT run_id, source_hash, target_hash, start_ts
  FROM   {self.control_table}
  WHERE  source_hash != target_hash;
  {"=" * 60}""")

    def print_summary(self, run_id: str) -> None:
        """Print a human-readable summary of the batch run from the control and reject tables."""
        _ctrl = spark.read.table(self.control_table).filter(col("run_id") == run_id).collect()
        _rejs = spark.read.table(self.reject_table).filter(col("run_id") == run_id)

        if not _ctrl:
            print(f"⚠️  No control record found for run_id: {run_id}")
            return

        r = _ctrl[0]
        _status_icon = {"SUCCESS": "✅", "FAILURE": "❌", "ERROR": "🔴", "RUNNING": "🔄"}.get(r["run_status"], "❓")
        _hash_match  = (
            "✅ YES" if r["source_hash"] == r["target_hash"] and r["source_hash"] is not None
            else ("❌ NO" if r["source_hash"] is not None else "— N/A")
        )
        print(f"""
  ETL BATCH SUMMARY
  {"=" * 50}
  Run ID      : {r['run_id']}
  Status      : {_status_icon}  {r['run_status']}
  Environment : {r['environment']}
  Source      : {r['source_system']}
  Target      : {r['target_system']}

  TIMING
    Started   : {r['start_ts']}
    Ended     : {r['end_ts']}
    Duration  : {r['total_runtime_min']} min

  RECORD COUNTS
    Input     : {r['input_count']}
    Loaded    : {r['target_count']}
    Rejected  : {r['reject_count']}

  DATA INTEGRITY
    Hash match: {_hash_match}
  {"=" * 50}""")

        if r["reject_count"] and r["reject_count"] > 0:
            print("\n  REJECT BREAKDOWN BY CATEGORY:")
            _rejs.groupBy("error_category").count().orderBy("count", ascending=False).show(truncate=False)

    def run_etl(self, partition_column: str, environment: str = "PROD", watermark_from: str = None, watermark_to: str = None) -> str:
        """
        Execute the complete ETL pipeline with validation and error handling.
        This method orchestrates the end-to-end ETL process including data extraction,
        validation, transformation, and loading with comprehensive quality checks.
        
        Parameters:
            partition_column (str): Column name to use for partition-level hash validation
        
        Workflow:
            1. Generate unique run_id (UUID) and verify uniqueness in control/reject tables
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
            str: The run_id (UUID) for tracking this ETL run
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
        # Generate run_id and ensure it's unique
        run_id = str(uuid.uuid4())
        # Check if run_id already exists in control or reject tables
        try:
            existing_in_control = spark.read.table(self.control_table).filter(col("run_id") == run_id).count()
            existing_in_reject = spark.read.table(self.reject_table).filter(col("run_id") == run_id).count()
            
            # Regenerate if duplicate found (extremely rare with UUID)
            while existing_in_control > 0 or existing_in_reject > 0:
                print(f"⚠️ Duplicate run_id detected: {run_id}. Regenerating...")
                run_id = str(uuid.uuid4())
                existing_in_control = spark.read.table(self.control_table).filter(col("run_id") == run_id).count()
                existing_in_reject = spark.read.table(self.reject_table).filter(col("run_id") == run_id).count()
        except Exception as e:
            print(f"ℹ️ Could not check for duplicate run_id (tables may be empty): {e}")
        start_ts = datetime.now(ZoneInfo("America/Toronto"))
        self.previous_version = self.get_version()
        print(f"\n🔒 Starting ETL job: {run_id}")

        # Create metrics early so every exit path can log to the control table
        metrics = BatchMetrics(
            run_id          = run_id,
            pipeline_name   = "Bank Accounts ETL",
            execution_mode  = "BATCH",
            environment     = environment,
            processing_node = spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "unknown"),
            source_system   = self.source_table,
            target_system   = self.target_table,
            watermark_from  = watermark_from,
            watermark_to    = watermark_to,
            data_date       = datetime.now(ZoneInfo("America/Toronto")).strftime("%Y-%m-%d"),
            start_ts        = start_ts,
        )

        # Read source
        df_source = self.read_source()
        if df_source is None:
            metrics.end_ts   = datetime.now(ZoneInfo("America/Toronto"))
            metrics.run_error = "Source table not found or not accessible"
            self.log_control(metrics)
            return run_id

        df_target = spark.read.table(self.target_table)
        source_count = df_source.count()
        metrics.input_count = source_count

        # Validate schema
        df_schema_validated = self.check_schema(df_source, df_target, metrics)
        if df_schema_validated is None:
            return run_id  # End ETL if schema validation failed

        # Validate records
        df_validated = self.check_business_rules(df_schema_validated)

        # Check for duplicates
        df_checked = self.check_duplicates(df_validated)

        # Split good and bad records
        df_bad  = df_checked.filter(col("validation_status") == "REJECT")
        df_good = df_checked.filter(col("validation_status") == "PASS")
        reject_count  = df_bad.count()
        success_count = source_count - reject_count
        metrics.reject_count = reject_count

        # Decision logic
        if reject_count > 0:
            # FAILURE PATH
            print(f"❌ FAILURE: Found {reject_count} invalid records. Aborting write to Target.")
            self.log_rejects(run_id, df_bad)
            self.log_control(metrics)
            return run_id
        else:
            # SUCCESS PATH
            self.write_to_target(run_id, df_good)
            self.current_version = self.get_version()

            df_target_batch = spark.read.table(self.target_table).filter(col("run_id") == run_id)
            metrics.target_count = df_target_batch.count()

            # Count validation
            # df_source = df_source.union(df_source)
            df_check_count = self.check_count(df_source, df_target_batch)
            count_failure_count = df_check_count.filter(col("validation_status") == "REJECT").count()
            if count_failure_count > 0:
                metrics.reject_count = count_failure_count
                self.log_rejects(run_id, df_check_count.filter(col("validation_status") == "REJECT"))
                self.log_control(metrics)
                return run_id

            # Hash validation
            current_batch_target = df_target_batch.drop("load_timestamp", "run_id")
            # Testcases: Modify a row to test hash validation
            """   
            current_batch_target = current_batch_target.withColumn(
                "amount",
                when(col("transaction_id") == current_batch_target.first()["transaction_id"], lit(999.99))
                .otherwise(col("amount"))
            )"""

            df_hash_validated = self.check_hash(df_source, current_batch_target, partition_column)
            hash_failure_count = df_hash_validated.filter(col("validation_status") != "PASS").count()

            # Compute partition-level hashes for the control table
            source_hash = str(df_source
                .withColumn("_h", xxhash64(*[col(c).cast("string") for c in df_source.columns]))
                .agg({"_h": "sum"}).collect()[0][0])
            target_hash = str(current_batch_target
                .withColumn("_h", xxhash64(*[col(c).cast("string") for c in current_batch_target.columns]))
                .agg({"_h": "sum"}).collect()[0][0])
            metrics.source_hash = source_hash
            metrics.target_hash = target_hash

            if hash_failure_count > 0:
                metrics.reject_count = hash_failure_count
                self.log_rejects(run_id, df_hash_validated.filter(col("validation_status") == "REJECT"))
                print(f"❌ Hash validation failed for {hash_failure_count} records.")
                self._restore_or_prompt(reason="hash mismatch")
                self.log_control(metrics)
                return run_id

            metrics.end_ts = datetime.now(ZoneInfo("America/Toronto"))
            self.log_control(metrics)
            return run_id

# ==========================================
# 3. EXECUTE PIPELINE
# ==========================================
infra = Infrastructure(source_table, target_table, control_table, reject_table)
infra.create_tables()

# Define which fields to mask and how:
#   "redact"  → replaced with "***"         e.g. amount  : 594.01  → "***"
#   "partial" → last 4 chars, *** prefix    e.g. account_id: "ACCT-016730" → "***6730"
#   "nullify" → replaced with null
pii_fields = {
    "amount":     "redact",
    "account_id": "partial",
}

etl = ETL(source_table, target_table, control_table, reject_table,
          mask_pii     = False,      # set True to mask PII fields in raw_record
          pii_fields   = pii_fields,
          auto_restore = False)       # set False to print manual restore command instead of auto-rollback
last_run_id = etl.run_etl(
    partition_column  = "transaction_date",
    environment       = "PROD",
    watermark_from    = None,
    watermark_to      = None,
)

etl.print_summary(last_run_id)
etl.print_queries(last_run_id)