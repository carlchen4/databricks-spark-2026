from pyspark.sql.types import *
from pyspark.sql import functions as F
import json
import os

class UniversalDummyGenerator:
    """
    Generate a dummy Spark DataFrame that mirrors any table, DataFrame, or
    JSON schema -- with unique values on chosen primary-key columns.

    Quick start
    -----------
    # From an existing table
    gen = UniversalDummyGenerator(spark, table_name="catalog.db.my_table")

    # From an existing DataFrame
    gen = UniversalDummyGenerator(spark, df=existing_df)

    # From a saved schema JSON
    gen = UniversalDummyGenerator(spark, json_path="/Workspace/Shared/schema.json")

    # Generate rows
    dummy_df = gen.generate(n_rows=500)

    # Save schema to Databricks Workspace or DBFS
    gen.save_schema_to_databricks("/Workspace/Shared/schema.json")

    # Save dummy data as a Delta table
    gen.save_as_table("catalog.db.dummy_table")

    Parameters
    ----------
    spark        : SparkSession
    table_name   : str, optional  -- Databricks table (catalog.schema.table)
    df           : DataFrame, optional -- use schema of an existing DataFrame
    json_path    : str, optional  -- path to a previously saved schema JSON
    n_rows       : int, default 1000 -- number of dummy rows to generate
    path         : str, optional  -- default save path for schema JSON
    load_mode    : str, optional  -- "table" | "df" | "json" (auto-detected)
    primary_keys : str | list, optional -- column(s) that must be unique
                   - Numeric: sequential 0, 1, 2 ...
                   - String:  UUID
    """

    # ------------------------------------------------------------------ #
    # Source mode constants                                                #
    # ------------------------------------------------------------------ #
    LOAD_FROM_TABLE = "table"
    LOAD_FROM_DF    = "df"
    LOAD_FROM_JSON  = "json"

    # ------------------------------------------------------------------ #
    # Constructor                                                          #
    # ------------------------------------------------------------------ #
    def __init__(self, spark, table_name=None, df=None, json_path=None,
                 n_rows=1000, path=None, load_mode=None, primary_keys=None):

        self.spark        = spark
        self.n_rows       = n_rows
        self.path         = path
        self.primary_keys = (
            [primary_keys] if isinstance(primary_keys, str)
            else (primary_keys or [])
        )

        # Auto-detect load mode
        if load_mode is None:
            if table_name:
                load_mode = self.LOAD_FROM_TABLE
            elif df is not None:
                load_mode = self.LOAD_FROM_DF
            elif json_path:
                load_mode = self.LOAD_FROM_JSON
            else:
                raise ValueError(
                    "Provide one of: table_name, df, or json_path."
                )

        self.load_mode = load_mode

        # Load schema
        if self.load_mode == self.LOAD_FROM_TABLE:
            if not table_name:
                raise ValueError("load_mode='table' requires table_name.")
            self.schema = spark.table(table_name).schema

        elif self.load_mode == self.LOAD_FROM_DF:
            if df is None:
                raise ValueError("load_mode='df' requires df.")
            self.schema = df.schema

        elif self.load_mode == self.LOAD_FROM_JSON:
            if not json_path:
                raise ValueError("load_mode='json' requires json_path.")
            with open(json_path, "r") as f:
                self.schema = StructType.fromJson(json.load(f))

        else:
            raise ValueError(
                f"Invalid load_mode '{load_mode}'. "
                f"Choose from: 'table', 'df', 'json'."
            )

    # ------------------------------------------------------------------ #
    # Public methods                                                       #
    # ------------------------------------------------------------------ #

    def generate(self, n_rows=None):
        """
        Generate and return a dummy DataFrame.

        Parameters
        ----------
        n_rows : int, optional -- overrides the value set in the constructor.
        """
        n_rows = n_rows if n_rows is not None else self.n_rows

        # __row_index__ is a safe internal counter -- never clashes with
        # any real schema field name.
        df = self.spark.range(n_rows).withColumnRenamed("id", "__row_index__")

        for field in self.schema.fields:
            if field.name in self.primary_keys:
                col_expr = self._generate_pk_column(field.dataType)
            else:
                col_expr = self._generate_column(field.dataType)
            df = df.withColumn(field.name, col_expr)

        return df.select([f.name for f in self.schema.fields])

    def export_schema_json(self):
        """Return the schema as a formatted JSON string."""
        return json.dumps(self.schema.jsonValue(), indent=2)

    def save_schema_to_databricks(self, path=None, overwrite=True):
        """
        Save the schema JSON to Databricks.
        Automatically picks the right write method based on the path prefix:

          /Workspace/...  ->  Python open()     (Databricks Workspace)
          dbfs:/...       ->  dbutils.fs.put()  (DBFS / Unity Catalog Volumes)

        Parameters
        ----------
        path      : str  -- destination path (uses constructor value if omitted)
        overwrite : bool, default True
                    Set to False to raise FileExistsError if the file exists.

        Examples
        --------
        gen.save_schema_to_databricks("/Workspace/Shared/schema.json")
        gen.save_schema_to_databricks("dbfs:/FileStore/schema.json")
        gen.save_schema_to_databricks("dbfs:/Volumes/catalog/schema/vol/schema.json")
        """
        from pyspark.dbutils import DBUtils

        path = path or self.path
        if not path:
            raise ValueError(
                "path is required. Pass it as an argument or set it "
                "in the constructor (path=...)."
            )

        content = self.export_schema_json()

        if path.startswith("/Workspace/"):
            # Workspace path: use Python open()
            os.makedirs(os.path.dirname(path), exist_ok=True)
            file_exists = os.path.exists(path)
            if file_exists and not overwrite:
                raise FileExistsError(
                    f"File already exists: '{path}'. "
                    "Use overwrite=True to replace it."
                )
            with open(path, "w") as f:
                f.write(content)

        else:
            # DBFS / UC Volume: use dbutils.fs.put()
            dbutils = DBUtils(self.spark)
            try:
                dbutils.fs.ls(path)
                file_exists = True
            except Exception:
                file_exists = False

            if file_exists and not overwrite:
                raise FileExistsError(
                    f"File already exists: '{path}'. "
                    "Use overwrite=True to replace it."
                )
            dbutils.fs.put(path, content, overwrite=overwrite)

        status = "overwritten" if file_exists else "created"
        print(f"Schema saved to {path} ({status})")

    def save_as_table(self, table_name, n_rows=None, mode="overwrite"):
        """
        Generate dummy data and save it as a Delta table.

        Parameters
        ----------
        table_name : str  -- fully-qualified table name (catalog.schema.table)
        n_rows     : int, optional -- overrides the constructor value
        mode       : str, default "overwrite" -- Spark write mode

        Returns
        -------
        DataFrame -- the generated dummy DataFrame
        """
        df = self.generate(n_rows)
        df.write.format("delta").mode(mode).saveAsTable(table_name)
        return df

    # ------------------------------------------------------------------ #
    # Private helpers                                                      #
    # ------------------------------------------------------------------ #

    def _generate_pk_column(self, dtype):
        """Unique values for primary-key columns."""
        if isinstance(dtype, StringType):
            return F.expr("uuid()")
        # Numeric: sequential __row_index__ (0, 1, 2 ...) cast to target type
        return F.col("__row_index__").cast(dtype)

    def _generate_column(self, dtype):
        """Random values for non-PK columns, covering all Databricks types."""

        # Numeric
        if isinstance(dtype, ByteType):
            return (F.rand() * 127).cast("byte")
        if isinstance(dtype, ShortType):
            return (F.rand() * 32767).cast("short")
        if isinstance(dtype, IntegerType):
            return (F.rand() * 100000).cast("int")
        if isinstance(dtype, LongType):
            return (F.rand() * 1_000_000_000).cast("long")
        if isinstance(dtype, FloatType):
            return (F.rand() * 10000).cast("float")
        if isinstance(dtype, DoubleType):
            return (F.rand() * 100000).cast("double")
        if isinstance(dtype, DecimalType):
            precision = dtype.precision or 10
            scale     = dtype.scale or 2
            return (F.rand() * 10 ** (precision - scale)).cast(dtype)

        # String / binary
        if isinstance(dtype, StringType):
            return F.concat(F.lit("dummy_"), F.monotonically_increasing_id())
        if isinstance(dtype, BinaryType):
            return F.encode(
                F.concat(F.lit("bin_"), F.monotonically_increasing_id()),
                "utf-8"
            )

        # Boolean
        if isinstance(dtype, BooleanType):
            return (F.rand() > 0.5)

        # Date / time
        if isinstance(dtype, DateType):
            return (F.current_date() - (F.rand() * 1095).cast("int"))
        if isinstance(dtype, TimestampType):
            return F.current_timestamp()
        if isinstance(dtype, TimestampNTZType):
            return F.current_timestamp().cast("timestamp_ntz")

        # Complex
        if isinstance(dtype, ArrayType):
            elem = self._generate_column(dtype.elementType)
            return F.array(elem, elem, elem)
        if isinstance(dtype, MapType):
            return F.create_map(
                self._generate_column(dtype.keyType),
                self._generate_column(dtype.valueType)
            )
        if isinstance(dtype, StructType):
            return F.struct(*[
                self._generate_column(f.dataType).alias(f.name)
                for f in dtype.fields
            ])

        # Fallback
        return F.lit(None)
