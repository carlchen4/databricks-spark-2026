# ============================================================
# UniversalDummyGenerator – Usage Examples
# ============================================================
# Run these examples inside a Databricks notebook or job.
# %pip install faker   ← run once on the cluster if needed
# ============================================================

from createdummy import UniversalDummyGenerator

# ------------------------------------------------------------
# Example 1: From an existing DataFrame
# ------------------------------------------------------------
# Suppose you already have a real DataFrame in memory.
existing_df = spark.createDataFrame(
    [
        (1,   "Alice",  "alice@example.com", 30),
        (2,   "Bob",    "bob@example.com",   25),
    ],
    schema="id INT, name STRING, email STRING, age INT"
)

gen = UniversalDummyGenerator(
    spark,
    df          = existing_df,      # source schema
    n_rows      = 500,
    primary_keys= "id",             # id column will be unique
    use_faker   = True,
    faker_locale= "en",             # or "fr"
)

dummy_df = gen.generate()
dummy_df.show(5)


# ------------------------------------------------------------
# Example 2: From an existing DataFrame – French locale
# ------------------------------------------------------------
gen_fr = UniversalDummyGenerator(
    spark,
    df          = existing_df,
    n_rows      = 200,
    primary_keys= "id",
    use_faker   = True,
    faker_locale= "fr",
)

dummy_fr = gen_fr.generate()
dummy_fr.show(5)


# ------------------------------------------------------------
# Example 3: From an existing DataFrame – prompted locale
# (leave out faker_locale and the user will be asked)
# ------------------------------------------------------------
gen_prompted = UniversalDummyGenerator(
    spark,
    df      = existing_df,
    n_rows  = 100,
    use_faker = True,               # will prompt: [en] / [fr]
)

dummy_prompted = gen_prompted.generate()
dummy_prompted.show(5)


# ------------------------------------------------------------
# Example 4: From an existing DataFrame – save result as table
# ------------------------------------------------------------
gen.save_as_table("my_catalog.my_schema.dummy_users")


# ------------------------------------------------------------
# Example 5: Export schema to Databricks Workspace
# ------------------------------------------------------------
gen.save_schema_to_databricks(
    path      = "/Workspace/Users/you@example.com/users_schema.json",
    overwrite = True
)
