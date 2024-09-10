# Databricks notebook source
# Importieren Sie die dbutils Bibliothek
dbutils.widgets.text("SetCatalogForExtraction", "portfolio_analyse")
dbutils.widgets.text("SetSchemaForExtraction", "bronze_reddit_deltatable")
dbutils.widgets.text("SetRetention_hours", "160")

# COMMAND ----------

# Lesen Sie die Parameter
catalog = dbutils.widgets.get("SetCatalogForExtraction")
# Verwenden Sie die Parameter im Notebook
print(f"Parameter 1: {catalog}")

# Lesen Sie die Parameter
schema_to_extract = dbutils.widgets.get("SetSchemaForExtraction")
# Verwenden Sie die Parameter im Notebook
print(f"Parameter 2: {schema_to_extract}")

# Lesen Sie die Parameter
retention_hours = dbutils.widgets.get("SetRetention_hours")
# Verwenden Sie die Parameter im Notebook
print(f"Parameter 3: {retention_hours}")

# COMMAND ----------

retention_hours=int(retention_hours)

# COMMAND ----------

# Lese alle Tabellen mit *posts im Namen zur Weiterverarbeitung
result_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema_to_extract}")
display(result_df)

# COMMAND ----------

# Extrahiere die Tabellennamen in eine Liste
table_names = result_df.select("tableName").rdd.flatMap(lambda x: x).collect()
print(table_names)  # Liste der Tabellennamen anzeigen

# COMMAND ----------

# Setzen Sie die Konfiguration, um die Retentionsperiodenprüfung zu deaktivieren
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

for table_name in table_names:
    full_table_name = f"{catalog}.{schema_to_extract}.{table_name}"
    spark.sql(f"VACUUM {full_table_name} RETAIN {retention_hours} HOURS")

