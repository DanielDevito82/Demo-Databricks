# Databricks notebook source
# MAGIC %md
# MAGIC # Deklaration

# COMMAND ----------



# COMMAND ----------

catalog = "portfolio_analyse"
schema_to_extract = "bronze_reddit_deltatable"
schema_to_store = "silver_social_media_by_databricks"

# COMMAND ----------

# Definiere die Felder und Verarbeitung, die du auswählen möchtest zum Speichern für Posts
fields_posts = [
    "author",
    "author_premium",
    "author_is_blocked",
    "approved_by",
    "cast(approved_at_utc AS TIMESTAMP) AS approved_at_utc",
    "category",
    "cast(created AS TIMESTAMP) AS created",
    "cast(created_utc AS TIMESTAMP) AS created_utc",
    "cast(edited AS TIMESTAMP) AS edited",
    "fullname",
    "id",
    "mod_note",
    "mod_reason_by",
    "mod_reason_title",
    "mod_reports",
    "name",
    "num_comments",
    "num_crossposts",
    "num_reports",
    "over_18",
    "parent_whitelist_status",
    "removal_reason",
    "removed_by",
    "removed_by_category",
    "report_reasons",
    "score",
    "selftext",
    "shortlink",
    "subreddit",
    "subreddit_id",
    "subreddit_name_prefixed",
    "subreddit_subscribers",
    "subreddit_type",
    "suggested_sort",
    "title",
    "top_awarded_type",
    "total_awards_received",
    "url",
    "user_reports",
    "view_count",
    "visited",
    "whitelist_status"
]

# COMMAND ----------

# Definiere die Felder und Verarbeitung, die du auswählen möchtest zum Speichern für Comments
fields_comments = [
    "all_awardings",
    "cast(approved_at_utc AS TIMESTAMP) AS approved_at_utc",
    "approved_by",
    "author",
    "author_fullname",
    "author_is_blocked",
    "author_premium",
    "banned_at_utc",
    "banned_by",
    "body",
    "cast(created_utc AS TIMESTAMP) AS created_utc",
    "comment_type",
    "controversiality",
    "cast(created AS TIMESTAMP) AS created",
    "depth",
    "edited",
    "id",
    "likes",
    "link_id",
    "locked",
    "mod_note",
    "mod_reason_by",
    "mod_reason_title",
    "mod_reports",
    "name",
    "num_reports",
    "parent_id",
    "permalink",
    "report_reasons",
    "score",
    "subreddit",
    "subreddit_id",
    "subreddit_name_prefixed",
    "subreddit_type",
    "top_awarded_type",
    "total_awards_received",
    "user_reports"
]


# COMMAND ----------

# MAGIC %md
# MAGIC # Logik zum Zusammenführen aller *Posts Tabellen inkl. Verarbeitungslogik

# COMMAND ----------

# Lese alle Tabellen mit *posts im Namen zur Weiterverarbeitung
result_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema_to_extract} LIKE '*posts'")

# COMMAND ----------

# Extrahiere die Tabellennamen in eine Liste
table_names = result_df.select("tableName").rdd.flatMap(lambda x: x).collect()
print(table_names)  # Liste der Tabellennamen anzeigen

# COMMAND ----------

# Erstelle dynamisch den SQL UNION ALL Befehl
union_query = " UNION ".join([f" SELECT {', '.join(fields_posts)} FROM {catalog}.{schema_to_extract}.{table_name}" for table_name in table_names])
print(union_query)

# COMMAND ----------

# MAGIC %md
# MAGIC Prüfung auf ungleiche Spaltenanzahl in Tabelle

# COMMAND ----------

# Führe den dynamischen SQL-Befehl aus
result_union_df = spark.sql(union_query)

# Speichern des DataFrames als Delta-Tabelle
result_union_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema_to_store}.t_Reddit_Posts")

# COMMAND ----------

# MAGIC %md
# MAGIC # Logik zum Zusammenführen aller *Comments Tabellen inkl. Verarbeitungslogik

# COMMAND ----------

# Lese alle Tabellen mit *comments im Namen zur Weiterverarbeitung
result_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema_to_extract} LIKE '*comments'")

# COMMAND ----------

# Extrahiere die Tabellennamen in eine Liste
table_names = result_df.select("tableName").rdd.flatMap(lambda x: x).collect()

print(table_names)  # Liste der Tabellennamen anzeigen

# COMMAND ----------

# Erstelle dynamisch den SQL UNION ALL Befehl
union_query = " UNION ALL ".join([f" SELECT {', '.join(fields_comments)} FROM {catalog}.{schema_to_extract}.{table_name}" for table_name in table_names])

print(union_query)

# COMMAND ----------

# Führe den dynamischen SQL-Befehl aus
result_union_df = spark.sql(union_query)

# Speichern des DataFrames als Delta-Tabelle
result_union_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema_to_store}.t_Reddit_Comments")
