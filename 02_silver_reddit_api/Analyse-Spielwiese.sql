-- Databricks notebook source
SELECT 
  count(*)
FROM portfolio_analyse.default.t_reddit_posts2

-- COMMAND ----------

SELECT
  count(*)
FROM portfolio_analyse.silver_social_media.t_reddit_posts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql(union_query)
-- MAGIC display(df)

-- COMMAND ----------

SELECT 
  *
FROM portfolio_analyse.default.t_reddit_posts2
WHERE id = "1ekp3ej"

-- COMMAND ----------

SELECT 
  *
FROM portfolio_analyse.default.t_blocked_authors_in_posts

-- COMMAND ----------

SELECT 
 *
FROM portfolio_analyse.silver_social_media.t_reddit_posts
WHERE id = "1ekp3ej"

-- COMMAND ----------

SELECT 
 *
FROM portfolio_analyse.bronze_reddit_deltatable.t_subreddit_dataanalyst_posts
WHERE author = 'Vegetable-Cucumber26'

-- COMMAND ----------

UPDATE portfolio_analyse.bronze_reddit_deltatable.t_subreddit_dataanalyst_posts
 SET author_is_blocked = 'true'
WHERE author = 'Vegetable-Cucumber26'


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Definiere die Felder und Verarbeitung, die du auswählen möchtest zum Speichern für Posts
-- MAGIC fields_posts = [
-- MAGIC     # "author",
-- MAGIC     # "author_premium",
-- MAGIC     #  "author_is_blocked",
-- MAGIC     #  "approved_by",
-- MAGIC     # "cast(approved_at_utc AS TIMESTAMP) AS approved_at_utc",
-- MAGIC     #  "category",
-- MAGIC     # "cast(created AS TIMESTAMP) AS created",
-- MAGIC     # "cast(created_utc AS TIMESTAMP) AS created_utc",
-- MAGIC     # "cast(edited AS TIMESTAMP) AS edited",
-- MAGIC     # "fullname",
-- MAGIC     # "id",
-- MAGIC     # "mod_note",
-- MAGIC     # "mod_reason_by",
-- MAGIC     # "mod_reason_title",
-- MAGIC     # "mod_reports",
-- MAGIC     # "name",
-- MAGIC     # "num_comments",
-- MAGIC     # "num_crossposts",
-- MAGIC     # "num_reports",
-- MAGIC     # "over_18",
-- MAGIC     # "parent_whitelist_status",
-- MAGIC     # "removal_reason",
-- MAGIC     # "removed_by",
-- MAGIC     # "removed_by_category",
-- MAGIC     # "report_reasons",
-- MAGIC     # "score",
-- MAGIC     # "selftext",
-- MAGIC     # "shortlink",
-- MAGIC     # "subreddit",
-- MAGIC     # "subreddit_id",
-- MAGIC     # "subreddit_name_prefixed",
-- MAGIC     # "subreddit_subscribers",
-- MAGIC     # "subreddit_type",
-- MAGIC     # "suggested_sort",
-- MAGIC     # "title",
-- MAGIC     # "top_awarded_type",
-- MAGIC     # "total_awards_received",
-- MAGIC     # "url",
-- MAGIC     # "user_reports",
-- MAGIC     # "view_count",
-- MAGIC     # "visited",
-- MAGIC     # "whitelist_status"
-- MAGIC ]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = "portfolio_analyse"
-- MAGIC schema_to_extract = "bronze_reddit_deltatable"
-- MAGIC schema_to_store = "default"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Lese alle Tabellen mit *posts im Namen zur Weiterverarbeitung
-- MAGIC result_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema_to_extract} LIKE '*posts'")
-- MAGIC
-- MAGIC # Extrahiere die Tabellennamen in eine Liste
-- MAGIC table_names = [row['tableName'] for row in result_df.collect()]
-- MAGIC
-- MAGIC # Erstelle dynamisch den SQL UNION ALL Befehl
-- MAGIC union_query = " UNION ALL ".join([f" SELECT count(*) {', '.join(fields_posts)} FROM {catalog}.{schema_to_extract}.{table_name}" for table_name in table_names])
-- MAGIC print(union_query)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql(union_query)
-- MAGIC display(df)
