-- Databricks notebook source
CREATE OR REPLACE TABLE portfolio_analyse.silver_social_media_by_databricks.t_reddit_posts_ai
AS
SELECT 
  *,
  portfolio_analyse.functions.analyze_for_software_product_opionion(selftext) AS sentiment
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_posts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df =  spark.sql(""
-- MAGIC SELECT
-- MAGIC  link_id,
-- MAGIC  id,
-- MAGIC  parent_id,
-- MAGIC  subreddit_id,
-- MAGIC  permalink
-- MAGIC FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_comments
-- MAGIC WHERE parent_id = 't3_1f25xuj'"
-- MAGIC )

-- COMMAND ----------

SELECT
    portfolio_analyse.functions.analyze_for_software_product_opionion("I don't like Databricks. Is Snowflake better?") AS sentiment

-- COMMAND ----------

ALTER TABLE portfolio_analyse.silver_social_media_by_dbt.t_reddit_comments ALTER COLUMN id SET NOT NULL

-- COMMAND ----------

ALTER TABLE portfolio_analyse.silver_social_media_by_dbt.t_reddit_comments ADD CONSTRAINT KEY_COMMENTS PRIMARY KEY(id)

-- COMMAND ----------

ALTER TABLE portfolio_analyse.silver_social_media_by_dbt.t_reddit_comments DROP CONSTRAINT PRIMARY KEY

-- COMMAND ----------

ALTER TABLE portfolio_analyse.silver_social_media_by_dbt.t_reddit_posts ALTER COLUMN id SET NOT NULL

-- COMMAND ----------

ALTER TABLE portfolio_analyse.silver_social_media_by_dbt.t_reddit_posts ADD CONSTRAINT KEY_POSTS PRIMARY KEY(id)

-- COMMAND ----------

SELECT
  count(id)
FROM portfolio_analyse.silver_social_media_by_dbt.t_reddit_comments
WHERE id is NULL

-- COMMAND ----------

SELECT 
  gildings
  FROM
portfolio_analyse.bronze_reddit_deltatable.t_subreddit_bigdata_posts

-- COMMAND ----------

SELECT 
  count(*)
FROM portfolio_analyse.default.t_reddit_posts

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
