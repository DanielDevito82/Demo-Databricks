-- Databricks notebook source
SELECT 
 shortlink 
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_posts
LIMIT 10

-- COMMAND ----------

SELECT 
  count(*)
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations

-- COMMAND ----------

SELECT 
  id,
  shortlink,
  context_with_conversation,
  portfolio_analyse.functions.analyze_for_software_product_opinion(context_with_conversation) AS software_product_opinion
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations
LIMIT 100


-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = "SELECT DISTINCT shortlink FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations"

-- COMMAND ----------

SELECT 
  DISTINCT shortlink, 
  count(shortlink) 
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations 
GROUP BY shortlink

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark_df_shortlinks = spark.sql(query)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC shortlinks = [row.shortlink for row in spark_df_shortlinks.collect()]
-- MAGIC shortlinks

-- COMMAND ----------

CREATE OR REPLACE TABLE portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations_software_product_opinion
AS
SELECT 
  id,
  shortlink,
  context_with_conversation,
  portfolio_analyse.functions.analyze_for_software_product_opinion(context_with_conversation) AS software_product_opinion
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations
LIMIT 10

-- COMMAND ----------



-- COMMAND ----------

   CREATE TABLE IF NOT EXISTS portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations_software_product_opinion (
     id STRING,
     shortlink STRING,
     context_with_conversation STRING,
     software_product_opinion STRING
   );

-- COMMAND ----------

-- Neue Datensätze anhängen
INSERT INTO portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations_software_product_opinion
SELECT 
  id,
  shortlink,
  context_with_conversation,
  portfolio_analyse.functions.analyze_for_software_product_opinion(context_with_conversation) AS software_product_opinion
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations src
LEFT JOIN portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations_software_product_opinion tgt
ON src.id = tgt.id
WHERE tgt.id IS NULL;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for shortlink_value in shortlinks: 
-- MAGIC   query = f"""INSERT INTO portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations_software_product_opinion
-- MAGIC   SELECT 
-- MAGIC     id,
-- MAGIC     shortlink,
-- MAGIC     context_with_conversation,
-- MAGIC     portfolio_analyse.functions.analyze_for_software_product_opinion(context_with_conversation) AS software_product_opinion
-- MAGIC   FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations
-- MAGIC   WHERE shortlink = '{shortlink_value}';"""
-- MAGIC   print(query)
-- MAGIC
-- MAGIC   # spark.sql(query)
