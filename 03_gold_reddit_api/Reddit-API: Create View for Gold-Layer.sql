-- Databricks notebook source
SELECT 
 link_id,
 body
FROM portfolio_analyse.silver_social_media.t_reddit_comments
WHERE link_id = "t3_1edrq0b"

-- COMMAND ----------

SELECT 
 id,
 shortlink,
 CONCAT('t3_', id) AS _KEY_LINK_POSTS_COMMENTS
FROM portfolio_analyse.silver_social_media.t_reddit_posts
WHERE id = "1edrq0b"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Lesen der Spaltennamen aus tabelle1 und tabelle2
-- MAGIC posts_columns = spark.table("portfolio_analyse.silver_social_media.t_reddit_posts").columns
-- MAGIC comments_columns = spark.table("portfolio_analyse.silver_social_media.t_reddit_comments").columns
-- MAGIC
-- MAGIC # Erstellen der dynamischen SELECT-Klausel
-- MAGIC select_clause = []
-- MAGIC
-- MAGIC for col in posts_columns:
-- MAGIC     select_clause.append(f"posts.{col} AS posts_{col}")
-- MAGIC
-- MAGIC for col in comments_columns:
-- MAGIC     select_clause.append(f"comments.{col} AS comments_{col}")
-- MAGIC
-- MAGIC select_clause_str = ", ".join(select_clause)
-- MAGIC print(select_clause_str)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Erstellen der vollständigen SQL-Abfrage
-- MAGIC sql_query = f"""
-- MAGIC CREATE VIEW IF NOT EXISTS portfolio_analyse.gold_social_media_by_databricks.v_reddit_posts_comments AS
-- MAGIC SELECT {select_clause_str}
-- MAGIC FROM portfolio_analyse.silver_social_media.t_reddit_posts AS posts
-- MAGIC LEFT JOIN portfolio_analyse.silver_social_media.t_reddit_comments AS comments
-- MAGIC ON comments.link_id = CONCAT('t3_', posts.id)
-- MAGIC """

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC try:
-- MAGIC     # Ausführen der dynamischen SQL-Abfrage
-- MAGIC     spark.sql(sql_query)
-- MAGIC     print("View successfully created.")
-- MAGIC except Exception as e:
-- MAGIC     print(f"An error occurred: {e}")

-- COMMAND ----------

-- CREATE VIEW portfolio_analyse.gold_social_media_by_databricks.v_reddit_posts_comments AS
-- SELECT 
--   posts.id AS posts_id,
--   posts.title AS posts_title,
--   posts.author AS posts_author,
--   posts.shortlink AS posts_shortlink,
--   comments_body,
--   comments_author,
--   comments_id,
--   comments_link_id
-- FROM 
-- portfolio_analyse.silver_social_media.t_reddit_posts AS posts
-- LEFT JOIN 
-- (SELECT 
--   comments.body AS comments_body,
--   comments.author AS comments_author,
--   comments.id AS comments_id,
--   comments.link_id AS comments_link_id
-- FROM 
-- portfolio_analyse.silver_social_media.t_reddit_comments AS comments) 
-- ON CONCAT('t3_', posts.id) = comments_link_id

-- COMMAND ----------

SELECT count(*)
FROM portfolio_analyse.gold_social_media_by_databricks.v_reddit_posts_comments

-- COMMAND ----------

SELECT * 
FROM portfolio_analyse.gold_social_media_by_databricks.v_reddit_posts_comments
WHERE posts_id = "1edrq0b"
