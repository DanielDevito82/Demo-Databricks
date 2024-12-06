-- Databricks notebook source
-- SELECT 
--  id,
--  CONCAT ('Subreddit: ', subreddit, ' \nAuthor of the post: ', author, '\Title of the post: ', title, '\nText of the post: ', selftext) AS context
-- FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_posts
-- WHERE id = '1f25xuj'


-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql import SparkSession
-- MAGIC from pyspark.sql.functions import col, lit, concat, collect_list, concat_ws, udf
-- MAGIC from pyspark.sql.types import StringType, StructType, StructField
-- MAGIC import pyspark.sql.functions as F

-- COMMAND ----------

-- SELECT
--   id,
--   author,
--   parent_id,
--   depth,
--   body
-- FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_comments
-- WHERE link_id = 't3_1f25xuj'

-- COMMAND ----------

SELECT 
  *
FROM portfolio_analyse.bronze_reddit_deltatable.t_userprofiles as userprofiles
WHERE name = 'iOsiris'


-- COMMAND ----------

SELECT 
  comments.author,
  userprofiles.name,
  userprofiles.total_karma
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_comments AS comments  
  LEFT JOIN portfolio_analyse.bronze_reddit_deltatable.t_userprofiles AS userprofiles
ON userprofiles.name = comments.author
WHERE comments.link_id = 't3_1f45qzp'

-- COMMAND ----------

SELECT
  id,
  author,
  parent_id,
  depth,
  body,
  link_id
FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_comments AS comments
WHERE author = 'AndrewLucksFlipPhone' AND link_id = 't3_1f45qzp'
LEFT JOIN portfolio_analyse.bronze_reddit_deltatable.t_userprofiles as userprofiles
ON userprofiles.name = comments.author

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """
-- MAGIC SELECT
-- MAGIC   comments.id,
-- MAGIC   comments.author,
-- MAGIC   comments.parent_id,
-- MAGIC   comments.depth,
-- MAGIC   comments.body,
-- MAGIC   comments.link_id,
-- MAGIC   userprofiles.total_karma,
-- MAGIC   userprofiles.link_karma,
-- MAGIC   userprofiles.comment_karma,
-- MAGIC   userprofiles.is_gold
-- MAGIC FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_comments AS comments
-- MAGIC LEFT JOIN portfolio_analyse.bronze_reddit_deltatable.t_userprofiles AS userprofiles
-- MAGIC ON userprofiles.name = comments.author
-- MAGIC """

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df =  spark.sql(query)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC schau nach der ID in parent_id "t1_id", wenn da dann nehme den Author und erstelle Context als String. Speichere den String und suche wieder ob es eine Antwort auf den Context gibt über die ID in parent:id, falls ja dann ergänze den String.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Kombiniere 'author', 'total_karma', 'link_karma', 'comment_karma', 'is_gold und 'body' zu einem neuen Feld 'author_body'
-- MAGIC df = df.withColumn("author_body", concat(
-- MAGIC lit("Author of the comment: "), col("author"),
-- MAGIC lit("\nTotal karma of the author: "), col("total_karma"),
-- MAGIC lit("\nPost karma of the author: "), col("link_karma"),
-- MAGIC lit("\nComment karma of the author: "), col("comment_karma"),
-- MAGIC lit("\nGold status of the author: "), col("is_gold"),
-- MAGIC lit("\nComment of the author: "), col("body"), lit("\n")))
-- MAGIC
-- MAGIC # Erstelle ein Dictionary, um die Kommentare nach id zu speichern
-- MAGIC comments_dict = {row['id']: row['author_body'] for row in df.collect()}      

-- COMMAND ----------

-- MAGIC %python
-- MAGIC comments_dict

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Erstelle ein Dictionary, um die Kommentare nach parent_id zu speichern
-- MAGIC parent_dict = {}
-- MAGIC for row in df.collect():
-- MAGIC     parent_id = row['parent_id']
-- MAGIC     if parent_id not in parent_dict:
-- MAGIC         parent_dict[parent_id] = []
-- MAGIC     parent_dict[parent_id].append(row['id'])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Rekursive Funktion, um die Konversation aufzubauen
-- MAGIC def get_conversation(parent_id):
-- MAGIC     conversation = []
-- MAGIC     if parent_id in parent_dict:
-- MAGIC         for child_id in parent_dict[parent_id]:
-- MAGIC             if comments_dict.get(child_id):
-- MAGIC                 conversation.append(comments_dict[child_id])
-- MAGIC                 conversation.extend(get_conversation('t1_' + child_id))
-- MAGIC     return conversation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Starte die Konversation mit allen root parent_ids
-- MAGIC conversations = []
-- MAGIC
-- MAGIC # Überprüfe alle Kommentare auf der obersten Ebene (depth = 0)
-- MAGIC for row in df.filter(col("depth") == 0).collect():
-- MAGIC     if comments_dict.get(row['id']):
-- MAGIC         parent_id = 't1_' + row['id']
-- MAGIC         conversation = [comments_dict[row['id']]]
-- MAGIC         conversation.extend(get_conversation(parent_id))
-- MAGIC  
-- MAGIC         conversation_None = [c for c in conversation if c is None]
-- MAGIC         print(conversation_None)
-- MAGIC         # Filtere NoneType-Elemente heraus    
-- MAGIC         conversation = [c for c in conversation if c is not None]
-- MAGIC         conversations.append((row['link_id'], ' '.join(conversation)))
-- MAGIC
-- MAGIC # Erstelle ein Schema für die neue Tabelle
-- MAGIC schema = StructType([
-- MAGIC     StructField("link_id", StringType(), True),
-- MAGIC     StructField("conversation_string", StringType(), True)
-- MAGIC ])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC conversations

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Erstelle ein Schema für die neue Tabelle
-- MAGIC schema = StructType([
-- MAGIC     StructField("link_id", StringType(), True),
-- MAGIC     StructField("conversation_string", StringType(), True)
-- MAGIC ])
-- MAGIC
-- MAGIC # Erstelle einen DataFrame aus der Liste der Konversationen
-- MAGIC conversation_df = spark.createDataFrame(conversations, schema)
-- MAGIC
-- MAGIC # Speichere den DataFrame in einer neuen Tabelle
-- MAGIC conversation_df.write.mode("overwrite").saveAsTable("portfolio_analyse.silver_social_media_by_databricks.t_reddit_conversations")
-- MAGIC
-- MAGIC # Zeige die gespeicherten Konversationen an
-- MAGIC conversation_df.show(truncate=False)

-- COMMAND ----------

SELECT 
    posts.id,
    posts.shortlink,
    CONCAT(
        'Subreddit: ', posts.subreddit, 
        ' \nAuthor of the post: ', posts.author,
        '\nTotal karma of the author: ',  userprofiles.total_karma,
        '\nPost karma of the author: ', userprofiles.link_karma,
        '\nComment karma of the author: ', userprofiles.comment_karma,
        '\nGold status of the author: ', userprofiles.is_gold,      
        '\nTitle of the post: ', posts.title, 
        '\nText of the post: ', posts.selftext,
        '\n--- Conversation ---\n', 
        conversations.conversation_string
    ) AS context_with_conversation
FROM 
    portfolio_analyse.silver_social_media_by_databricks.t_reddit_posts AS posts
LEFT JOIN portfolio_analyse.bronze_reddit_deltatable.t_userprofiles AS userprofiles
ON userprofiles.name = posts.author  
JOIN 
    portfolio_analyse.silver_social_media_by_databricks.t_reddit_conversations AS conversations
ON 
    posts.id = SUBSTRING(conversations.link_id, 4, LENGTH(conversations.link_id) - 3) AND posts.id = '1fq82wq'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC query = """
-- MAGIC SELECT 
-- MAGIC     posts.id,
-- MAGIC     posts.shortlink,
-- MAGIC     CONCAT(
-- MAGIC         'Subreddit: ', posts.subreddit, 
-- MAGIC         ' \nAuthor of the post: ', posts.author,
-- MAGIC         '\nTotal karma of the author: ',  userprofiles.total_karma,
-- MAGIC         '\nPost karma of the author: ', userprofiles.link_karma,
-- MAGIC         '\nComment karma of the author: ', userprofiles.comment_karma,
-- MAGIC         '\nGold status of the author: ', userprofiles.is_gold,      
-- MAGIC         '\nTitle of the post: ', posts.title, 
-- MAGIC         '\nText of the post: ', posts.selftext,
-- MAGIC         '\n--- Conversation ---\n', 
-- MAGIC         conversations.conversation_string
-- MAGIC     ) AS context_with_conversation
-- MAGIC FROM 
-- MAGIC     portfolio_analyse.silver_social_media_by_databricks.t_reddit_posts AS posts
-- MAGIC LEFT JOIN portfolio_analyse.bronze_reddit_deltatable.t_userprofiles AS userprofiles
-- MAGIC ON userprofiles.name = posts.author  
-- MAGIC JOIN 
-- MAGIC     portfolio_analyse.silver_social_media_by_databricks.t_reddit_conversations AS conversations
-- MAGIC ON 
-- MAGIC     posts.id = SUBSTRING(conversations.link_id, 4, LENGTH(conversations.link_id) - 3)
-- MAGIC """

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # DataFrame durch Ausführen der SQL-Abfrage erstellen
-- MAGIC result_df = spark.sql(query)
-- MAGIC
-- MAGIC # Speichere den DataFrame in einer neuen Tabelle
-- MAGIC result_df.write.mode("overwrite").option("overwriteSchema", "True").saveAsTable("portfolio_analyse.silver_social_media_by_databricks.t_reddit_context_with_conversations")
-- MAGIC
-- MAGIC # Zeige die gespeicherten Ergebnisse an
-- MAGIC result_df.show(truncate=False)
