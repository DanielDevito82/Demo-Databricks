# Databricks notebook source
# MAGIC %md
# MAGIC The analyze_for_software_product_opionion should looking for optionions about software products in the desciption of a Reddit Post

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS portfolio_analyse.functions;
# MAGIC
# MAGIC CREATE OR REPLACE function portfolio_analyse.functions.analyze_for_software_product_opinion(context STRING)
# MAGIC RETURNS ARRAY<STRUCT<product_name: STRING, sentiment: STRING>>
# MAGIC RETURN FROM_JSON(ai_query(
# MAGIC  'databricks-meta-llama-3-1-70b-instruct',
# MAGIC     request =>  "Analyze the context of a conversation in a Subreddit post. The context include the Name of the Author of the post, the Title of the post and the Text of the post. Additionally the --- Conversation --- in the comments of the post. The conversation includes the the Name of the Author of the comment and the Comment of the author.
# MAGIC
# MAGIC Extract all entities mentioned an opinion about a Software Product on the Software Market. 
# MAGIC For each entity:
# MAGIC - classify sentiment as [POSITIVE,NEUTRAL,NEGATIVE]
# MAGIC
# MAGIC Exclude your analysis if the texts starts with: I am a bot
# MAGIC
# MAGIC Return JSON ONLY. No other text outside the JSON. JSON format:
# MAGIC
# MAGIC    product_name: <product name>,
# MAGIC    sentiment: <review sentiment, one of [POSITIVE,NEUTRAL,NEGATIVE]>,
# MAGIC
# MAGIC ### Instruction:"||context||"
# MAGIC ### Response:"),"ARRAY<STRUCT<product_name: STRING, sentiment: STRING>>")
