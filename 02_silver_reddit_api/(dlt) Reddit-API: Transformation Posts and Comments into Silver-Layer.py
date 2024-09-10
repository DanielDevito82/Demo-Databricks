# Databricks notebook source
# MAGIC %md
# MAGIC # Deklaration

# COMMAND ----------

import dlt
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType, ArrayType, IntegerType, MapType, FloatType
from pyspark.sql.functions import concat, lit

# COMMAND ----------

catalog = "portfolio_analyse"
schema_to_extract = "bronze_reddit_deltatable"
schema_to_store = "silver_social_media"

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
# MAGIC # Silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logik zum Zusammenführen aller *Posts Tabellen inkl. Verarbeitungslogik

# COMMAND ----------

# Lese alle Tabellen mit *posts im Namen zur Weiterverarbeitung
result_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema_to_extract} LIKE '*posts'")

# Extrahiere die Tabellennamen in eine Liste
table_names = [row['tableName'] for row in result_df.collect()]

# Erstelle dynamisch den SQL UNION ALL Befehl
union_query = " UNION ".join([f" SELECT {', '.join(fields_posts)} FROM {catalog}.{schema_to_extract}.{table_name}" for table_name in table_names])

# print(table_names)

# COMMAND ----------

# print(union_query)

# COMMAND ----------

rules = {}
rules["approved_by"] = "(approved_by IS NOT NULL)"
rules["author_is_blocked"] = "(author_is_blocked IS NOT NULL)"
rules["author"] = "(author IS NOT NULL)"

# COMMAND ----------

# Definieren Sie die DLT-Tabelle für Posts

@dlt.table(
    name = "t_Reddit_Posts"
#    name=f"{schema_to_store}.t_Reddit_Posts2",
)
@dlt.expect_all(rules)
def union_posts_for_silver():
    return spark.sql(union_query)
#    return spark.sql("SELECT id, author_premium FROM portfolio_analyse.bronze_reddit_deltatable.t_subreddit_dataanalyst_posts UNION SELECT id, author_premium FROM portfolio_analyse.bronze_reddit_deltatable.t_subreddit_dataarchitect_posts UNION SELECT id, author_premium FROM portfolio_analyse.bronze_reddit_deltatable.t_subreddit_dataengineering_posts")

# COMMAND ----------

rules = {}
rules["author_is_blocked"] = "(author_is_blocked = 'true')"

# COMMAND ----------

# Definieren Sie eine Erwartung, dass das Feld 'author_is_blocked' nicht NULL ist
@dlt.table(
  name="t_blocked_authors_in_posts"
)
@dlt.expect_all_or_drop(rules)
def blocked_authors_in_posts():
    return dlt.read("t_Reddit_Posts")

# COMMAND ----------

# # Erfassen Sie die fehlerhaften Datensätze
# @dlt.view(
#     name="v_invalid_reddit_posts",
# )
# def invalid_reddit_posts():
#     return dlt.read("blocked_authors_in_posts").filter(col("author_is_blocked").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC Prüfung auf ungleiche Spaltenanzahl in Tabelle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logik zum Zusammenführen aller *Comments Tabellen inkl. Verarbeitungslogik

# COMMAND ----------

# Lese alle Tabellen mit *comments im Namen zur Weiterverarbeitung
result_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema_to_extract} LIKE '*comments'")

# Extrahiere die Tabellennamen in eine Liste
table_names = [row['tableName'] for row in result_df.collect()]

# Erstelle dynamisch den SQL UNION ALL Befehl
union_query_comments = " UNION ALL ".join([f" SELECT {', '.join(fields_comments)} FROM {catalog}.{schema_to_extract}.{table_name}" for table_name in table_names])

#print(union_query)

# COMMAND ----------

# Definieren Sie die DLT-Tabelle für Posts
@dlt.table(
   name = "t_Reddit_Comments"
)
def union_comments_for_silver():
    return (spark.sql(union_query_comments))


# COMMAND ----------

# rules = {}
# rules["author_is_blocked"] = "(author_is_blocked = 'true')"

# COMMAND ----------

# # Definieren Sie eine Erwartung, dass das Feld 'author_is_blocked' nicht NULL ist
# @dlt.expect_all_or_drop(rules)
# @dlt.table(
#   name="t_blocked_authors_in_comments",
#   temporary=False
# )
# def blocked_authors_in_comments():
#     return dlt.read("t_Reddit_Comments2")

# COMMAND ----------

# # Erfassen Sie die fehlerhaften Datensätze
# @dlt.view(
#     name="v_invalid_reddit_comments",
# )
# def invalid_reddit_comments():
#     return dlt.read("blocked_authors_in_comments").filter(col("author_is_blocked").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join tables to view

# COMMAND ----------

# Lesen der Spaltennamen aus tabelle1 und tabelle2
posts_columns = spark.table("portfolio_analyse.default.t_reddit_posts").columns
comments_columns = spark.table("portfolio_analyse.default.t_reddit_comments").columns

# Erstellen der dynamischen SELECT-Klausel
select_clause = []

for col in posts_columns:
    select_clause.append(f"t_Reddit_Posts.{col} AS posts_{col}")

for col in comments_columns:
    select_clause.append(f"t_Reddit_Comments.{col} AS comments_{col}")

# Konvertieren der Liste in eine Liste von Zeichenketten
select_clause_str = select_clause
print(select_clause_str)

# COMMAND ----------

# # Erstellen der vollständigen SQL-Abfrage
# sql_query = f"""
# SELECT {select_clause_str}
# FROM portfolio_analyse.default.t_reddit_posts AS posts
# LEFT JOIN portfolio_analyse.default.t_reddit_comments AS comments
# ON comments.link_id = CONCAT('t3_', posts.id)
# """

# COMMAND ----------

# Erstellen der vollständigen SQL-Abfrage
sql_query = f"""CREATE OR REFRESH LIVE TABLE v_reddit_posts_comments
COMMENT "Posts & Comments"
AS SELECT {select_clause_str}
FROM portfolio_analyse.default.t_reddit_posts AS posts
LEFT JOIN portfolio_analyse.default.t_reddit_comments AS comments
ON comments.link_id = CONCAT('t3_', posts.id)
"""

# COMMAND ----------

print(select_clause_str)

# COMMAND ----------

@dlt.view(
    name = "v_reddit_posts_comments"
)
def create_view_Gold_Layer():
    t_reddit_posts = dlt.read("t_Reddit_Posts")
    t_reddit_comments = dlt.read("t_Reddit_Comments")

    return t_reddit_posts.join(
        t_reddit_comments,
        concat(lit('t3_'), t_reddit_posts["id"]) == t_reddit_comments["link_id"],
        "left"
    ).selectExpr(*select_clause_str)
