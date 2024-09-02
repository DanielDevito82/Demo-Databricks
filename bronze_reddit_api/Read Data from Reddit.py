# Databricks notebook source
# MAGIC %md
# MAGIC # Summary
# MAGIC This Code is extraction data from Reddit and store it into a delta table and iceberg table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build up the connection to Reddit

# COMMAND ----------

# Importieren Sie die dbutils Bibliothek
dbutils.widgets.text("SetChannelForExtraction", "bigdata")
dbutils.widgets.text("SetStoreFileFormat", "DeltaTable")

# COMMAND ----------

# Lesen Sie die Parameter
setChannel = dbutils.widgets.get("SetChannelForExtraction")
# Verwenden Sie die Parameter im Notebook
print(f"Parameter 1: {setChannel}")

# COMMAND ----------

# Lesen Sie die Parameter
setStoreFileFormat = dbutils.widgets.get("SetStoreFileFormat")
# Verwenden Sie die Parameter im Notebook
print(f"Parameter 2: {setStoreFileFormat}")

# COMMAND ----------

# Importiere die Funktion create_reddit_client direkt aus dem Modul
import pandas as pd
import asyncpraw 
# import json

from Reddit_API_Connector import create_reddit_client, setSubreddit, getPost, get_comments, print_conversion_table

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType, ArrayType, IntegerType, MapType, FloatType
from pyspark.sql.functions import col, exp, when, isnan

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# COMMAND ----------

# # Konfigurationsdatei laden
# def load_config(config_path):
#     with open(config_path, 'r') as config_file:
#         config = json.load(config_file)
#     return config

# # Fehlerbehandlung für die Konfigurationsdatei
# try:
#     config = load_config("config.json")
# except FileNotFoundError:
#     print("Konfigurationsdatei nicht gefunden.")
#     raise
# except json.JSONDecodeError:
#     print("Fehler beim Parsen der Konfigurationsdatei.")
#     raise

# COMMAND ----------

# Create Connection to reddit
reddit = await create_reddit_client()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Subreddit
# MAGIC

# COMMAND ----------

# Set Channel
subreddit = await setSubreddit(reddit, setChannel)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the posts of the channel

# COMMAND ----------

# Get Posts
posts, headers = await getPost(subreddit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the comments of the posts

# COMMAND ----------

comments = []
for post in posts:
  comment = await get_comments(reddit, post['id'])
  comments.append(comment)

# COMMAND ----------

# Close the connection to Reddit
await reddit.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clearing of the Dataframe

# COMMAND ----------

def clean_data(data):
    cleaned_data = []
    for item in data:
        if isinstance(item, list):
            for sub_item in item:
                cleaned_data.append(clean_item(sub_item))
        else:
            cleaned_data.append(clean_item(item))
    return cleaned_data

# Funktion zum Bereinigen der Daten
def clean_item(item):
    cleaned_item = {}
    for key, value in item.items():
        if key in ['score', 'downs', 'total_awards_received', 'gilded', 'ups', 'controversiality', 'depth']:
            cleaned_item[key] = clean_integer(value)  # Bereinigung von Integer-Feldern
        elif key in ['created_utc','edited','created','created_utc']:
            cleaned_item[key] =  clean_timestamp(value) # Bereinigung von Timestamps
        elif isinstance(value, asyncpraw.models.Redditor):
            cleaned_item[key] = str(value)  # Konvertiere Redditor-Objekte in Strings
        elif isinstance(value, asyncpraw.models.reddit.submission.SubmissionFlair):
            cleaned_item[key] = str(value)  # Konvertiere SubmissionFlair-Objekte in Strings
        elif isinstance(value, list):
            cleaned_item[key] = str(value)  # Konvertiere Listen in Strings
        elif isinstance(value, dict):
            cleaned_item[key] = str(value)  # Konvertiere Dictionaries in Strings
        else:
            cleaned_item[key] = str(value) # if value is not None else None  # Konvertiere alle anderen Objekte in Strings, außer None
    return cleaned_item

def clean_integer(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0  # oder ein anderer Standardwert
    
def clean_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0  # oder ein anderer Standardwert
    
def clean_timestamp(value):
    if value == 0:
        return 0
    else:
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0  # oder ein anderer Standardwert

# COMMAND ----------

# # Bereinigen der Posts und Kommentare
# cleaned_posts = clean_data(posts)
# cleaned_comments = clean_data(comments)

# # Konvertieren in Pandas DataFrames
# df_posts = pd.DataFrame(cleaned_posts)
# df_comments = pd.DataFrame(cleaned_comments)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the structure of the schema for the Spark Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### For Post

# COMMAND ----------

# Definieren des Schemas für die Kommentare
schema_posts = StructType([
    StructField("STR_FIELD", StringType(), True),
    StructField("all_awardings", ArrayType(StringType()), True),
    StructField("allow_live_comments", BooleanType(), True),
    StructField("approved_at_utc", StringType(), True),
    StructField("approved_by", StringType(), True),
    StructField("archived", BooleanType(), True),
    StructField("author", StringType(), True),
    StructField("author_flair_background_color", StringType(), True),
    StructField("author_flair_css_class", StringType(), True),
    StructField("author_flair_richtext", ArrayType(StringType()), True),
    StructField("author_flair_template_id", StringType(), True),
    StructField("author_flair_text", StringType(), True),
    StructField("author_flair_text_color", StringType(), True),
    StructField("author_flair_type", StringType(), True),
    StructField("author_fullname", StringType(), True),
    StructField("author_is_blocked", BooleanType(), True),
    StructField("author_patreon_flair", BooleanType(), True),
    StructField("author_premium", BooleanType(), True),
    StructField("awarders", ArrayType(StringType()), True),
    StructField("banned_at_utc", StringType(), True),
    StructField("banned_by", StringType(), True),
    StructField("can_gild", BooleanType(), True),
    StructField("can_mod_post", BooleanType(), True),
    StructField("category", StringType(), True),
    StructField("clicked", BooleanType(), True),
    StructField("comment_limit", IntegerType(), True),
    StructField("comment_sort", StringType(), True),
    StructField("content_categories", StringType(), True),
    StructField("contest_mode", BooleanType(), True),
    StructField("created", FloatType(), True),
    StructField("created_utc", FloatType(), True),
    StructField("discussion_type", StringType(), True),
    StructField("distinguished", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("downs", IntegerType(), True),
    StructField("edited", StringType(), True),  # Hier könnte eine spezielle Behandlung notwendig sein
    StructField("flair", StringType(), True),
    StructField("fullname", StringType(), True),
    StructField("gilded", IntegerType(), True),
    StructField("gildings", MapType(StringType(), StringType()), True),
    StructField("hidden", BooleanType(), True),
    StructField("hide_score", BooleanType(), True),
    StructField("id", StringType(), True),
    StructField("is_created_from_ads_ui", BooleanType(), True),
    StructField("is_crosspostable", BooleanType(), True),
    StructField("is_meta", BooleanType(), True),
    StructField("is_original_content", BooleanType(), True),
    StructField("is_reddit_media_domain", BooleanType(), True),
    StructField("is_robot_indexable", BooleanType(), True),
    StructField("is_self", BooleanType(), True),
    StructField("is_video", BooleanType(), True),
    StructField("likes", StringType(), True),
    StructField("link_flair_background_color", StringType(), True),
    StructField("link_flair_css_class", StringType(), True),
    StructField("link_flair_richtext", ArrayType(StringType()), True),
    StructField("link_flair_text", StringType(), True),
    StructField("link_flair_text_color", StringType(), True),
    StructField("link_flair_type", StringType(), True),
    StructField("locked", BooleanType(), True),
    StructField("media", StringType(), True),
    StructField("media_embed", MapType(StringType(), StringType()), True),  # Anpassen des Schemas für media_embed
    StructField("media_only", BooleanType(), True),
    StructField("mod", StringType(), True),
    StructField("mod_note", StringType(), True),
    StructField("mod_reason_by", StringType(), True),
    StructField("mod_reason_title", StringType(), True),
    StructField("mod_reports", ArrayType(StringType()), True),
    StructField("name", StringType(), True),
    StructField("no_follow", BooleanType(), True),
    StructField("num_comments", IntegerType(), True),
    StructField("num_crossposts", IntegerType(), True),
    StructField("num_reports", StringType(), True),
    StructField("over_18", BooleanType(), True),
    StructField("parent_whitelist_status", StringType(), True),
    StructField("permalink", StringType(), True),
    StructField("pinned", BooleanType(), True),
    StructField("pwls", IntegerType(), True),
    StructField("quarantine", BooleanType(), True),
    StructField("removal_reason", StringType(), True),
    StructField("removed_by", StringType(), True),
    StructField("removed_by_category", StringType(), True),
    StructField("report_reasons", StringType(), True),
    StructField("saved", BooleanType(), True),
    StructField("score", IntegerType(), True),
    StructField("secure_media", StringType(), True),
    StructField("secure_media_embed", MapType(StringType(), StringType()), True),
    StructField("selftext", StringType(), True),
    StructField("selftext_html", StringType(), True),
    StructField("send_replies", BooleanType(), True),
    StructField("shortlink", StringType(), True),
    StructField("spoiler", BooleanType(), True),
    StructField("stickied", BooleanType(), True),
    StructField("subreddit", StringType(), True),
    StructField("subreddit_id", StringType(), True),
    StructField("subreddit_name_prefixed", StringType(), True),
    StructField("subreddit_subscribers", IntegerType(), True),
    StructField("subreddit_type", StringType(), True),
    StructField("suggested_sort", StringType(), True),
    StructField("thumbnail", StringType(), True),
    StructField("title", StringType(), True),
    StructField("top_awarded_type", StringType(), True),
    StructField("total_awards_received", IntegerType(), True),
    StructField("treatment_tags", ArrayType(StringType()), True),
    StructField("ups", IntegerType(), True),
    StructField("upvote_ratio", FloatType(), True),
    StructField("url", StringType(), True),
    StructField("url_overridden_by_dest", StringType(), True),
    StructField("user_reports", ArrayType(StringType()), True),
    StructField("view_count", StringType(), True),
    StructField("visited", BooleanType(), True),
    StructField("whitelist_status", StringType(), True),
    StructField("wls", IntegerType(), True),
    StructField("media_metadata", StringType(), True),
    StructField("crosspost_parent", StringType(), True),
    StructField("crosspost_parent_list", StringType(), True),
    StructField("author_cakeday", StringType(), True)
])

# COMMAND ----------


# Liste der benötigten Felder
required_fields = [field.name for field in schema_posts]

# COMMAND ----------

def transform_posts(posts, required_fields):
    transformed_posts = []
    for post in posts:
        if isinstance(post, dict):  # Sicherstellen, dass post ein Dictionary ist
            transformed_post = {field: post.get(field, None) for field in required_fields}
            
            # Konvertiere nicht serialisierbare Objekte in Strings
            if isinstance(transformed_post.get('author'), asyncpraw.models.Redditor):
                transformed_post['author'] = transformed_post['author'].name
            if isinstance(transformed_post.get('flair'), asyncpraw.models.reddit.submission.SubmissionFlair):
                transformed_post['flair'] = str(transformed_post['flair'])
            if isinstance(transformed_post.get('mod'), asyncpraw.models.reddit.submission.SubmissionModeration):
                transformed_post['mod'] = str(transformed_post['mod'])
            if isinstance(transformed_post.get('subreddit'), asyncpraw.models.Subreddit):
                transformed_post['subreddit'] = transformed_post['subreddit'].display_name
            
            # Sicherstellen, dass media_embed ein Dictionary ist
            if 'media_embed' in transformed_post and transformed_post['media_embed'] is None:
                transformed_post['media_embed'] = {}
            if 'secure_media_embed' in transformed_post and transformed_post['secure_media_embed'] is None:
                transformed_post['secure_media_embed'] = {}    
            transformed_posts.append(transformed_post)
    return transformed_posts

# COMMAND ----------

# Transformieren Sie die Posts
transformed_posts = transform_posts(posts, required_fields)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store posts as DeltaTable

# COMMAND ----------

# Konfigurieren Sie Spark mit Delta Lake
builder = SparkSession.builder.appName("DeltaLake_Reddit_DeltaTable_Posts") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Deaktivieren Sie Arrow-Optimierung

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Set spark.sql.ansi.enabled to false
#spark.conf.set("spark.sql.ansi.enabled", "false")

# Konvertieren Sie die Liste der Posts direkt in einen Spark DataFrame
spark_df_posts = spark.createDataFrame(transformed_posts, schema_posts)

# COMMAND ----------

# Speichern Sie den Spark DataFrame als DeltaTable
catalog_name = "portfolio_analyse"  # Ersetzen Sie dies durch den Namen Ihres Katalogs
schema_name = "bronze_reddit_deltatable"  # Ersetzen Sie dies durch den Namen Ihres Schemas
table_name = f"t_subreddit_{setChannel}_posts"    # Ersetzen Sie dies durch den Namen Ihrer Tabelle
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
delta_table_path = f"/mnt/delta/{schema_name}/{table_name}"

# COMMAND ----------

# Löschen eines Verzeichnisses und seines Inhalts
#dbutils.fs.rm("/mnt/delta/", True)

# COMMAND ----------

# Überprüfen, ob die Tabelle im Metastore existiert
if spark.catalog.tableExists(full_table_name):
    delta_table = DeltaTable.forName(spark, full_table_name)
    # Upsert-Operation durchführen (Einfügen oder Aktualisieren)
    delta_table.alias("existing") \
        .merge(
            spark_df_posts.alias("new"),
            "existing.id = new.id"
        ) \
        .whenMatchedUpdateAll(
            # set={
            #     "existing.likes": "new.likes",
            #     "existing.edited": "new.edited",
            #     "existing.mod": "new.mod",
            #     "existing.mod_note": "new.mod_note",
            #     "existing.mod_reason_title": "new.mod_reason_title",
            #     "existing.mod_reason_by": "new.mod_reason_by",
            #     "existing.mod_reports": "new.mod_reports",
            #     "existing.STR_FIELD": "new.STR_FIELD",
            #     "existing.all_awardings": "new.all_awardings",
            #     "existing.allow_live_comments": "new.allow_live_comments",  
            #     "existing.approved_at_utc": "new.approved_at_utc",
            #     "existing.approved_by": "new.approved_by",
            #     "existing.archived": "new.archived"                                                              
            # }
        ) \
        .whenNotMatchedInsertAll() \
        .execute()    

    # Historie der letzten Transaktion abrufen
    last_operation = delta_table.history(1).collect()[0]

    # Anzahl der eingefügten und aktualisierten Zeilen extrahieren
    num_added_rows = last_operation['operationMetrics']['numTargetRowsInserted']
    num_updated_rows = last_operation['operationMetrics']['numTargetRowsUpdated']

    # Logging der Anzahl der betroffenen Zeilen
    print(f"{num_added_rows} Zeilen wurden eingefügt.")
    print(f"{num_updated_rows} Zeilen wurden aktualisiert.")

else:    
    # Speichern der Daten als Delta-Tabelle
    spark_df_posts.write.format("delta").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
    print(f"Tabelle {full_table_name} erstellt.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store posts as iceberg table

# COMMAND ----------

# from pyspark.sql import SparkSession

# COMMAND ----------

# # Konfigurieren Sie Spark mit Iceberg
# builder = SparkSession.builder.appName("Iceberg_Reddit_Table_Posts") \
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
#     .config("spark.sql.catalog.spark_catalog.type", "hive") \
#     .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Deaktivieren Sie Arrow-Optimierung

# spark = builder.getOrCreate()

# # Set spark.sql.ansi.enabled to false
# spark.conf.set("spark.sql.ansi.enabled", "false")

# # Konvertieren Sie die Liste der Posts direkt in einen Spark DataFrame
# spark_df_posts = spark.createDataFrame(df_posts)

# # Speichern Sie den Spark DataFrame als Iceberg-Tabelle
# catalog_name = "portfolio_analyse"  # Ersetzen Sie dies durch den Namen Ihres Katalogs
# schema_name = "bronze_reddit_iceberg"  # Ersetzen Sie dies durch den Namen Ihres Schemas
# table_name = f"t_subreddit_{setChannel}_posts"  # Ersetzen Sie dies durch den Namen Ihrer Tabelle
# full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

# # Überprüfen, ob die Tabelle im Metastore existiert
# if spark.catalog.tableExists(full_table_name):
#     # Upsert-Operation durchführen (Einfügen oder Aktualisieren)
#     spark_df_posts.createOrReplaceTempView("temp_posts")
#     spark.sql(f"""
#         MERGE INTO {full_table_name} AS existing
#         USING temp_posts AS new
#         ON existing.id = new.id
#         WHEN MATCHED THEN UPDATE SET
#             existing.likes = new.likes,
#             existing.edited = new.edited,
#             existing.mod = new.mod,
#             existing.mod_note = new.mod_note,
#             existing.mod_reason_title = new.mod_reason_title,
#             existing.mod_reason_by = new.mod_reason_by,
#             existing.mod_reports = new.mod_reports
#         WHEN NOT MATCHED THEN INSERT *
#     """)
# else:
#     # Speichern der Daten als Iceberg-Tabelle
#     spark_df_posts.writeTo(full_table_name).using("org.apache.iceberg.spark.source.IcebergSource").create()
#     org.apache.iceberg.spark.source.IcebergSource
#     # Speichern der Daten als Iceberg-Tabelle
#     #spark_df_posts.writeTo(full_table_name).using("iceberg").create()


# COMMAND ----------

# MAGIC %md
# MAGIC ### For comments

# COMMAND ----------

def flatten_comments(nested_comments):
    flat_comments = []
    for item in nested_comments:
        if isinstance(item, list):
            flat_comments.extend(flatten_comments(item))
        else:
            flat_comments.append(item)
    return flat_comments

# COMMAND ----------

def transform_comments(comments):
    transformed_comments = []
    for comment in comments:
        if isinstance(comment, dict):  # Sicherstellen, dass comment ein Dictionary ist
            transformed_comment = {
                "subreddit_id": comment.get("subreddit_id"),
                "approved_at_utc": comment.get("approved_at_utc"),
                "author_is_blocked": comment.get("author_is_blocked"),
                "comment_type": comment.get("comment_type"),
                "awarders": comment.get("awarders"),
                "mod_reason_by": comment.get("mod_reason_by"),
                "banned_by": comment.get("banned_by"),
                "author_flair_type": comment.get("author_flair_type"),
                "total_awards_received": comment.get("total_awards_received"),
                "subreddit": comment.get("subreddit").display_name if comment.get("subreddit") else None,
                "author_flair_template_id": comment.get("author_flair_template_id"),
                "likes": comment.get("likes"),
                "user_reports": comment.get("user_reports"),
                "saved": comment.get("saved"),
                "id": comment.get("id"),
                "banned_at_utc": comment.get("banned_at_utc"),
                "mod_reason_title": comment.get("mod_reason_title"),
                "gilded": comment.get("gilded"),
                "archived": comment.get("archived"),
                "collapsed_reason_code": comment.get("collapsed_reason_code"),
                "no_follow": comment.get("no_follow"),
                "author": comment.get("author").name if comment.get("author") else None,
                "can_mod_post": comment.get("can_mod_post"),
                "created_utc": comment.get("created_utc"),
                "send_replies": comment.get("send_replies"),
                "parent_id": comment.get("parent_id"),
                "score": comment.get("score"),
                "author_fullname": comment.get("author_fullname"),
                "approved_by": comment.get("approved_by"),
                "mod_note": comment.get("mod_note"),
                "all_awardings": comment.get("all_awardings"),
                "collapsed": comment.get("collapsed"),
                "body": comment.get("body"),
                "edited": comment.get("edited"),
                "top_awarded_type": comment.get("top_awarded_type"),
                "author_flair_css_class": comment.get("author_flair_css_class"),
                "name": comment.get("name"),
                "is_submitter": comment.get("is_submitter"),
                "downs": comment.get("downs"),
                "author_flair_richtext": comment.get("author_flair_richtext"),
                "author_patreon_flair": comment.get("author_patreon_flair"),
                "body_html": comment.get("body_html"),
                "removal_reason": comment.get("removal_reason"),
                "collapsed_reason": comment.get("collapsed_reason"),
                "distinguished": comment.get("distinguished"),
                "associated_award": comment.get("associated_award"),
                "stickied": comment.get("stickied"),
                "author_premium": comment.get("author_premium"),
                "can_gild": comment.get("can_gild"),
                "gildings": comment.get("gildings"),
                "unrepliable_reason": comment.get("unrepliable_reason"),
                "author_flair_text_color": comment.get("author_flair_text_color"),
                "score_hidden": comment.get("score_hidden"),
                "permalink": comment.get("permalink"),
                "subreddit_type": comment.get("subreddit_type"),
                "locked": comment.get("locked"),
                "report_reasons": comment.get("report_reasons"),
                "created": comment.get("created"),
                "author_flair_text": comment.get("author_flair_text"),
                "treatment_tags": comment.get("treatment_tags"),
                "link_id": comment.get("link_id"),
                "subreddit_name_prefixed": comment.get("subreddit_name_prefixed"),
                "controversiality": comment.get("controversiality"),
                "depth": comment.get("depth"),
                "author_flair_background_color": comment.get("author_flair_background_color"),
                "collapsed_because_crowd_control": comment.get("collapsed_because_crowd_control"),
                "mod_reports": comment.get("mod_reports"),
                "num_reports": comment.get("num_reports"),
                "ups": comment.get("ups")
            }
            transformed_comments.append(transformed_comment)
        else:
            print(f"Kommentar ist kein Dictionary: {comment}")
    return transformed_comments

# Kommentare flach machen
flat_comments = flatten_comments(comments)

# Debugging: Überprüfen der Struktur und des Inhalts von flat_comments
print("Anzahl der flachen Kommentare:", len(flat_comments))
if len(flat_comments) > 0:
    print("Beispielkommentar vor der Transformation:", flat_comments[0])

# Transformieren Sie die Kommentare
transformed_comments = transform_comments(flat_comments)

# Debugging: Überprüfen der Struktur und des Inhalts von transformed_comments
print("Anzahl der Kommentare:", len(transformed_comments))
if len(transformed_comments) > 0:
    print("Beispielkommentar nach der Transformation:", transformed_comments[0])


# COMMAND ----------

schema_comments = StructType([
    StructField("subreddit_id", StringType(), True),
    StructField("approved_at_utc", StringType(), True),
    StructField("author_is_blocked", BooleanType(), True),
    StructField("comment_type", StringType(), True),
    StructField("awarders", ArrayType(StringType()), True),
    StructField("mod_reason_by", StringType(), True),
    StructField("banned_by", StringType(), True),
    StructField("author_flair_type", StringType(), True),
    StructField("total_awards_received", IntegerType(), True),
    StructField("subreddit", StringType(), True),
    StructField("author_flair_template_id", StringType(), True),
    StructField("likes", StringType(), True),
    StructField("user_reports", ArrayType(StringType()), True),
    StructField("saved", BooleanType(), True),
    StructField("id", StringType(), True),
    StructField("banned_at_utc", StringType(), True),
    StructField("mod_reason_title", StringType(), True),
    StructField("gilded", LongType(), True),
    StructField("archived", BooleanType(), True),
    StructField("collapsed_reason_code", StringType(), True),
    StructField("no_follow", BooleanType(), True),
    StructField("author", StringType(), True),
    StructField("can_mod_post", BooleanType(), True),
    StructField("created_utc", DoubleType(), True),
    StructField("send_replies", BooleanType(), True),
    StructField("parent_id", StringType(), True),
    StructField("score", LongType(), True),
    StructField("author_fullname", StringType(), True),
    StructField("approved_by", StringType(), True),
    StructField("mod_note", StringType(), True),
    StructField("all_awardings", ArrayType(StringType()), True),
    StructField("collapsed", BooleanType(), True),
    StructField("body", StringType(), True),
    StructField("edited", StringType(), True),
    StructField("top_awarded_type", StringType(), True),
    StructField("author_flair_css_class", StringType(), True),
    StructField("name", StringType(), True),
    StructField("is_submitter", BooleanType(), True),
    StructField("downs", LongType(), True),
    StructField("author_flair_richtext", ArrayType(StringType()), True),
    StructField("author_patreon_flair", BooleanType(), True),
    StructField("body_html", StringType(), True),
    StructField("removal_reason", StringType(), True),
    StructField("collapsed_reason", StringType(), True),
    StructField("distinguished", StringType(), True),
    StructField("associated_award", StringType(), True),
    StructField("stickied", BooleanType(), True),
    StructField("author_premium", BooleanType(), True),
    StructField("can_gild", BooleanType(), True),
    StructField("gildings", MapType(StringType(), LongType()), True),
    StructField("unrepliable_reason", StringType(), True),
    StructField("author_flair_text_color", StringType(), True),
    StructField("score_hidden", BooleanType(), True),
    StructField("permalink", StringType(), True),
    StructField("subreddit_type", StringType(), True),
    StructField("locked", BooleanType(), True),
    StructField("report_reasons", StringType(), True),
    StructField("created", DoubleType(), True),
    StructField("author_flair_text", StringType(), True),
    StructField("treatment_tags", ArrayType(StringType()), True),
    StructField("link_id", StringType(), True),
    StructField("subreddit_name_prefixed", StringType(), True),
    StructField("controversiality", LongType(), True),
    StructField("depth", LongType(), True),
    StructField("author_flair_background_color", StringType(), True),
    StructField("collapsed_because_crowd_control", StringType(), True),
    StructField("mod_reports", ArrayType(StringType()), True),
    StructField("num_reports", StringType(), True),
    StructField("ups", LongType(), True)
])


# COMMAND ----------

# schema_comments = StructType([
#     StructField("subreddit_id", StringType(), True),
#     StructField("approved_at_utc", StringType(), True),
#     StructField("author_is_blocked", BooleanType(), True),
#     StructField("comment_type", StringType(), True),
#     StructField("awarders", ArrayType(StringType()), True),
#     StructField("mod_reason_by", StringType(), True),
#     StructField("banned_by", StringType(), True),
#     StructField("author_flair_type", StringType(), True),
#     StructField("total_awards_received", IntegerType(), True),
#     StructField("subreddit", StringType(), True),
#     StructField("author_flair_template_id", StringType(), True),
#     StructField("likes", StringType(), True),
#     StructField("user_reports", ArrayType(StringType()), True),
#     StructField("saved", BooleanType(), True),
#     StructField("id", StringType(), True),
#     StructField("banned_at_utc", StringType(), True),
#     StructField("mod_reason_title", StringType(), True),
#     StructField("gilded", LongType(), True),
#     StructField("archived", BooleanType(), True),
#     StructField("collapsed_reason_code", StringType(), True),
#     StructField("no_follow", BooleanType(), True),
#     StructField("author", StringType(), True),
#     StructField("can_mod_post", BooleanType(), True),
#     StructField("created_utc", DoubleType(), True),
#     StructField("send_replies", BooleanType(), True),
#     StructField("parent_id", StringType(), True),
#     StructField("score", LongType(), True),
#     StructField("author_fullname", StringType(), True),
#     StructField("approved_by", StringType(), True),
#     StructField("mod_note", StringType(), True),
#     StructField("all_awardings", ArrayType(StringType()), True),
#     StructField("collapsed", BooleanType(), True),
#     StructField("body", StringType(), True),
#     StructField("edited", BooleanType(), True),
#     StructField("top_awarded_type", StringType(), True),
#     StructField("author_flair_css_class", StringType(), True),
#     StructField("name", StringType(), True),
#     StructField("is_submitter", BooleanType(), True),
#     StructField("downs", LongType(), True),
#     StructField("author_flair_richtext", StringType(), True),
#     StructField("author_patreon_flair", StringType(), True),
#     StructField("body_html", StringType(), True),
#     StructField("removal_reason", StringType(), True),
#     StructField("collapsed_reason", StringType(), True),
#     StructField("distinguished", StringType(), True),
#     StructField("associated_award", StringType(), True),
#     StructField("stickied", BooleanType(), True),
#     StructField("author_premium", BooleanType(), True),
#     StructField("can_gild", BooleanType(), True),
#     StructField("gildings", MapType(StringType(), LongType()), True),
#     StructField("unrepliable_reason", StringType(), True),
#     StructField("author_flair_text_color", StringType(), True),
#     StructField("score_hidden", BooleanType(), True),
#     StructField("permalink", StringType(), True),
#     StructField("subreddit_type", StringType(), True),
#     StructField("locked", BooleanType(), True),
#     StructField("report_reasons", StringType(), True),
#     StructField("created", DoubleType(), True),
#     StructField("author_flair_text", StringType(), True),
#     StructField("treatment_tags", ArrayType(StringType()), True),
#     StructField("link_id", StringType(), True),
#     StructField("subreddit_name_prefixed", StringType(), True),
#     StructField("controversiality", LongType(), True),
#     StructField("depth", LongType(), True),
#     StructField("author_flair_background_color", StringType(), True),
#     StructField("collapsed_because_crowd_control", StringType(), True),
#     StructField("mod_reports", ArrayType(StringType()), True),
#     StructField("num_reports", StringType(), True),
#     StructField("ups", LongType(), True)
# ])

# COMMAND ----------

# Konfigurieren Sie Spark mit Delta Lake
builder = SparkSession.builder.appName("DeltaLake_Reddit_DeltaTable_Comments") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
   .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Deaktivieren Sie Arrow-Optimierung

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Set spark.sql.ansi.enabled to false
spark.conf.set("spark.sql.ansi.enabled", "false")

# Konvertieren Sie die Liste der Posts direkt in einen Spark DataFrame
spark_df_comments = spark.createDataFrame(transformed_comments, schema_comments)

# COMMAND ----------

# Speichern Sie den Spark DataFrame als DeltaTable
catalog_name = "portfolio_analyse"  # Ersetzen Sie dies durch den Namen Ihres Katalogs
schema_name = "bronze_reddit_deltatable"  # Ersetzen Sie dies durch den Namen Ihres Schemas
table_name = f"t_subreddit_{setChannel}_comments"    # Ersetzen Sie dies durch den Namen Ihrer Tabelle
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
delta_table_path = f"/mnt/delta/{schema_name}/{table_name}"

# COMMAND ----------

# Überprüfen, ob die Tabelle im Metastore existiert
if spark.catalog.tableExists(full_table_name):
    delta_table = DeltaTable.forName(spark, full_table_name)
    # Upsert-Operation durchführen (Einfügen oder Aktualisieren)
    delta_table.alias("existing") \
        .merge(
            spark_df_comments.alias("new"),
            "existing.id = new.id"
        ) \
        .whenMatchedUpdateAll(
            # set={
            #     "existing.likes": "new.likes",
            #     "existing.author_is_blocked": "new.author_is_blocked",
            #     "existing.mod_reason_title": "new.mod_reason_title",
            #     "existing.mod_reason_by": "new.mod_reason_by",
            #     "existing.total_awards_received": "new.total_awards_received",
            #     "existing.archived": "new.archived",
            #     "existing.send_replies": "new.send_replies",
            #     "existing.score": "new.score",
            #     "existing.mod_note": "new.mod_note",
            #     "existing.edited": "new.edited",
            #     "existing.removal_reason": "new.removal_reason",
            #     "existing.collapsed_reason": "new.collapsed_reason",
            #     "existing.score_hidden": "new.score_hidden",
            #     "existing.mod_reports": "new.mod_reports",
            #     "existing.locked": "new.locked",
            #     "existing.report_reasons": "new.report_reasons"
            # }
        ) \
        .whenNotMatchedInsertAll() \
        .execute()

    # Historie der letzten Transaktion abrufen
    last_operation = delta_table.history(1).collect()[0]

    # Anzahl der eingefügten und aktualisierten Zeilen extrahieren
    num_added_rows = last_operation['operationMetrics']['numTargetRowsInserted']
    num_updated_rows = last_operation['operationMetrics']['numTargetRowsUpdated']

    # Logging der Anzahl der betroffenen Zeilen
    print(f"{num_added_rows} Zeilen wurden eingefügt.")
    print(f"{num_updated_rows} Zeilen wurden aktualisiert.")            
else:    
    # Speichern der Daten als Delta-Tabelle
    spark_df_comments.write.format("delta").saveAsTable(f"{catalog_name}.{schema_name}.{table_name}")
    print(f"Tabelle {full_table_name} erstellt.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store comments as ice berg table

# COMMAND ----------

# # Konfigurieren Sie Spark mit Iceberg
# builder = SparkSession.builder.appName("Iceberg_Reddit_Table_Comments") \
#     .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
#     .config("spark.sql.catalog.spark_catalog.type", "hive") \
#     .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Deaktivieren Sie Arrow-Optimierung

# spark = builder.getOrCreate()

# # Set spark.sql.ansi.enabled to false
# spark.conf.set("spark.sql.ansi.enabled", "false")

# # Konvertieren Sie die Liste der Posts direkt in einen Spark DataFrame
# spark_df_comments = spark.createDataFrame(df_comments)

# # Anwenden der sicheren Cast-Funktion auf den DataFrame
# spark_df_comments = safe_cast_to_boolean(spark_df_comments, boolean_columns)

# # Speichern Sie den Spark DataFrame als Iceberg-Tabelle
# catalog_name = "portfolio_analyse"  # Ersetzen Sie dies durch den Namen Ihres Katalogs
# schema_name = "bronze_reddit_iceberg"  # Ersetzen Sie dies durch den Namen Ihres Schemas
# table_name = f"t_subreddit_{setChannel}_comments"  # Ersetzen Sie dies durch den Namen Ihrer Tabelle
# full_table_name = f"{catalog_name}.{schema_name}.{table_name}"

# # Überprüfen, ob die Tabelle im Metastore existiert
# if spark.catalog.tableExists(full_table_name):
#     # Upsert-Operation durchführen (Einfügen oder Aktualisieren)
#     spark_df_comments.createOrReplaceTempView("temp_comments")
#     spark.sql(f"""
#         MERGE INTO {full_table_name} AS existing
#         USING temp_comments AS new
#         ON existing.id = new.id
#         WHEN MATCHED THEN UPDATE SET
#                 existing.likes = new.likes,
#                 existing.author_is_blocked = new.author_is_blocked,
#                 existing.mod_reason_title = new.mod_reason_title,
#                 existing.mod_reason_by = new.mod_reason_by,
#                 existing.total_awards_received = new.total_awards_received,
#                 existing.archived = new.archived,
#                 existing.send_replies = new.send_replies,
#                 existing.score = new.score,
#                 existing.mod_note = new.mod_note,
#                 existing.edited = new.edited,
#                 existing.removal_reason = new.removal_reason,
#                 existing.collapsed_reason = new.collapsed_reason,
#                 existing.score_hidden = new.score_hidden,
#                 existing.mod_reports = new.mod_reports,
#                 existing.locked = new.locked,
#                 existing.report_reasons = new.report_reasons
#         WHEN NOT MATCHED THEN INSERT *
#     """)
# else:
#     # Speichern der Daten als Iceberg-Tabelle
#     spark_df_comments.writeTo(full_table_name).using("iceberg").create()
