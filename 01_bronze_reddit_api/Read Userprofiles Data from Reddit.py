# Databricks notebook source
# MAGIC %md
# MAGIC # Summary
# MAGIC This Code is extraction data from Reddit and store it into a delta table and iceberg table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build up the connection to Reddit

# COMMAND ----------

# Importiere die Funktion create_reddit_client direkt aus dem Modul
import pandas as pd
import asyncpraw 
# import json

from Reddit_API_Connector import create_reddit_client, get_user_profiles, _fetch_user_profile, print_conversion_table

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType, TimestampType, ArrayType, IntegerType, MapType, FloatType
from pyspark.sql.functions import col, exp, when, isnan

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# COMMAND ----------

# Create Connection to reddit
reddit = await create_reddit_client()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get extracted user ids

# COMMAND ----------

query_posts_comments_authors = """
SELECT 
DISTINCT author 
FROM
(
    SELECT author FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_posts 
    UNION 
    SELECT author FROM portfolio_analyse.silver_social_media_by_databricks.t_reddit_comments
)"""

# COMMAND ----------

# Read the authors in posts and comments
spark_df_posts_comments_authors = spark.sql(query_posts_comments_authors)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get the masterdata of the users

# COMMAND ----------

# Extrahiere die Autoren aus dem DataFrame
authors = [row.author for row in spark_df_posts_comments_authors.collect()]

# COMMAND ----------

authors

# COMMAND ----------

# Initialisieren einer leeren Liste für die Benutzerprofile
userprofiles = []

userprofiles = await get_user_profiles(reddit, authors, 10, 5)

# COMMAND ----------

print_conversion_table(userprofiles)

# COMMAND ----------

type(userprofiles)

# COMMAND ----------

# Verarbeite die Benutzerprofile
for user in userprofiles:
    print(f"Benutzername: {user.name}")
    print(f"Benutzername: {user.is_friend}")
    print(f"Karma: {user.link_karma}")
    print(f"Erstellungsdatum: {user.created_utc}")
    # Weitere Attribute können hier hinzugefügt werden
    # Beispiel: print(f"Kommentar-Karma: {user.comment_karma}")

# COMMAND ----------

# Close the connection to Reddit
await reddit.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clearing of the Dataframe

# COMMAND ----------

def normalize_userprofile(user_dict, schema_fields):
    normalized_dict = {}
    for field in schema_fields:
        field_name = field.name
        normalized_dict[field_name] = user_dict.get(field_name, None)
    return normalized_dict

# COMMAND ----------

import asyncio

# COMMAND ----------

def fetch_and_process_userprofiles(userprofiles):
    #reddit = await create_reddit_client()
    #userprofiles = await get_Userprofiles(reddit, authors)
    #await reddit.close()
    
    if userprofiles:
        example_user = userprofiles[0]
        user_dict = example_user.__dict__
        
        schema_fields = []
        for key, value in user_dict.items():
            if isinstance(value, str):
                field_type = StringType()
            elif isinstance(value, int):
                field_type = LongType()
            elif isinstance(value, bool):
                field_type = BooleanType()
            elif isinstance(value, float):
                field_type = DoubleType()
            else:
                field_type = StringType()  # Default fallback

            schema_fields.append(StructField(key, field_type, True))
        
        schema_userprofiles = StructType(schema_fields)
        
        # Konvertiere die Benutzerprofile in ein serialisierbares Format
        userprofiles_data = [user.__dict__ for user in userprofiles]
        
        # Konvertiere bool-Werte zu int-Werten
        for user_data in userprofiles_data:
            for key, value in user_data.items():
                if isinstance(value, bool):
                    user_data[key] = int(value)
        
        return userprofiles_data, schema_userprofiles
    return None, None

# COMMAND ----------

userprofiles_data, schema_userprofiles = fetch_and_process_userprofiles(userprofiles)

# COMMAND ----------

userprofiles_data

# COMMAND ----------

# Führe die asynchronen Operationen aus und erhalte die verarbeiteten Daten
#loop = asyncio.get_event_loop()
userprofiles_data3, schema_userprofiles = loop.run_until_complete(fetch_and_process_userprofiles(userprofiles))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the structure of the schema for the Spark Dataframe

# COMMAND ----------

# Definieren des Schemas für die Kommentare
schema_userprofiles = StructType([
    #StructField('_listing_use_sort', LongType(), True), 
    StructField('name', StringType(), True), 
    #StructField('_reddit', StringType(), True), 
    #StructField('_fetched', LongType(), True), 
    StructField('is_employee', IntegerType(), True), 
    StructField('is_friend', IntegerType(), True), 
    StructField('subreddit', StringType(), True), 
    StructField('snoovatar_size', StringType(), True), 
    StructField('awardee_karma', IntegerType(), True), 
    StructField('id', StringType(), True), 
    StructField('verified', IntegerType(), True), 
    StructField('is_gold', IntegerType(), True), 
    StructField('is_mod', IntegerType(), True), 
    StructField('awarder_karma', IntegerType(), True), 
    StructField('has_verified_email', IntegerType(), True), 
    StructField('icon_img', StringType(), True), 
    StructField('hide_from_robots', IntegerType(), True), 
    StructField('link_karma', IntegerType(), True), 
    StructField('pref_show_snoovatar', IntegerType(), True), 
    StructField('is_blocked', IntegerType(), True), 
    StructField('total_karma', IntegerType(), True), 
    StructField('accept_chats', IntegerType(), True), 
    StructField('created', DoubleType(), True), 
    StructField('created_utc', DoubleType(), True), 
    StructField('snoovatar_img', StringType(), True), 
    StructField('comment_karma', IntegerType(), True), 
    StructField('accept_followers', IntegerType(), True), 
    StructField('has_subscribed', IntegerType(), True), 
    StructField('accept_pms', IntegerType(), True)
])

# COMMAND ----------

# Konvertiere die Benutzerprofile in ein serialisierbares Format und normalisiere sie
userprofiles_data = [normalize_userprofile(user.__dict__, schema_userprofiles.fields) for user in userprofiles]

# COMMAND ----------

# Verarbeite die Benutzerprofile
for user in userprofiles_data:
    print(f"Benutzername: {user.name}")
    print(f"Benutzername: {user.is_friend}")
    print(f"Karma: {user.link_karma}")
    print(f"Erstellungsdatum: {user.created_utc}")
    # Weitere Attribute können hier hinzugefügt werden
    # Beispiel: print(f"Kommentar-Karma: {user.comment_karma}")

# COMMAND ----------

# Bereinige die Daten
def clean_userprofile_data(data):
    cleaned_data = []
    for profile in data:
        cleaned_profile = {
            '_listing_use_sort': profile.get('_listing_use_sort', None),
            'name': profile.get('name', None),
            'is_employee': profile.get('is_employee', None),
            'is_friend': profile.get('is_friend', None),
            'subreddit': profile.get('subreddit', {}).get('display_name') if profile.get('subreddit') else None,
            'snoovatar_size': profile.get('snoovatar_size', None),
            'awardee_karma': profile.get('awardee_karma', None),
            'id': profile.get('id', None),
            'verified': profile.get('verified', None),
            'is_gold': profile.get('is_gold', None),
            'is_mod': profile.get('is_mod', None),
            'awarder_karma': profile.get('awarder_karma', None),
            'has_verified_email': profile.get('has_verified_email', None),
            'icon_img': profile.get('icon_img', None),
            'hide_from_robots': profile.get('hide_from_robots', None),
            'link_karma': profile.get('link_karma', None),
            'pref_show_snoovatar': profile.get('pref_show_snoovatar', None),
            'is_blocked': profile.get('is_blocked', None),
            'total_karma': profile.get('total_karma', None),
            'accept_chats': profile.get('accept_chats', None),
            'created': profile.get('created', None),
            'created_utc': profile.get('created_utc', None),
            'snoovatar_img': profile.get('snoovatar_img', None),
            'comment_karma': profile.get('comment_karma', None),
            'accept_followers': profile.get('accept_followers', None),
            'has_subscribed': profile.get('has_subscribed', None),
            'accept_pms': profile.get('accept_pms', None)
        }
        cleaned_data.append(cleaned_profile)
    return cleaned_data



# COMMAND ----------

userprofiles_data2 = clean_userprofile_data(userprofiles_data)

# COMMAND ----------

userprofiles_data2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store posts as DeltaTable

# COMMAND ----------

# Konfigurieren Sie Spark mit Delta Lake
builder = SparkSession.builder.appName("DeltaLake_Reddit_DeltaTable_Userprofiles") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # Deaktivieren Sie Arrow-Optimierung

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Set spark.sql.ansi.enabled to false
#spark.conf.set("spark.sql.ansi.enabled", "false")

# Konvertieren Sie die Liste der User Profiles direkt in einen Spark DataFrame
spark_df_userprofiles = spark.createDataFrame(userprofiles_data2, schema_userprofiles)

# COMMAND ----------



# COMMAND ----------

# Speichern Sie den Spark DataFrame als DeltaTable
catalog_name = "portfolio_analyse"  # Ersetzen Sie dies durch den Namen Ihres Katalogs
schema_name = "bronze_reddit_deltatable"  # Ersetzen Sie dies durch den Namen Ihres Schemas
table_name = "t_subreddit_userprofiles"    # Ersetzen Sie dies durch den Namen Ihrer Tabelle
full_table_name = f"{catalog_name}.{schema_name}.{table_name}"
delta_table_path = f"/mnt/delta/{schema_name}/{table_name}"

# COMMAND ----------

# Überprüfen, ob die Tabelle im Metastore existiert
if spark.catalog.tableExists(full_table_name):
    # Tabelle überschreiben
    spark_df_userprofiles.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
else:
    # Tabelle neu erstellen und Daten einfügen
    spark_df_userprofiles.write.format("delta").saveAsTable(full_table_name)



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM portfolio_analyse.bronze_reddit_deltatable.t_subreddit_userprofiles
# MAGIC WHERE is_gold = 1
