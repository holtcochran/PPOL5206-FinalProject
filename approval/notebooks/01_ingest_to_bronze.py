# Databricks notebook or Python script
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import glob
import os

spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# Paths (replace with DBFS if you're using `dbfs:/` paths)
## approval ratings
appr_csv_file = '/Workspace/Shared/approval/data/approval_ratings/approval.csv'

# Get all CSV files in comments and posts folders
#comment_csv_files = glob.glob(os.path.join("dbfs:/Shared/approval/data/comments", "*.csv"))
#posts_csv_files = glob.glob(os.path.join("dbfs:/Shared/approval/data/posts", "*.csv"))
comment_csv_files = [file.path for file in dbutils.fs.ls("dbfs:/Shared/approval/data/comments") if file.path.endswith(".csv")]
posts_csv_files = [file.path for file in dbutils.fs.ls("dbfs:/Shared/approval/data/posts") if file.path.endswith(".csv")]

# Load and write to Delta
# Write as Delta tables (overwrite on initial load, then use 'append')
approval_df = spark.read.option("header", True).csv(appr_csv_file)
#approval_df.write.format("delta").mode("overwrite").saveAsTable("reddit_raw.approval")
approval_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mdf_final_project.reddit_raw_approval")



for i, comments_csv in enumerate(comment_csv_files):
    comments_df = spark.read.option("header", True).csv(comments_csv)
    if i == 0:
        comments_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mdf_final_project.reddit_raw._comments")
    else:
        comments_df.write.format("delta").mode("append").saveAsTable("mdf_final_project.reddit_raw_comments")

for i, posts_csv in enumerate(posts_csv_files):
    posts_df = spark.read.option("header", True).csv(posts_csv)
    if i == 0:
        posts_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mdf_final_project.reddit_raw_posts")
    else:
        posts_df.write.format("delta").mode("append").saveAsTable("mdf_final_project.reddit_raw_posts")

