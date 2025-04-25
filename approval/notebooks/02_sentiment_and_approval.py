# Databricks notebook or script: 02_sentiment_alternative.py
#%pip install nltk
#%pip install TextBlob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, weekofyear, year, avg, to_date, expr, lit, when, try_divide
from pyspark.sql.types import FloatType, StructType, StructField, StringType
#from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob
import nltk
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd
from pyspark.sql.types import ArrayType

spark = SparkSession.getActiveSession()

# Download NLTK data outside of the UDF
#nltk.download('vader_lexicon')

# --- 1. Load Reddit posts and comments ---
posts_df = spark.table("mdf_final_project.reddit_raw_posts")
comments_df = spark.table("mdf_final_project.reddit_raw_comments")

# --- 2. Define sentiment UDFs ---
# VADER sentiment analysis
#@pandas_udf(FloatType())
#def get_vader_sentiment(texts: pd.Series) -> pd.Series:
#    sid = SentimentIntensityAnalyzer()
    
#    def analyze(text):
#        if not text or pd.isna(text):
#            return 0.0
#        try:
#            sentiment_scores = sid.polarity_scores(text)
#            # Return compound score which is normalized between -1 and 1
#            return sentiment_scores['compound']
#        except:
#            return 0.0
#    
#    return texts.apply(analyze)

# TextBlob sentiment analysis (keeping the original approach)
@pandas_udf(FloatType())
def get_textblob_sentiment(texts: pd.Series) -> pd.Series:
    def analyze(text):
        if not text or pd.isna(text):
            return 0.0
        try:
            return TextBlob(text).sentiment.polarity
        except:
            return 0.0
    
    return texts.apply(analyze)

# --- 3. Add multiple sentiment metrics ---
posts_sentiment = posts_df.withColumn("textblob_sentiment", get_textblob_sentiment(col("title")))

comments_sentiment = comments_df.withColumn("textblob_sentiment", get_textblob_sentiment(col("body")))

# Add sentiment categories for easier filtering/analysis
posts_sentiment = posts_sentiment.withColumn(
    "sentiment_category", 
    when(col("textblob_sentiment") >= 0.05, "positive")
    .when(col("textblob_sentiment") <= -0.05, "negative")
    .otherwise("neutral")
)

comments_sentiment = comments_sentiment.withColumn(
    "sentiment_category", 
    when(col("textblob_sentiment") >= 0.05, "positive")
    .when(col("textblob_sentiment") <= -0.05, "negative")
    .otherwise("neutral")
)

# --- 4. Add day of week for more granular analysis ---
posts_sentiment = posts_sentiment.withColumn("day_of_week", expr("date_format(created_utc, 'E')"))
comments_sentiment = comments_sentiment.withColumn("day_of_week", expr("date_format(created_utc, 'E')"))

# --- 5. Group by week ---
posts_weekly = posts_sentiment.withColumn("year", year("created_utc")) \
                              .withColumn("week", weekofyear("created_utc")) \
                              .groupBy("year", "week") \
                              .agg(
                                  avg("textblob_sentiment").alias("avg_post_textblob_sentiment"),
                                  avg("score").alias("avg_post_score"),
                                  avg(when(col("sentiment_category") == "positive", 1).otherwise(0)).alias("pct_positive_posts"),
                                  avg(when(col("sentiment_category") == "negative", 1).otherwise(0)).alias("pct_negative_posts")
                              )

comments_weekly = comments_sentiment.withColumn("year", year("created_utc")) \
                                    .withColumn("week", weekofyear("created_utc")) \
                                    .groupBy("year", "week") \
                                    .agg(
                                        avg("textblob_sentiment").alias("avg_comment_textblob_sentiment"),
                                        avg("score").alias("avg_comment_score"),
                                        avg(when(col("sentiment_category") == "positive", 1).otherwise(0)).alias("pct_positive_comments"),
                                        avg(when(col("sentiment_category") == "negative", 1).otherwise(0)).alias("pct_negative_comments")
                                    )

# --- 6. Combine post + comment sentiment ---
reddit_weekly = posts_weekly.join(comments_weekly, ["year", "week"], "outer").fillna(0)

# --- 7. Calculate weighted sentiment scores (for both VADER and TextBlob) ---
reddit_weekly = reddit_weekly.withColumn(
    "weighted_textblob_sentiment", 
    try_divide(
        (col("avg_post_textblob_sentiment") * col("avg_post_score") + 
         col("avg_comment_textblob_sentiment") * col("avg_comment_score")), 
        (col("avg_post_score") + col("avg_comment_score"))
    )
).fillna(0)

# --- 8. Add combined sentiment indices ---
reddit_weekly = reddit_weekly.withColumn(
    "textblob_sentiment_index", 
    (col("avg_post_textblob_sentiment") + col("avg_comment_textblob_sentiment")) / 2
)

# --- 9. Shift Reddit data by 1 week to align with future approval rating ---
reddit_weekly_lagged = reddit_weekly.withColumn("week", expr("week + 1"))

# Handle year rollover if week > 52
reddit_weekly_lagged = reddit_weekly_lagged.withColumn("year", expr("""
  CASE WHEN week > 52 THEN year + 1 ELSE year END
""")).withColumn("week", expr("""
  CASE WHEN week > 52 THEN week - 52 ELSE week END
"""))

# --- 10. Load approval rating data and join ---
approval_df = spark.table("approval")
approval_df = approval_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
approval_df = approval_df.withColumn("year", year("date")) \
                         .withColumn("week", weekofyear("date"))

# --- 11. Join Reddit sentiment with approval ratings ---
final_df = approval_df.join(reddit_weekly_lagged, on=["year", "week"], how="left")

# --- 12. Create a formatted week_start_date ---
final_df = final_df.withColumn("week_start_date", 
                             to_date(expr("make_date(year, 1, 1) + (week - 1) * 7")))

# --- 13. Calculate correlation metrics for both sentiment approaches ---
final_df = final_df.withColumn(
    "textblob_approval_diff", 
    col("textblob_sentiment_index") - col("approving")/100
)

# --- 14. Persist the detailed data for research ---
# Save daily sentiment values 
posts_sentiment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mdf_final_project.reddit_processed_posts_daily_sentiment")
comments_sentiment.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mdf_final_project.reddit_processed_comments_daily_sentiment")

# --- 15. Save final aggregated dataset ---
final_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mdf_final_project.reddit_processed_approval_sentiment_weekly_combined")

# --- 16. Create a comprehensive view for BI tools with both sentiment analyses ---
spark.sql("""
CREATE OR REPLACE VIEW mdf_final_project.reddit_processed_sentiment_approval_view AS
SELECT 
    week_start_date,
    approving,
    
    -- TextBlob sentiment metrics
    avg_post_textblob_sentiment,
    avg_comment_textblob_sentiment,
    textblob_sentiment_index,
    weighted_textblob_sentiment,
    
    -- Percentage metrics
    pct_positive_posts,
    pct_negative_posts,
    pct_positive_comments,
    pct_negative_comments,
    
    -- Difference metrics
    textblob_approval_diff
FROM 
    mdf_final_project.reddit_processed_approval_sentiment_weekly_combined
ORDER BY 
    week_start_date
""")

# --- 17. Optional: Create a view for comparing VADER vs TextBlob performance ---
spark.sql("""
CREATE OR REPLACE VIEW mdf_final_project.reddit_processed_sentiment_method_comparison AS
SELECT 
    week_start_date,
    approving,
    
    -- TextBlob metrics
    textblob_sentiment_index,
    weighted_textblob_sentiment,
    textblob_approval_diff
    
    
FROM 
    mdf_final_project.reddit_processed_approval_sentiment_weekly_combined
ORDER BY 
    week_start_date
""")