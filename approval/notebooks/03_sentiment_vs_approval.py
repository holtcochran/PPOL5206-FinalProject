from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from pyspark.sql.functions import col

# Initialize Spark session if not already active
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

# Check if the necessary views exist
views_exist = True
try:
    # Try to access the views created by script 2
    sentiment_approval_df = spark.table("mdf_final_project.reddit_processed_sentiment_approval_view")
    sentiment_method_df = spark.table("mdf_final_project.reddit_processed_sentiment_method_comparison")
except:
    views_exist = False
    print("Error: Required views from script 2 are not available. Please run script 2 first.")

if views_exist:
    # Convert to Pandas for visualization with matplotlib/seaborn
    sentiment_approval_pd = sentiment_approval_df.toPandas()
    sentiment_method_pd = sentiment_method_df.toPandas()
    
    # Create a dashboard with multiple visualizations
    
    # Set the style for better looking plots
    sns.set(style="whitegrid")
    plt.rcParams.update({'font.size': 12})
    
    # Create a figure with multiple subplots
    fig = plt.figure(figsize=(20, 24))
    
    # 1. Time Series Plot: Approval Rating vs. Sentiment Indices
    ax1 = plt.subplot(4, 1, 1)
    ax1.plot(sentiment_approval_pd['week_start_date'], sentiment_approval_pd['approving'], 
             label='Approval Rating', linewidth=2, color='blue')
    ax1.plot(sentiment_approval_pd['week_start_date'], sentiment_approval_pd['textblob_sentiment_index']*100, 
             label='TextBlob Sentiment Index (x100)', linewidth=2, color='green', linestyle='-.')
    ax1.set_title('Approval Rating vs. Sentiment Indices Over Time')
    ax1.set_xlabel('Week')
    ax1.set_ylabel('Rating/Index')
    ax1.legend()
    ax1.grid(True)
    
    # 2. Positive vs Negative Content Percentage
    ax2 = plt.subplot(4, 1, 2)
    ax2.fill_between(sentiment_approval_pd['week_start_date'], sentiment_approval_pd['pct_positive_posts']*100, 
                     color='green', alpha=0.3, label='Positive Posts %')
    ax2.fill_between(sentiment_approval_pd['week_start_date'], sentiment_approval_pd['pct_negative_posts']*100, 
                     color='red', alpha=0.3, label='Negative Posts %')
    ax2.plot(sentiment_approval_pd['week_start_date'], sentiment_approval_pd['pct_positive_comments']*100, 
             color='darkgreen', label='Positive Comments %')
    ax2.plot(sentiment_approval_pd['week_start_date'], sentiment_approval_pd['pct_negative_comments']*100, 
             color='darkred', label='Negative Comments %')
    ax2.set_title('Percentage of Positive vs Negative Content Over Time')
    ax2.set_xlabel('Week')
    ax2.set_ylabel('Percentage')
    ax2.legend()
    ax2.grid(True)
    
    # 4. Correlation Heatmap
    ax4 = plt.subplot(4, 1, 4)
    # Select numeric columns for correlation
    numeric_cols = [
        'approving', 'textblob_sentiment_index',
        'pct_positive_posts', 'pct_negative_posts', 
        'pct_positive_comments', 'pct_negative_comments'
    ]
    
    corr_matrix = sentiment_approval_pd[numeric_cols].corr()
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', ax=ax4, vmin=-1, vmax=1, 
                linewidths=.5, cbar_kws={"shrink": .8})
    ax4.set_title('Correlation Matrix of Sentiment Metrics and Approval Rating')
    
    plt.tight_layout()
    
    # Save the dashboard as an image
    dashboard_path = "/Workspace/Shared/approval/sentiment_dashboard.png"
    plt.savefig(dashboard_path)
    print(f"Dashboard saved to {dashboard_path}")
    
    # Display the dashboard in the notebook
    displayHTML(f"""
    <div style="text-align:center">
        <h1>Reddit Sentiment vs. Approval Rating Dashboard</h1>
        <img src="/mnt/sentiment_dashboard.png" style="width:100%">
    </div>
    """)
    
    # Create a Delta table for the dashboard data to be used in BI tools
    # Combine the most useful metrics from both views
    dashboard_data = spark.sql("""
    SELECT 
        a.week_start_date,
        a.approving,
        a.textblob_sentiment_index * 100 as textblob_sentiment_score,
        a.pct_positive_posts * 100 as positive_posts_pct,
        a.pct_negative_posts * 100 as negative_posts_pct,
        a.pct_positive_comments * 100 as positive_comments_pct,
        a.pct_negative_comments * 100 as negative_comments_pct
    FROM 
        mdf_final_project.reddit_processed_sentiment_approval_view a
    JOIN 
        mdf_final_project.reddit_processed_sentiment_method_comparison b
    ON 
        a.week_start_date = b.week_start_date
    ORDER BY 
        a.week_start_date
    """)
    
    # Write to Delta table for BI tools
    dashboard_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("mdf_final_project.reddit_dashboard_sentiment_dashboard")
    
    # Create a view with predictive analysis potential
    spark.sql("""
    CREATE OR REPLACE VIEW reddit_dashboard.sentiment_approval_predictions AS
    WITH lagged_data AS (
        SELECT 
            week_start_date,
            approving,
            textblob_sentiment_index,
            LAG(textblob_sentiment_index, 1) OVER (ORDER BY week_start_date) as prev_textblob_sentiment,
            LAG(approving, 1) OVER (ORDER BY week_start_date) as prev_approval_rating
        FROM 
            mdf_final_project.reddit_processed_sentiment_approval_view
    )
    SELECT 
        week_start_date,
        approving,
        textblob_sentiment_index * 100 as textblob_sentiment_score,
        prev_textblob_sentiment * 100 as prev_textblob_sentiment_score,
        prev_approval_rating as prev_approval_rating,
        (approving - prev_approval_rating) as approval_rating_change,
        (textblob_sentiment_index - prev_textblob_sentiment) * 100 as textblob_sentiment_change
    FROM 
        lagged_data
    WHERE 
        prev_approval_rating IS NOT NULL
    ORDER BY 
        week_start_date
    """)
    
    print("Dashboard creation completed successfully!")
    print("The following assets were created:")
    print("1. Visual dashboard: /mnt/sentiment_dashboard.png")
    print("2. Delta table: reddit_dashboard_sentiment_dashboard")
    print("3. Predictive analysis view: reddit_dashboard_sentiment_approval_predictions")
else:
    print("Dashboard creation failed due to missing views.")