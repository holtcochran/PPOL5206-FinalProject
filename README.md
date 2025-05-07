# PPOL5206-FinalProject
#### *By  Daniel Cardenas, Holt Cochran, Sam Cohen, Vaishnavi Singh, and Gabriel Soto* ####
This repository includes scripts and notebooks used in our final project for Massive Data Fundamentals (PPOL 5206). For this project, we used reddit comment sentiment data to predict presidential approval rating using XGBoost. This repository (most of which can be found in the "*approval*" folder) is structured as follows:

## Notebooks
Contains notebooks used to ingest, clean, and merge reddit comment and presidential approval rating. The scripts follow a standard "medallion" approach: 
- (1) Ingestion of reddit comment data (*01_ingest_to_bronze*);
- (2) Cleaning, merging with approval rating data, and getting comment sentiment (*02_sentiment_and_approval*); and
- (3) Dashboard creation and visualization (*03_sentiment_vs_approval*)

This folder as contains the script which scrapes the American Presidency Project website for weekly approval rating data. This can be found within the "*approval_rating*" sub-folder

## Scripts
Contains the reddit API script which pulls daily comments with specified parameters

## Misc. 
#### Pipeline
The blueprint for the ML pipeline (*sentiment_approval_pipeline*)

#### Dashboard
A snapshot of data visualizations created using databricks (*sentiment_dashboard*)

#### SQL
A text file containing some of the SQL queries we used for EDA and summary/descriptive statistics (*sql_summary_stats*)

---
## Other Items:
### Report 
A reporting detailing our motiviation, data, methodology, results, and analysis can be found in the *Report* folder. A PDF and Word version of the report are available

### Website
A GitHub pages website showing a succint and readable overview of our project can be found [here](https://sec178.github.io/ppol5206_presapproval.github.io/)

### Data
A sample version of the posts and comments from Reddit, to illustrate the structure, is [available](approval/data).
