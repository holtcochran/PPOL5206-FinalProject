# PPOL5206-FinalProject
#### *By  Daniel Cardenas, Holt Cochran, Sam Cohen, Vaishnavi Singh, and Gabriel Soto* ####
This repository includes scripts and notebooks used in our final project for Massive Data Fundamentals. For this project, we used reddit comment sentiment data to predict presidential approval rating using XGBoost. This repository (found in the "approval" folder) is structured as follows:

## Notebooks
Contains notebooks used to ingest, clean, and merge reddit comment and presidential approval rating. The scripts follow a standard "medallion" approach: 
- (1) Ingestion of reddit comment data (*01_ingest_to_bronze*);
- (2) Cleaning, merging with approval rating data, and getting comment sentiment (*02_sentiment_and_approval*); and
- (3) Dashboard creation and visualization (*03_sentiment_vs_approval*
This folder as contains the script which scrapes the American Presidency Project website for weekly approval rating data. This can be found within the "*approval_rating*" sub-folder.

## Scripts
Contains the reddit API script which pulls daily comments with specified parameters

## Pipeline
The blueprint for the ML pipeline (*sentiment_approval_pipeline*)

## Dashboard
A snapshot of data visualizations created using databricks (*sentiment_dashboard*)


