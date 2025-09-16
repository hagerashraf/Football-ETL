This project implements an ETL pipeline for managing and analyzing football data as part of my internship at Atos.
The pipeline extracts raw data from Excel files, performs data cleaning and quality assurance, and loads it into a SQL Server database.
It also creates analytical views for performance monitoring and strategic insights.


# ETL Workflow:

## Extraction

Source data extracted from multiple Excel files.

Python (pandas) used for reading and preprocessing.

## Transformation & Cleaning

Data validation (PK/FK checks, required fields, data types).

Error handling and logging into dedicated error tables.

Cleansing of inconsistent or invalid records.

## Loading

Data loaded into SQL Server tables using pyodbc.

Identity inserts and foreign key constraints handled during load.

Denormalized views created for analysis.

## Logging & Reporting

Log file tracks:

Number of rows extracted, transformed, and inserted.

Errors and mismatches.

## Scheduling

ETL can be automated with Task Scheduler for regular runs.

## Database Schema

The pipeline populates the following core tables:

# Teams – Team information, stadium, city, manager.

# Players – Player details, nationality, contract, market value.

# Matches – Match results, scores, referees, stadium.

# PlayerStats – Match statistics (goals, assists, cards, minutes).

# TransferHistory – Player transfers, fees, and contract details.

Additionally, a player performance view was created with:

Total goals, assists, average minutes played.

Flags for special conditions (Played > 300 minutes, Age 25–30, Scored 3+ goals in a match).

Estimated matches played.

Country-specific indicators (played in France, Germany, etc.).

# Tech Stack

## Python – ETL orchestration (pandas, pyodbc, logging).

## SQL Server – Database & views.

## Excel – Source data files.

Project Structure
├── etl_pipeline.py        # Main ETL script
├── logs/                  # ETL run logs
├── sql/                   # SQL table creation scripts & views
├── error_logs/            # Error tables populated during validation
├── README.md              # Project documentation

Key Features:

# Automated ETL process (extract → transform → load).

# Full data validation (PK/FK integrity, required fields).

# Error tracking in SQL tables + logging.

# Denormalized views for analytics.

# Scalable design with partitioning & indexing recommendations.
