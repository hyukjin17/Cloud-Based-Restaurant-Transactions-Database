# Cloud Based Restaurant Transaction Database (R, SQL)

## Overview
This project implements a cloud-hosted relational database to analyze restaurant visits, revenue, and transaction data. Using a synthetically generated dataset (as csv file), the project demonstrates the full database workflow: from data modeling to querying and analysis.

The system is designed following best practices in relational database design, normalization, and ETL (Extract, Transform, Load) processes, using R for automation and MariaDB as the backend database hosted on Aiven Cloud.

## Project Goals
- Design a normalized relational schema for restaurant transaction data
- Implement the schema on a cloud-hosted database
- Load data from CSV files into normalized tables using R
- Perform SQL queries to analyze visits, revenue trends, and sales performance
- Demonstrate integration between R, DBI, and cloud systems

## How to run
1. Download RStudio
2. Clone the repository and go to that directory
```bash
git clone https://github.com/hyukjin17/Cloud-Based-Restaurant-Transactions-Database.git
cd Cloud-Based-Restaurant-Transactions-Database
```
3. Modify the .env file with your own cloud database credentials
```
DB_NAME=your_database_name
DB_HOST=your_database_host
DB_PORT=your_port_number
DB_USER=your_username
DB_PASS=your_password
```
4. Open RStudio and install required packages
```r
install.packages(c("DBI", "dotenv", "kableExtra", "knitr"))
```
5. Run the scripts in order
- createDB.R (creates the database)
- loadDB.R (loads data from csv using ETL)
- configBusinessLogic.R (adds stored procedures for additional data)
- RevenueReport.Rmd (creates a sample report with trend analysis)
    - pre-generated report available as RevenueReport.pdf

## Results
After data load and analysis, the database creates a sample report. The database supports:
- Tracking visits and revenues per restaurant, date, and location
- Aggregation of monthly and yearly financial trends
- Insight into customer and staff activity patterns

## Security Notes
**Never commit your .env file**