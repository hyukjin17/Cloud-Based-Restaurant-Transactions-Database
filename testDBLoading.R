# Install RMariaDB package if it does not exist (required for MySQL)
if (!requireNamespace("RMariaDB", quietly = TRUE)) {
  install.packages("RMariaDB")
}
library(DBI)
library(RMariaDB)
library(dotenv)

load_dot_env(".env")  # loads environment variables

# Load the csv from a link
url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv"
# Converts any empty values or "N/A" into NA values in the table
df.orig <- read.csv(url, stringsAsFactors = FALSE, na.strings = c("", "N/A"))

# Connect to the Aiven MySQL DB
dbcon <- dbConnect(MariaDB(),
                   dbname = Sys.getenv("DB_NAME"),
                   host = Sys.getenv("DB_HOST"),
                   port = Sys.getenv("DB_PORT"),
                   user = Sys.getenv("DB_USER"),
                   password = Sys.getenv("DB_PASS"))

# Count the number of unique restaurants
restaurants_csv <- length(unique(na.omit(df.orig$Restaurant)))
restaurants_db <- as.integer(dbGetQuery(dbcon, "SELECT COUNT(DISTINCT Restaurant) FROM Visits")[1,1])
cat(sprintf("Restaurant count\nCSV: %d | DB: %d\n%s\n", restaurants_csv, restaurants_db,
            ifelse(restaurants_csv == restaurants_db, "RESULTS MATCH", "MISMATCH")))

# Count the number of unique customers
customers_csv <- length(unique(na.omit(df.orig$CustomerEmail)))
customers_db <- as.integer(dbGetQuery(dbcon, "SELECT COUNT(CustomerEmail) FROM Customers")[1,1])
cat(sprintf("Customer count\nCSV: %d | DB: %d\n%s\n", customers_csv, customers_db,
            ifelse(customers_csv == customers_db, "RESULTS MATCH", "MISMATCH")))

# Count the number of unique servers
servers_csv <- length(unique(na.omit(df.orig$ServerEmpID)))
servers_db <- as.integer(dbGetQuery(dbcon, "SELECT COUNT(ServerEmpID) FROM Servers")[1,1])
cat(sprintf("Server count\nCSV: %d | DB: %d\n%s\n", servers_csv, servers_db,
            ifelse(servers_csv == servers_db, "RESULTS MATCH", "MISMATCH")))

# Count the number of visits
visits_csv <- length(unique(na.omit(df.orig$VisitID)))
visits_db <- as.integer(dbGetQuery(dbcon, "SELECT COUNT(VisitID) FROM Visits")[1,1])
cat(sprintf("Server count\nCSV: %d | DB: %d\n%s\n", visits_csv, visits_db,
            ifelse(visits_csv == visits_db, "RESULTS MATCH", "MISMATCH")))

# Total amount spent on food
food_csv <- sum(df.orig$FoodBill, na.rm = TRUE)
food_db <- dbGetQuery(dbcon, "SELECT SUM(FoodBill) FROM Visits")[1,1]
cat(sprintf("Total amount spent on food\nCSV: %f | DB: %f\n%s\n", food_csv, food_db,
            ifelse(abs(food_csv - food_db) < 0.01, "RESULTS MATCH", "MISMATCH")))

# Total amount spent on alcohol
alcohol_csv <- sum(df.orig$AlcoholBill, na.rm = TRUE)
alcohol_db <- dbGetQuery(dbcon, "SELECT SUM(AlcoholBill) FROM Visits")[1,1]
cat(sprintf("Total amount spent on alcohol\nCSV: %f | DB: %f\n%s\n", alcohol_csv, alcohol_db,
            ifelse(abs(alcohol_csv - alcohol_db) < 0.01, "RESULTS MATCH", "MISMATCH")))

# Total amount spent on tips
tips_csv <- sum(df.orig$TipAmount, na.rm = TRUE)
tips_db <- dbGetQuery(dbcon, "SELECT SUM(TipAmount) FROM Visits")[1,1]
cat(sprintf("Total amount spent on tips\nCSV: %f | DB: %f\n%s\n", tips_csv, tips_db,
            ifelse(abs(tips_csv - tips_db) < 0.01, "RESULTS MATCH", "MISMATCH")))

# Disconnect
dbDisconnect(dbcon)
