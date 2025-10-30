# Install RMariaDB package if it does not exist (required for MySQL)
if (!requireNamespace("RMariaDB", quietly = TRUE)) {
  install.packages("RMariaDB")
}
library(DBI)
library(RMariaDB)
library(dotenv)

load_dot_env(".env")  # loads environment variables

# Connect to the Aiven MySQL DB
dbcon <- dbConnect(MariaDB(),
                   dbname = Sys.getenv("DB_NAME"),
                   host = Sys.getenv("DB_HOST"),
                   port = Sys.getenv("DB_PORT"),
                   user = Sys.getenv("DB_USER"),
                   password = Sys.getenv("DB_PASS"))

# Drop tables in reverse dependency order (child to parent)
tables <- c("Visits", "Servers", "Customers", "MealTypes", "PaymentTypes")
for (t in tables) {
  dbExecute(dbcon, paste0("DROP TABLE IF EXISTS ", t))
}

# Disconnect
dbDisconnect(dbcon)