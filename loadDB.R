# Install RMariaDB package if it does not exist (required for MySQL)
if (!requireNamespace("RMariaDB", quietly = TRUE)) {
  install.packages("RMariaDB")
}
library(DBI)
library(RMariaDB)
library(dotenv)

load_dot_env(".env")  # loads environment variables

url <- "https://s3.us-east-2.amazonaws.com/artificium.us/datasets/restaurant-visits-139874.csv"
# Used for testing only (using local csv file)
# df.orig <- read.csv("restaurant-visits-139874.csv", stringsAsFactors = FALSE,
#                     na.strings = c("", "N/A"))

# Used for submission (load from url instead of local file)
# Converts any empty values or "N/A" into NA values in the table
df.orig <- read.csv(url, stringsAsFactors = FALSE, na.strings = c("", "N/A"))

# Connect to the Aiven MySQL DB
dbcon <- dbConnect(MariaDB(),
                   dbname = Sys.getenv("DB_NAME"),
                   host = Sys.getenv("DB_HOST"),
                   port = Sys.getenv("DB_PORT"),
                   user = Sys.getenv("DB_USER"),
                   password = Sys.getenv("DB_PASS"))

# Convert sentinel values into NA values
df.orig$PartySize[df.orig$PartySize == 99] <- NA
df.orig$StartDateHired[df.orig$StartDateHired %in% c("", "0000-00-00", "N/A")] <- NA
df.orig$EndDateHired[df.orig$EndDateHired %in% c("", "9999-99-99", "N/A")] <- NA
df.orig$ServerBirthDate[df.orig$ServerBirthDate %in% c("", "N/A")] <- NA

# Convert appropriate values to correct data types
df.orig$ServerEmpID <- as.integer(df.orig$ServerEmpID)
df.orig$PartySize <- as.integer(df.orig$PartySize)
df.orig$WaitTime <- as.integer(df.orig$WaitTime)
# Match the date format in the csv
df.orig$StartDateHired <- as.Date(df.orig$StartDateHired, format = "%Y-%m-%d")
df.orig$EndDateHired <- as.Date(df.orig$EndDateHired, format = "%Y-%m-%d")
df.orig$ServerBirthDate <- as.Date(df.orig$ServerBirthDate, format = "%m/%d/%y")
df.orig$VisitDate <- as.Date(df.orig$VisitDate, format = "%Y-%m-%d")


# Insert unique MealTypes into the lookup table
meal_types <- unique(na.omit(df.orig$MealType))
for (meal in meal_types) {
  dbExecute(dbcon, sprintf("
                           INSERT IGNORE INTO MealTypes (MealType)
                           VALUES ('%s');", meal))
}

# Insert unique PaymentTypes into the lookup table
payment_types <- unique(na.omit(df.orig$PaymentMethod))
for (payment in payment_types) {
  dbExecute(dbcon, sprintf("
                         INSERT IGNORE INTO PaymentTypes (PayType)
                         VALUES ('%s');", payment))
}

# Insert unique Customers into the table
customers <- unique(df.orig[!is.na(df.orig$CustomerEmail),
                            c("CustomerEmail", "CustomerName",
                              "CustomerPhone", "LoyaltyMember")])
for (i in seq_len(nrow(customers))) {
  row <- customers[i, ]
  dbExecute(dbcon, "
            INSERT IGNORE INTO Customers (
            CustomerEmail, CustomerName, CustomerPhone, LoyaltyMember)
            VALUES (?, ?, ?, ?)",
            params = list(row$CustomerEmail, row$CustomerName,
                          row$CustomerPhone, row$LoyaltyMember))
}

# Insert unique Servers into the table
servers <- unique(df.orig[!is.na(df.orig$ServerEmpID),
                          c("ServerEmpID", "ServerName",
                            "StartDateHired", "EndDateHired",
                            "HourlyRate", "ServerBirthDate", "ServerTIN")])
for (i in seq_len(nrow(servers))) {
  row <- servers[i, ]
  params = list(row$ServerEmpID, row$ServerName, row$StartDateHired,
                row$EndDateHired, row$HourlyRate, row$ServerBirthDate,
                row$ServerTIN)
  dbExecute(dbcon, "
            INSERT IGNORE INTO Servers (
            ServerEmpID, ServerName, StartDateHired, EndDateHired,
            HourlyRate, ServerBirthDate, ServerTIN)
            VALUES (?, ?, ?, ?, ?, ?, ?)",
            params = params)
}

# Insert rows into Visits
batch_size <- 1000 
# Insert rows in batches with a single INSERT to speed up processing

# Keep track of inserted rows
inserted <- 0
total <- nrow(df.orig)

# Preload MealType and PayType into vectors to speed up queries
meal_map <- dbGetQuery(dbcon, "SELECT MealID, MealType FROM MealTypes")
meal_lookup <- setNames(meal_map$MealID, meal_map$MealType)
pay_map <- dbGetQuery(dbcon, "SELECT PayID, PayType FROM PaymentTypes")
pay_lookup <- setNames(pay_map$PayID, pay_map$PayType)

# Start batch loading
for (start in seq(1, total, by = batch_size)) {
  end <- min(start + batch_size - 1, total)
  chunk <- df.orig[start:end, ]
  if (nrow(chunk) == 0) next
  
  # Build placeholders and param list by concatenating all the attributes in the chunk
  attributes <- paste(rep("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            nrow(chunk)), collapse = ", ")
  
  query <- paste0("
  INSERT IGNORE INTO Visits (
    VisitID, Restaurant, ServerEmpID, VisitDate, VisitTime, MealType, 
    PartySize, Genders, WaitTime, CustomerEmail,
    FoodBill, TipAmount, DiscountApplied, PaymentMethod, 
    orderedAlcohol, AlcoholBill
  ) VALUES ", attributes)
  
  # Build params for each row in the chunk
  params <- list()
  for (i in seq_len(nrow(chunk))) {
    row <- chunk[i, ]
    # Use preloaded meal and pay types to find IDs
    meal_id <- meal_lookup[[row$MealType]]
    pay_id <- pay_lookup[[row$PaymentMethod]]
    
    params <- append(params, list(
      row$VisitID, row$Restaurant, row$ServerEmpID,
      row$VisitDate, row$VisitTime, meal_id,
      row$PartySize, row$Genders, row$WaitTime,
      row$CustomerEmail, row$FoodBill,
      row$TipAmount, row$DiscountApplied, pay_id,
      tolower(row$orderedAlcohol) == "yes", row$AlcoholBill
    ))
  }
  
  # Execute batch insert
  dbExecute(dbcon, query, params = params)
  
  # Print out current load status
  inserted <- inserted + nrow(chunk)
  cat(sprintf("Inserted %d of %d rows...\n", inserted, total))
}

# Disconnect
dbDisconnect(dbcon)
