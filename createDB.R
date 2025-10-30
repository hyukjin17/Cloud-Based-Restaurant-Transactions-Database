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

# Create MealTypes lookup table
dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS MealTypes (
  MealID INT PRIMARY KEY AUTO_INCREMENT,
  MealType VARCHAR(50) UNIQUE NOT NULL
);")

# Create PaymentTypes lookup table
dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS PaymentTypes (
  PayID INT PRIMARY KEY AUTO_INCREMENT,
  PayType VARCHAR(50) UNIQUE NOT NULL
);")

# Create Customers table
dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Customers (
  CustomerEmail VARCHAR(100) PRIMARY KEY,
  CustomerName VARCHAR(100) NOT NULL,
  CustomerPhone VARCHAR(20) NOT NULL,
  LoyaltyMember BOOLEAN DEFAULT FALSE
);")

# Create Servers table
dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Servers (
  ServerEmpID INT PRIMARY KEY,
  ServerName VARCHAR(50) NOT NULL,
  StartDateHired DATE DEFAULT NULL,
  EndDateHired DATE DEFAULT NULL,
  HourlyRate DOUBLE DEFAULT 0.0,
  ServerBirthDate DATE DEFAULT NULL,
  ServerTIN VARCHAR(50) UNIQUE
);")

# Create Visits table
dbExecute(dbcon, "
CREATE TABLE IF NOT EXISTS Visits (
  VisitID INT PRIMARY KEY,
  Restaurant VARCHAR(50) NOT NULL,
  ServerEmpID INT,
  VisitDate DATE NOT NULL,
  VisitTime TIME,
  MealType INT NOT NULL,
  PartySize INT DEFAULT 1,
  Genders VARCHAR(50) NOT NULL,
  WaitTime INT DEFAULT 0,
  CustomerEmail VARCHAR(100),
  FoodBill DOUBLE DEFAULT 0.0,
  TipAmount DOUBLE DEFAULT 0.0,
  DiscountApplied DOUBLE DEFAULT 0.0,
  PaymentMethod INT,
  orderedAlcohol BOOLEAN DEFAULT FALSE,
  AlcoholBill DOUBLE DEFAULT 0.0,
  FOREIGN KEY (ServerEmpID) REFERENCES Servers(ServerEmpID),
  FOREIGN KEY (MealType) REFERENCES MealTypes(MealID),
  FOREIGN KEY (CustomerEmail) REFERENCES Customers(CustomerEmail),
  FOREIGN KEY (PaymentMethod) REFERENCES PaymentTypes(PayID)
);")

# Disconnect
dbDisconnect(dbcon)