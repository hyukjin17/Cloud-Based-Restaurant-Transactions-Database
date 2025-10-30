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

# Define the procedure
create_storeVisit <- "
CREATE PROCEDURE storeVisit(
    IN p_VisitID INT,
    IN p_Restaurant VARCHAR(100),
    IN p_ServerEmpID INT,
    IN p_CustomerEmail VARCHAR(100),
    IN p_VisitDate DATE,
    IN p_VisitTime TIME,
    IN p_MealType VARCHAR(50),
    IN p_PartySize INT,
    IN p_Genders VARCHAR(50),
    IN p_WaitTime INT,
    IN p_FoodBill DOUBLE,
    IN p_TipAmount DOUBLE,
    IN p_DiscountApplied DOUBLE,
    IN p_PaymentMethod VARCHAR(50),
    IN p_OrderedAlcohol BOOLEAN,
    IN p_AlcoholBill DOUBLE
)
BEGIN
    DECLARE p_MealID INT;
    DECLARE p_PayID INT;

    SELECT MealID INTO p_MealID
    FROM MealTypes
    WHERE MealType = p_MealType
    LIMIT 1;
    
    SELECT PayID INTO p_PayID
    FROM PaymentTypes
    WHERE PayType = p_PaymentMethod
    LIMIT 1;

    INSERT INTO Visits (
        VisitID, Restaurant, ServerEmpID, VisitDate, VisitTime, MealType,
        PartySize, Genders, WaitTime, CustomerEmail,
        FoodBill, TipAmount, DiscountApplied, PaymentMethod,
        orderedAlcohol, AlcoholBill
    )
    VALUES (
        p_VisitID, p_Restaurant, p_ServerEmpID, p_VisitDate, p_VisitTime, p_MealID,
        p_PartySize, p_Genders, p_WaitTime, p_CustomerEmail,
        p_FoodBill, p_TipAmount, p_DiscountApplied, p_PayID,
        p_OrderedAlcohol, p_AlcoholBill
    );
END;
"

# Run the command to create the stored procedure
dbExecute(dbcon, "DROP PROCEDURE IF EXISTS storeVisit;")
dbExecute(dbcon, create_storeVisit)



create_storeNewVisit <- "
CREATE PROCEDURE storeNewVisit(
    IN p_VisitID INT,
    IN p_Restaurant VARCHAR(100),
    IN p_ServerEmpID INT,
    IN p_ServerName VARCHAR(50),
    IN p_StartDateHired DATE,
    IN p_EndDateHired DATE,
    IN p_HourlyRate DOUBLE,
    IN p_ServerBirthDate DATE,
    IN p_ServerTIN VARCHAR(50),
    IN p_CustomerEmail VARCHAR(100),
    IN p_CustomerName VARCHAR(100),
    IN p_CustomerPhone VARCHAR(20),
    IN p_LoyaltyMember BOOLEAN,
    IN p_VisitDate DATE,
    IN p_VisitTime TIME,
    IN p_MealType VARCHAR(50),
    IN p_PartySize INT,
    IN p_Genders VARCHAR(50),
    IN p_WaitTime INT,
    IN p_FoodBill DOUBLE,
    IN p_TipAmount DOUBLE,
    IN p_DiscountApplied DOUBLE,
    IN p_PaymentMethod VARCHAR(50),
    IN p_OrderedAlcohol BOOLEAN,
    IN p_AlcoholBill DOUBLE
)
BEGIN
    DECLARE p_MealID INT;
    DECLARE p_PayID INT;

    -- Insert Customer if not exists
    IF NOT EXISTS (SELECT 1 FROM Customers WHERE CustomerEmail = p_CustomerEmail) THEN
        INSERT INTO Customers (CustomerEmail, CustomerName, CustomerPhone, LoyaltyMember)
        VALUES (p_CustomerEmail, p_CustomerName, p_CustomerPhone, p_LoyaltyMember);
    END IF;

    -- Insert Server if not exists
    IF NOT EXISTS (SELECT 1 FROM Servers WHERE ServerEmpID = p_ServerEmpID) THEN
        INSERT INTO Servers (ServerEmpID, ServerName, StartDateHired, EndDateHired,
                             HourlyRate, ServerBirthDate, ServerTIN)
        VALUES (p_ServerEmpID, p_ServerName, p_StartDateHired, p_EndDateHired,
                p_HourlyRate, p_ServerBirthDate, p_ServerTIN);
    END IF;

    -- Get MealID from MealTypes
    SELECT MealID INTO p_MealID
    FROM MealTypes
    WHERE MealType = p_MealType
    LIMIT 1;

    -- Get PayID from PaymentTypes
    SELECT PayID INTO p_PayID
    FROM PaymentTypes
    WHERE PayType = p_PaymentMethod
    LIMIT 1;

    -- Insert Visit
    INSERT INTO Visits (
        VisitID, Restaurant, ServerEmpID, VisitDate, VisitTime, MealType,
        PartySize, Genders, WaitTime, CustomerEmail,
        FoodBill, TipAmount, DiscountApplied, PaymentMethod,
        orderedAlcohol, AlcoholBill
    )
    VALUES (
        p_VisitID, p_Restaurant, p_ServerEmpID, p_VisitDate, p_VisitTime, p_MealID,
        p_PartySize, p_Genders, p_WaitTime, p_CustomerEmail,
        p_FoodBill, p_TipAmount, p_DiscountApplied, p_PayID,
        p_OrderedAlcohol, p_AlcoholBill
    );
END;
"

# Run the command to create the stored procedure
dbExecute(dbcon, "DROP PROCEDURE IF EXISTS storeNewVisit;")
dbExecute(dbcon, create_storeNewVisit)

# Testing
# Create a new visit
dbExecute(dbcon, "
  CALL storeVisit(
    999999, 'Diner', 1843, 'ultricies.sem@hotmail.ca', '2025-07-01', '12:00:00',
    'Lunch', 3, 'mmf', 12, 38.50, 8.00, 0.1, 'Cash', FALSE, 0.00
  );
")
# Check that the visit was properly inserted
dbGetQuery(dbcon, "SELECT * FROM Visits WHERE VisitID = 999999")
# Delete the entry after testing
dbExecute(dbcon, "DELETE FROM Visits WHERE VisitID = 999999")

# Create a new visit with new customer and new server
dbExecute(dbcon, "
  CALL storeNewVisit(
    888888, 'Diner', 99999, 'Jones, John', '2000-01-01', '2025-07-02', 20.00,
    '1980-05-05', '111-11-1111', 'example@test.com', 'James, LeBron',
    '(111) 111-1111', FALSE, '2025-07-01', '12:00:00',
    'Lunch', 3, 'mmf', 20, 38.50, 8.00, 0.1, 'Cash', FALSE, 0.00
  );
")
# Check that the visit was properly inserted
dbGetQuery(dbcon, "SELECT * FROM Visits WHERE VisitID = 888888")
dbGetQuery(dbcon, "SELECT * FROM Customers WHERE CustomerEmail = 'example@test.com'")
dbGetQuery(dbcon, "SELECT * FROM Servers WHERE ServerEmpID = 99999")
# Delete the entry after testing
dbExecute(dbcon, "DELETE FROM Visits WHERE VisitID = 888888")
dbExecute(dbcon, "DELETE FROM Customers WHERE CustomerEmail = 'example@test.com'")
dbExecute(dbcon, "DELETE FROM Servers WHERE ServerEmpID = 99999")

#Disconnect
dbDisconnect(dbcon)
