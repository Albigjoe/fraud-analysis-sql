-- Load the entire dataset from the database
SELECT *
FROM work.dbo.Fraud_dataset;


-- -------------------------------
-- CHECKING FOR DUPLICATE RECORDS
-- -------------------------------
WITH dup_cte AS (
    SELECT Transaction_ID, [User_ID],
        ROW_NUMBER() OVER (PARTITION BY Transaction_ID ORDER BY [User_ID]) AS dup
    FROM work.dbo.Fraud_dataset
)
SELECT *
FROM dup_cte
WHERE dup > 1; -- Returns records with duplicate Transaction_IDs


-- ---------------------------------------------------------
-- PERCENTAGE OF FRAUDULENT AND LEGITIMATE TRANSACTIONS
-- ---------------------------------------------------------

-- Count of fraudulent transactions
WITH Fraud_Trans_cte AS (
    SELECT COUNT(Transaction_ID) AS Fraudulent_Transaction
    FROM work.dbo.Fraud_dataset
    WHERE Fraud_Label = 1
),
-- Count of all transactions
Total_Trans_cte AS (
    SELECT COUNT(Transaction_ID) AS Total_Transaction 
    FROM work.dbo.Fraud_dataset
),
-- Count of legitimate transactions
legit_cte AS (
    SELECT COUNT(Transaction_ID) AS Legit_Transaction 
    FROM work.dbo.Fraud_dataset
    WHERE Fraud_Label = 0
)
-- Calculating fraud and legit percentages
SELECT 
    Fraudulent_Transaction,
    Total_Transaction, 
    Legit_Transaction,
    (CAST(Fraudulent_Transaction AS FLOAT)/Total_Transaction) * 100 AS Fraud_Percentage,
    (CAST(Legit_Transaction AS FLOAT)/Total_Transaction) * 100 AS Legit_Percentage
FROM Fraud_Trans_cte, Total_Trans_cte, legit_cte;


-- -----------------------------------
-- TOTAL TRANSACTION AMOUNT ANALYSIS
-- -----------------------------------

-- Total amount across all transactions
SELECT SUM(Transaction_Amount) AS Total_Transaction_Amount 
FROM work.dbo.Fraud_dataset;

-- Total amount of fraudulent transactions only
SELECT SUM(Transaction_Amount) AS Total_Fraud_Transaction_Amount 
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1;

-- Total amount of legitimate transactions only
SELECT SUM(Transaction_Amount) AS Total_Legit_Transaction_Amount 
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 0;


-- --------------------------
-- PEAK FRAUDULENT DAYS
-- --------------------------
WITH Fraud_cte AS (
    SELECT [Date], COUNT(Transaction_ID) AS Num_fraud_Trans
    FROM work.dbo.Fraud_dataset
    WHERE Fraud_Label = 1
    GROUP BY [Date]
)
SELECT 
    [Date],
    Num_fraud_Trans,
    DENSE_RANK() OVER (ORDER BY Num_fraud_Trans DESC) AS Ranking
FROM Fraud_cte;


-- --------------------------
-- TOP FRAUDSTERS (USERS)
-- --------------------------
SELECT [User_ID], COUNT([User_ID]) AS Num_Times_Labeled_As_Fraud
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1
GROUP BY [User_ID]
ORDER BY Num_Times_Labeled_As_Fraud DESC;


-- --------------------------------------------
-- TRANSACTION COUNTS BY TRANSACTION TYPE
-- --------------------------------------------
-- All transactions
SELECT COUNT(Transaction_ID) AS Total_Transactions, Transaction_Type
FROM work.dbo.Fraud_dataset
GROUP BY Transaction_Type
ORDER BY Total_Transactions DESC;

-- Fraudulent transactions only
SELECT COUNT(Transaction_ID) AS Total_Fraud_Transactions, Transaction_Type
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1
GROUP BY Transaction_Type
ORDER BY Total_Fraud_Transactions DESC;


-- --------------------------------------------
-- TRANSACTION COUNTS BY DEVICE TYPE
-- --------------------------------------------
-- All transactions
SELECT COUNT(Transaction_ID) AS Total_Transactions, Device_Type
FROM work.dbo.Fraud_dataset
GROUP BY Device_Type
ORDER BY Total_Transactions DESC;

-- Fraudulent transactions only
SELECT COUNT(Transaction_ID) AS Total_Fraud_Transactions, Device_Type
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1
GROUP BY Device_Type
ORDER BY Total_Fraud_Transactions DESC;


-- ----------------------------------------
-- TOP LOCATIONS WITH FRAUD CASES
-- ----------------------------------------

-- Total fraudulent transactions by location
SELECT COUNT(Transaction_ID) AS Total_Fraud_Transactions, [Location]
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1
GROUP BY [Location]
ORDER BY Total_Fraud_Transactions DESC;

-- Number of unique users involved in fraud by location
SELECT COUNT(DISTINCT [User_ID]) AS Total_Users, [Location]
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1
GROUP BY [Location]
ORDER BY Total_Users DESC;


-- ----------------------------------------
-- FRAUD BY CARD TYPE
-- ----------------------------------------
SELECT COUNT(Transaction_ID) AS Total_Fraud_Transactions, Card_Type
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1
GROUP BY Card_Type
ORDER BY Total_Fraud_Transactions DESC;


-- ----------------------------------------
-- FRAUD BY AUTHENTICATION METHOD
-- ----------------------------------------
SELECT COUNT(Transaction_ID) AS Total_Fraud_Transactions, Authentication_Method
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1
GROUP BY Authentication_Method
ORDER BY Total_Fraud_Transactions DESC;


-- ----------------------------------------
-- FRAUD BY MERCHANT CATEGORY
-- ----------------------------------------
SELECT COUNT(Transaction_ID) AS Total_Fraud_Transactions, Merchant_Category
FROM work.dbo.Fraud_dataset
WHERE Fraud_Label = 1
GROUP BY Merchant_Category
ORDER BY Total_Fraud_Transactions DESC;
