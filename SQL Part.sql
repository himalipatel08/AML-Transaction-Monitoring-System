CREATE SCHEMA AML;
USE AML
show tables

#Calculate Total Transaction, Suspicious Transaction Count, Suspicious Percentage
SELECT COUNT(*) AS Total_Transaction,SUM(CASE WHEN Transaction_Pattern IN ('STACK','CYCLE')THEN 1 ELSE 0 END)AS Suspicious_Transaction,
ROUND(SUM(CASE WHEN Transaction_Pattern IN ('STACK','CYCLE')THEN 1 ELSE 0 END)*100.00/COUNT(*),2)AS Suspicious_Percentage
FROM AMLDATA;

#Breakdown by Transaction Pattern
SELECT Transaction_Pattern, COUNT(*)AS Transaction_Count
FROM AMLDATA
GROUP BY Transaction_Pattern

ALTER TABLE amldata
RENAME COLUMN `FROM BANK NAME` TO `From_Bank_Name`

#Suspicious Transactions by Bank (Page 2 validation)
SELECT 
    From_Bank_Name, 
    Transaction_Pattern,
    COUNT(*) AS Count_Transactions
FROM amldata
WHERE Transaction_Pattern IN ('STACK','CYCLE')
GROUP BY From_Bank_Name, Transaction_Pattern
ORDER BY Count_Transactions DESC;


ALTER TABLE amldata
RENAME COLUMN `FROM Entity type` TO `From_entity_type`

# Suspicious Transactions by Entity Type (Page 3 validation)
SELECT 
    From_Entity_Type,
    Transaction_Pattern,
    COUNT(*) AS Count_Transactions
FROM amldata
WHERE Transaction_Pattern IN ('STACK','CYCLE')
GROUP BY From_Entity_Type, Transaction_Pattern;

ALTER TABLE amldata
RENAME COLUMN `Amount Paid` TO `Amount_paid`

# High Value Suspicious Transactions
SELECT 
    Account,
    From_Bank_Name,
    Transaction_Pattern,
    ROUND(SUM(Amount_Paid),2) AS Total_Amount
FROM amldata
WHERE Transaction_Pattern IN ('STACK','CYCLE') 
  AND Amount_Paid > 10000
GROUP BY Account, From_Bank_Name, Transaction_Pattern
ORDER BY Total_Amount DESC;

#Accounts Involved in Both STACK & CYCLE
SELECT Account
FROM amldata
WHERE Transaction_Pattern IN ('STACK','CYCLE')
GROUP BY Account
HAVING COUNT(DISTINCT Transaction_Pattern) > 1;


# Bank Risk Summary
SELECT 
    From_Bank_Name,
    COUNT(*) AS Total_Transactions,
    SUM(CASE WHEN Transaction_Pattern IN ('STACK','CYCLE') THEN 1 ELSE 0 END) AS Suspicious_Transactions,
    ROUND(SUM(CASE WHEN Transaction_Pattern IN ('STACK','CYCLE') THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS Suspicious_Percentage
FROM amldata
GROUP BY From_Bank_Name
ORDER BY Suspicious_Percentage DESC;


