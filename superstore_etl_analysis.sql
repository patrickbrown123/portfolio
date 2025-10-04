-- ============================================
-- SUPERSTORE SALES SQL PIPELINE
-- Database: SuperstoreDB
-- Purpose: Data importing, cleaning, enrichment and exploration
-- ============================================

-- Connect to Superstore database
USE SuperstoreDB;

-- ============================================
-- 1. CREATE RAW TABLE
-- ============================================
CREATE TABLE SuperstoreSales (
    RowID INT,
    OrderID VARCHAR(20),
    OrderDate DATE,
    ShipDate DATE,
    ShipMode VARCHAR(50),
    CustomerID VARCHAR(20),
    CustomerName VARCHAR(100),
    Segment VARCHAR(50),
    Country VARCHAR(50),
    City VARCHAR(50),
    State VARCHAR(50),
    PostalCode VARCHAR(20),
    Region VARCHAR(50),
    ProductID VARCHAR(20),
    Category VARCHAR(50),
    SubCategory VARCHAR(50),
    ProductName VARCHAR(200),
    Sales DECIMAL(10,2),
    Quantity INT,
    Discount DECIMAL(5,2),
    Profit DECIMAL(10,2)
);

-- ============================================
-- 2. DATA IMPORTING
-- ============================================
-- Load data from TSV (tab-delimited) file
-- Note: OpenOffice saves as .csv but actually tab-delimited
-- ROWTERMINATOR depends on system:
--   Windows: '\r\n'
--   Linux/Unix: '\n'
BULK INSERT SuperstoreSales
FROM 'C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\Superstore.csv'
WITH (
    FIELDTERMINATOR = '\t',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);

-- ============================================
-- 3. DATA QUALITY CHECKS & CLEANING
-- ============================================

-- 3.1 Verify dataset loaded correctly
SELECT COUNT(*) AS TotalRows, COUNT(*) * 21 AS TotalCells FROM SuperstoreSales;
SELECT * FROM SuperstoreSales;

-- 3.2 Check for missing values
SELECT
  SUM(CASE WHEN OrderID IS NULL THEN 1 ELSE 0 END) AS missing_order_id,
  SUM(CASE WHEN CustomerName IS NULL THEN 1 ELSE 0 END) AS missing_customer_name,
  SUM(CASE WHEN Sales IS NULL THEN 1 ELSE 0 END) AS missing_sales
FROM SuperstoreSales;

-- (Optional demonstration only)
-- UPDATE SuperstoreSales SET OrderID = 'Unknown' WHERE OrderID IS NULL;
-- UPDATE SuperstoreSales SET CustomerName = 'Unknown' WHERE CustomerName IS NULL;
-- UPDATE SuperstoreSales SET Sales = 0 WHERE Sales IS NULL;

-- 3.3 Check for duplicates (allowed in sales data but worth verifying)
SELECT OrderID, ProductID, Sales, COUNT(*) AS duplicate_count
FROM SuperstoreSales
GROUP BY OrderID, ProductID, Sales
HAVING COUNT(*) > 1;

-- (Optional demonstration only: remove duplicates)
-- WITH cte AS (
--     SELECT MIN(RowID) AS id_to_keep
--     FROM SuperstoreSales
--     GROUP BY OrderID, ProductID
-- )
-- DELETE FROM SuperstoreSales
-- WHERE RowID NOT IN (SELECT id_to_keep FROM cte);

-- 3.4 Standardize categorical values
UPDATE SuperstoreSales SET Category = TRIM(Category);
UPDATE SuperstoreSales SET SubCategory = UPPER(SubCategory);

-- 3.5 Validate and fix dates (ShipDate should not precede OrderDate)
UPDATE SuperstoreSales
SET ShipDate = OrderDate
WHERE ShipDate < OrderDate;

-- 3.6 Handle invalid numeric values
-- Negative sales
SELECT * FROM SuperstoreSales WHERE Sales < 0;
-- Correct negative quantities
UPDATE SuperstoreSales SET Quantity = ABS(Quantity) WHERE Quantity < 0;

-- 3.7 Outlier flagging
ALTER TABLE SuperstoreSales ADD Outlier_Flag BIT DEFAULT 0; --RUN THIS LINE FIRST

UPDATE SuperstoreSales
SET Outlier_Flag = 1
WHERE Sales > 10000 OR Quantity > 100;

-- ============================================
-- 4. DATA TRANSFORMATION
-- ============================================

-- 4.1 Customer segmentation based on revenue
ALTER TABLE SuperstoreSales ADD CustomerSegment VARCHAR(50); --RUN THIS LINE FIRST

WITH CustomerSeg AS (
    SELECT 
       CustomerID,
       SUM(Sales) AS TotalRevenue,
       CASE 
           WHEN SUM(Sales) >= 5000 THEN 'High Value'
           WHEN SUM(Sales) BETWEEN 2000 AND 4999 THEN 'Medium Value'
           ELSE 'Low Value'
       END AS CustomerSegment
    FROM SuperstoreSales
    GROUP BY CustomerID
)
UPDATE s
SET s.CustomerSegment = c.CustomerSegment
FROM SuperstoreSales s
JOIN CustomerSeg c ON s.CustomerID = c.CustomerID;

-- 4.2 First order date and months since first order
ALTER TABLE SuperstoreSales ADD MinOrderDate DATE;
ALTER TABLE SuperstoreSales ADD OrderRelativeMonth INT;

WITH OrderCTE AS (
    SELECT
        CustomerID,
        OrderDate,
        MIN(OrderDate) OVER(PARTITION BY CustomerID) AS MinOrderDate,
        DATEDIFF(MONTH, MIN(OrderDate) OVER(PARTITION BY CustomerID), OrderDate) AS OrderRelativeMonth
    FROM SuperstoreSales
)
UPDATE s
SET s.MinOrderDate = c.MinOrderDate, 
    s.OrderRelativeMonth = c.OrderRelativeMonth
FROM SuperstoreSales s
JOIN OrderCTE c ON s.CustomerID = c.CustomerID AND s.OrderDate = c.OrderDate;

-- 4.3 Create view for BI tools
CREATE VIEW superstoresales_clean AS
SELECT * FROM SuperstoreSales;

-- ============================================
-- 5. DATA EXPLORATION
-- ============================================

-- 5.1 Monthly sales & profit with YoY comparison
WITH MonthlyData AS (
    SELECT 
        MONTH(OrderDate) AS OrderMonth,
        YEAR(OrderDate) AS OrderYear,
        SUM(Sales) AS MonthlySales,
        SUM(Profit) AS MonthlyProfit
    FROM SuperstoreSales
    GROUP BY YEAR(OrderDate), MONTH(OrderDate)
)
SELECT
    OrderMonth,
    OrderYear,
    MonthlySales,
    MonthlyProfit,
    LAG(MonthlySales, 12) OVER (ORDER BY OrderYear, OrderMonth) AS Sales_LastYear,
    CASE 
        WHEN LAG(MonthlySales, 12) OVER (ORDER BY OrderYear, OrderMonth) IS NULL THEN NULL
        ELSE (MonthlySales - LAG(MonthlySales, 12) OVER (ORDER BY OrderYear, OrderMonth)) 
             * 100.0 / LAG(MonthlySales, 12) OVER (ORDER BY OrderYear, OrderMonth)
    END AS Sales_YoY_Percent,
    LAG(MonthlyProfit, 12) OVER (ORDER BY OrderYear, OrderMonth) AS Profit_LastYear,
    CASE 
        WHEN LAG(MonthlyProfit, 12) OVER (ORDER BY OrderYear, OrderMonth) IS NULL THEN NULL
        ELSE (MonthlyProfit - LAG(MonthlyProfit, 12) OVER (ORDER BY OrderYear, OrderMonth)) 
             * 100.0 / LAG(MonthlyProfit, 12) OVER (ORDER BY OrderYear, OrderMonth)
    END AS Profit_YoY_Percent
FROM MonthlyData
ORDER BY OrderYear, OrderMonth;

-- 5.2 Yearly sales & profit with YoY comparison
WITH YearlySales AS (
    SELECT
        YEAR(OrderDate) AS OrderYear,
        SUM(Sales) AS TotalSales,
        SUM(Profit) AS TotalProfit
    FROM SuperstoreSales
    GROUP BY YEAR(OrderDate)
)
SELECT
    y1.OrderYear,
    y1.TotalSales,
    y1.TotalProfit,
    y2.TotalSales AS LastYearSales,
    y2.TotalProfit AS LastYearProfit,
    CASE 
        WHEN y2.TotalSales IS NULL THEN NULL
        ELSE (y1.TotalSales - y2.TotalSales) * 100.0 / y2.TotalSales
    END AS Sales_YoY_Percent,
    CASE 
        WHEN y2.TotalProfit IS NULL THEN NULL
        ELSE (y1.TotalProfit - y2.TotalProfit) * 100.0 / y2.TotalProfit
    END AS Profit_YoY_Percent
FROM YearlySales y1
LEFT JOIN YearlySales y2
    ON y1.OrderYear = y2.OrderYear + 1
ORDER BY y1.OrderYear;

-- 5.3 Unique counts
SELECT COUNT(DISTINCT OrderID) AS unique_orders,
       COUNT(DISTINCT CustomerID) AS unique_customers,
       COUNT(DISTINCT ProductID) AS unique_products
FROM SuperstoreSales;

-- 5.4 Profitability by Category & SubCategory
SELECT Category,
       SubCategory,
       SUM(Sales) AS TotalSales,
       SUM(Profit) AS TotalProfit,
       ROUND(SUM(Profit) / NULLIF(SUM(Sales),0), 2) AS ProfitMargin
FROM SuperstoreSales
GROUP BY Category, SubCategory
ORDER BY ProfitMargin DESC;

-- 5.5 TotalSales by region
SELECT Region,
       SUM(Sales) AS TotalSales,
       COUNT(DISTINCT OrderID) AS TotalOrders
FROM SuperstoreSales
GROUP BY Region
ORDER BY TotalSales DESC;

-- 5.6 Top 5 products by TotalSales within each Category
WITH RankedProducts AS (
    SELECT ProductName,
           Category,
           SUM(Sales) AS TotalSales,
           RANK() OVER(PARTITION BY Category ORDER BY SUM(Sales) DESC) AS RankInCategory
    FROM SuperstoreSales
    GROUP BY ProductName, Category
)
SELECT * 
FROM RankedProducts
WHERE RankInCategory <= 5;

-- 5.7 TotalSales by CustomerSegment
SELECT CustomerSegment,
       SUM(Sales) AS TotalSales
FROM SuperstoreSales
GROUP BY CustomerSegment
ORDER BY CustomerSegment, TotalSales DESC;

-- 5.8 TotalSales by Segment
SELECT Segment,
       SUM(Sales) AS TotalSales
FROM SuperstoreSales
GROUP BY Segment
ORDER BY Segment, TotalSales DESC;