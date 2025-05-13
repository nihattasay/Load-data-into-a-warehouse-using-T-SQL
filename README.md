# Load-data-into-a-warehouse-using-T-SQL


# Microsoft Fabric Lab: Load Data into a Warehouse Using T-SQL

This lab guides you through the process of creating and loading a data warehouse in Microsoft Fabric using T-SQL. You will create a workspace, a lakehouse, ingest data, create warehouse tables and views, load data, and perform analytical queries.

## Prerequisites

- A Microsoft Fabric trial or a workspace with Fabric capacity enabled.
- A modern web browser.
- Basic knowledge of T-SQL and data warehousing concepts.

---

## Lab Outline

### 1. Create a Fabric Workspace

1. Navigate to: [https://app.fabric.microsoft.com/home?experience=fabric](https://app.fabric.microsoft.com/home?experience=fabric)
2. Sign in with your Microsoft Fabric credentials.
3. Select **Workspaces** from the sidebar.
4. Click **+ New workspace**, give it a name, and select a licensing mode that includes Fabric capacity (e.g., Trial).

---

### 2. Create a Lakehouse and Upload Data

1. In the workspace, click **+ New item** and choose **Lakehouse**.
2. After the lakehouse is created, download the dataset:
   - [Download sales.csv](https://github.com/MicrosoftLearning/dp-data/raw/main/sales.csv)
3. In the Lakehouse Explorer, use the **...** menu on the *Files* folder to upload `sales.csv`.

---

### 3. Create a Table in the Lakehouse

1. Click the **...** menu on `sales.csv` and choose **Load to tables > New table**.
2. Set the following options:
   - Table name: `staging_sales`
   - Use header for column names: ✓
   - Separator: `,`
3. Click **Load**.

---

### 4. Create a Warehouse

1. From the workspace, click **+ Create > Warehouse**, and give it a unique name.
2. Open the newly created warehouse.

---

### 5. Create Fact Table, Dimensions, and View

1. Open a **New SQL query** and run the following script:

```sql
-- Create schema and tables
CREATE SCHEMA [Sales];
GO

-- Fact table
CREATE TABLE Sales.Fact_Sales (
    CustomerID VARCHAR(255) NOT NULL,
    ItemID VARCHAR(255) NOT NULL,
    SalesOrderNumber VARCHAR(30),
    SalesOrderLineNumber INT,
    OrderDate DATE,
    Quantity INT,
    TaxAmount FLOAT,
    UnitPrice FLOAT
);

-- Dimension tables
CREATE TABLE Sales.Dim_Customer (
    CustomerID VARCHAR(255) NOT NULL,
    CustomerName VARCHAR(255) NOT NULL,
    EmailAddress VARCHAR(255) NOT NULL,
    CONSTRAINT PK_Dim_Customer PRIMARY KEY NONCLUSTERED (CustomerID) NOT ENFORCED
);

CREATE TABLE Sales.Dim_Item (
    ItemID VARCHAR(255) NOT NULL,
    ItemName VARCHAR(255) NOT NULL,
    CONSTRAINT PK_Dim_Item PRIMARY KEY NONCLUSTERED (ItemID) NOT ENFORCED
);

    Create a view to link lakehouse data:

CREATE VIEW Sales.Staging_Sales AS
SELECT * FROM [<YourLakehouseName>].[dbo].[staging_sales];

Replace <YourLakehouseName> with your actual lakehouse name.
6. Load Data to the Warehouse

    Create a stored procedure to load data:

CREATE OR ALTER PROCEDURE Sales.LoadDataFromStaging (@OrderYear INT)
AS
BEGIN
    INSERT INTO Sales.Dim_Customer (CustomerID, CustomerName, EmailAddress)
    SELECT DISTINCT CustomerName, CustomerName, EmailAddress
    FROM [Sales].[Staging_Sales]
    WHERE YEAR(OrderDate) = @OrderYear
      AND NOT EXISTS (
          SELECT 1 FROM Sales.Dim_Customer
          WHERE CustomerName = Sales.Staging_Sales.CustomerName
            AND EmailAddress = Sales.Staging_Sales.EmailAddress
      );

    INSERT INTO Sales.Dim_Item (ItemID, ItemName)
    SELECT DISTINCT Item, Item
    FROM [Sales].[Staging_Sales]
    WHERE YEAR(OrderDate) = @OrderYear
      AND NOT EXISTS (
          SELECT 1 FROM Sales.Dim_Item
          WHERE ItemName = Sales.Staging_Sales.Item
      );

    INSERT INTO Sales.Fact_Sales (CustomerID, ItemID, SalesOrderNumber, SalesOrderLineNumber, OrderDate, Quantity, TaxAmount, UnitPrice)
    SELECT CustomerName, Item, SalesOrderNumber, CAST(SalesOrderLineNumber AS INT), CAST(OrderDate AS DATE),
           CAST(Quantity AS INT), CAST(TaxAmount AS FLOAT), CAST(UnitPrice AS FLOAT)
    FROM [Sales].[Staging_Sales]
    WHERE YEAR(OrderDate) = @OrderYear;
END;

    Run the stored procedure:

EXEC Sales.LoadDataFromStaging 2021;

7. Run Analytical Queries

-- Top customers by sales in 2021
SELECT c.CustomerName, SUM(s.UnitPrice * s.Quantity) AS TotalSales
FROM Sales.Fact_Sales s
JOIN Sales.Dim_Customer c ON s.CustomerID = c.CustomerID
WHERE YEAR(s.OrderDate) = 2021
GROUP BY c.CustomerName
ORDER BY TotalSales DESC;

-- Top items by sales in 2021
SELECT i.ItemName, SUM(s.UnitPrice * s.Quantity) AS TotalSales
FROM Sales.Fact_Sales s
JOIN Sales.Dim_Item i ON s.ItemID = i.ItemID
WHERE YEAR(s.OrderDate) = 2021
GROUP BY i.ItemName
ORDER BY TotalSales DESC;

-- Top customer per product category
WITH CategorizedSales AS (
    SELECT
        CASE
            WHEN i.ItemName LIKE '%Helmet%' THEN 'Helmet'
            WHEN i.ItemName LIKE '%Bike%' THEN 'Bike'
            WHEN i.ItemName LIKE '%Gloves%' THEN 'Gloves'
            ELSE 'Other'
        END AS Category,
        c.CustomerName,
        s.UnitPrice * s.Quantity AS Sales
    FROM Sales.Fact_Sales s
    JOIN Sales.Dim_Customer c ON s.CustomerID = c.CustomerID
    JOIN Sales.Dim_Item i ON s.ItemID = i.ItemID
    WHERE YEAR(s.OrderDate) = 2021
),
RankedSales AS (
    SELECT
        Category,
        CustomerName,
        SUM(Sales) AS TotalSales,
        ROW_NUMBER() OVER (PARTITION BY Category ORDER BY SUM(Sales) DESC) AS SalesRank
    FROM CategorizedSales
    WHERE Category IN ('Helmet', 'Bike', 'Gloves')
    GROUP BY Category, CustomerName
)
SELECT Category, CustomerName, TotalSales
FROM RankedSales
WHERE SalesRank = 1
ORDER BY TotalSales DESC;

8. Clean Up Resources

To avoid unnecessary resource usage:

    Go to your workspace.

    Select Workspace settings.

    Scroll down and select Remove this workspace.

    Confirm by clicking Delete.

Summary

In this lab, you:

    Created a workspace, lakehouse, and warehouse

    Loaded CSV data into a lakehouse

    Created fact and dimension tables

    Used cross-database queries to load and analyze data with T-SQL

