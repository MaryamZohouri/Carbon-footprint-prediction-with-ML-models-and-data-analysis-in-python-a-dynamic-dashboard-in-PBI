use CO2
select * from PCF

select ProductName from PCF
where PCF=(select MAX(PCF) from PCF)

-- Created by GitHub Copilot in SSMS - review carefully before executing
-- Rename columns in dbo.PCF to remove leading '*' character
-- Run on a test/dev copy first. Script validated for syntax/binding.

BEGIN TRANSACTION;

-- Verify current columns that start with '*' (visual check)
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = 'PCF'
  AND COLUMN_NAME LIKE '*%';

-- Rename statements
EXEC sp_rename 'dbo.PCF.[*PCF-ID]', 'PCF-ID', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Stage-level CO2e available]', 'Stage-level CO2e available', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Company''s sector]', 'Company''s sector', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Source for product weight]', 'Source for product weight', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Carbon intensity]', 'Carbon intensity', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Change reason category]', 'Change reason category', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*%Upstream estimated from %Operations]', '%Upstream estimated from %Operations', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Upstream CO2e (fraction of total PCF)]', 'Upstream CO2e (fraction of total PCF)', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Operations CO2e (fraction of total PCF)]', 'Operations CO2e (fraction of total PCF)', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Downstream CO2e (fraction of total PCF)]', 'Downstream CO2e (fraction of total PCF)', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Transport CO2e (fraction of total PCF)]', 'Transport CO2e (fraction of total PCF)', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*EndOfLife CO2e (fraction of total PCF)]', 'EndOfLife CO2e (fraction of total PCF)', 'COLUMN';
EXEC sp_rename 'dbo.PCF.[*Adjustments to raw data (if any)]', 'Adjustments to raw data (if any)', 'COLUMN';

-- Verify results
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = 'PCF'
  AND (COLUMN_NAME LIKE '%PCF-ID' OR COLUMN_NAME LIKE '%Carbon intensity' OR COLUMN_NAME LIKE '%Adjustments to raw data%');

-- If everything looks good, COMMIT; otherwise ROLLBACK
-- ROLLBACK TRANSACTION; -- uncomment to undo
COMMIT TRANSACTION;

select * from INFORMATION_SCHEMA.COLUMNS

-- More Preprocessing
-- Count NULL values per column for dbo.PCF (STRING_AGG separator and LOB handling fixed)
SET NOCOUNT ON;

DECLARE @sql NVARCHAR(MAX);
DECLARE @sep NVARCHAR(4000) = N' UNION ALL ';

SELECT @sql = STRING_AGG(
    CAST(N'SELECT ' + QUOTENAME(c.name,'''') + N' AS ColumnName, SUM(CASE WHEN [' + REPLACE(c.name,']',']]') + N'] IS NULL THEN 1 ELSE 0 END) AS NullCount, COUNT(1) AS TotalRows FROM dbo.PCF' AS NVARCHAR(MAX)),
    @sep
)
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = N'dbo' AND t.name = N'PCF';

IF @sql IS NULL
BEGIN
    SELECT 'No columns found for dbo.PCF' AS Message;
END
ELSE
BEGIN
    SET @sql = @sql + N' ORDER BY ColumnName';
    EXEC sp_executesql @sql;
END;

--Feature analysis
--Which columns including NULLs are categorical and which ones are numerical?
-- Compute NULL counts per column for dbo.PCF and classify as Numerical or Categorical (cursor-based)
SET NOCOUNT ON;

CREATE TABLE #null_counts (
    ColumnName SYSNAME,
    DataType NVARCHAR(128),
    NullCount BIGINT,
    TotalRows BIGINT,
    Classification VARCHAR(20)
);

DECLARE @col SYSNAME;
DECLARE @dtype NVARCHAR(128);
DECLARE @sql NVARCHAR(MAX);
DECLARE @nc BIGINT;
DECLARE @tr BIGINT;

DECLARE col_cursor CURSOR FOR
SELECT c.name, TYPE_NAME(c.user_type_id)
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = N'dbo' AND t.name = N'PCF';
OPEN col_cursor;
FETCH NEXT FROM col_cursor INTO @col, @dtype;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'SELECT @nc_out = SUM(CASE WHEN ' + QUOTENAME(@col) + N' IS NULL THEN 1 ELSE 0 END), @tr_out = COUNT(1) FROM dbo.PCF';
    EXEC sp_executesql @sql, N'@nc_out BIGINT OUTPUT, @tr_out BIGINT OUTPUT', @nc_out=@nc OUTPUT, @tr_out=@tr OUTPUT;

    INSERT INTO #null_counts (ColumnName, DataType, NullCount, TotalRows, Classification)
    VALUES (
        @col,
        @dtype,
        ISNULL(@nc,0),
        ISNULL(@tr,0),
        CASE WHEN LOWER(@dtype) IN ('int','bigint','smallint','tinyint','decimal','numeric','float','real','money','smallmoney','bit') THEN 'Numerical' ELSE 'Categorical' END
    );

    FETCH NEXT FROM col_cursor INTO @col, @dtype;
END;
CLOSE col_cursor;
DEALLOCATE col_cursor;

SELECT ColumnName, DataType, NullCount, TotalRows, Classification
FROM #null_counts
WHERE NullCount > 0
ORDER BY Classification, ColumnName;

DROP TABLE #null_counts;
--Output: 
--Categorical columns with NULL: Product detail includes 10 NULL
--Numerical columns with NULL: Transport CO2, and three important columns:
--Downstream, Operations, and Upstream

--So, I have decided now to fill numerical columns with NULL using mean strategy.
--update NULLs to the computed mean.
SET NOCOUNT ON;

BEGIN TRANSACTION;

CREATE TABLE #mean_log (
    ColumnName SYSNAME NOT NULL,
    DataType NVARCHAR(128) NULL,
    MeanValue FLOAT NULL,
    RowsUpdated BIGINT NOT NULL,
    ComputedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);

DECLARE @col SYSNAME;
DECLARE @dtype NVARCHAR(128);
DECLARE @sql NVARCHAR(MAX);
DECLARE @mean FLOAT;
DECLARE @rows BIGINT;

DECLARE col_cursor CURSOR FOR
SELECT c.name, TYPE_NAME(c.user_type_id)
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = N'dbo' AND t.name = N'PCF'
  AND TYPE_NAME(c.user_type_id) IN (
    'int','bigint','smallint','tinyint','decimal','numeric','float','real','money','smallmoney'
  );

OPEN col_cursor;
FETCH NEXT FROM col_cursor INTO @col, @dtype;
WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = N'SELECT @mean_out = AVG(CONVERT(FLOAT, ' + QUOTENAME(@col) + N')) FROM dbo.PCF WHERE ' + QUOTENAME(@col) + N' IS NOT NULL';
    SET @mean = NULL; SET @rows = 0;
    EXEC sp_executesql @sql, N'@mean_out FLOAT OUTPUT', @mean_out=@mean OUTPUT;

    IF @mean IS NOT NULL
    BEGIN
        SET @sql = N'UPDATE dbo.PCF SET ' + QUOTENAME(@col) + N' = @mean_val WHERE ' + QUOTENAME(@col) + N' IS NULL';
        EXEC sp_executesql @sql, N'@mean_val FLOAT', @mean_val=@mean;
        SET @rows = @@ROWCOUNT;
    END


    INSERT INTO #mean_log (ColumnName, DataType, MeanValue, RowsUpdated)
    VALUES (@col, @dtype, @mean, ISNULL(@rows,0));

    FETCH NEXT FROM col_cursor INTO @col, @dtype;
END;

CLOSE col_cursor;
DEALLOCATE col_cursor;

-- Review this resultset; if it looks correct COMMIT TRANSACTION; otherwise ROLLBACK TRANSACTION;
SELECT * FROM #mean_log ORDER BY ColumnName;

--ROLLBACK TRANSACTION; -- uncomment to undo
--COMMIT TRANSACTION; -- uncomment to persist

DROP TABLE #mean_log;

--Which products produce the most carbon foot print(CO2)?
--Arrange PCF w.r.t ProductName descending
--Show products ranked by total PCF (highest PCF first). Adjust @TopN as needed.

DECLARE @TopN INT = 100; -- change to return fewer/more rows

WITH ProductAgg AS (
    SELECT
        p.ProductName,
        SUM(p.[PCF]) AS TotalPCF,
        AVG(p.[PCF]) AS AvgPCF,
        COUNT_BIG(1) AS TotalRows,
        SUM(CASE WHEN p.[PCF] IS NULL THEN 1 ELSE 0 END) AS NullPCFCount
    FROM dbo.PCF AS p
    GROUP BY p.ProductName
)
SELECT
    ROW_NUMBER() OVER (ORDER BY TotalPCF DESC, ProductName) AS ProductRank,
    ProductName,
    TotalPCF,
    AvgPCF,
    TotalRows,
    NullPCFCount
FROM ProductAgg
ORDER BY TotalPCF DESC, ProductName
OFFSET 0 ROWS FETCH NEXT @TopN ROWS ONLY;

-- So, Wind Turbines produce the most PCF
select ProductName, Company, Country, PCF from PCF
where PCF=(select MAX(PCF) from PCF)

--



