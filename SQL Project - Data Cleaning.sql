/***************************************************************************************
                       Full Project - Data Cleaning in SQL Server
****************************************************************************************/


-- Create the database:
CREATE DATABASE world_layoffs;


-- Select the newly created database:
USE [world_layoffs]
GO

SELECT * 
FROM [dbo].[layoffs];

--- Steps for cleaning the raw data:
-- 1. Remove duplicates
-- 2. Standardize the data (issues with spelling, white spaces at the beganning etc)
-- 3. NULL values or blank values
-- 4. Remove Any Columns OR Rows




--This method creates a new table with the same structure but without copying any data.
SELECT *
INTO layoffs_staging
FROM layoffs
WHERE 1 = 0;


-- selecting the data to make sure table is created.
SELECT * 
FROM [dbo].[layoffs_staging];



-- insert the data from layoffs to layoffs_staging table.
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;


-- check for duplicates.
WITH duplicate_cte AS
(
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY company,[location],industry, total_laid_off, percentage_laid_off, [date],
										stage, country, funds_raised_millions
        ORDER BY (SELECT NULL)) AS row_num
    FROM [dbo].[layoffs_staging]
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;



SELECT *
FROM layoffs_staging;

-- creating staging 2:
CREATE TABLE layoffs_staging2(
company text,
[location] text,
industry text,
total_laid_off int,
percentage_laid_off text,
[date] text,
stage text,
country text,
funds_raised_millions int,
row_num int
);

-- Insert data into layoffs_staging2 with row numbers
INSERT INTO layoffs_staging2
(company,
[location],
industry,
total_laid_off,
percentage_laid_off,
[date],
stage,
country,
funds_raised_millions,
row_num)
SELECT company,
       [location],
       industry,
       total_laid_off,
       percentage_laid_off,
       [date],
       stage,
       country,
       funds_raised_millions,
       ROW_NUMBER() OVER (
           PARTITION BY company, [location], industry, total_laid_off, percentage_laid_off, [date], stage, country, funds_raised_millions
           ORDER BY (SELECT NULL)
       ) AS row_num
FROM layoffs_staging;

-- now that we have the above we can delete rows were row_num is greater than and equal to 2.






-- select the data then insert into it:
SELECT * 
FROM [dbo].[layoffs_staging2];


INSERT INTO [dbo].[layoffs_staging2] (company, [location], industry, total_laid_off, percentage_laid_off, [date], stage, country, funds_raised_millions)
SELECT company, [location], industry, total_laid_off, percentage_laid_off,
       CASE
           WHEN ISDATE([date]) = 1 THEN CONVERT(DATE, [date])
           ELSE NULL  -- or any other default value you want to use
       END AS [date],
       stage, country, funds_raised_millions
FROM [dbo].[layoffs_staging];




-- delete the duplicates:
DELETE
FROM [dbo].[layoffs_staging2]
WHERE row_num > 1;


-- check to make sure its deleted:
SELECT * 
FROM [dbo].[layoffs_staging2];







--- Standardizing data:

--- triming the white spaces 

SELECT company, (TRIM(CAST(company AS VARCHAR(MAX))))
FROM layoffs_staging2;


-- Step 1: Alter the column type to VARCHAR(MAX)
ALTER TABLE layoffs_staging2
ALTER COLUMN company VARCHAR(MAX);

-- Step 2: Update the column with TRIM function
UPDATE layoffs_staging2
SET company = TRIM(company);




-- standardizing the industry column:
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;


--- update the all the crypto currency, crytocurrency and crypto to just crypto:
SELECT * 
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';



UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';




SELECT DISTINCT CAST(industry AS VARCHAR(MAX))
FROM [dbo].[layoffs_staging2];


SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;


--- there are 2 United States, one with a dot.

SELECT DISTINCT country, 
       RTRIM(REPLACE(CAST(country AS VARCHAR(MAX)), '.', '')) AS trimmed_country
FROM (SELECT CAST(country AS VARCHAR(MAX)) AS country FROM layoffs_staging2) AS sub
ORDER BY 1;



-- Update the country column to remove only trailing periods
UPDATE layoffs_staging2
SET country = REVERSE(SUBSTRING(REVERSE(CAST(country AS VARCHAR(MAX))), 
                                CHARINDEX('.', REVERSE(CAST(country AS VARCHAR(MAX)))) + 1, 
                                LEN(CAST(country AS VARCHAR(MAX)))))
WHERE country LIKE '%.'; -- Update only rows where country ends with a period



--- change date column to date type:
SELECT [date],
       CONVERT(DATE, CAST([date] AS VARCHAR(MAX)), 101) AS formatted_date
FROM layoffs_staging2
WHERE ISDATE(CAST([date] AS VARCHAR(MAX))) = 1;



/*******************************************************************************************
-- Update the dates with a convertible format
Explanation

Add a New Column:
The new column new_date will store the converted date values.

Populate the New Column:
This query converts the text type date to varchar, and then to date. If the conversion is valid,
it populates the new_date column.

Drop and Rename:
Once the new column is populated with the correct date values, drop the old date
column and rename new_date to date.

*********************************************************************************************/

--Add a New Column:
ALTER TABLE layoffs_staging2 ADD new_date DATE;


--Populate the New Column:
UPDATE layoffs_staging2
SET new_date = TRY_CONVERT(DATE, TRY_CONVERT(VARCHAR(10), [date]), 101)
WHERE TRY_CONVERT(DATE, TRY_CONVERT(VARCHAR(10), [date]), 101) IS NOT NULL;


--Drop and Rename:
ALTER TABLE layoffs_staging2 DROP COLUMN [date];
EXEC sp_rename 'layoffs_staging2.new_date', 'date', 'COLUMN';



-- select the data:
SELECT * 
FROM [dbo].[layoffs_staging2];








-----Empty spaces:

--1. set empty space to null:

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = ''
ORDER BY industry;



-- we should set the blanks to nulls since those are typically easier to work with
UPDATE [dbo].[layoffs_staging2]
SET industry = NULL
WHERE industry = '';


-- now if we check those are all null
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


-- now we need to populate those nulls if possible

UPDATE t1
SET t1.industry = t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 ON t1.company = t2.company
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;




-- and if we check it looks like Bally's was the only one without a populated row to populate this null values
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;


SELECT *
FROM layoffs_staging2
WHERE company = 'Bally%';




-- get rid of both column nulls(if total_laid_off AND percentage_laid_off both are null 
--                              then we can't trust that data therefore, we have to get tid of it)
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;



/********************************************************************************************************
We conducted data cleaning on the layoffs_staging2 table to ensure the data is accurate, consistent, 
and ready for analysis. This process included creating a new table with appropriate data types, transferring
and formatting the data, handling date conversion issues, trimming text fields, replacing empty values with
NULL, and filling in missing industry values based on company matches. Data cleaning is crucial because it
eliminates errors, inconsistencies, and inaccuracies, which can significantly impact the quality and 
reliability of any analysis or insights derived from the data. Clean data ensures that analysis results 
are valid and trustworthy, leading to better decision-making.
**********************************************************************************************************/