

# DATA CLEANING

SELECT *
FROM layoffs
;

-- 1. REMOVE DUPLICATE
-- 2. STANDARDIZE THE DATA
-- 3. NULL VALUES AND BLANKS VALAUE
-- 4. REMOVE UNUSEFUL COLUMNS AND ROWS

-- creating a working table named as layoffs_staging
CREATE TABLE layoffs_staging
LIKE layoffs;

-- copying everything into our working table
INSERT INTO layoffs_staging
SELECT *
FROM layoffs;
 
 -- creating row numbers
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

-- CTE
WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
				PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
							'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging;


-- creating a new table so we can delect duplicate
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL, `row_num` int
)
 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

-- inserting into layoofs_staging2 data from the table with the row numbers
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, 'date') AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- deleting the duplicate now
DELETE 
FROM layoffs_staging2
WHERE row_num > 1;


-- STANDARDIZING DATA
SELECT distinct(company)
FROM layoffs_staging2;

SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry
FROM layoffs_staging2;


-- TAKING THE PERIOD '.' AT THE END OF ONE OF THE (USA) 
-- YOU CAN USE THE CRYPTO FORMAT TO DO IT.

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%';

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- FORMATTING THE DATE AND UPDATING IT
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT `date`
FROM layoffs_staging2;

-- FINDING BLANKS AND UPDATING IT TO NULL SO WE CAN DELETE THEM LATER
UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';


-- FINDING COMPANIES WITH THE SAME NAME AND LOCATION, UPDATING THE ONES WITH NULLS AND EMPTY 
-- INDUSTRIES WITH THE ONES THAT HAS INDUSTRY 
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

SELECT *
FROM layoffs_staging2 AS T1
JOIN layoffs_staging2 AS T2
	ON T1.company =T2.company
	AND T1.location = T2.location
WHERE T1.industry IS NULL 
AND T2.industry IS NOT NULL;

UPDATE layoffs_staging2 AS T1
JOIN layoffs_staging2 AS T2
	ON T1.company =T2.company
SET T1.industry = T2.industry
WHERE T1.industry IS NULL 
AND T2.industry IS NOT NULL;



-- FINDING NULL TO DELETE 
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


-- FINDING AND DELETING COLUMNS WE WONT NEED
SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;



-- EXPLORATORY DATA ANALYSIS
SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;


SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- GROUPING THEM BY THE YEARS TO SEE THE NUMBERS OF PEOPLE GOT LAIDOFF EACH YEAR
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- ROLLING TOTAL OF LAYOFF
SELECT SUBSTRING(`date`  ,1,7) AS `MONTH` , SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`  ,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
;

WITH Rolling_Total AS
(SELECT SUBSTRING(`date`  ,1,7) AS `MONTH` , SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`  ,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, 
SUM(total_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM Rolling_Total
;

-- LOOKING AT NUMBER OF PEOPLE EACG COMPANY LAIDOFF EACH YEAR
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;

-- RANKING THE YEAR THEY LAIDOFF MOST EMPLOYEES
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


WITH Company_Year(Company, Year, Total_laid_off) AS 
(SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *
FROM Company_Year;


WITH Company_Year(Company, years, Total_laid_off) AS 
(SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
)
SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
ORDER BY Ranking ASC
;

-- FILTERING THE RANKING FOR THE FIRST FIVE
WITH Company_Year(Company, years, Total_laid_off) AS 
(SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5
;







